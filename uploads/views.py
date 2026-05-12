"""Batch upload endpoint. Mirrors FastAPI routes/upload.py.

Contract (JSON body):
  {
    "table":       "blinkit_inventory",
    "data":        [ {...}, {...} ],
    "unique_key":  "sku,warehouse",   // comma-separated; optional
    "upsert":      true                // default true
  }

Returns: {"success": N, "failed": M, "error": "..." | null}
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from difflib import SequenceMatcher
import re

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Tables the uploader is allowed to write to (mirrors FastAPI).
UPLOAD_ALLOWED_TABLES = {
    # Inventory
    "blinkit_inventory", "zepto_inventory", "swiggy_inventory",
    "bigbasket_inventory", "jiomart_inventory", "amazon_inventory",
    "zomato_inventory", "citymall_inventory",
    # Secondary sells
    "blinkitSec", "zeptoSec", "swiggySec", "flipkartSec",
    "jiomartSec", "bigbasketSec", "amazon_sec_daily", "amazon_sec_range",
    "amazon_price_data", "amazon_sec_range_margins",
    "fk_grocery", "flipkart_grocery_master",
    "zomatoSec", "citymallSec",
    # Primary
    "zepto_grn", "zepto_prim",
    "blinkit_grn", "blinkit_prim",
    "bigbasket_prim",
    "swiggy_grn", "swiggy_prim",
}

BATCH_SIZE = 50

UPLOAD_FORCED_UNIQUE_KEYS = {
    "swiggy_grn": (
        "grn_number,purchase_order_number,facility_name,vendor_name,"
        "invoice_number,invoice_date,created_at_date,dn_quantity,dn_value,"
        "sku_code,sku_description,received_qty,lot_expiry_date,total_amount"
    ),
}


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _upload_table_columns(table: str) -> set[str]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            [table],
        )
        return {row[0] for row in cur.fetchall()}


@api_view(["POST"])
@permission_classes([require("upload.use")])
def batch_upload(request):
    return _batch_upload(request.data or {})


def _batch_upload(body, *, forced_table: str | None = None):
    body = body or {}
    table = forced_table or body.get("table")
    data = body.get("data") or []
    unique_key = body.get("unique_key") or ""
    upsert = bool(body.get("upsert", True))

    if table not in UPLOAD_ALLOWED_TABLES:
        return Response(
            {"detail": f"Table '{table}' is not allowed for upload"},
            status=400,
        )
    if not _IDENT.match(table):
        return Response({"detail": "Invalid table name."}, status=400)
    if not isinstance(data, list) or not data:
        return Response({"success": 0, "failed": 0, "error": None})

    if upsert and table in UPLOAD_FORCED_UNIQUE_KEYS:
        unique_key = UPLOAD_FORCED_UNIQUE_KEYS[table]

    if table == "zepto_prim":
        for row in data:
            if row.get("created_at") in ("", None):
                row.pop("created_at", None)
    missing_rates = _collect_zepto_missing_rates(data) if table == "zeptoSec" else []

    table_columns = _upload_table_columns(table)
    columns = list(data[0].keys())
    invalid = [c for c in columns if c not in table_columns]
    if invalid:
        return Response(
            {"detail": f"Unknown columns for table '{table}': {invalid}"},
            status=400,
        )

    quoted_cols = [_quote_ident(c) for c in columns]
    col_list = ", ".join(quoted_cols)
    placeholders = ", ".join(["%s"] * len(columns))

    upsert_clause = ""
    if upsert and unique_key:
        keys = [k.strip() for k in unique_key.split(",") if k.strip()]
        invalid_keys = [k for k in keys if k not in table_columns]
        if invalid_keys:
            return Response(
                {"detail": f"Unknown unique_key columns for table '{table}': {invalid_keys}"},
                status=400,
            )
        conflict_cols = ", ".join(_quote_ident(k) for k in keys)
        update_cols = [
            f'{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}'
            for c in columns
            if c not in keys
        ]
        if update_cols:
            upsert_clause = (
                f' ON CONFLICT ({conflict_cols}) DO UPDATE SET {", ".join(update_cols)}'
            )
        else:
            upsert_clause = f" ON CONFLICT ({conflict_cols}) DO NOTHING"

    sql = f'INSERT INTO {_quote_ident(table)} ({col_list}) VALUES ({placeholders}){upsert_clause}'
    tracks_upsert_counts = bool(upsert and unique_key)
    if tracks_upsert_counts:
        sql += " RETURNING (xmax::text = '0') AS inserted"

    success = 0
    created = 0
    updated = 0
    skipped = 0
    failed = 0
    last_error: str | None = None

    with connection.cursor() as cur:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            for row in batch:
                try:
                    cur.execute(sql, [row.get(c) for c in columns])
                    if tracks_upsert_counts:
                        result = cur.fetchone()
                        if result is None:
                            skipped += 1
                        elif result[0]:
                            created += 1
                        else:
                            updated += 1
                    else:
                        created += 1
                    success += 1
                except Exception as e:
                    failed += 1
                    last_error = str(e)

    return Response(
        {
            "success": success,
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "duplicates": updated + skipped,
            "failed": failed,
            "error": last_error,
            "warnings": [
                f"Missing rate for {r['item']}, {r['month_label']} ({r['rows']} rows)"
                for r in missing_rates
            ],
            "missing_rates": missing_rates,
        }
    )


@api_view(["POST"])
@permission_classes([require("upload.use")])
def flipkart_grocery_raw_upload(request):
    """Upload raw Flipkart Grocery rows into the staging table `fk_grocery`.

    Body matches /api/upload/batch, except `table` is forced to fk_grocery:
      { "data": [{...}], "unique_key": "optional,columns", "upsert": true }
    """
    return _batch_upload(request.data or {}, forced_table="fk_grocery")


def _as_decimal(value, default=Decimal("0")):
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value).replace(",", ""))
    except Exception:
        return default


def _parse_date(value):
    if isinstance(value, date):
        return value
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _format_dmy(value):
    return value.strftime("%d-%m-%Y") if value else None


def _month_label(value: date) -> str:
    return value.strftime("%B %Y")


def _collect_zepto_missing_rates(data) -> list[dict]:
    missing = {}
    with connection.cursor() as cur:
        for row in data:
            sku_code = str(row.get("SKU Number") or "").strip()
            row_date = _parse_date(row.get("Date"))
            if not sku_code or row_date is None:
                continue
            rate_month = row_date.replace(day=1).isoformat()
            cur.execute(
                """
                SELECT 1
                FROM monthly_landing_rate
                WHERE UPPER(TRIM(sku_code::text)) = UPPER(TRIM(%s))
                  AND REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
                  AND month = %s
                LIMIT 1
                """,
                [sku_code, rate_month],
            )
            if cur.fetchone():
                continue

            cur.execute(
                """
                SELECT item, product_name
                FROM master_sheet
                WHERE UPPER(TRIM(format_sku_code::text)) = UPPER(TRIM(%s))
                LIMIT 1
                """,
                [sku_code],
            )
            master = cur.fetchone()
            label = (
                str(master[0] or master[1] or "").strip()
                if master
                else str(row.get("SKU Name") or sku_code).strip()
            )
            key = (sku_code.upper(), rate_month, label)
            hit = missing.setdefault(
                key,
                {
                    "sku_code": sku_code,
                    "item": label,
                    "month": rate_month,
                    "month_label": _month_label(row_date),
                    "rows": 0,
                },
            )
            hit["rows"] += 1
    return list(missing.values())


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _table_count(cur, table: str) -> int:
    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _date_bounds(cur, table: str, column: str):
    cur.execute(f'SELECT MIN("{column}"), MAX("{column}") FROM "{table}"')
    row = cur.fetchone()
    if not row:
        return None, None
    return _jsonable(row[0]), _jsonable(row[1])


@api_view(["GET"])
@permission_classes([require("upload.use")])
def flipkart_grocery_upload_schema(request):
    """Return the client-facing contract for Flipkart Grocery uploads."""
    return Response(
        {
            "raw": {
                "endpoint": "/api/upload/flipkart-grocery/raw",
                "table": "fk_grocery",
                "required": ["data"],
                "optional": ["unique_key", "upsert"],
            },
            "master": {
                "endpoint": "/api/upload/flipkart-grocery/master",
                "table": "flipkart_grocery_master",
                "required_row_fields": ["sku_id or fsn", "date or real_date or raw_date"],
                "optional_row_fields": ["qty", "brand"],
                "derived_from": ["master_sheet", "monthly_landing_rate"],
                "output_columns": [
                    "date",
                    "sku_id",
                    "brand",
                    "qty",
                    "per_ltr",
                    "per_ltr_unit",
                    "uom",
                    "ltr_sold",
                    "real_date",
                    "month",
                    "year",
                    "item",
                    "landing_rate",
                    "basic_rate",
                    "sale_amt_inclusive",
                    "sale_amt_exclusive",
                    "category",
                    "sub_category",
                    "item_head",
                ],
            },
        }
    )


@api_view(["GET"])
@permission_classes([require("upload.use")])
def flipkart_grocery_upload_status(request):
    """Small health/status payload for the Flipkart Grocery uploader."""
    status = {
        "raw": {"table": "fk_grocery", "exists": False, "count": 0},
        "master": {
            "table": "flipkart_grocery_master",
            "exists": False,
            "count": 0,
            "min_real_date": None,
            "max_real_date": None,
        },
    }
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name IN ('fk_grocery', 'flipkart_grocery_master')
                """
            )
            existing = {row[0] for row in cur.fetchall()}

            if "fk_grocery" in existing:
                status["raw"]["exists"] = True
                status["raw"]["count"] = _table_count(cur, "fk_grocery")

            if "flipkart_grocery_master" in existing:
                status["master"]["exists"] = True
                status["master"]["count"] = _table_count(cur, "flipkart_grocery_master")
                min_date, max_date = _date_bounds(cur, "flipkart_grocery_master", "real_date")
                status["master"]["min_real_date"] = min_date
                status["master"]["max_real_date"] = max_date
    except Exception as e:
        return Response({"ok": False, "error": str(e), "status": status}, status=500)

    return Response({"ok": True, "status": status})


def _ensure_fk_grocery_master(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS "flipkart_grocery_master" (
            "date" VARCHAR(10),
            "sku_id" VARCHAR,
            "brand" VARCHAR,
            "qty" NUMERIC,
            "per_ltr" NUMERIC,
            "per_ltr_unit" VARCHAR,
            "uom" VARCHAR,
            "ltr_sold" NUMERIC,
            "real_date" DATE,
            "month" INTEGER,
            "year" INTEGER,
            "item" VARCHAR,
            "landing_rate" NUMERIC,
            "basic_rate" NUMERIC,
            "sale_amt_inclusive" NUMERIC,
            "sale_amt_exclusive" NUMERIC,
            "category" VARCHAR,
            "sub_category" VARCHAR,
            "item_head" VARCHAR
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS flipkart_grocery_master_sku_date_uq
        ON "flipkart_grocery_master" ("sku_id", "real_date")
        """
    )


def _get_master_row(cur, sku_id):
    cur.execute(
        """
        SELECT format_sku_code, product_name, item, category, sub_category,
               per_unit, item_head, brand, uom, per_unit_value
        FROM master_sheet
        WHERE format_sku_code = %s
        LIMIT 1
        """,
        [sku_id],
    )
    return cur.fetchone()


def _get_price_row(cur, sku_id, product_name, real_date):
    target_month = real_date.replace(day=1).isoformat()
    params = [sku_id, target_month]
    name_clause = ""
    if product_name:
        name_clause = " OR LOWER(TRIM(sku_name)) = LOWER(TRIM(%s))"
        params.insert(1, product_name)

    cur.execute(
        f"""
        SELECT landing_rate, basic_rate
        FROM monthly_landing_rate
        WHERE UPPER(TRIM(format)) = 'FLIPKART GROCERY'
          AND (sku_code = %s{name_clause})
          AND month = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    if row:
        return row

    params = [sku_id]
    name_clause = ""
    if product_name:
        name_clause = " OR LOWER(TRIM(sku_name)) = LOWER(TRIM(%s))"
        params.append(product_name)

    cur.execute(
        f"""
        SELECT landing_rate, basic_rate
        FROM monthly_landing_rate
        WHERE UPPER(TRIM(format)) = 'FLIPKART GROCERY'
          AND (sku_code = %s{name_clause})
        ORDER BY month DESC, created_at DESC
        LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    if row:
        return row

    cur.execute(
        """
        SELECT sku_code, sku_name, landing_rate, basic_rate
        FROM monthly_landing_rate
        WHERE UPPER(TRIM(format)) = 'FLIPKART GROCERY'
        ORDER BY month DESC, created_at DESC
        """
    )
    candidates = cur.fetchall()

    def norm(value):
        return "".join(ch for ch in str(value or "").upper() if ch.isalnum()).replace(
            "O", "0"
        )

    sku_norm = norm(sku_id)
    product_norm = norm(product_name)
    best = None
    best_score = 0
    for cand_sku, cand_name, landing_rate, basic_rate in candidates:
        sku_score = SequenceMatcher(None, sku_norm, norm(cand_sku)).ratio()
        name_score = (
            SequenceMatcher(None, product_norm, norm(cand_name)).ratio()
            if product_norm
            else 0
        )
        score = max(sku_score, name_score)
        if score > best_score:
            best_score = score
            best = (landing_rate, basic_rate)

    return best if best_score >= 0.88 else None


@api_view(["POST"])
@permission_classes([require("upload.use")])
def fk_grocery_master_upload(request):
    body = request.data or {}
    data = body.get("data") or []
    upsert = bool(body.get("upsert", True))
    if not isinstance(data, list) or not data:
        return Response({"success": 0, "failed": 0, "error": None})

    rows = []
    failed = 0
    missing_master = set()
    missing_price = set()
    master_cache = {}
    price_cache = {}

    try:
        with connection.cursor() as cur:
            _ensure_fk_grocery_master(cur)

            for row in data:
                sku_id = str(row.get("sku_id") or row.get("fsn") or "").strip()
                real_date = _parse_date(
                    row.get("real_date") or row.get("raw_date") or row.get("date")
                )
                if not sku_id or real_date is None:
                    failed += 1
                    continue

                qty = _as_decimal(row.get("qty"))
                brand = str(row.get("brand") or "").strip() or None
                if sku_id not in master_cache:
                    master_cache[sku_id] = _get_master_row(cur, sku_id)
                master = master_cache[sku_id]
                if not master:
                    missing_master.add(sku_id)

                product_name = master[1] if master else None
                price_key = (sku_id, product_name or "", real_date.replace(day=1))
                if price_key not in price_cache:
                    price_cache[price_key] = _get_price_row(
                        cur, sku_id, product_name, real_date
                    )
                price = price_cache[price_key]
                if not price:
                    missing_price.add(sku_id)

                per_ltr = _as_decimal(master[9] if master else None)
                landing_rate = _as_decimal(price[0] if price else None)
                basic_rate = _as_decimal(price[1] if price else None)

                rows.append(
                    (
                        _format_dmy(real_date),
                        sku_id,
                        brand or (master[7] if master else None),
                        qty,
                        per_ltr,
                        master[5] if master else None,
                        master[8] if master else None,
                        per_ltr * qty,
                        real_date,
                        real_date.month,
                        real_date.year,
                        master[2] if master else None,
                        landing_rate,
                        basic_rate,
                        landing_rate * qty,
                        basic_rate * qty,
                        master[3] if master else None,
                        master[4] if master else None,
                        master[6] if master else None,
                    )
                )

            if not rows:
                return Response(
                    {
                        "success": 0,
                        "failed": failed or len(data),
                        "error": "No valid rows to upload",
                    },
                    status=400,
                )

            sql = """
                INSERT INTO "flipkart_grocery_master" (
                    "date", "sku_id", "brand", "qty", "per_ltr", "per_ltr_unit",
                    "uom", "ltr_sold", "real_date", "month", "year", "item",
                    "landing_rate", "basic_rate", "sale_amt_inclusive",
                    "sale_amt_exclusive", "category", "sub_category", "item_head"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            if upsert:
                sql += """
                    ON CONFLICT ("sku_id", "real_date") DO UPDATE SET
                        "date" = EXCLUDED."date",
                        "brand" = EXCLUDED."brand",
                        "qty" = EXCLUDED."qty",
                        "per_ltr" = EXCLUDED."per_ltr",
                        "per_ltr_unit" = EXCLUDED."per_ltr_unit",
                        "uom" = EXCLUDED."uom",
                        "ltr_sold" = EXCLUDED."ltr_sold",
                        "month" = EXCLUDED."month",
                        "year" = EXCLUDED."year",
                        "item" = EXCLUDED."item",
                        "landing_rate" = EXCLUDED."landing_rate",
                        "basic_rate" = EXCLUDED."basic_rate",
                        "sale_amt_inclusive" = EXCLUDED."sale_amt_inclusive",
                        "sale_amt_exclusive" = EXCLUDED."sale_amt_exclusive",
                        "category" = EXCLUDED."category",
                        "sub_category" = EXCLUDED."sub_category",
                        "item_head" = EXCLUDED."item_head"
                """
            else:
                sql += ' ON CONFLICT ("sku_id", "real_date") DO NOTHING'

            cur.executemany(sql, rows)

        return Response(
            {
                "success": len(rows),
                "failed": failed,
                "error": None,
                "missing_master": len(missing_master),
                "missing_price": len(missing_price),
                "table": "flipkart_grocery_master",
            }
        )
    except Exception as e:
        return Response({"detail": str(e)}, status=500)
