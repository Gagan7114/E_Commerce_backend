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

from django.db import connection, transaction
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
    "zepto_grn", "zepto_prim", "zepto_prim_grn_update",
    "blinkit_grn", "blinkit_prim",
    "bigbasket_prim",
    "flipkart_grocery_prim",
    "swiggy_grn", "swiggy_prim",
    "zomato_prim",
    "citymall_prim",
    # Ads
    "blinkit_ads",
    "amazon_ads",
    "swiggy_ads",
    "zepto_ads",
    "bigbasket_ads",
    "flipkart_ads",
    # Brand Fund
    "zepto_brandfund",
    "swiggy_brandfund",
    "blinkit_brandfund",
    # Coupons (Amazon)
    "amazon_coupon",
}

BATCH_SIZE = 50

MASTER_SHEET_COLUMNS = [
    "format_sku_code",
    "product_name",
    "item",
    "format",
    "sku_sap_code",
    "sku_sap_name",
    "category",
    "sub_category",
    "case_pack",
    "per_unit",
    "item_head",
    "brand",
    "uom",
    "per_unit_value",
    "category_head",
    "is_litre",
    "is_litre_oil",
    "packaging_cost",
    "tax_rate",
]

MASTER_SHEET_NUMERIC_COLUMNS = {
    "case_pack",
    "per_unit_value",
    "packaging_cost",
    "tax_rate",
}

MASTER_SHEET_SEARCH_COLUMNS = [
    "format_sku_code",
    "sku_sap_code",
    "product_name",
    "item",
    "sku_sap_name",
    "brand",
    "category",
    "sub_category",
]

UPLOAD_FORCED_UNIQUE_KEYS = {
    "swiggy_grn": (
        "grn_number,purchase_order_number,facility_name,vendor_name,"
        "invoice_number,invoice_date,created_at_date,dn_quantity,dn_value,"
        "sku_code,sku_description,received_qty,lot_expiry_date,total_amount"
    ),
}

PRIMARY_UPLOAD_REPLACE_KEYS = {
    # Primary PO rows are identified by platform PO + platform SKU. Status,
    # dates, vendor, rates, and quantities are mutable row data.
    "blinkit_prim": (("po_number",), ("item_id",)),
    "zepto_prim": (("po_no",), ("sku_code", "sku")),
    "swiggy_prim": (("po_number",), ("sku_code",)),
    "bigbasket_prim": (("po_number",), ("sku_code",)),
    "flipkart_grocery_prim": (("po_number",), ("sku_code",)),
    "zomato_prim": (("po_number",), ("sku_code",)),
    "citymall_prim": (("po_number",), ("sku_code",)),
}

INVENTORY_DOH_UPLOAD_PLATFORMS = {
    "blinkit_inventory": "blinkit",
    "zepto_inventory": "zepto",
    "swiggy_inventory": "swiggy",
    "bigbasket_inventory": "bigbasket",
    "amazon_inventory": "amazon",
    "blinkitSec": "blinkit",
    "zeptoSec": "zepto",
    "swiggySec": "swiggy",
    "bigbasketSec": "bigbasket",
    "amazon_sec_range": "amazon",
}



def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _public_table_regclass_name(table: str) -> str:
    return f"public.{_quote_ident(table)}"


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


def _sync_table_id_sequence(table: str, table_columns: set[str], upload_columns: list[str]) -> None:
    if "id" not in table_columns or "id" in upload_columns:
        return
    with connection.cursor() as cur:
        cur.execute(
            "SELECT pg_get_serial_sequence(%s, 'id')",
            [_public_table_regclass_name(table)],
        )
        result = cur.fetchone()
        sequence_name = result[0] if result else None
        if not sequence_name:
            return
        cur.execute(f"SELECT COALESCE(MAX(\"id\"), 0) FROM {_quote_ident(table)}")
        max_id = cur.fetchone()[0] or 0
        cur.execute(f"SELECT last_value FROM {sequence_name}")
        last_value = cur.fetchone()[0] or 0
        if max_id > last_value:
            cur.execute("SELECT setval(%s, %s, true)", [sequence_name, max_id])


def _master_sheet_select_columns() -> str:
    quoted = ", ".join(_quote_ident(col) for col in MASTER_SHEET_COLUMNS)
    return f'ctid::text AS "row_id", {quoted}'


def _coerce_master_sheet_value(column: str, value):
    if value == "":
        return None
    if value is None:
        return None
    if column not in MASTER_SHEET_NUMERIC_COLUMNS:
        return str(value).strip()

    try:
        if column == "case_pack":
            return int(value)
        return Decimal(str(value).strip())
    except Exception:
        raise ValueError(f"{column} must be a number.")


def _master_sheet_payload(data) -> dict:
    if not isinstance(data, dict):
        raise ValueError("Row data is required.")
    row = {}
    for column in MASTER_SHEET_COLUMNS:
        if column in data:
            row[column] = _coerce_master_sheet_value(column, data.get(column))
    return row


def _master_sheet_sku_key(value) -> str:
    return str(value or "").strip().upper()


def _master_sheet_existing_by_sku(sku_keys: list[str]) -> dict[str, dict]:
    keys = sorted({key for key in sku_keys if key})
    if not keys:
        return {}

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (UPPER(TRIM(format_sku_code::text)))
                   {_master_sheet_select_columns()},
                   UPPER(TRIM(format_sku_code::text)) AS norm_sku
            FROM master_sheet
            WHERE UPPER(TRIM(format_sku_code::text)) = ANY(%s)
            ORDER BY UPPER(TRIM(format_sku_code::text)), ctid::text
            """,
            [keys],
        )
        cols = [c[0] for c in cur.description]
        rows = []
        for values in cur.fetchall():
            row = dict(zip(cols, values))
            norm_sku = row.pop("norm_sku", "")
            rows.append((norm_sku, row))
    return {norm_sku: row for norm_sku, row in rows}


def _master_sheet_bulk_rows(data) -> list[dict]:
    rows = (data or {}).get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("rows must be a list.")

    parsed = []
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            parsed.append({
                "index": index,
                "sku": "",
                "sku_key": "",
                "row": {},
                "valid": False,
                "reason": "Row must be an object.",
            })
            continue

        try:
            payload = _master_sheet_payload(raw)
        except ValueError as exc:
            parsed.append({
                "index": index,
                "sku": str(raw.get("format_sku_code") or "").strip(),
                "sku_key": _master_sheet_sku_key(raw.get("format_sku_code")),
                "row": raw,
                "valid": False,
                "reason": str(exc),
            })
            continue

        sku = str(payload.get("format_sku_code") or "").strip()
        if not sku:
            parsed.append({
                "index": index,
                "sku": "",
                "sku_key": "",
                "row": payload,
                "valid": False,
                "reason": "format_sku_code is required.",
            })
            continue

        payload["format_sku_code"] = sku
        parsed.append({
            "index": index,
            "sku": sku,
            "sku_key": _master_sheet_sku_key(sku),
            "row": payload,
            "valid": True,
            "reason": "",
        })
    return parsed


def _master_sheet_bulk_preview_payload(parsed_rows: list[dict]) -> dict:
    existing = _master_sheet_existing_by_sku([row["sku_key"] for row in parsed_rows if row.get("valid")])
    seen_new = set()
    preview_rows = []
    summary = {"insert": 0, "update": 0, "invalid": 0, "total": len(parsed_rows)}

    for row in parsed_rows:
        if not row.get("valid"):
            summary["invalid"] += 1
            preview_rows.append({
                "index": row["index"],
                "action": "invalid",
                "sku": row.get("sku", ""),
                "reason": row.get("reason", "Invalid row."),
                "row": row.get("row", {}),
            })
            continue

        sku_key = row["sku_key"]
        action = "update" if sku_key in existing or sku_key in seen_new else "insert"
        if action == "insert":
            seen_new.add(sku_key)
        summary[action] += 1
        preview_rows.append({
            "index": row["index"],
            "action": action,
            "sku": row["sku"],
            "reason": "",
            "row": row["row"],
            "existing": existing.get(sku_key),
        })

    return {
        "columns": MASTER_SHEET_COLUMNS,
        "summary": summary,
        "rows": preview_rows,
    }


def _master_sheet_row_by_id(row_id: str):
    rows = _master_sheet_rows('WHERE ctid = %s::tid', [row_id], limit=1)
    return rows[0] if rows else None


def _master_sheet_rows(where_sql: str = "", params: list | None = None, *, limit: int = 50, offset: int = 0):
    params = list(params or [])
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_master_sheet_select_columns()}
            FROM master_sheet
            {where_sql}
            ORDER BY COALESCE(format, ''), COALESCE(format_sku_code, ''), COALESCE(product_name, '')
            LIMIT %s OFFSET %s
            """,
            [*params, limit, offset],
        )
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


@api_view(["GET"])
@permission_classes([require("upload.use")])
def master_sheet_list(request):
    query = str(request.query_params.get("search") or "").strip()
    fmt = str(
        request.query_params.get("format_name")
        or request.query_params.get("platform_format")
        or ""
    ).strip()
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(100, max(1, int(request.query_params.get("page_size", 25))))
    except ValueError:
        page, page_size = 0, 25

    where = []
    params = []
    rank_expr = "3"
    if query:
        like = f"%{query}%"
        exact = query.upper()
        where.append(
            "("
            + " OR ".join(f"CAST({_quote_ident(col)} AS text) ILIKE %s" for col in MASTER_SHEET_SEARCH_COLUMNS)
            + ")"
        )
        params.extend([like] * len(MASTER_SHEET_SEARCH_COLUMNS))
        rank_expr = """
            CASE
              WHEN UPPER(TRIM(format_sku_code::text)) = %s THEN 0
              WHEN UPPER(TRIM(sku_sap_code::text)) = %s THEN 1
              WHEN product_name ILIKE %s OR item ILIKE %s OR sku_sap_name ILIKE %s THEN 2
              ELSE 3
            END
        """
    if fmt:
        where.append("UPPER(TRIM(format::text)) = UPPER(TRIM(%s))")
        params.append(fmt)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = page * page_size

    with connection.cursor() as cur:
        count_params = list(params)
        cur.execute(f"SELECT COUNT(*) FROM master_sheet {where_sql}", count_params)
        total = cur.fetchone()[0]

        rank_params = []
        if query:
            rank_params.extend([query.upper(), query.upper(), f"{query}%", f"{query}%", f"{query}%"])
        cur.execute(
            f"""
            SELECT {_master_sheet_select_columns()}
            FROM master_sheet
            {where_sql}
            ORDER BY {rank_expr}, COALESCE(format, ''), COALESCE(format_sku_code, ''), COALESCE(product_name, '')
            LIMIT %s OFFSET %s
            """,
            [*params, *rank_params, page_size, offset],
        )
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    return Response({
        "columns": MASTER_SHEET_COLUMNS,
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
    })


@api_view(["POST"])
@permission_classes([require("upload.use")])
def master_sheet_bulk_preview(request):
    try:
        parsed_rows = _master_sheet_bulk_rows(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    return Response(_master_sheet_bulk_preview_payload(parsed_rows))


@api_view(["POST"])
@permission_classes([require("upload.use")])
def master_sheet_bulk_upsert(request):
    try:
        parsed_rows = _master_sheet_bulk_rows(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    existing = _master_sheet_existing_by_sku([row["sku_key"] for row in parsed_rows if row.get("valid")])
    result_rows = []
    summary = {"inserted": 0, "updated": 0, "invalid": 0, "total": len(parsed_rows)}

    with transaction.atomic(), connection.cursor() as cur:
        for parsed in parsed_rows:
            if not parsed.get("valid"):
                summary["invalid"] += 1
                result_rows.append({
                    "index": parsed["index"],
                    "action": "invalid",
                    "sku": parsed.get("sku", ""),
                    "reason": parsed.get("reason", "Invalid row."),
                    "row": parsed.get("row", {}),
                })
                continue

            sku_key = parsed["sku_key"]
            row = parsed["row"]
            existing_row = existing.get(sku_key)

            if existing_row:
                update_columns = [
                    col for col in MASTER_SHEET_COLUMNS
                    if col != "format_sku_code" and col in row and row[col] is not None
                ]
                if update_columns:
                    assignments = ", ".join(f"{_quote_ident(col)} = %s" for col in update_columns)
                    values = [row[col] for col in update_columns]
                    cur.execute(
                        f"""
                        UPDATE master_sheet
                        SET {assignments}
                        WHERE ctid = %s::tid
                        RETURNING {_master_sheet_select_columns()}
                        """,
                        [*values, existing_row["row_id"]],
                    )
                    cols = [c[0] for c in cur.description]
                    saved_row = dict(zip(cols, cur.fetchone()))
                else:
                    saved_row = existing_row

                existing[sku_key] = saved_row
                summary["updated"] += 1
                result_rows.append({
                    "index": parsed["index"],
                    "action": "update",
                    "sku": parsed["sku"],
                    "reason": "",
                    "row": saved_row,
                })
                continue

            insert_columns = [
                col for col in MASTER_SHEET_COLUMNS
                if col in row and (row[col] is not None or col == "format_sku_code")
            ]
            placeholders = ", ".join(["%s"] * len(insert_columns))
            cur.execute(
                f"""
                INSERT INTO master_sheet ({", ".join(_quote_ident(col) for col in insert_columns)})
                VALUES ({placeholders})
                RETURNING {_master_sheet_select_columns()}
                """,
                [row[col] for col in insert_columns],
            )
            cols = [c[0] for c in cur.description]
            saved_row = dict(zip(cols, cur.fetchone()))
            existing[sku_key] = saved_row
            summary["inserted"] += 1
            result_rows.append({
                "index": parsed["index"],
                "action": "insert",
                "sku": parsed["sku"],
                "reason": "",
                "row": saved_row,
            })

    return Response({
        "ok": True,
        "columns": MASTER_SHEET_COLUMNS,
        "summary": summary,
        "rows": result_rows,
    })


@api_view(["POST"])
@permission_classes([require("upload.use")])
def master_sheet_create(request):
    try:
        row = _master_sheet_payload(request.data or {})
        if not row.get("format_sku_code"):
            return Response({"detail": "format_sku_code is required."}, status=400)
        if not row.get("format"):
            return Response({"detail": "format is required."}, status=400)
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    columns = [col for col in MASTER_SHEET_COLUMNS if col in row]
    placeholders = ", ".join(["%s"] * len(columns))
    values = [row[col] for col in columns]
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO master_sheet ({", ".join(_quote_ident(col) for col in columns)})
            VALUES ({placeholders})
            RETURNING {_master_sheet_select_columns()}
            """,
            values,
        )
        cols = [c[0] for c in cur.description]
        created = dict(zip(cols, cur.fetchone()))

    return Response({"ok": True, "row": created})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def master_sheet_update(request):
    row_id = str((request.data or {}).get("row_id") or "").strip()
    if not row_id:
        return Response({"detail": "row_id is required."}, status=400)
    try:
        row = _master_sheet_payload((request.data or {}).get("row") or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)
    if not row:
        return Response({"detail": "No fields to update."}, status=400)

    assignments = ", ".join(f"{_quote_ident(col)} = %s" for col in row)
    values = [row[col] for col in row]
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(
            f"""
            UPDATE master_sheet
            SET {assignments}
            WHERE ctid = %s::tid
            RETURNING {_master_sheet_select_columns()}
            """,
            [*values, row_id],
        )
        updated_row = cur.fetchone()
        if not updated_row:
            return Response({"detail": "Row was not found. Please search again."}, status=404)
        cols = [c[0] for c in cur.description]

    return Response({"ok": True, "row": dict(zip(cols, updated_row))})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def master_sheet_delete(request):
    row_id = str((request.data or {}).get("row_id") or "").strip()
    if not row_id:
        return Response({"detail": "row_id is required."}, status=400)
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(
            """
            DELETE FROM master_sheet
            WHERE ctid = %s::tid
            RETURNING format_sku_code, product_name, format
            """,
            [row_id],
        )
        deleted = cur.fetchone()
        if not deleted:
            return Response({"detail": "Row was not found. Please search again."}, status=404)

    return Response({
        "ok": True,
        "deleted": {
            "format_sku_code": deleted[0],
            "product_name": deleted[1],
            "format": deleted[2],
        },
    })


# ─── ads_master_bs ───
# Manual mapping table: (month, campaign_id, sku_id, item, format).
# Unique key = (month, campaign_id, sku_id). The frontend exposes this as
# "ADS Master" (without the historical "_bs" suffix), but the DB table
# itself keeps the original name to avoid breaking the swiggy_ads_master
# and blinkit_ads_master views that JOIN to it.

ADS_MASTER_COLUMNS = ["month", "campaign_id", "sku_id", "item", "format"]
ADS_MASTER_KEY_COLUMNS = ["month", "campaign_id", "sku_id"]
ADS_MASTER_SEARCH_COLUMNS = ["month", "campaign_id", "sku_id", "item", "format"]


def _ads_master_select_columns() -> str:
    quoted = ", ".join(_quote_ident(col) for col in ADS_MASTER_COLUMNS)
    return f'ctid::text AS "row_id", {quoted}'


def _ads_master_payload(data) -> dict:
    if not isinstance(data, dict):
        raise ValueError("Row data is required.")
    row = {}
    for column in ADS_MASTER_COLUMNS:
        if column not in data:
            continue
        value = data.get(column)
        if value is None:
            row[column] = None
        else:
            text = str(value).strip()
            row[column] = text if text else None
    return row


@api_view(["GET"])
@permission_classes([require("upload.use")])
def ads_master_list(request):
    query = str(request.query_params.get("search") or "").strip()
    fmt = str(
        request.query_params.get("format_name")
        or request.query_params.get("platform_format")
        or ""
    ).strip()
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(100, max(1, int(request.query_params.get("page_size", 25))))
    except ValueError:
        page, page_size = 0, 25

    where = []
    params = []
    if query:
        like = f"%{query}%"
        where.append(
            "("
            + " OR ".join(
                f"CAST({_quote_ident(col)} AS text) ILIKE %s"
                for col in ADS_MASTER_SEARCH_COLUMNS
            )
            + ")"
        )
        params.extend([like] * len(ADS_MASTER_SEARCH_COLUMNS))
    if fmt:
        where.append("UPPER(TRIM(format::text)) = UPPER(TRIM(%s))")
        params.append(fmt)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = page * page_size

    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ads_master_bs {where_sql}", list(params))
        total = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT {_ads_master_select_columns()}
            FROM ads_master_bs
            {where_sql}
            ORDER BY COALESCE(month, ''), COALESCE(campaign_id, ''), COALESCE(sku_id, '')
            LIMIT %s OFFSET %s
            """,
            [*params, page_size, offset],
        )
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    return Response({
        "columns": ADS_MASTER_COLUMNS,
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
    })


@api_view(["POST"])
@permission_classes([require("upload.use")])
def ads_master_create(request):
    try:
        row = _ads_master_payload(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    missing = [col for col in ADS_MASTER_KEY_COLUMNS if not row.get(col)]
    if missing:
        return Response(
            {"detail": f"Required fields missing: {', '.join(missing)}."},
            status=400,
        )

    # Normalize key columns to NOT NULL TEXT (Postgres treats NULL as distinct
    # in unique indexes, so blanks must be empty strings, not NULL).
    insert_row = dict(row)
    for col in ADS_MASTER_KEY_COLUMNS:
        insert_row[col] = insert_row.get(col) or ""

    columns = [col for col in ADS_MASTER_COLUMNS if col in insert_row]
    placeholders = ", ".join(["%s"] * len(columns))
    values = [insert_row[col] for col in columns]

    with transaction.atomic(), connection.cursor() as cur:
        try:
            cur.execute(
                f"""
                INSERT INTO ads_master_bs ({", ".join(_quote_ident(col) for col in columns)})
                VALUES ({placeholders})
                RETURNING {_ads_master_select_columns()}
                """,
                values,
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "ads_master_bs_dedup_idx" in msg or "duplicate key" in msg.lower():
                return Response(
                    {"detail": "A row with the same month, campaign_id and sku_id already exists."},
                    status=409,
                )
            return Response({"detail": msg}, status=400)
        cols = [c[0] for c in cur.description]
        created = dict(zip(cols, cur.fetchone()))

    return Response({"ok": True, "row": created})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def ads_master_update(request):
    row_id = str((request.data or {}).get("row_id") or "").strip()
    if not row_id:
        return Response({"detail": "row_id is required."}, status=400)
    try:
        row = _ads_master_payload((request.data or {}).get("row") or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)
    if not row:
        return Response({"detail": "No fields to update."}, status=400)

    # Empty key columns must remain '' (NOT NULL) to keep the unique index sane.
    for col in ADS_MASTER_KEY_COLUMNS:
        if col in row and row[col] is None:
            row[col] = ""

    assignments = ", ".join(f"{_quote_ident(col)} = %s" for col in row)
    values = [row[col] for col in row]

    with transaction.atomic(), connection.cursor() as cur:
        try:
            cur.execute(
                f"""
                UPDATE ads_master_bs
                SET {assignments}, updated_at = NOW()
                WHERE ctid = %s::tid
                RETURNING {_ads_master_select_columns()}
                """,
                [*values, row_id],
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "ads_master_bs_dedup_idx" in msg or "duplicate key" in msg.lower():
                return Response(
                    {"detail": "Another row with the same month, campaign_id and sku_id already exists."},
                    status=409,
                )
            return Response({"detail": msg}, status=400)
        updated_row = cur.fetchone()
        if not updated_row:
            return Response({"detail": "Row was not found. Please search again."}, status=404)
        cols = [c[0] for c in cur.description]

    return Response({"ok": True, "row": dict(zip(cols, updated_row))})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def ads_master_delete(request):
    row_id = str((request.data or {}).get("row_id") or "").strip()
    if not row_id:
        return Response({"detail": "row_id is required."}, status=400)
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(
            """
            DELETE FROM ads_master_bs
            WHERE ctid = %s::tid
            RETURNING month, campaign_id, sku_id
            """,
            [row_id],
        )
        deleted = cur.fetchone()
        if not deleted:
            return Response({"detail": "Row was not found. Please search again."}, status=404)

    return Response({
        "ok": True,
        "deleted": {
            "month": deleted[0],
            "campaign_id": deleted[1],
            "sku_id": deleted[2],
        },
    })


# ─── ads_master_bs bulk upload (paste/CSV) ───
# Mirrors master_sheet bulk upload: dedupe key is (month, campaign_id, sku_id);
# matching keys update in place, new keys insert.

ADS_MASTER_HEADER_ALIASES = {
    "month_name": "month",
    "campaign": "campaign_id",
    "campaignid": "campaign_id",
    "campaign_code": "campaign_id",
    "sku": "sku_id",
    "skuid": "sku_id",
    "sku_code": "sku_id",
    "format_name": "format",
    "platform": "format",
}


def _ads_master_key(month, campaign_id, sku_id) -> str:
    return "|".join([
        str(month or "").strip().upper(),
        str(campaign_id or "").strip().upper(),
        str(sku_id or "").strip().upper(),
    ])


def _ads_master_existing_by_key(keys: list[str]) -> dict[str, dict]:
    seen = sorted({k for k in keys if k and k != "||"})
    if not seen:
        return {}
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_ads_master_select_columns()},
                   UPPER(TRIM(month::text)) || '|'
                || UPPER(TRIM(campaign_id::text)) || '|'
                || UPPER(TRIM(sku_id::text)) AS norm_key
            FROM ads_master_bs
            WHERE (UPPER(TRIM(month::text)) || '|'
                || UPPER(TRIM(campaign_id::text)) || '|'
                || UPPER(TRIM(sku_id::text))) = ANY(%s)
            """,
            [seen],
        )
        cols = [c[0] for c in cur.description]
        out = {}
        for values in cur.fetchall():
            row = dict(zip(cols, values))
            key = row.pop("norm_key", "")
            out[key] = row
    return out


def _ads_master_bulk_rows(data) -> list[dict]:
    rows = (data or {}).get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("rows must be a list.")

    parsed = []
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            parsed.append({
                "index": index,
                "key": "",
                "row": {},
                "valid": False,
                "reason": "Row must be an object.",
            })
            continue

        try:
            payload = _ads_master_payload(raw)
        except ValueError as exc:
            parsed.append({
                "index": index,
                "key": "",
                "row": raw,
                "valid": False,
                "reason": str(exc),
            })
            continue

        missing = [col for col in ADS_MASTER_KEY_COLUMNS if not (payload.get(col) or "").strip()]
        if missing:
            parsed.append({
                "index": index,
                "key": "",
                "row": payload,
                "valid": False,
                "reason": f"Required fields missing: {', '.join(missing)}.",
            })
            continue

        # Normalize key columns to empty string (NOT NULL); unique index would
        # otherwise treat NULL as distinct.
        for col in ADS_MASTER_KEY_COLUMNS:
            payload[col] = (payload.get(col) or "").strip()

        parsed.append({
            "index": index,
            "key": _ads_master_key(payload["month"], payload["campaign_id"], payload["sku_id"]),
            "row": payload,
            "valid": True,
            "reason": "",
        })
    return parsed


def _ads_master_bulk_preview_payload(parsed_rows: list[dict]) -> dict:
    existing = _ads_master_existing_by_key([r["key"] for r in parsed_rows if r.get("valid")])
    seen_new: set[str] = set()
    preview_rows = []
    summary = {"insert": 0, "update": 0, "invalid": 0, "total": len(parsed_rows)}

    for row in parsed_rows:
        if not row.get("valid"):
            summary["invalid"] += 1
            preview_rows.append({
                "index": row["index"],
                "action": "invalid",
                "key": row.get("key", ""),
                "reason": row.get("reason", "Invalid row."),
                "row": row.get("row", {}),
            })
            continue

        key = row["key"]
        action = "update" if key in existing or key in seen_new else "insert"
        if action == "insert":
            seen_new.add(key)
        summary[action] += 1
        preview_rows.append({
            "index": row["index"],
            "action": action,
            "key": key,
            "reason": "",
            "row": row["row"],
            "existing": existing.get(key),
        })

    return {
        "columns": ADS_MASTER_COLUMNS,
        "summary": summary,
        "rows": preview_rows,
    }


@api_view(["POST"])
@permission_classes([require("upload.use")])
def ads_master_bulk_preview(request):
    try:
        parsed_rows = _ads_master_bulk_rows(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)
    try:
        return Response(_ads_master_bulk_preview_payload(parsed_rows))
    except Exception as exc:  # noqa: BLE001
        return Response({"detail": f"Preview failed: {exc}"}, status=400)


@api_view(["POST"])
@permission_classes([require("upload.use")])
def ads_master_bulk_upsert(request):
    try:
        parsed_rows = _ads_master_bulk_rows(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    try:
        existing = _ads_master_existing_by_key([r["key"] for r in parsed_rows if r.get("valid")])
    except Exception as exc:  # noqa: BLE001
        return Response({"detail": f"Could not read ads_master_bs: {exc}"}, status=400)

    result_rows = []
    summary = {"inserted": 0, "updated": 0, "invalid": 0, "total": len(parsed_rows)}

    with transaction.atomic(), connection.cursor() as cur:
        for parsed in parsed_rows:
            if not parsed.get("valid"):
                summary["invalid"] += 1
                result_rows.append({
                    "index": parsed["index"],
                    "action": "invalid",
                    "key": parsed.get("key", ""),
                    "reason": parsed.get("reason", "Invalid row."),
                    "row": parsed.get("row", {}),
                })
                continue

            key = parsed["key"]
            row = parsed["row"]
            existing_row = existing.get(key)

            if existing_row:
                # Update non-key columns only (changing a key column would be
                # an insert under a new key, not an update).
                update_columns = [
                    col for col in ADS_MASTER_COLUMNS
                    if col not in ADS_MASTER_KEY_COLUMNS and col in row
                ]
                if update_columns:
                    assignments = ", ".join(f"{_quote_ident(col)} = %s" for col in update_columns)
                    values = [row[col] for col in update_columns]
                    cur.execute(
                        f"""
                        UPDATE ads_master_bs
                        SET {assignments}, updated_at = NOW()
                        WHERE ctid = %s::tid
                        RETURNING {_ads_master_select_columns()}
                        """,
                        [*values, existing_row["row_id"]],
                    )
                    cols = [c[0] for c in cur.description]
                    saved_row = dict(zip(cols, cur.fetchone()))
                else:
                    saved_row = existing_row

                existing[key] = saved_row
                summary["updated"] += 1
                result_rows.append({
                    "index": parsed["index"],
                    "action": "update",
                    "key": key,
                    "reason": "",
                    "row": saved_row,
                })
                continue

            insert_columns = [col for col in ADS_MASTER_COLUMNS if col in row]
            placeholders = ", ".join(["%s"] * len(insert_columns))
            cur.execute(
                f"""
                INSERT INTO ads_master_bs ({", ".join(_quote_ident(col) for col in insert_columns)})
                VALUES ({placeholders})
                RETURNING {_ads_master_select_columns()}
                """,
                [row[col] for col in insert_columns],
            )
            cols = [c[0] for c in cur.description]
            saved_row = dict(zip(cols, cur.fetchone()))
            existing[key] = saved_row
            summary["inserted"] += 1
            result_rows.append({
                "index": parsed["index"],
                "action": "insert",
                "key": key,
                "reason": "",
                "row": saved_row,
            })

    return Response({
        "ok": True,
        "columns": ADS_MASTER_COLUMNS,
        "summary": summary,
        "rows": result_rows,
    })


@api_view(["POST"])
@permission_classes([require("upload.use")])
def batch_upload(request):
    return _batch_upload(request.data or {})


def _is_blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _row_value(row: dict, source):
    if isinstance(source, (tuple, list)):
        for key in source:
            value = row.get(key)
            if not _is_blank(value):
                return value
        return None
    return row.get(source)


def _normalize_upload_key(value) -> str:
    return str(value or "").strip().lower()


def _primary_upload_key_parts(table: str, row: dict) -> tuple[str, ...] | None:
    key_specs = PRIMARY_UPLOAD_REPLACE_KEYS.get(table)
    if not key_specs:
        return None

    parts = tuple(_normalize_upload_key(_row_value(row, spec)) for spec in key_specs)
    if any(not part for part in parts):
        return None
    return parts


def _primary_upload_key_sql(spec: tuple[str, ...]) -> str:
    expressions = [f"t.{_quote_ident(column)}::text" for column in spec]
    if len(expressions) == 1:
        value_sql = expressions[0]
    else:
        value_sql = "COALESCE(" + ", ".join(
            f"NULLIF(TRIM({expr}), '')" for expr in expressions
        ) + ")"
    return f"LOWER(TRIM(COALESCE({value_sql}, '')))"


def _delete_existing_primary_upload_row(cur, table: str, row: dict) -> int:
    """Remove existing platform-primary rows with the same PO + SKU.

    The source tables do not all have a database unique constraint on this
    business key, so using DELETE + INSERT is the most reliable way to make a
    status refresh replace the old row instead of creating a duplicate.
    """
    key_parts = _primary_upload_key_parts(table, row)
    key_specs = PRIMARY_UPLOAD_REPLACE_KEYS.get(table)
    if not key_parts or not key_specs:
        return 0

    where = " AND ".join(
        f"{_primary_upload_key_sql(spec)} = %s"
        for spec in key_specs
    )
    cur.execute(
        f"DELETE FROM {_quote_ident(table)} AS t WHERE {where}",
        list(key_parts),
    )
    return cur.rowcount or 0


def _update_zepto_prim_grn_dates(data: list[dict]) -> Response:
    """Update Zepto primary rows from the lean GRN upload.

    The GRN file gives a PO id and created date. We use the PO id to update
    only existing PO lines and never insert rows.
    """
    success = 0
    updated = 0
    skipped = 0
    failed = 0
    last_error: str | None = None
    seen_po_ids: set[str] = set()

    with connection.cursor() as cur:
        for row in data:
            try:
                po_id = str(row.get("po_id") or "").strip()
                created_on = str(row.get("created_on") or "").strip()

                if not po_id or not created_on:
                    skipped += 1
                    continue

                key = po_id.lower()
                if key in seen_po_ids:
                    skipped += 1
                    continue
                seen_po_ids.add(key)

                cur.execute(
                    """
                    UPDATE zepto_prim
                       SET grn_date = %s
                     WHERE LOWER(TRIM(po_no::text)) = LOWER(TRIM(%s))
                    """,
                    [created_on, po_id],
                )
                line_count = cur.rowcount or 0
                if line_count:
                    success += 1
                    updated += line_count
                else:
                    skipped += 1
            except Exception as exc:
                failed += 1
                last_error = str(exc)

    return Response(
        {
            "success": success,
            "created": 0,
            "updated": updated,
            "skipped": skipped,
            "failed": failed,
            "error": last_error,
        }
    )


# master_po sync helpers were removed once the master_po table was retired.
# Uploads now go straight to each platform's per-tenant table via the upsert
# `INSERT ... ON CONFLICT` path in `_batch_upload`.


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

    if table == "zepto_prim_grn_update":
        return _update_zepto_prim_grn_dates(data)

    if upsert and table in UPLOAD_FORCED_UNIQUE_KEYS:
        unique_key = UPLOAD_FORCED_UNIQUE_KEYS[table]

    replace_by_primary_key = table in PRIMARY_UPLOAD_REPLACE_KEYS
    if replace_by_primary_key:
        unique_key = ""

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
    _sync_table_id_sequence(table, table_columns, columns)

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
    platform_created = 0
    platform_updated = 0
    platform_skipped = 0
    failed = 0
    last_error: str | None = None

    with connection.cursor() as cur:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            for row in batch:
                try:
                    replaced_rows = 0
                    if replace_by_primary_key:
                        with transaction.atomic():
                            replaced_rows = _delete_existing_primary_upload_row(cur, table, row)
                            cur.execute(sql, [row.get(c) for c in columns])
                    else:
                        cur.execute(sql, [row.get(c) for c in columns])

                    if replace_by_primary_key:
                        if replaced_rows:
                            updated += 1
                            platform_updated += 1
                        else:
                            created += 1
                            platform_created += 1
                    elif tracks_upsert_counts:
                        result = cur.fetchone()
                        if result is None:
                            skipped += 1
                            platform_skipped += 1
                        elif result[0]:
                            created += 1
                            platform_created += 1
                        else:
                            updated += 1
                            platform_updated += 1
                    else:
                        created += 1
                        platform_created += 1
                    success += 1
                except Exception as e:
                    failed += 1
                    last_error = str(e)

    notification_result = None
    if success and table in INVENTORY_DOH_UPLOAD_PLATFORMS:
        try:
            from platforms.services.inventory_doh_alerts import upsert_low_doh_notifications

            notification_result = upsert_low_doh_notifications(
                platform_slug=INVENTORY_DOH_UPLOAD_PLATFORMS[table],
                send_firebase=True,
            )
        except Exception as exc:
            notification_result = {"error": str(exc)}

    return Response({
        "success": success,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "platform_created": platform_created,
        "platform_updated": platform_updated,
        "platform_skipped": platform_skipped,
        "duplicates": updated + skipped,
        "failed": failed,
        "error": last_error,
        "warnings": [
            f"Landing rate missing for {r['item']}, {r['month_label']} ({r['rows']} rows)"
            for r in missing_rates
        ],
        "missing_rates": missing_rates,
        "inventory_doh_notifications": notification_result,
    })


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
    missing_landing_rate = set()
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

                per_ltr = _as_decimal(master[9] if master else None)
                landing_rate = _as_decimal(price[0] if price else None)
                basic_rate = _as_decimal(price[1] if price else None)
                if not price or landing_rate == Decimal("0"):
                    missing_landing_rate.add(sku_id)

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
                "missing_landing_rate": len(missing_landing_rate),
                "missing_landing_rate_skus": sorted(missing_landing_rate),
                "missing_price": len(missing_landing_rate),
                "table": "flipkart_grocery_master",
            }
        )
    except Exception as e:
        return Response({"detail": str(e)}, status=500)
