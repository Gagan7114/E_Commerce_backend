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
from decimal import Decimal, ROUND_HALF_UP
from difflib import SequenceMatcher
import logging
import re
import threading

from django.core.cache import cache
from django.db import IntegrityError, connection, transaction
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require

logger = logging.getLogger(__name__)

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
    # Amazon Secondary — city-wise variant (View By=[City]); cumulative
    # month-to-date ranges, only the freshest range per business+month is kept.
    "amazon_sec_city",
    # Flipkart Secondary — state-wise variant (B2C "Sales Report" GST export)
    "flipkart_state_sales",
    # Amazon Marketplace GST MTR B2B report (raw, stored as-is)
    "amazon_mp",
    # Primary
    "total_po", "total_po_zbs", "total_po_grn_update", "total_po_zbs_grn_update",
    # Ads
    "blinkit_ads",
    "amazon_ads",
    "swiggy_ads",
    "zepto_ads",
    "bigbasket_ads",
    "flipkart_ads",
    # Meta (Facebook/Instagram) ads campaigns
    "meta_data",
    # Flipkart "Consolidated FSN Report" — raw 14-col upload; the 5 master_sheet
    # columns are joined live in the consolidated_fsn_report_master view.
    "consolidated_fsn_report",
    # Ads — daily copy tables (the "Daily" option of the ads uploaders writes
    # here; "Range" keeps writing the originals above). Same schema/dedup keys.
    "swiggyads_daily",
    "zeptoads_daily",
    "bigbasketads_daily",
    # Brand Fund
    "zepto_brandfund",
    "swiggy_brandfund",
    "blinkit_brandfund",
    # Coupons (Amazon)
    "amazon_coupon",
    # Vendor Central per-appointment commit (Carton/Unit count scraped from
    # Amazon appointment detail pages; upsert keyed on appointment_id).
    "appointment_commit",
}

BATCH_SIZE = 1000

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

UPLOAD_FORCED_UNIQUE_KEYS = {}

PRIMARY_UPLOAD_REPLACE_KEYS = {
    # Primary PO rows are identified by platform PO + platform SKU. Status,
    # dates, vendor, rates, and quantities are mutable row data.
    "total_po": (("po_number",), ("sku_code",)),
    "total_po_zbs": (("po_number",), ("sku_code",)),
}

PRIMARY_UPLOAD_AUTHORITATIVE_COLUMNS = [
    "po_date",
    "po_expiry_date",
    "grn_date",
    "vendor_name",
    "status",
    "sku_name",
    "order_qty",
    "delivered_qty",
    "basic_rate",
    "landing_rate",
    "location",
    "format",
    "remark",
]

PRIMARY_GRN_COMPLETED_STATUS = "Fulfilled"

PRIMARY_UPLOAD_TABLES = {
    "total_po",
    "total_po_zbs",
    "total_po_grn_update",
    "total_po_zbs_grn_update",
}

# Primary PO insert tables that must only contain our own-brand SKUs. Rows whose
# sku_name mentions none of the accepted brands are dropped on upload (e.g. a
# third-party item accidentally attached to a PO export). Amazon primary POs use
# a separate uploader/table (amazon_po) and are intentionally not covered here.
PRIMARY_PO_JIVO_ONLY_TABLES = frozenset({"total_po", "total_po_zbs"})
# Brands accepted into the primary PO tables, matched case-insensitively as a
# substring of sku_name (e.g. "Jivo - Canola Refined Oil", "Sano - Pomace Olive Oil").
PRIMARY_PO_ACCEPTED_BRANDS = ("jivo", "sano")

# Standard GST multipliers (1 + rate) used to restore a precise landing_rate when
# a platform PO file ships it pre-rounded to a whole rupee (e.g. City Mall stores
# 144 where basic_rate 137.14 x 1.05 = 143.997). On ingest we only override the
# rounded value when exactly one of these slabs, applied to basic_rate and rounded
# to the nearest rupee, reproduces the file's value — so margin-based ratios
# (e.g. x1.40) and already-decimal rates are left untouched. See
# _restore_precise_landing_rate.
PRIMARY_PO_GST_MULTIPLIERS = (
    Decimal("1.05"),
    Decimal("1.12"),
    Decimal("1.18"),
    Decimal("1.28"),
)

# Maps an upload table to the column(s) a date-range delete filters on. A plain
# string is a single date column; a (start, end) tuple is for sources whose rows
# span a window (Amazon Secondary "range": from_date → to_date) — both endpoints
# must fall inside the selected delete window.
UPLOAD_DATE_DELETE_TABLES = {
    "amazon_ads": "date",
    "blinkit_ads": "date",
    "swiggy_ads": "date",
    "zepto_ads": "date",
    "bigbasket_ads": "date",
    "flipkart_ads": "date",
    "swiggyads_daily": "date",
    "zeptoads_daily": "date",
    "bigbasketads_daily": "date",
    "amazon_coupon": "date",
    "amazon_sec_daily": "report_date",
    "amazon_sec_range": ("from_date", "to_date"),
    "amazon_sec_city": ("from_date", "to_date"),
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


# Which uploaded tables feed each dashboard matview. An upload that doesn't
# touch a matview's sources doesn't need to refresh it (e.g. an ads/inventory/
# secondary upload never changes master_po_mv).
_MASTER_PO_SOURCE_TABLES = frozenset({
    "total_po", "total_po_zbs",
    "total_po_grn_update", "total_po_zbs_grn_update",
    "master_sheet",
})
_AMAZON_MP_SOURCE_TABLES = frozenset({"amazon_mp", "master_sheet"})
# Secondary tables that feed the SecMaster matview (secmaster_mv); master_sheet +
# monthly_landing_rate are its lateral-join inputs (item_head / landing rates).
_SECMASTER_SOURCE_TABLES = frozenset({
    "swiggySec", "bigbasketSec", "jiomartSec", "flipkartSec",
    "zeptoSec", "blinkitSec", "master_sheet", "monthly_landing_rate",
})
# Tables feeding the Blinkit/Swiggy ads-master matviews (uploads migration 0060):
# the raw ads rows, the campaign -> SKU bridge, and the SKU master (for item /
# category / per-litre enrichment).
_ADS_MASTER_SOURCE_TABLES = frozenset({
    "blinkit_ads", "swiggy_ads", "ads_master_bs", "master_sheet",
})

# Serializes background refreshes so two near-simultaneous uploads don't run
# REFRESH concurrently (the later one includes the earlier's rows, so the final
# matview state is correct and no data is missed).
_MATVIEW_REFRESH_LOCK = threading.Lock()


def _refresh_matviews_async(
    do_master_po: bool, do_amazon_mp: bool, do_secmaster: bool = False,
    do_ads_master: bool = False,
) -> None:
    """Refresh the dashboard matviews OFF the request thread so the upload
    responds immediately. The matviews still refresh (REFRESH recomputes the
    same rows — no data is changed or dropped); only the wait moves off the
    upload's response. The dashboard reflects the upload a few seconds later."""
    if not (do_master_po or do_amazon_mp or do_secmaster or do_ads_master):
        return

    def _worker():
        from django.db import connection
        from platforms.master_po_refresh import (
            refresh_ads_master_mvs,
            refresh_amazon_mp_master,
            refresh_master_po_mv,
            refresh_secmaster_mv,
        )
        with _MATVIEW_REFRESH_LOCK:
            try:
                if do_master_po:
                    refresh_master_po_mv()
                if do_amazon_mp:
                    refresh_amazon_mp_master()
                if do_secmaster:
                    # ~10s rebuild for DRR / Secondary / Summary dashboards.
                    refresh_secmaster_mv()
                if do_ads_master:
                    # Blinkit / Swiggy ADS dashboards.
                    refresh_ads_master_mvs()
                # The matviews are now fresh. Clear the cache AGAIN: a dashboard
                # read that raced in before this refresh finished would have
                # cached a stale response (TTL 60s) — drop it so the next read
                # serves fresh data and the dashboard updates without waiting out
                # the cache or a manual refresh.
                cache.clear()
            except Exception:  # noqa: BLE001 - a refresh failure must not crash
                logger.exception("Background matview refresh failed")
            finally:
                connection.close()  # don't leak this worker thread's connection

    threading.Thread(target=_worker, name="matview-refresh", daemon=True).start()


def _clear_upload_dependent_cache(table: str | None = None) -> None:
    """Invalidate cached dashboard payloads + refresh the dependent matviews
    after an upload.

    The matview refresh now runs in the BACKGROUND so the upload returns
    immediately instead of waiting ~5s for a full REFRESH, and only the matviews
    whose source table was actually uploaded are refreshed — an unrelated upload
    (ads / inventory / secondary) skips them entirely. `table=None` refreshes
    both (the safe default for callers that don't pass their table). No data is
    altered and the UI is unchanged: it's the same REFRESH, just off the
    response path and only when relevant."""
    try:
        cache.clear()
    except Exception:  # noqa: BLE001 - cache invalidation should not fail uploads
        logger.exception("Failed to clear cache after upload write")
    do_master_po = table is None or table in _MASTER_PO_SOURCE_TABLES
    do_amazon_mp = table is None or table in _AMAZON_MP_SOURCE_TABLES
    do_secmaster = table is None or table in _SECMASTER_SOURCE_TABLES
    do_ads_master = table is None or table in _ADS_MASTER_SOURCE_TABLES
    try:
        _refresh_matviews_async(do_master_po, do_amazon_mp, do_secmaster, do_ads_master)
    except Exception:  # noqa: BLE001 - scheduling must never break an upload
        logger.exception("Failed to schedule dashboard matview refresh")


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


def _upload_table_column_types(table: str) -> dict[str, str]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            [table],
        )
        return {row[0]: row[1] for row in cur.fetchall()}


_BLANK_AS_NULL_TYPES = {
    "bigint",
    "boolean",
    "date",
    "double precision",
    "integer",
    "numeric",
    "real",
    "smallint",
    "timestamp with time zone",
    "timestamp without time zone",
}


def _normalize_upload_value(value, data_type: str | None):
    if isinstance(value, str) and value.strip() == "" and data_type in _BLANK_AS_NULL_TYPES:
        return None
    if isinstance(value, str) and data_type in {"date", "timestamp with time zone", "timestamp without time zone"}:
        text = value.strip()
        date_match = re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})(?:\s+.*)?$", text)
        if date_match:
            day, month, year = date_match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        iso_match = re.match(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:\s+.*)?$", text)
        if iso_match:
            year, month, day = iso_match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return value


def _upload_row_values(row: dict, columns: list[str], column_types: dict[str, str]) -> list:
    return [
        _normalize_upload_value(row.get(column), column_types.get(column))
        for column in columns
    ]


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

    text = str(value).strip()
    # Excel renders percentages like "5%" for an underlying 0.05; pasting the
    # cell carries the literal "5%". Strip the sign and divide by 100 so the
    # stored value stays a fraction, matching existing rows (e.g. tax_rate=0.05).
    if text.endswith("%"):
        try:
            return Decimal(text[:-1].strip()) / Decimal(100)
        except Exception:
            raise ValueError(f"{column} must be a number.")

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


def _propagate_master_sheet_to_amazon(format_sku_codes=None, items=None):
    """After a master_sheet edit, re-derive the matching reporting."Amazon PO"
    rows so the change propagates to Amazon the way it already does for the
    view-backed platforms. Runs in its own transaction and never fails the
    caller's save (logs and moves on)."""
    codes = [c for c in (format_sku_codes or []) if c and str(c).strip()]
    items = [i for i in (items or []) if i and str(i).strip()]
    if not codes and not items:
        return
    try:
        from .amazon_uploads import refresh_amazon_po_from_master_sheet

        with transaction.atomic(), connection.cursor() as cur:
            refresh_amazon_po_from_master_sheet(
                cur, format_sku_codes=codes, items=items
            )
    except Exception:
        logger.exception("Amazon PO master_sheet propagation failed")


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

    touched = [r["row"] for r in result_rows if r["action"] in ("update", "insert")]
    _propagate_master_sheet_to_amazon(
        format_sku_codes=[r.get("format_sku_code") for r in touched],
        items=[r.get("item") for r in touched],
    )
    if summary["inserted"] or summary["updated"]:
        _clear_upload_dependent_cache()

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

    _propagate_master_sheet_to_amazon(
        format_sku_codes=[created.get("format_sku_code")],
        items=[created.get("item")],
    )
    _clear_upload_dependent_cache()

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

    saved = dict(zip(cols, updated_row))
    _propagate_master_sheet_to_amazon(
        format_sku_codes=[saved.get("format_sku_code")],
        items=[saved.get("item")],
    )
    _clear_upload_dependent_cache()

    return Response({"ok": True, "row": saved})


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
    _clear_upload_dependent_cache()

    return Response({
        "ok": True,
        "deleted": {
            "format_sku_code": deleted[0],
            "product_name": deleted[1],
            "format": deleted[2],
        },
    })


# ─── pincode_mapping ───
# City -> State -> PIN code reference table with just those three columns (plus
# the id PK). One row per city; `pincode` is filled in by ops. Seeded
# (city, state) from city_state_mapping. Managed by the same Master-Sheet-style
# UI (search / edit / bulk paste-upsert). Consumed by the dashboard's State-wise
# Sales map to resolve Amazon's city-wise feed to states, and grown
# automatically with each Amazon city-wise upload (see
# _sync_amazon_cities_to_pincode_mapping).
#
# There's no stored key/source/updated_at column: uniqueness is enforced by a
# functional UNIQUE index on the normalised city (uq_pincode_mapping_city), and
# upsert matches on that same expression so re-uploads update instead of
# duplicating.
PINCODE_MAPPING_COLUMNS = ["city", "state", "pincode"]
PINCODE_MAPPING_SEARCH_COLUMNS = ["city", "state", "pincode"]

# Normalised-city SQL expression; MUST match both _pincode_city_key (below) and
# the uq_pincode_mapping_city index expression (uploads migration 0066).
_PINCODE_CITY_KEY_SQL = "btrim(regexp_replace(upper(city), '[^A-Z0-9]+', ' ', 'g'))"


def _pincode_city_key(value) -> str:
    """Normalise a city the same way _PINCODE_CITY_KEY_SQL / the unique index do:
    UPPER, every run of non-alphanumerics collapsed to a single space, trimmed."""
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def _pincode_mapping_select_columns() -> str:
    quoted = ", ".join(_quote_ident(col) for col in PINCODE_MAPPING_COLUMNS)
    return f'id::text AS "row_id", {quoted}'


def _pincode_mapping_payload(data) -> dict:
    if not isinstance(data, dict):
        raise ValueError("Row data is required.")
    row = {}
    for column in PINCODE_MAPPING_COLUMNS:
        if column not in data:
            continue
        value = data.get(column)
        if value is None:
            row[column] = None
        else:
            text = str(value).strip()
            row[column] = text if text else None
    return row


def _pincode_mapping_existing_by_key(city_keys: list[str]) -> dict[str, dict]:
    keys = sorted({key for key in city_keys if key})
    if not keys:
        return {}
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON ({_PINCODE_CITY_KEY_SQL})
                   {_pincode_mapping_select_columns()}, {_PINCODE_CITY_KEY_SQL} AS ckey
            FROM pincode_mapping
            WHERE {_PINCODE_CITY_KEY_SQL} = ANY(%s)
            ORDER BY {_PINCODE_CITY_KEY_SQL}, id
            """,
            [keys],
        )
        cols = [c[0] for c in cur.description]
        result = {}
        for values in cur.fetchall():
            row = dict(zip(cols, values))
            key = row.pop("ckey")
            result[key] = row
    return result


def _pincode_mapping_bulk_rows(data) -> list[dict]:
    rows = (data or {}).get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("rows must be a list.")

    parsed = []
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            parsed.append({
                "index": index, "city": "", "city_key": "", "row": {},
                "valid": False, "reason": "Row must be an object.",
            })
            continue

        try:
            payload = _pincode_mapping_payload(raw)
        except ValueError as exc:
            parsed.append({
                "index": index,
                "city": str(raw.get("city") or "").strip(),
                "city_key": _pincode_city_key(raw.get("city")),
                "row": raw, "valid": False, "reason": str(exc),
            })
            continue

        city = str(payload.get("city") or "").strip()
        if not city:
            parsed.append({
                "index": index, "city": "", "city_key": "", "row": payload,
                "valid": False, "reason": "city is required.",
            })
            continue
        if not str(payload.get("state") or "").strip():
            parsed.append({
                "index": index, "city": city, "city_key": _pincode_city_key(city),
                "row": payload, "valid": False, "reason": "state is required.",
            })
            continue

        payload["city"] = city
        parsed.append({
            "index": index, "city": city, "city_key": _pincode_city_key(city),
            "row": payload, "valid": True, "reason": "",
        })
    return parsed


def _pincode_mapping_bulk_preview_payload(parsed_rows: list[dict]) -> dict:
    existing = _pincode_mapping_existing_by_key(
        [row["city_key"] for row in parsed_rows if row.get("valid")]
    )
    seen_new = set()
    preview_rows = []
    summary = {"insert": 0, "update": 0, "invalid": 0, "total": len(parsed_rows)}

    for row in parsed_rows:
        if not row.get("valid"):
            summary["invalid"] += 1
            preview_rows.append({
                "index": row["index"], "action": "invalid",
                "city": row.get("city", ""),
                "reason": row.get("reason", "Invalid row."),
                "row": row.get("row", {}),
            })
            continue

        key = row["city_key"]
        action = "update" if key in existing or key in seen_new else "insert"
        if action == "insert":
            seen_new.add(key)
        summary[action] += 1
        preview_rows.append({
            "index": row["index"], "action": action, "city": row["city"],
            "reason": "", "row": row["row"], "existing": existing.get(key),
        })

    return {
        "columns": PINCODE_MAPPING_COLUMNS,
        "summary": summary,
        "rows": preview_rows,
    }


@api_view(["GET"])
@permission_classes([require("upload.use")])
def pincode_mapping_list(request):
    query = str(request.query_params.get("search") or "").strip()
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(200, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 0, 50

    where = []
    params = []
    if query:
        like = f"%{query}%"
        where.append(
            "("
            + " OR ".join(
                f"CAST({_quote_ident(col)} AS text) ILIKE %s"
                for col in PINCODE_MAPPING_SEARCH_COLUMNS
            )
            + ")"
        )
        params.extend([like] * len(PINCODE_MAPPING_SEARCH_COLUMNS))

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = page * page_size

    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM pincode_mapping {where_sql}", list(params))
        total = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT {_pincode_mapping_select_columns()}
            FROM pincode_mapping
            {where_sql}
            ORDER BY COALESCE(state, ''), COALESCE(city, '')
            LIMIT %s OFFSET %s
            """,
            [*params, page_size, offset],
        )
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    return Response({
        "columns": PINCODE_MAPPING_COLUMNS,
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
    })


@api_view(["POST"])
@permission_classes([require("upload.use")])
def pincode_mapping_bulk_preview(request):
    try:
        parsed_rows = _pincode_mapping_bulk_rows(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)
    return Response(_pincode_mapping_bulk_preview_payload(parsed_rows))


@api_view(["POST"])
@permission_classes([require("upload.use")])
def pincode_mapping_bulk_upsert(request):
    try:
        parsed_rows = _pincode_mapping_bulk_rows(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    existing = _pincode_mapping_existing_by_key(
        [row["city_key"] for row in parsed_rows if row.get("valid")]
    )
    result_rows = []
    summary = {"inserted": 0, "updated": 0, "invalid": 0, "total": len(parsed_rows)}

    with transaction.atomic(), connection.cursor() as cur:
        for parsed in parsed_rows:
            if not parsed.get("valid"):
                summary["invalid"] += 1
                result_rows.append({
                    "index": parsed["index"], "action": "invalid",
                    "city": parsed.get("city", ""),
                    "reason": parsed.get("reason", "Invalid row."),
                    "row": parsed.get("row", {}),
                })
                continue

            key = parsed["city_key"]
            row = parsed["row"]
            existing_row = existing.get(key)

            if existing_row:
                # Only overwrite state/pincode with the pasted non-null values.
                update_columns = [
                    col for col in PINCODE_MAPPING_COLUMNS
                    if col != "city" and col in row and row[col] is not None
                ]
                if update_columns:
                    assignments = ", ".join(
                        f"{_quote_ident(col)} = %s" for col in update_columns
                    )
                    values = [row[col] for col in update_columns]
                    cur.execute(
                        f"""
                        UPDATE pincode_mapping
                        SET {assignments}
                        WHERE id = %s::bigint
                        RETURNING {_pincode_mapping_select_columns()}
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
                    "index": parsed["index"], "action": "update",
                    "city": parsed["city"], "reason": "", "row": saved_row,
                })
                continue

            insert_columns = ["city", "state"]
            if row.get("pincode") is not None:
                insert_columns.append("pincode")
            insert_data = {**row, "city": parsed["city"]}
            placeholders = ", ".join(["%s"] * len(insert_columns))
            cur.execute(
                f"""
                INSERT INTO pincode_mapping ({", ".join(_quote_ident(col) for col in insert_columns)})
                VALUES ({placeholders})
                RETURNING {_pincode_mapping_select_columns()}
                """,
                [insert_data[col] for col in insert_columns],
            )
            cols = [c[0] for c in cur.description]
            saved_row = dict(zip(cols, cur.fetchone()))
            existing[key] = saved_row
            summary["inserted"] += 1
            result_rows.append({
                "index": parsed["index"], "action": "insert",
                "city": parsed["city"], "reason": "", "row": saved_row,
            })

    return Response({
        "ok": True,
        "columns": PINCODE_MAPPING_COLUMNS,
        "summary": summary,
        "rows": result_rows,
    })


@api_view(["POST"])
@permission_classes([require("upload.use")])
def pincode_mapping_create(request):
    try:
        row = _pincode_mapping_payload(request.data or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    city = str(row.get("city") or "").strip()
    if not city:
        return Response({"detail": "city is required."}, status=400)
    if not str(row.get("state") or "").strip():
        return Response({"detail": "state is required."}, status=400)

    columns = ["city", "state"]
    if row.get("pincode") is not None:
        columns.append("pincode")
    insert_data = {**row, "city": city}
    placeholders = ", ".join(["%s"] * len(columns))

    try:
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO pincode_mapping ({", ".join(_quote_ident(col) for col in columns)})
                VALUES ({placeholders})
                RETURNING {_pincode_mapping_select_columns()}
                """,
                [insert_data[col] for col in columns],
            )
            cols = [c[0] for c in cur.description]
            created = dict(zip(cols, cur.fetchone()))
    except IntegrityError:
        return Response({"detail": f"A row for '{city}' already exists."}, status=409)

    return Response({"ok": True, "row": created})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def pincode_mapping_update(request):
    row_id = str((request.data or {}).get("row_id") or "").strip()
    if not row_id:
        return Response({"detail": "row_id is required."}, status=400)
    try:
        row = _pincode_mapping_payload((request.data or {}).get("row") or {})
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)
    if not row:
        return Response({"detail": "No fields to update."}, status=400)

    assignments = ", ".join(f"{_quote_ident(col)} = %s" for col in row)
    values = [row[col] for col in row]

    try:
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute(
                f"""
                UPDATE pincode_mapping
                SET {assignments}
                WHERE id = %s::bigint
                RETURNING {_pincode_mapping_select_columns()}
                """,
                [*values, row_id],
            )
            updated_row = cur.fetchone()
            if not updated_row:
                return Response({"detail": "Row was not found. Please search again."}, status=404)
            cols = [c[0] for c in cur.description]
    except IntegrityError:
        return Response({"detail": "Another row already uses that city."}, status=409)

    return Response({"ok": True, "row": dict(zip(cols, updated_row))})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def pincode_mapping_delete(request):
    row_id = str((request.data or {}).get("row_id") or "").strip()
    if not row_id:
        return Response({"detail": "row_id is required."}, status=400)
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(
            """
            DELETE FROM pincode_mapping
            WHERE id = %s::bigint
            RETURNING city, state, pincode
            """,
            [row_id],
        )
        deleted = cur.fetchone()
        if not deleted:
            return Response({"detail": "Row was not found. Please search again."}, status=404)

    return Response({
        "ok": True,
        "deleted": {"city": deleted[0], "state": deleted[1], "pincode": deleted[2]},
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

    _clear_upload_dependent_cache()
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

    _clear_upload_dependent_cache()
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

    _clear_upload_dependent_cache()
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

    if summary["inserted"] or summary["updated"]:
        _clear_upload_dependent_cache()

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


def _parse_upload_delete_date(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    match = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", text)
    if match:
        day, month, year = match.groups()
        try:
            return date(int(year), int(month), int(day))
        except ValueError:
            return None
    return None


@api_view(["POST"])
# Delete-by-date is more destructive than a normal upload, so it is gated on a
# dedicated permission (NOT the shared `upload.use`). Users keep their normal
# upload rights but cannot delete rows unless they explicitly hold this code.
# Superusers bypass all permission checks.
@permission_classes([require("upload.delete_by_date")])
def delete_upload_rows_by_date(request):
    body = request.data or {}
    table = str(body.get("table") or "").strip()
    requested_column = str(body.get("date_column") or "").strip()
    start_date = _parse_upload_delete_date(
        body.get("from_date")
        or body.get("date_from")
        or body.get("start_date")
        or body.get("date")
    )
    end_date = _parse_upload_delete_date(
        body.get("to_date")
        or body.get("date_to")
        or body.get("end_date")
        or body.get("date")
    )

    table_config = UPLOAD_DATE_DELETE_TABLES.get(table)
    if not table_config:
        return Response(
            {"detail": "Date delete is allowed only for Ads, Coupon and Amazon Secondary upload tables."},
            status=400,
        )
    # Single date column, or a (start, end) pair for window rows (see the
    # UPLOAD_DATE_DELETE_TABLES comment). The end column is config-driven (never
    # taken from the request) so it can be safely interpolated into the SQL.
    if isinstance(table_config, (tuple, list)):
        date_column, end_date_column = table_config
    else:
        date_column, end_date_column = table_config, None
    if requested_column and requested_column != date_column:
        return Response({"detail": "Invalid date column for this upload table."}, status=400)
    if not start_date or not end_date:
        return Response({"detail": "Select a valid from date and to date before deleting rows."}, status=400)
    if start_date > end_date:
        return Response({"detail": "From date cannot be after to date."}, status=400)

    with transaction.atomic():
        with connection.cursor() as cur:
            if end_date_column:
                # Window rows: delete only those whose whole [start, end] span
                # falls inside the selected delete window.
                cur.execute(
                    f"""
                    DELETE FROM {_quote_ident(table)}
                    WHERE {_quote_ident(date_column)} >= %s
                      AND {_quote_ident(end_date_column)} <= %s
                    """,
                    [start_date, end_date],
                )
            else:
                cur.execute(
                    f"""
                    DELETE FROM {_quote_ident(table)}
                    WHERE {_quote_ident(date_column)} BETWEEN %s AND %s
                    """,
                    [start_date, end_date],
                )
            deleted = cur.rowcount

    if deleted:
        _clear_upload_dependent_cache()

    return Response({
        "ok": True,
        "table": table,
        "date_column": date_column,
        "end_date_column": end_date_column,
        "from_date": start_date.isoformat(),
        "to_date": end_date.isoformat(),
        "deleted": deleted,
    })


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


def _normalize_platform_format(value) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _row_mentions_jivo(row: dict) -> bool:
    """True when a primary-PO row's SKU name mentions an accepted own-brand
    (Jivo or Sano — see PRIMARY_PO_ACCEPTED_BRANDS)."""
    name = str(row.get("sku_name") or "").lower()
    return any(brand in name for brand in PRIMARY_PO_ACCEPTED_BRANDS)


def _filter_primary_jivo_rows(data: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split primary-PO rows by brand: keep our own-brand SKUs, drop everything else.

    Platform PO exports occasionally include third-party items (e.g. another
    brand's SKU mistakenly attached to the PO). Only own-brand SKUs (Jivo or
    Sano) belong in the primary PO tables, so other-brand rows are skipped on
    upload. Applies to every platform's primary uploader except Amazon
    (separate uploader/table).
    """
    kept: list[dict] = []
    skipped: list[dict] = []
    for row in data:
        (kept if _row_mentions_jivo(row) else skipped).append(row)
    return kept, skipped


def _default_blank_status_to_pending(data: list[dict]) -> int:
    """A blank/missing PO status means the PO is still pending, not expired.

    The master_po view defaults an empty status to EXPIRED, which then books the
    whole undelivered order as 'missed'. Stamping PENDING on ingest keeps such
    POs OPEN until they actually expire (mirrors the source sheet). Only
    blank-status rows are touched; existing statuses are left as-is. Returns the
    number of rows defaulted.
    """
    count = 0
    for row in data:
        if not str(row.get("status") or "").strip():
            row["status"] = "PENDING"
            count += 1
    return count


def _restore_precise_landing_rate(data: list[dict]) -> int:
    """Restore the full-precision landing_rate when a file shipped it pre-rounded.

    Some platform PO exports (e.g. City Mall) store landing_rate already rounded
    to a whole rupee, so the downstream amount (order_qty x landing_rate) drifts a
    few rupees from the source sheet, which keeps the exact basic_rate x GST value.

    For each row we only override the value when it is provably a rounded GST
    figure: landing_rate is a whole number and exactly ONE standard GST slab
    (PRIMARY_PO_GST_MULTIPLIERS), applied to basic_rate and rounded to the nearest
    rupee, reproduces it. That proves the slab, so we replace the rounded value
    with basic_rate x slab. Rows that already carry decimals, margin-based ratios
    that no slab reproduces (e.g. x1.40), or ambiguous matches are left untouched.
    Returns the number of rows whose landing_rate was made precise.
    """
    count = 0
    for row in data:
        if "landing_rate" not in row or "basic_rate" not in row:
            continue
        basic = _as_decimal(row.get("basic_rate"), default=None)
        landing = _as_decimal(row.get("landing_rate"), default=None)
        if basic is None or landing is None or basic <= 0:
            continue
        # Only act on a value the file rounded to a whole rupee; decimals are
        # already precise and must not be touched.
        if landing != landing.to_integral_value():
            continue
        matches = []
        for slab in PRIMARY_PO_GST_MULTIPLIERS:
            precise = basic * slab
            if precise.to_integral_value(rounding=ROUND_HALF_UP) == landing:
                matches.append(precise)
        if len(matches) != 1:
            continue
        precise = matches[0].quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if precise == landing:
            continue
        row["landing_rate"] = str(precise)
        count += 1
    return count


def _validate_primary_upload_format(table: str, data: list[dict], expected_format: str | None):
    if table not in PRIMARY_UPLOAD_TABLES or not expected_format:
        return None

    expected_key = _normalize_platform_format(expected_format)
    if not expected_key:
        return None

    mismatches = []
    for index, row in enumerate(data, start=1):
        actual = row.get("format")
        actual_key = _normalize_platform_format(actual)
        if not actual_key or actual_key != expected_key:
            mismatches.append(
                {
                    "row": index,
                    "format": actual,
                    "expected": expected_format,
                }
            )
            if len(mismatches) >= 5:
                break

    if mismatches:
        return Response(
            {
                "detail": (
                    f"{expected_format} primary uploader only accepts "
                    f"{expected_format} data. Found another platform format."
                ),
                "mismatches": mismatches,
            },
            status=400,
        )
    return None


def _validate_primary_upload_source(
    table: str,
    data: list[dict],
    expected_format: str | None,
    source_format: str | None,
):
    if table not in PRIMARY_UPLOAD_TABLES or not expected_format:
        return None

    expected_key = _normalize_platform_format(expected_format)
    if not expected_key:
        return None

    source_key = _normalize_platform_format(source_format)
    if source_key and source_key != expected_key:
        return Response(
            {
                "detail": (
                    f"{expected_format} primary uploader only accepts "
                    f"{expected_format} data. The upload source was marked as another platform."
                )
            },
            status=400,
        )

    mismatches = []
    for index, row in enumerate(data, start=1):
        actual_key = _normalize_platform_format(row.get("__source_platform"))
        if actual_key and actual_key != expected_key:
            mismatches.append(index)
            if len(mismatches) >= 5:
                break

    if mismatches:
        return Response(
            {
                "detail": (
                    f"{expected_format} primary uploader only accepts "
                    f"{expected_format} data. Found another platform source marker."
                ),
                "rows": mismatches,
            },
            status=400,
        )
    return None


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


def _update_existing_primary_upload_row(
    cur,
    table: str,
    row: dict,
    columns: list[str],
    column_types: dict[str, str],
) -> int:
    """Update existing platform-primary rows with the same PO + SKU."""
    key_parts = _primary_upload_key_parts(table, row)
    key_specs = PRIMARY_UPLOAD_REPLACE_KEYS.get(table)
    if not key_parts or not key_specs:
        return 0

    assignments = ", ".join(
        f"{_quote_ident(column)} = %s"
        for column in columns
    )
    where = " AND ".join(
        f"{_primary_upload_key_sql(spec)} = %s"
        for spec in key_specs
    )
    cur.execute(
        f"UPDATE {_quote_ident(table)} AS t SET {assignments} WHERE {where}",
        [
            *_upload_row_values(row, columns, column_types),
            *key_parts,
        ],
    )
    return cur.rowcount or 0


def _upsert_primary_upload_row(
    cur,
    table: str,
    row: dict,
    columns: list[str],
    column_types: dict[str, str],
    insert_sql: str,
) -> bool:
    """Update matching primary row; insert only when no PO + SKU row exists.

    Returns True when a new row was inserted, False when an existing row was
    updated.
    """
    updated_rows = _update_existing_primary_upload_row(
        cur,
        table,
        row,
        columns,
        column_types,
    )
    if updated_rows:
        return False
    cur.execute(insert_sql, _upload_row_values(row, columns, column_types))
    return True


def _execute_batch_insert_rows(
    cur,
    qtable: str,
    col_list: str,
    placeholders: str,
    upsert_clause: str,
    rows: list[dict],
    columns: list[str],
    column_types: dict[str, str],
    *,
    tracks_upsert_counts: bool,
):
    """Insert many rows with a single multi-row INSERT per sub-chunk — one DB
    round-trip each — instead of one execute() per row. This is the hot path for
    every non-primary upload; the per-row version made an upload of N rows do N
    network round-trips to the DB (the visible multi-second lag). Sub-chunks stay
    well under PostgreSQL's 65535 bind-parameter limit. Returns the RETURNING
    rows ([(inserted_bool,), ...]) when counting upserts, else None.
    """
    results = []
    if not rows:
        return results if tracks_upsert_counts else None
    returning = " RETURNING (xmax::text = '0') AS inserted" if tracks_upsert_counts else ""
    ncols = max(1, len(columns))
    max_rows = max(1, min(len(rows), 50000 // ncols))
    for start in range(0, len(rows), max_rows):
        chunk = rows[start : start + max_rows]
        values_sql = ", ".join([f"({placeholders})"] * len(chunk))
        stmt = f"INSERT INTO {qtable} ({col_list}) VALUES {values_sql}{upsert_clause}{returning}"
        params: list = []
        for row in chunk:
            params.extend(_upload_row_values(row, columns, column_types))
        cur.execute(stmt, params)
        if tracks_upsert_counts:
            results.extend(cur.fetchall())
    return results if tracks_upsert_counts else None


# Zepto GRN sheets can list the SAME po_number + sku_code more than once — one
# line per physical GRN receipt, each with its OWN unique GRN id (grn_code).
# Only Zepto uses this multi-receipt model; every other platform keeps the
# original update-in-place behaviour, so this is strictly format-scoped.
ZEPTO_GRN_FORMAT = "ZEPTO"


def _zepto_grn_line_match(row: dict, table_columns: set[str]) -> tuple[str | None, str]:
    """Pick the column that pins a Zepto GRN line to its PO row.

    The Zepto GRN export is PO + location level (no SKU column), so the SKU/rates
    are pulled from the existing PO row by matching on this discriminator:
    prefer sku_code when the sheet happens to carry it, else fall back to
    location (e.g. 'KTPL-KOL-DRY-MH NEW'). Returns (column, value); column is
    None when neither is available."""
    sku = str(row.get("sku_code") or "").strip()
    if sku and "sku_code" in table_columns:
        return ("sku_code", sku)
    loc = str(row.get("location") or "").strip()
    if loc and "location" in table_columns:
        return ("location", loc)
    return (None, "")


def _is_zepto_grn_code_row(target_table: str, row: dict, table_columns: set[str]) -> bool:
    """True for a Zepto GRN line that carries its own unique GRN id + a line key.

    These rows are stored one-per-unique-grn_code instead of being merged into
    the single matching PO row. Requires the grn_code column to exist (migration
    0061), the row to be Zepto with a grn_code, and a usable line discriminator
    (sku_code or location)."""
    if target_table != "total_po_zbs" or "grn_code" not in table_columns:
        return False
    if str(row.get("format") or "").strip().upper() != ZEPTO_GRN_FORMAT:
        return False
    if not str(row.get("grn_code") or "").strip():
        return False
    match_col, _ = _zepto_grn_line_match(row, table_columns)
    return match_col is not None


def _apply_zepto_grn_code_row(cur, target_table, row, column_types, table_columns) -> str:
    """Upsert ONE Zepto GRN line keyed by (po_number, <line>, grn_code), where
    <line> is the row's SKU or, for the PO+location level GRN export, its
    location.

      1) the exact grn_code is already stored -> update its grn_date/status/qty
         (idempotent re-upload);
      2) first GRN for this PO line            -> claim the existing grn_code-less
         PO row so no orphan base row is left;
      3) another unique grn_code               -> CLONE the PO line row (copying
         the missing fields: sku_code, sku_name, order_qty, rates, ...) and
         override only the GRN fields, so the new receipt becomes its own row;
      4) no matching PO row exists yet         -> insert a minimal standalone row
         so a GRN uploaded before its Primary PO isn't silently dropped.

    Returns 'updated', 'created', or 'skipped'."""
    qtable = _quote_ident(target_table)
    po = str(row.get("po_number") or "").strip()
    grn = str(row.get("grn_code") or "").strip()
    match_col, match_val = _zepto_grn_line_match(row, table_columns)

    row_for_write = dict(row)
    # A GRN means the PO is fulfilled: default the status when the line omits it.
    if (
        "status" in table_columns
        and str(row_for_write.get("grn_date") or "").strip()
        and not str(row_for_write.get("status") or "").strip()
    ):
        row_for_write["status"] = PRIMARY_GRN_COMPLETED_STATUS
    row_for_write["grn_code"] = grn

    # ── SKU-less GRN (matched on LOCATION, no SKU column) ────────────────────
    # A Zepto GRN export at PO+location level carries a PO-level received qty that
    # CANNOT be attributed to a specific SKU. Writing it onto (or cloning) an
    # individual SKU row mis-attributes and inflates delivery — e.g. an 844-unit
    # GRN booked against a 64-unit SKU line, or an orphan blank-SKU duplicate. The
    # authoritative per-SKU delivered_qty is owned by the Primary/master upload
    # (which matches the source sheet); this GRN's only job here is to stamp the
    # delivery DATE. So fill grn_date/status on the PO's existing rows and NEVER
    # touch delivered_qty, and never create/clone rows. (SKU-carrying GRN lines
    # fall through to the per-SKU upsert below, which is safe.)
    if match_col != "sku_code":
        grn_date_val = _normalize_upload_value(
            row_for_write.get("grn_date"), column_types.get("grn_date")
        )
        status_val = row_for_write.get("status")
        stamp = []
        stamp_params: list = []
        if "grn_date" in table_columns and str(row_for_write.get("grn_date") or "").strip():
            stamp.append("grn_date = COALESCE(t.grn_date, %s::date)")
            stamp_params.append(grn_date_val)
        if "status" in table_columns and str(status_val or "").strip():
            stamp.append("status = COALESCE(NULLIF(TRIM(t.status::text), ''), %s)")
            stamp_params.append(status_val)
        if not stamp:
            return "skipped"
        cur.execute(
            f"UPDATE {qtable} AS t SET {', '.join(stamp)} "
            "WHERE LOWER(TRIM(t.po_number::text)) = %s AND UPPER(TRIM(t.format::text)) = %s",
            [*stamp_params, po.lower(), ZEPTO_GRN_FORMAT],
        )
        return "updated" if cur.rowcount else "skipped"

    # Columns a GRN line writes onto a row (only those that exist + are present).
    set_columns = [
        c
        for c in ("grn_date", "status", "delivered_qty", "grn_code")
        if c in table_columns and (c == "grn_code" or c in row_for_write)
    ]
    assignments = ", ".join(f"{_quote_ident(c)} = %s" for c in set_columns)
    set_values = _upload_row_values(row_for_write, set_columns, column_types)

    match_ident = _quote_ident(match_col)
    base_where = (
        "LOWER(TRIM(t.po_number::text)) = %s "
        f"AND LOWER(TRIM(t.{match_ident}::text)) = %s "
        "AND UPPER(TRIM(t.format::text)) = %s"
    )
    base_values = [po.lower(), match_val.lower(), ZEPTO_GRN_FORMAT]
    sub_where = base_where.replace("t.", "t2.")

    # 1) Same GRN id already stored -> idempotent update.
    cur.execute(
        f"UPDATE {qtable} AS t SET {assignments} "
        f"WHERE {base_where} AND LOWER(TRIM(COALESCE(t.grn_code::text, ''))) = %s",
        [*set_values, *base_values, grn.lower()],
    )
    if cur.rowcount:
        return "updated"

    # 2) First GRN for this PO+SKU -> claim the existing grn_code-less PO row
    #    (exactly one, picked by ctid) instead of inserting a duplicate.
    cur.execute(
        f"UPDATE {qtable} AS t SET {assignments} WHERE t.ctid = ("
        f"  SELECT t2.ctid FROM {qtable} t2 "
        f"  WHERE {sub_where} "
        f"    AND NULLIF(TRIM(COALESCE(t2.grn_code::text, '')), '') IS NULL "
        f"  LIMIT 1)",
        [*set_values, *base_values],
    )
    if cur.rowcount:
        return "updated"

    # 3) Another unique GRN id -> clone a PO+SKU row, override only GRN fields.
    insert_columns = [c for c in column_types if c != "id"]
    override = dict(zip(set_columns, set_values))
    select_exprs, insert_params = [], []
    for c in insert_columns:
        if c in override:
            select_exprs.append("%s")
            insert_params.append(override[c])
        else:
            select_exprs.append(f"t.{_quote_ident(c)}")
    col_list = ", ".join(_quote_ident(c) for c in insert_columns)
    cur.execute(
        f"INSERT INTO {qtable} ({col_list}) "
        f"SELECT {', '.join(select_exprs)} FROM {qtable} t WHERE t.ctid = ("
        f"  SELECT t2.ctid FROM {qtable} t2 WHERE {sub_where} LIMIT 1)",
        [*insert_params, *base_values],
    )
    if cur.rowcount:
        return "created"

    # 4) No matching PO row yet (GRN before its Primary PO) -> minimal row.
    #    (Only reached for SKU-carrying GRN lines; SKU-less lines return early
    #    above and never insert an orphan row.)
    minimal = {"po_number": po, "grn_code": grn, "format": ZEPTO_GRN_FORMAT, match_col: match_val}
    for c in ("grn_date", "status", "delivered_qty"):
        if c in row_for_write:
            minimal[c] = row_for_write[c]
    minimal_columns = [c for c in minimal if c in table_columns]
    placeholders = ", ".join(["%s"] * len(minimal_columns))
    cur.execute(
        f"INSERT INTO {qtable} ({', '.join(_quote_ident(c) for c in minimal_columns)}) "
        f"VALUES ({placeholders})",
        _upload_row_values(minimal, minimal_columns, column_types),
    )
    return "created" if cur.rowcount else "skipped"


def _update_total_po_grn_dates(data: list[dict], target_table: str = "total_po") -> Response:
    """Update existing PO rows in a primary PO table from a lean GRN upload.

    The GRN sheet supplies po_number and grn_date. A GRN means the PO has moved
    out of the open bucket, so status/grn_date are applied to every matching PO
    row. When a SKU is present, SKU-specific values such as delivered_qty are
    applied only to that PO + SKU row. No new rows are inserted from GRN files.
    """
    if target_table not in {"total_po", "total_po_zbs"}:
        return Response(
            {"detail": f"Table '{target_table}' is not allowed for GRN update"},
            status=400,
        )

    quoted_target_table = _quote_ident(target_table)
    column_types = _upload_table_column_types(target_table)
    table_columns = set(column_types)
    allowed_update_columns = [
        column
        for column in PRIMARY_UPLOAD_AUTHORITATIVE_COLUMNS
        if column in table_columns
    ]
    prepared: dict[tuple[str, str], dict] = {}
    skipped = 0

    def _grn_qty(value) -> float:
        try:
            return float(str(value).replace(",", "").strip() or 0)
        except (TypeError, ValueError):
            return 0.0

    for row in data:
        po_number = str(row.get("po_number") or "").strip()
        sku_code = str(row.get("sku_code") or "").strip()
        grn_date = str(row.get("grn_date") or "").strip()
        if not po_number or not grn_date:
            skipped += 1
            continue

        # Zepto multi-receipt: a unique grn_code is its own row, so it joins the
        # key and is NOT merged. Identical grn_code re-listed in one file is a
        # true duplicate -> first wins. All other platforms keep merging by
        # (po, sku) as before.
        zepto_code = _is_zepto_grn_code_row(target_table, row, table_columns)
        grn_code = str(row.get("grn_code") or "").strip().lower()
        if zepto_code:
            _, _disc = _zepto_grn_line_match(row, table_columns)
            key = (po_number.lower(), _disc.lower(), grn_code)
        else:
            key = (po_number.lower(), sku_code.lower())
        existing = prepared.get(key)
        if existing is not None:
            if not zepto_code:
                # The same PO+SKU can legitimately appear on several GRN lines:
                # more than one lot, or a receipt line plus a debit-note /
                # zero-qty adjustment line. Received qty is additive across those
                # lines, so SUM delivered_qty rather than keeping a single line.
                # Previously a later line overwrote the earlier one, so a 0-qty DN
                # line silently wiped a real receipt (e.g. MBJPO71303 SKU 240878:
                # 56 received + a 0-qty DN line -> stored 0, losing 14 L). Keep
                # the first non-empty grn_date / status.
                if "delivered_qty" in existing or "delivered_qty" in row:
                    total_qty = _grn_qty(existing.get("delivered_qty")) + _grn_qty(
                        row.get("delivered_qty")
                    )
                    existing["delivered_qty"] = (
                        str(int(total_qty)) if total_qty == int(total_qty) else str(total_qty)
                    )
                for fld in ("grn_date", "status"):
                    if not str(existing.get(fld) or "").strip() and str(row.get(fld) or "").strip():
                        existing[fld] = row[fld]
            skipped += 1
            continue
        prepared[key] = row

    success = 0
    updated = 0
    created = 0
    failed = 0
    last_error: str | None = None

    if prepared:
        try:
            with transaction.atomic(), connection.cursor() as cur:
                for row in prepared.values():
                    # Zepto multi-receipt rows take the dedicated keyed upsert
                    # (update existing grn_code / claim base row / clone-insert).
                    if _is_zepto_grn_code_row(target_table, row, table_columns):
                        outcome = _apply_zepto_grn_code_row(
                            cur, target_table, row, column_types, table_columns
                        )
                        if outcome == "created":
                            created += 1
                            success += 1
                        elif outcome == "updated":
                            updated += 1
                            success += 1
                        else:
                            skipped += 1
                        continue
                    po_number = str(row.get("po_number") or "").strip()
                    sku_code = str(row.get("sku_code") or "").strip()
                    has_sku = bool(sku_code)
                    row_for_update = dict(row)
                    if (
                        "status" in table_columns
                        and "grn_date" in row_for_update
                        and not str(row_for_update.get("status") or "").strip()
                    ):
                        row_for_update["status"] = PRIMARY_GRN_COMPLETED_STATUS

                    format_value = str(row_for_update.get("format") or "").strip()
                    po_where = "LOWER(TRIM(t.po_number::text)) = %s"
                    po_where_values = [po_number.lower()]
                    if format_value and "format" in table_columns:
                        po_where += " AND UPPER(TRIM(t.format::text)) = UPPER(TRIM(%s))"
                        po_where_values.append(format_value)

                    po_update_columns = [
                        column
                        for column in ("grn_date", "status")
                        if column in table_columns and column in row_for_update
                    ]
                    row_updated = 0
                    if po_update_columns:
                        assignments = ", ".join(
                            f"{_quote_ident(column)} = %s"
                            for column in po_update_columns
                        )
                        values = _upload_row_values(row_for_update, po_update_columns, column_types)
                        cur.execute(
                            f"UPDATE {quoted_target_table} AS t SET {assignments} WHERE {po_where}",
                            [*values, *po_where_values],
                        )
                        row_updated += cur.rowcount or 0

                    update_columns = [
                        column
                        for column in allowed_update_columns
                        if (
                            column in row_for_update
                            and column not in {"grn_date", "status"}
                            and (has_sku or column in {"format", "remark"})
                        )
                    ]
                    if not has_sku and "delivered_qty" in update_columns:
                        update_columns.remove("delivered_qty")
                    # A PO-level GRN file (no SKU column) can still post
                    # delivered_qty when the PO has exactly ONE SKU row in the
                    # table — the PO-level GRN qty is unambiguously that row's
                    # delivered qty. With multiple SKUs we can't split it safely,
                    # so it stays skipped (date/status only, as before).
                    if (
                        not has_sku
                        and "delivered_qty" not in update_columns
                        and "delivered_qty" in allowed_update_columns
                        and str(row_for_update.get("delivered_qty") or "").strip() != ""
                    ):
                        cur.execute(
                            f"SELECT COUNT(*) FROM {quoted_target_table} AS t WHERE {po_where}",
                            list(po_where_values),
                        )
                        if (cur.fetchone() or [0])[0] == 1:
                            update_columns.append("delivered_qty")
                    if not update_columns:
                        if row_updated:
                            updated += row_updated
                            success += 1
                        else:
                            skipped += 1
                        continue

                    assignments = ", ".join(
                        f"{_quote_ident(column)} = %s"
                        for column in update_columns
                    )
                    values = _upload_row_values(row_for_update, update_columns, column_types)
                    where = po_where
                    where_values = list(po_where_values)
                    if has_sku:
                        where += " AND LOWER(TRIM(t.sku_code::text)) = %s"
                        where_values.append(sku_code.lower())

                    cur.execute(
                        f"UPDATE {quoted_target_table} AS t SET {assignments} WHERE {where}",
                        [*values, *where_values],
                    )
                    rowcount = cur.rowcount or 0
                    row_updated += rowcount
                    if row_updated:
                        updated += row_updated
                        success += 1
        except Exception as exc:
            failed = len(prepared)
            last_error = str(exc)

    skipped += max(0, len(prepared) - success)
    if updated or created:
        _clear_upload_dependent_cache(target_table)

    return Response(
        {
            "success": success,
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "failed": failed,
            "error": last_error,
        }
    )


def _propagate_swiggy_po_grn_dates(po_numbers: list[str]) -> int:
    """Swiggy-only: fill the delivery date + status on a PO's undelivered SKU rows.

    A Swiggy PO often has several SKUs. When the PO is (partially) delivered, the
    Swiggy primary export carries a delivery date only on the delivered SKU lines
    and leaves it blank on the rest, so those rows land in total_po_zbs with a
    NULL grn_date even though the PO itself is matched/delivered. The user wants
    every SKU of a matched PO to read consistently, so we copy the PO's delivery
    date (its latest grn_date) onto any NULL-grn_date sibling rows, and backfill a
    blank status from the PO's delivered rows. Only fills holes (COALESCE) — never
    overwrites an existing date/status — and only for POs that have at least one
    delivered row. Scoped to format='SWIGGY'. Best-effort; never raises.

    Returns the number of rows updated.
    """
    pons = sorted(
        {str(p or "").strip().lower() for p in po_numbers if str(p or "").strip()}
    )
    if not pons:
        return 0
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                UPDATE total_po_zbs AS t
                SET grn_date = COALESCE(t.grn_date, po.max_grn),
                    status   = COALESCE(NULLIF(TRIM(t.status::text), ''), po.rep_status)
                FROM (
                    SELECT LOWER(TRIM(po_number::text)) AS pon,
                           MAX(grn_date) AS max_grn,
                           (ARRAY_AGG(status::text ORDER BY grn_date DESC NULLS LAST)
                              FILTER (WHERE NULLIF(TRIM(status::text), '') IS NOT NULL))[1]
                              AS rep_status
                    FROM total_po_zbs
                    WHERE UPPER(TRIM(format::text)) = 'SWIGGY'
                      AND LOWER(TRIM(po_number::text)) = ANY(%s)
                    GROUP BY LOWER(TRIM(po_number::text))
                    HAVING MAX(grn_date) IS NOT NULL
                ) po
                WHERE LOWER(TRIM(t.po_number::text)) = po.pon
                  AND UPPER(TRIM(t.format::text)) = 'SWIGGY'
                  AND (t.grn_date IS NULL OR NULLIF(TRIM(t.status::text), '') IS NULL)
                """,
                [pons],
            )
            return cur.rowcount or 0
    except Exception:  # noqa: BLE001 - propagation must never break an upload
        logger.exception("Failed to propagate Swiggy PO grn_date/status")
        return 0


# master_po sync helpers were removed once the master_po table was retired.
# Uploads now go straight to each platform's per-tenant table via the upsert
# `INSERT ... ON CONFLICT` path in `_batch_upload`.


# City spellings that are junk, not real cities — never worth a mapping row.
_PINCODE_SYNC_JUNK = {
    "NA", "N A", "NULL", "NONE", "UNKNOWN", "UNK", "OTHER", "OTHERS", "CITY",
}


def _amazon_city_prune_stale_ranges(rows):
    """Amazon city-wise Secondary files are cumulative month-to-date exports
    (1-28, then 1-29, then 1-30 ...), so each newer file fully contains the
    older ones. Keep only the LATEST range per business + month:

    - rows already stored for the same business + month with an OLDER to_date
      are deleted before this upload's rows go in;
    - incoming rows that are OLDER than what's stored (or older than another
      range inside the same upload) are dropped, not inserted.

    Either way the table always holds exactly one — the max-to_date — snapshot
    per business + month. Returns (rows_to_insert, deleted, skipped_stale)."""
    def _d(value):
        try:
            return date.fromisoformat(str(value or "").strip()[:10])
        except ValueError:
            return None

    latest = {}  # (business, year, month) -> max incoming to_date
    parsed = []  # (row, group_key, to_date)
    for row in rows:
        fd, td = _d((row or {}).get("from_date")), _d((row or {}).get("to_date"))
        if fd is None or td is None:
            parsed.append((row, None, None))  # let normal validation handle it
            continue
        key = (str(row.get("business") or "").strip().upper(), fd.year, fd.month)
        parsed.append((row, key, td))
        if key not in latest or td > latest[key]:
            latest[key] = td

    deleted = 0
    stale_groups = set()
    if latest:
        with connection.cursor() as cur:
            for (business, year, month), td in latest.items():
                # Stale upload guard: the table already holds a fresher range
                # for this business+month — don't let the old file regress it.
                cur.execute(
                    "SELECT MAX(to_date) FROM amazon_sec_city "
                    "WHERE UPPER(TRIM(COALESCE(business, ''))) = %s "
                    "  AND EXTRACT(YEAR FROM from_date) = %s "
                    "  AND EXTRACT(MONTH FROM from_date) = %s",
                    [business, year, month],
                )
                existing_max = (cur.fetchone() or [None])[0]
                if existing_max is not None and existing_max > td:
                    stale_groups.add((business, year, month))
                    continue
                cur.execute(
                    "DELETE FROM amazon_sec_city "
                    "WHERE UPPER(TRIM(COALESCE(business, ''))) = %s "
                    "  AND EXTRACT(YEAR FROM from_date) = %s "
                    "  AND EXTRACT(MONTH FROM from_date) = %s "
                    "  AND to_date < %s",
                    [business, year, month, td],
                )
                deleted += cur.rowcount

    keep, skipped = [], 0
    for row, key, td in parsed:
        if key is None:
            keep.append(row)
        elif key in stale_groups or td < latest[key]:
            skipped += 1
        else:
            keep.append(row)
    return keep, deleted, skipped


def _sync_amazon_cities_to_pincode_mapping(rows):
    """After an amazon_sec_city (city-wise Amazon Secondary) upload, grow
    pincode_mapping with any city it doesn't know yet.

    Only usable rows are added: the city must look like a real name (not
    blank / junk / purely numeric) AND its state must be resolvable from
    city_state_mapping — pincode_mapping.state is NOT NULL, and a city without
    a state is dead weight for the map. Cities whose state can't be resolved
    are returned in `unmapped` so ops can add them (with their state) through
    the Pincode Mapping manager. The dashboard's Amazon state resolution reads
    pincode_mapping, so a city added here shows on the map immediately.
    Failures are reported, never raised — the upload itself already succeeded."""
    cities = {}  # normalised key -> display spelling (first seen in the file)
    for row in rows:
        city = str((row or {}).get("city") or "").strip()
        key = _pincode_city_key(city)
        if not key or len(key) < 2 or key in _PINCODE_SYNC_JUNK:
            continue
        if key.replace(" ", "").isdigit():
            continue
        cities.setdefault(key, city)
    if not cities:
        return {"added": 0, "unmapped": []}

    keys = sorted(cities)
    added, unmapped = 0, []
    try:
        with connection.cursor() as cur:
            cur.execute(
                f"SELECT {_PINCODE_CITY_KEY_SQL} FROM pincode_mapping "
                f"WHERE {_PINCODE_CITY_KEY_SQL} = ANY(%s)",
                [keys],
            )
            known = {r[0] for r in cur.fetchall()}
            missing = [k for k in keys if k not in known]
            if not missing:
                return {"added": 0, "unmapped": []}
            cur.execute(
                "SELECT city_key, state FROM city_state_mapping WHERE city_key = ANY(%s)",
                [missing],
            )
            states = dict(cur.fetchall())
            # Second chance for messy spellings ("ADOOR, PATHANAMTHITTA DIST"):
            # match on the part before the first separator — usually the city.
            prefixes = {
                key: _pincode_city_key(re.split(r"[,(/:;-]", cities[key])[0])
                for key in missing
                if key not in states
            }
            prefix_keys = sorted({p for p in prefixes.values() if p and len(p) >= 3})
            if prefix_keys:
                cur.execute(
                    "SELECT city_key, state FROM city_state_mapping WHERE city_key = ANY(%s)",
                    [prefix_keys],
                )
                by_prefix = dict(cur.fetchall())
                for key, prefix in prefixes.items():
                    if prefix in by_prefix:
                        states[key] = by_prefix[prefix]
            for key in missing:
                state = str(states.get(key) or "").strip().upper()
                if not state:
                    unmapped.append(cities[key])
                    continue
                cur.execute(
                    "INSERT INTO pincode_mapping (city, state) VALUES (%s, %s) "
                    "ON CONFLICT ((btrim(regexp_replace(upper(city), "
                    "'[^A-Z0-9]+', ' ', 'g')))) DO NOTHING",
                    [cities[key], state],
                )
                added += cur.rowcount
    except Exception as exc:
        logger.warning(
            "pincode_mapping sync after amazon_sec_city upload failed: %s", exc
        )
        return {"added": added, "unmapped": sorted(unmapped), "error": str(exc)}
    return {"added": added, "unmapped": sorted(unmapped)}


def _batch_upload(body, *, forced_table: str | None = None):
    body = body or {}
    table = forced_table or body.get("table")
    data = body.get("data") or []
    unique_key = body.get("unique_key") or ""
    upsert = bool(body.get("upsert", True))
    expected_platform_format = body.get("expected_platform_format")
    source_platform_format = body.get("source_platform_format")
    # Full-snapshot datasets (e.g. the Flipkart Consolidated FSN Report, which
    # carries no per-row date) send replace_all on their FIRST chunk to wipe the
    # table before reloading. Never honoured for primary PO tables (guarded
    # below) so it can't be misused to clear order history.
    replace_all = bool(body.get("replace_all", False))

    if table not in UPLOAD_ALLOWED_TABLES:
        return Response(
            {"detail": f"Table '{table}' is not allowed for upload"},
            status=400,
        )
    if not _IDENT.match(table):
        return Response({"detail": "Invalid table name."}, status=400)
    if not isinstance(data, list) or not data:
        return Response({"success": 0, "failed": 0, "error": None})

    source_error = _validate_primary_upload_source(
        table,
        [row for row in data if isinstance(row, dict)],
        expected_platform_format,
        source_platform_format,
    )
    if source_error is not None:
        return source_error

    data = [
        {key: value for key, value in row.items() if not str(key).startswith("__")}
        for row in data
        if isinstance(row, dict)
    ]
    if not data:
        return Response({"success": 0, "failed": 0, "error": None})

    format_error = _validate_primary_upload_format(table, data, expected_platform_format)
    if format_error is not None:
        return format_error

    if table == "total_po_grn_update":
        return _update_total_po_grn_dates(data)
    if table == "total_po_zbs_grn_update":
        return _update_total_po_grn_dates(data, target_table="total_po_zbs")

    # Primary PO tables only accept our own-brand SKUs (Jivo or Sano); drop any
    # other-brand rows accidentally included in a platform PO export.
    skipped_non_jivo = 0
    defaulted_pending_status = 0
    if table in PRIMARY_PO_JIVO_ONLY_TABLES:
        data, non_jivo_rows = _filter_primary_jivo_rows(data)
        skipped_non_jivo = len(non_jivo_rows)
        if skipped_non_jivo:
            logger.info(
                "Skipped %s non-Jivo row(s) on %s upload", skipped_non_jivo, table
            )
        if not data:
            return Response({
                "success": 0,
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "failed": 0,
                "skipped_non_jivo": skipped_non_jivo,
                "error": None,
            })
        # A blank PO status means pending, not expired. Default it to PENDING so
        # the master_po view does not fall back to its EXPIRED default and
        # over-count the order as missed.
        defaulted_pending_status = _default_blank_status_to_pending(data)
        if defaulted_pending_status:
            logger.info(
                "Defaulted %s blank-status row(s) to PENDING on %s upload",
                defaulted_pending_status, table,
            )
        # Some PO files (e.g. City Mall) ship landing_rate pre-rounded to a whole
        # rupee, which drifts the derived amount from the source sheet. Restore the
        # precise basic_rate x GST value where it can be proven (see helper).
        restored_landing_rates = _restore_precise_landing_rate(data)
        if restored_landing_rates:
            logger.info(
                "Restored precise landing_rate on %s row(s) for %s upload",
                restored_landing_rates, table,
            )

    if upsert and table in UPLOAD_FORCED_UNIQUE_KEYS:
        unique_key = UPLOAD_FORCED_UNIQUE_KEYS[table]

    replace_by_primary_key = table in PRIMARY_UPLOAD_REPLACE_KEYS
    if replace_by_primary_key:
        unique_key = ""

    missing_rates = _collect_zepto_missing_rates(data) if table == "zeptoSec" else []

    # City-wise Amazon Secondary: cumulative ranges — clear this month's older
    # ranges and drop incoming rows staler than what's stored (see helper).
    pruned_ranges = 0
    skipped_stale = 0
    if table == "amazon_sec_city":
        data, pruned_ranges, skipped_stale = _amazon_city_prune_stale_ranges(data)
        if not data:
            return Response({
                "success": 0, "created": 0, "updated": 0,
                "skipped": skipped_stale, "failed": 0,
                "pruned_ranges": pruned_ranges, "skipped_stale": skipped_stale,
                "error": None,
                "warnings": [
                    "Upload skipped: the table already holds a fresher "
                    "month-to-date range for this business and month."
                ] if skipped_stale else [],
            })

    column_types = _upload_table_column_types(table)
    table_columns = set(column_types)
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
        # Wipe-and-reload snapshot upload: clear the whole table before inserting
        # this (first) chunk's rows. Guarded off the primary PO tables.
        if replace_all and table not in PRIMARY_UPLOAD_TABLES:
            cur.execute(f'DELETE FROM {_quote_ident(table)}')
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            if batch and replace_by_primary_key:
                try:
                    with transaction.atomic():
                        batch_created = 0
                        batch_updated = 0
                        for row in batch:
                            inserted = _upsert_primary_upload_row(
                                cur,
                                table,
                                row,
                                columns,
                                column_types,
                                sql,
                            )
                            if inserted:
                                batch_created += 1
                            else:
                                batch_updated += 1
                    created += batch_created
                    platform_created += batch_created
                    updated += batch_updated
                    platform_updated += batch_updated
                    success += batch_created + batch_updated
                    continue
                except Exception as batch_exc:
                    last_error = str(batch_exc)
                    try:
                        connection.rollback()
                    except Exception:
                        pass

            if batch and not replace_by_primary_key:
                try:
                    batch_results = _execute_batch_insert_rows(
                        cur,
                        _quote_ident(table),
                        col_list,
                        placeholders,
                        upsert_clause,
                        batch,
                        columns,
                        column_types,
                        tracks_upsert_counts=tracks_upsert_counts,
                    )
                    if tracks_upsert_counts:
                        for result in batch_results or []:
                            if result[0]:
                                created += 1
                                platform_created += 1
                            else:
                                updated += 1
                                platform_updated += 1
                        success += len(batch_results or [])
                        skipped_in_batch = len(batch) - len(batch_results or [])
                        if skipped_in_batch > 0:
                            skipped += skipped_in_batch
                            platform_skipped += skipped_in_batch
                    else:
                        created += len(batch)
                        platform_created += len(batch)
                        success += len(batch)
                    continue
                except Exception as batch_exc:
                    last_error = str(batch_exc)
                    try:
                        connection.rollback()
                    except Exception:
                        pass

            for row in batch:
                try:
                    if replace_by_primary_key:
                        with transaction.atomic():
                            inserted = _upsert_primary_upload_row(
                                cur,
                                table,
                                row,
                                columns,
                                column_types,
                                sql,
                            )
                    else:
                        cur.execute(sql, _upload_row_values(row, columns, column_types))

                    if replace_by_primary_key:
                        if inserted:
                            created += 1
                            platform_created += 1
                        else:
                            updated += 1
                            platform_updated += 1
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

    # City-wise Amazon Secondary: teach pincode_mapping any city it hasn't seen
    # yet (state auto-resolved; junk / unresolvable cities skipped — see helper).
    pincode_sync = None
    if success and table == "amazon_sec_city":
        pincode_sync = _sync_amazon_cities_to_pincode_mapping(data)

    if success:
        # Swiggy-only: a partially-delivered PO leaves its undelivered SKU rows
        # with a NULL delivery date. Fill those from the PO's delivered rows so
        # every SKU of a matched PO reads consistently (see helper docstring).
        if table == "total_po_zbs":
            swiggy_pos = [
                row.get("po_number")
                for row in data
                if str(row.get("format") or "").strip().upper() == "SWIGGY"
            ]
            if swiggy_pos:
                _propagate_swiggy_po_grn_dates(swiggy_pos)
        _clear_upload_dependent_cache(table)

    return Response({
        "success": success,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "platform_created": platform_created,
        "platform_updated": platform_updated,
        "platform_skipped": platform_skipped,
        "skipped_non_jivo": skipped_non_jivo,
        "defaulted_pending_status": defaulted_pending_status,
        "duplicates": updated + skipped,
        "failed": failed,
        "error": last_error,
        "warnings": [
            f"Landing rate missing for {r['item']}, {r['month_label']} ({r['rows']} rows)"
            for r in missing_rates
        ] + ([
            "New cities without a known state (add them in Pincode Mapping to "
            "show on the map): " + ", ".join(pincode_sync["unmapped"])
        ] if pincode_sync and pincode_sync.get("unmapped") else []),
        "missing_rates": missing_rates,
        "pincode_sync": pincode_sync,
        "pruned_ranges": pruned_ranges,
        "skipped_stale": skipped_stale,
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

        if rows:
            _clear_upload_dependent_cache()

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


@api_view(["POST"])
@permission_classes([require("upload.use")])
def fk_grocery_master_reprocess(request):
    """Re-enrich existing flipkart_grocery_master rows from master_sheet and monthly_landing_rate.

    Useful when rows were uploaded before their SKUs existed in master_sheet.
    Accepts optional body: {"month": 5, "year": 2026} to target a specific period.
    Without filters, re-processes all rows where item_head IS NULL.
    """
    body = request.data or {}
    month_param = body.get("month")
    year_param = body.get("year")

    try:
        period_filter = ""
        period_params: list = []
        if month_param is not None and year_param is not None:
            period_filter = 'AND fgm."month" = %s AND fgm."year" = %s'
            period_params = [int(month_param), int(year_param)]

        with connection.cursor() as cur:
            # Step 1: bulk update from master_sheet for all NULL item_head rows
            cur.execute(
                f"""
                UPDATE "flipkart_grocery_master" AS fgm
                SET
                    item         = ms.item,
                    category     = ms.category,
                    sub_category = ms.sub_category,
                    item_head    = ms.item_head,
                    per_ltr      = ms.per_unit_value::NUMERIC,
                    per_ltr_unit = ms.per_unit,
                    uom          = ms.uom,
                    brand        = COALESCE(fgm.brand, ms.brand),
                    ltr_sold     = ms.per_unit_value::NUMERIC * fgm.qty
                FROM master_sheet ms
                WHERE ms.format_sku_code = fgm.sku_id
                  AND fgm.item_head IS NULL
                  {period_filter}
                """,
                period_params,
            )
            master_updated = cur.rowcount

            # Step 2: update sale amounts from monthly_landing_rate (prefer month-matched rate)
            cur.execute(
                f"""
                UPDATE "flipkart_grocery_master" AS fgm
                SET
                    landing_rate        = mlr.landing_rate,
                    basic_rate          = mlr.basic_rate,
                    sale_amt_inclusive  = mlr.landing_rate * fgm.qty,
                    sale_amt_exclusive  = mlr.basic_rate * fgm.qty
                FROM (
                    SELECT DISTINCT ON (sku_code)
                        sku_code, landing_rate, basic_rate
                    FROM monthly_landing_rate
                    WHERE UPPER(TRIM(format)) = 'FLIPKART GROCERY'
                    ORDER BY sku_code, month DESC, created_at DESC
                ) mlr
                WHERE mlr.sku_code = fgm.sku_id
                  AND (fgm.sale_amt_exclusive IS NULL OR fgm.sale_amt_exclusive = 0)
                  {period_filter}
                """,
                period_params,
            )
            price_updated = cur.rowcount

            # Report how many SKUs still have no item_head (not in master_sheet)
            cur.execute(
                f"""
                SELECT DISTINCT sku_id FROM "flipkart_grocery_master" fgm
                WHERE fgm.item_head IS NULL {period_filter}
                """,
                period_params,
            )
            missing_master_skus = sorted(r[0] for r in cur.fetchall())

            cur.execute(
                f"""
                SELECT DISTINCT sku_id FROM "flipkart_grocery_master" fgm
                WHERE (fgm.sale_amt_exclusive IS NULL OR fgm.sale_amt_exclusive = 0)
                  AND fgm.item_head IS NOT NULL
                  {period_filter}
                """,
                period_params,
            )
            missing_rate_skus = sorted(r[0] for r in cur.fetchall())

        if master_updated or price_updated:
            _clear_upload_dependent_cache()

        return Response({
            "master_updated": master_updated,
            "price_updated": price_updated,
            "missing_master": len(missing_master_skus),
            "missing_master_skus": missing_master_skus,
            "missing_landing_rate": len(missing_rate_skus),
            "missing_landing_rate_skus": missing_rate_skus,
        })
    except Exception as e:
        return Response({"detail": str(e)}, status=500)
