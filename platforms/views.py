import re
from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.core.cache import cache
from django.db import connection, transaction
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import NotFound, PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import can_access_platform, require
from config.perf_cache import cached_get

from .models import PlatformConfig
from .primary_po_columns import order_primary_master_po_row

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LANDING_BASIC_DIVISOR = Decimal("1.05")
PRIMARY_PO_VIEW = "master_po"


def _safe_ident(name: str) -> str:
    if not name or not _IDENT.match(name):
        raise ValidationError(f"Invalid table identifier: {name!r}")
    return name


def _safe_col(name: str) -> str | None:
    return name if name and _IDENT.match(name) else None


def _scalar(sql: str, params: list):
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def _dict_rows(sql: str, params: list) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# The primary-dashboard endpoints run 6+ SELECTs that each rebuild the same
# heavy CTE chain (regex pack parsing, text-date parsing, COALESCE
# normalization). _materialize_primary_normalized() runs that chain ONCE per
# request and stores the `normalized` rows in a PostgreSQL session-private
# TEMP TABLE — also named `normalized` so the downstream queries can keep
# their existing `FROM normalized` references unchanged (the temp table
# shadows the CTE name in subsequent queries).
# Pair every call with _drop_primary_normalized() in a try/finally so the
# table cannot survive into the next request on a persistent (CONN_MAX_AGE)
# connection.
_PRIMARY_NORMALIZED_TEMP = "normalized"


def _materialize_primary_normalized(cte_sql: str) -> None:
    with connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS pg_temp.{_PRIMARY_NORMALIZED_TEMP}")
        cur.execute(
            f"CREATE TEMP TABLE {_PRIMARY_NORMALIZED_TEMP} AS "
            f"{cte_sql} SELECT * FROM normalized"
        )
        # No ANALYZE: this temp table is a single platform's slice (a few thousand
        # rows) and every downstream query just seq-scans + aggregates it, so the
        # planner gains nothing from stats. ANALYZE was costing ~1s per request.


def _drop_primary_normalized() -> None:
    with connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS pg_temp.{_PRIMARY_NORMALIZED_TEMP}")


# Empty WITH-clause stub. Substituted into queries that previously prefixed
# the full CTE so the existing f-string SQL (which often extends with
# `, item_agg AS (...)`) keeps its comma-prefixed grammar valid.
_PRIMARY_CTE_STUB = "WITH _stub AS (SELECT 1)"

_PRIMARY_DASHBOARD_CACHE_TTL = 60  # seconds
_PRIMARY_DASHBOARD_CACHE_VERSION = 22


# Platforms hidden from the whole app. Kept in code/DB (not deleted), but the
# backend refuses to resolve them so every platform data endpoint 404s for these
# slugs. Remove a slug here to bring the platform back.
HIDDEN_PLATFORM_SLUGS = frozenset({"jiomart"})


def _get_platform(slug: str) -> PlatformConfig:
    if slug in HIDDEN_PLATFORM_SLUGS:
        raise NotFound(f"Platform '{slug}' is not available.")
    return get_object_or_404(PlatformConfig, slug=slug, is_active=True)


def _ensure_scope(user, slug: str) -> None:
    if not can_access_platform(user, slug):
        raise PermissionDenied(f"Your account is not authorized for the '{slug}' platform.")


def _page(request) -> tuple[int, int]:
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(200, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 0, 50
    return page, page_size


# Approximate row counts from pg_class. Postgres maintains `reltuples` via
# ANALYZE/autovacuum, so this is O(1) vs a full COUNT(*) scan. Good enough
# for a stat card; exact totals are not required.
def _approx_count(table: str) -> int:
    try:
        val = _scalar(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = %s",
            [table],
        )
        return int(val) if val and val > 0 else 0
    except Exception:
        return 0


# ── Real stats-card metrics (audit finding #1) ───────────────────────────────
# The card previously showed lifetime pg_class row-count estimates for both
# "Inventory Items" and "Secondary", which are meaningless (and 0 for the 7
# platforms whose PlatformConfig.secondary_table points at a table that doesn't
# exist). These compute the real numbers instead.

# Normalised formats that live in secmaster_mv (the unified QC secondary view).
_STATS_SECMASTER_FORMATS = {
    "blinkit", "zepto", "swiggy", "bigbasket", "flipkart", "jiomart",
}


def _stats_inventory_items(inv_table: str | None) -> int:
    """Count of SKUs on the LATEST inventory snapshot day — i.e. items currently
    in stock, matching the "Inventory Items" card label. Every `*_inventory`
    table is a daily snapshot keyed on inventory_date; the old code counted every
    snapshot row ever ingested. Falls back to the approx count if the table has
    no inventory_date column, and to 0 if there's no inventory feed at all."""
    if not inv_table:
        return 0
    try:
        val = _scalar(
            f'SELECT COUNT(*) FROM "{inv_table}" '
            f'WHERE inventory_date = (SELECT MAX(inventory_date) FROM "{inv_table}")',
            [],
        )
        return int(val or 0)
    except Exception:
        return _approx_count(inv_table)


def _stats_secondary_units(slug: str) -> int:
    """Units sold in the current calendar month for the platform's secondary
    feed. secmaster_mv covers the QC formats + Flipkart marketplace; Amazon has
    its own daily feed; primary-only platforms (zomato/citymall/flipkart_grocery)
    have no secondary sales -> 0."""
    today = date.today()
    key = re.sub(r"[^a-z0-9]+", "", slug.lower())
    try:
        if key in _STATS_SECMASTER_FORMATS:
            val = _scalar(
                "SELECT COALESCE(SUM(quantity), 0) FROM secmaster_mv "
                "WHERE REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = %s "
                "  AND UPPER(TRIM(month::text)) = %s AND year::numeric = %s",
                [key, _month_name(today.month), today.year],
            )
            return int(val or 0)
        if key == "amazon":
            val = _scalar(
                "SELECT COALESCE(SUM(shipped_units), 0) FROM amazon_sec_daily "
                "WHERE EXTRACT(MONTH FROM report_date) = %s "
                "  AND EXTRACT(YEAR FROM report_date) = %s",
                [today.month, today.year],
            )
            return int(val or 0)
    except Exception:
        return 0
    return 0


# ─── /{slug}/stats ───
_STATS_CACHE_TTL = 60  # seconds


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.stats")
def platform_stats(request, slug: str):
    _ensure_scope(request.user, slug)

    cache_key = f"platform_stats:{slug}"
    cached = cache.get(cache_key)
    if cached is not None:
        return Response(cached)

    p = _get_platform(slug)
    inv = _safe_ident(p.inventory_table) if p.inventory_table else None
    sec = _safe_ident(p.secondary_table) if p.secondary_table else None
    master = _safe_ident(PRIMARY_PO_VIEW)

    filter_col = _safe_col(p.po_filter_column or "platform") or "platform"
    filter_val = p.po_filter_value or p.slug

    inventory_count = _stats_inventory_items(inv)
    sells_count = _stats_secondary_units(slug)

    try:
        open_pos = _scalar(
            f'SELECT COUNT(*) FROM "{master}" WHERE "{filter_col}" ILIKE %s',
            [f"%{filter_val}%"],
        ) or 0
    except Exception:
        open_pos = 0

    payload = {
        "inventory": int(inventory_count),
        "sells": int(sells_count),
        "openPOs": int(open_pos),
        "activeTrucks": 0,
    }
    cache.set(cache_key, payload, _STATS_CACHE_TTL)
    return Response(payload)


# ─── /{slug}/pos ───
@api_view(["GET"])
@permission_classes([require("platform.po.view")])
def platform_pos(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    master = _safe_ident(PRIMARY_PO_VIEW)
    filter_col = _safe_col(p.po_filter_column or "platform") or "platform"
    filter_val = p.po_filter_value or p.slug
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    where = f'WHERE "{filter_col}" ILIKE %s'
    params: list = [f"%{filter_val}%"]
    if search:
        where += (
            ' AND ("po_number" ILIKE %s OR "sku_name" ILIKE %s OR "sku_code" ILIKE %s)'
        )
        s = f"%{search}%"
        params.extend([s, s, s])

    try:
        total = _scalar(f'SELECT COUNT(*) FROM "{master}" {where}', params) or 0
        rows = _dict_rows(
            f'SELECT * FROM "{master}" {where} LIMIT %s OFFSET %s',
            params + [page_size, offset],
        )
        rows = [order_primary_master_po_row(row) for row in rows]
    except Exception:
        total = 0
        rows = []

    return Response({
        "data": rows,
        "count": int(total),
        "page": page,
        "page_size": page_size,
    })


# ─── /{slug}/inventory-match?sku= ───
def _bigbasket_primary_zero_row(item_head: str | None = None, item: str | None = None) -> dict:
    row = {
        "order_value": 0.0,
        "order_ltrs": 0.0,
        "order_qty": 0.0,
        "projection_value": 0.0,
        "projection_ltrs": 0.0,
        "projection_qty": 0.0,
        "done_value": 0.0,
        "done_ltrs": 0.0,
        "done_qty": 0.0,
        "pending_value": 0.0,
        "pending_ltrs": 0.0,
        "pending_qty": 0.0,
        "dp_value": 0.0,
        "dp_ltrs": 0.0,
        "expired_value": 0.0,
        "expired_ltrs": 0.0,
        "cancelled_value": 0.0,
        "cancelled_ltrs": 0.0,
    }
    if item_head is not None:
        row["item_head"] = item_head
    if item is not None:
        row["item"] = item
    return row


def _bigbasket_primary_normalize_row(row: dict, *, include_cancelled: bool = True) -> dict:
    result = {
        "item_head": row.get("item_head"),
        "order_value": _num(row.get("order_value")),
        "order_ltrs": _num(row.get("order_ltrs")),
        "order_qty": _num(row.get("order_qty")),
        "projection_value": _num(row.get("projection_value")),
        "projection_ltrs": _num(row.get("projection_ltrs")),
        "projection_qty": _num(row.get("projection_qty")),
        "done_value": _num(row.get("done_value")),
        "done_ltrs": _num(row.get("done_ltrs")),
        "done_qty": _num(row.get("done_qty")),
        "pending_value": _num(row.get("pending_value")),
        "pending_ltrs": _num(row.get("pending_ltrs")),
        "pending_qty": _num(row.get("pending_qty")),
        "expired_value": _num(row.get("expired_value")),
        "expired_ltrs": _num(row.get("expired_ltrs")),
    }
    result["dp_value"] = result["done_value"] + result["pending_value"]
    result["dp_ltrs"] = result["done_ltrs"] + result["pending_ltrs"]
    if "item" in row:
        result["item"] = row.get("item")
    if "category" in row:
        result["category"] = row.get("category")
    if "sub_category" in row:
        result["sub_category"] = row.get("sub_category")
    if "per_ltr" in row:
        result["per_ltr"] = row.get("per_ltr")
    if include_cancelled:
        result["cancelled_value"] = _num(row.get("cancelled_value"))
        result["cancelled_ltrs"] = _num(row.get("cancelled_ltrs"))
    return result


def _bigbasket_primary_total(rows: list[dict], *, include_cancelled: bool = True) -> dict:
    fields = [
        "order_value",
        "order_ltrs",
        "order_qty",
        "projection_value",
        "projection_ltrs",
        "projection_qty",
        "done_value",
        "done_ltrs",
        "done_qty",
        "pending_value",
        "pending_ltrs",
        "pending_qty",
        "dp_value",
        "dp_ltrs",
        "expired_value",
        "expired_ltrs",
    ]
    if include_cancelled:
        fields.extend(["cancelled_value", "cancelled_ltrs"])
    return {field: sum(_num(row.get(field)) for row in rows) for field in fields}


_PRIMARY_DASHBOARD_FORMATS = {
    "zepto": "ZEPTO",
    "bigbasket": "BIG BASKET",
    "blinkit": "BLINKIT",
    "citymall": "CITY MALL",
    "flipkart": "FLIPKART GROCERY",
    "flipkart_grocery": "FLIPKART GROCERY",
    "swiggy": "SWIGGY",
    "zomato": "ZOMATO",
}
_PRIMARY_DASHBOARD_DONE_VALUE_COLUMNS = {
    ("bigbasket", "DEL MONTH"): "total_deliver_amt_inclusive",
    ("bigbasket", "PO MONTH"): "total_deliver_amt_inclusive",
    ("blinkit", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("blinkit", "PO MONTH"): "total_delivered_amt_exclusive",
    ("citymall", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("citymall", "PO MONTH"): "total_delivered_amt_exclusive",
    ("flipkart", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("flipkart", "PO MONTH"): "total_delivered_amt_exclusive",
    ("flipkart_grocery", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("flipkart_grocery", "PO MONTH"): "total_delivered_amt_exclusive",
    ("swiggy", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("swiggy", "PO MONTH"): "total_delivered_amt_exclusive",
    ("zomato", "DEL MONTH"): "total_order_amt_exclusive",
    ("zomato", "PO MONTH"): "total_delivered_amt_exclusive",
}
_TOTAL_PO_KPI_DASHBOARD_SLUGS = set()
_MASTER_PO_ORDER_MINUS_DELIVER_KPI_SLUGS = {
    "bigbasket",
    "citymall",
    "flipkart",
    "flipkart_grocery",
}


def _primary_total_po_kpi_total(
    platform_format: str,
    mode: str,
    month: int,
    year: int,
    period_end_cap: date,
) -> dict:
    """KPI-card totals for current primary upload data stored in total_po."""
    period_col = "po_date" if mode == "PO MONTH" else "grn_date"
    period_start = date(year, month, 1)
    period_end = min(date(year, month, monthrange(year, month)[1]), period_end_cap)
    if period_end < period_start:
        return _primary_zero_metrics()

    format_key = re.sub(
        r"[^a-z0-9]+",
        "",
        str(platform_format or "").strip().lower(),
    )
    rows = _dict_rows(
        f"""
        WITH base AS (
            SELECT
                COALESCE(order_qty, 0) AS order_qty,
                COALESCE(delivered_qty, 0) AS delivered_qty,
                GREATEST(COALESCE(order_qty, 0) - COALESCE(delivered_qty, 0), 0) AS pending_qty,
                COALESCE(basic_rate, 0) AS basic_rate,
                UPPER(COALESCE(sku_name, '')) AS pack_text,
                regexp_match(
                    UPPER(COALESCE(sku_name, '')),
                    '([0-9]+(?:\\.[0-9]+)?)\\s*(?:LTR|LITRE|LITER|L)\\s*\\+\\s*([0-9]+(?:\\.[0-9]+)?)\\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'
                ) AS combo_full_match,
                regexp_match(
                    UPPER(COALESCE(sku_name, '')),
                    '([0-9]+(?:\\.[0-9]+)?)\\s*\\+\\s*([0-9]+(?:\\.[0-9]+)?)\\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'
                ) AS combo_compact_match,
                regexp_match(
                    UPPER(COALESCE(sku_name, '')),
                    '([0-9]+(?:\\.[0-9]+)?)\\s*(?:ML|MLS|M)(?:[^A-Z0-9]|$)'
                ) AS ml_match,
                regexp_match(
                    UPPER(COALESCE(sku_name, '')),
                    '([0-9]+(?:\\.[0-9]+)?)\\s*(?:LTR|LITRE|LITER)(?:[^A-Z0-9]|$)'
                ) AS ltr_match,
                regexp_match(
                    UPPER(COALESCE(sku_name, '')),
                    '([0-9]+(?:\\.[0-9]+)?)\\s*L(?:[^A-Z0-9]|$)'
                ) AS l_match
            FROM public.total_po
            WHERE REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = %s
              AND {period_col} >= %s
              AND {period_col} <= %s
        ),
        metric_base AS (
            SELECT
                *,
                COALESCE(
                    CASE
                        WHEN combo_full_match IS NOT NULL
                            THEN combo_full_match[1]::numeric + combo_full_match[2]::numeric
                        WHEN combo_compact_match IS NOT NULL
                            THEN combo_compact_match[1]::numeric + combo_compact_match[2]::numeric
                        WHEN ml_match IS NOT NULL
                            THEN ml_match[1]::numeric / 1000
                        WHEN ltr_match IS NOT NULL
                            THEN ltr_match[1]::numeric
                        WHEN l_match IS NOT NULL
                            THEN l_match[1]::numeric
                        ELSE NULL
                    END,
                    1
                ) AS effective_per_liter
            FROM base
        )
        SELECT
            COALESCE(SUM(order_qty * basic_rate), 0) AS order_value,
            COALESCE(SUM(order_qty * effective_per_liter), 0) AS order_ltrs,
            COALESCE(SUM(order_qty), 0) AS order_qty,
            0 AS projection_value,
            0 AS projection_ltrs,
            0 AS projection_qty,
            COALESCE(SUM(delivered_qty * basic_rate), 0) AS done_value,
            COALESCE(SUM(delivered_qty * effective_per_liter), 0) AS done_ltrs,
            COALESCE(SUM(delivered_qty), 0) AS done_qty,
            COALESCE(SUM(pending_qty * basic_rate), 0) AS pending_value,
            COALESCE(SUM(pending_qty * effective_per_liter), 0) AS pending_ltrs,
            COALESCE(SUM(pending_qty), 0) AS pending_qty,
            COALESCE(SUM(pending_qty * effective_per_liter), 0) AS missed_ltrs
        FROM metric_base
        """,
        [format_key, period_start, period_end],
    )
    return _primary_metrics(rows[0] if rows else None)


def _primary_master_po_order_minus_deliver_kpi_total(
    platform_format: str,
    mode: str,
    month: int,
    year: int,
) -> dict:
    period_col = "po_date" if mode == "PO MONTH" else "delivery_date"
    period_start = date(year, month, 1)
    next_month, next_year = _shift_month(month, year, 1)
    period_end = date(next_year, next_month, 1)
    format_key = re.sub(
        r"[^a-z0-9]+",
        "",
        str(platform_format or "").strip().lower(),
    )
    rows = _dict_rows(
        f"""
        SELECT
            COALESCE(SUM(COALESCE(total_order_amt_inclusive, 0)), 0) AS order_value,
            COALESCE(SUM(COALESCE(total_order_liters, 0)), 0) AS order_ltrs,
            COALESCE(SUM(COALESCE(order_qty, 0)), 0) AS order_qty,
            0 AS projection_value,
            0 AS projection_ltrs,
            0 AS projection_qty,
            COALESCE(SUM(COALESCE(total_deliver_amt_inclusive, 0)), 0) AS done_value,
            COALESCE(SUM(COALESCE(total_delivered_liters, 0)), 0) AS done_ltrs,
            COALESCE(SUM(COALESCE(delivered_qty, 0)), 0) AS done_qty,
            -- Pending = short-delivered ("missed") balance. The open_close
            -- column is empty for every row, so the old open_close='OPEN'
            -- filter always yielded 0. missed_qty x basic_rate / missed_ltrs /
            -- missed_qty match the source DB exactly.
            COALESCE(SUM(COALESCE(missed_qty, 0) * COALESCE(basic_rate, 0)), 0) AS pending_value,
            COALESCE(SUM(COALESCE(missed_ltrs, 0)), 0) AS pending_ltrs,
            COALESCE(SUM(COALESCE(missed_qty, 0)), 0) AS pending_qty,
            COALESCE(SUM(COALESCE(missed_ltrs, 0)), 0) AS missed_ltrs,
            MAX({period_col}) AS projection_max_date
        FROM public.master_po
        WHERE REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = %s
          AND {period_col} >= %s
          AND {period_col} < %s
        """,
        [format_key, period_start, period_end],
    )
    metrics = _primary_metrics(rows[0] if rows else None)
    elapsed_day = _sec_elapsed_day((rows[0] if rows else {}).get("projection_max_date"))
    days_in_month = monthrange(year, month)[1]
    metrics["projection_value"] = _safe_div(metrics["done_value"], elapsed_day) * days_in_month
    metrics["projection_ltrs"] = _safe_div(metrics["done_ltrs"], elapsed_day) * days_in_month
    metrics["projection_qty"] = _safe_div(metrics["done_qty"], elapsed_day) * days_in_month
    return metrics


def _bigbasket_primary_period_bounds(month_name: str, year: int) -> tuple[date, date]:
    month_num = _MONTH_NAME_TO_NUM[month_name]
    next_month, next_year = _shift_month(month_num, year, 1)
    return date(year, month_num, 1), date(next_year, next_month, 1)


@api_view(["GET"])
@permission_classes([require("platform.po.view")])
@cached_get(timeout=60, prefix="plat.bigbasket_primary")
def bigbasket_primary_dashboard(request, slug: str):
    return _bigbasket_primary_dashboard_response(request, slug)


def _bigbasket_primary_dashboard_response(request, slug: str):
    _ensure_scope(request.user, slug)
    platform_format = _PRIMARY_DASHBOARD_FORMATS.get(slug)
    if not platform_format:
        raise ValidationError(
            "Primary Dashboard is available only for BigBasket, Blinkit, Zomato, CityMall, Flipkart Grocery, and Swiggy."
        )

    month_type, _month_col, date_col, month_name, year, defaulted_to_latest = (
        _parse_bigbasket_primary_period(request.query_params, platform_format)
    )
    done_value_col = _PRIMARY_DASHBOARD_DONE_VALUE_COLUMNS.get(
        (slug, month_type),
        "total_delivered_amt_exclusive",
    )
    date_expr = _primary_text_date_expr(date_col)
    period_start, period_end = _bigbasket_primary_period_bounds(month_name, year)
    selected_period = f"({date_expr}) >= %s AND ({date_expr}) < %s"
    pending_status = "UPPER(TRIM(\"po_status\"::text)) IN ('APPOINTMENT DONE', 'PENDING')"
    platform_format_key = re.sub(r"[^a-z0-9]+", "", platform_format.lower())
    platform_format_where = (
        "REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g') = %s"
    )
    period_where = (
        f"{platform_format_where} "
        f"AND ({selected_period})"
    )
    filtered_cte = f"""
        WITH filtered AS (
            SELECT
                *,
                TRUE AS in_selected_period
            FROM "master_po"
            WHERE {period_where}
        )
    """
    filtered_params = [
        platform_format_key,
        period_start,
        period_end,
    ]

    max_date = _scalar(
        f"""
        SELECT MAX({date_expr})
        FROM "master_po"
        WHERE {platform_format_where}
          AND ({date_expr}) >= %s
          AND ({date_expr}) < %s
        """,
        [platform_format_key, period_start, period_end],
    )

    summary_raw = _dict_rows(
        f"""
        {filtered_cte}
        SELECT
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("total_order_amt_inclusive"), 0) AS order_value,
            COALESCE(SUM("total_order_liters"), 0) AS order_ltrs,
            COALESCE(SUM("order_qty"), 0) AS order_qty,
            0 AS projection_ltrs,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                THEN "{done_value_col}" ELSE 0 END), 0) AS done_value,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                THEN "total_delivered_liters" ELSE 0 END), 0) AS done_ltrs,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                THEN "delivered_qty" ELSE 0 END), 0) AS done_qty,
            COALESCE(SUM(COALESCE("missed_qty", 0) * COALESCE("basic_rate", 0)), 0) AS pending_value,
            COALESCE(SUM(COALESCE("missed_ltrs", 0)), 0) AS pending_ltrs,
            COALESCE(SUM(COALESCE("missed_qty", 0)), 0) AS pending_qty,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'EXPIRED'
                THEN "total_order_amt_exclusive" ELSE 0 END), 0) AS expired_value,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'EXPIRED'
                THEN "total_order_liters" ELSE 0 END), 0) AS expired_ltrs,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'CANCELLED'
                THEN "total_order_amt_exclusive" ELSE 0 END), 0) AS cancelled_value,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'CANCELLED'
                THEN "total_order_liters" ELSE 0 END), 0) AS cancelled_ltrs
        FROM filtered
        GROUP BY 1
        """,
        filtered_params,
    )
    summary_by_head = {
        str(row.get("item_head") or "OTHER").upper(): _bigbasket_primary_normalize_row(row)
        for row in summary_raw
    }
    summary = [
        summary_by_head.get(item_head) or _bigbasket_primary_zero_row(item_head)
        for item_head in _BIGBASKET_PRIMARY_ITEM_HEADS
    ]

    item_raw = _dict_rows(
        f"""
        {filtered_cte},
        grouped AS (
            SELECT
                COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
                COALESCE(NULLIF(UPPER(TRIM("item"::text)), ''), 'UNMAPPED') AS item,
                COALESCE(SUM("total_order_amt_inclusive"), 0) AS order_value,
                COALESCE(SUM("total_order_liters"), 0) AS order_ltrs,
                COALESCE(SUM("order_qty"), 0) AS order_qty,
                0 AS projection_ltrs,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "{done_value_col}" ELSE 0 END), 0) AS done_value,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "total_delivered_liters" ELSE 0 END), 0) AS done_ltrs,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "delivered_qty" ELSE 0 END), 0) AS done_qty,
                COALESCE(SUM(COALESCE("missed_qty", 0) * COALESCE("basic_rate", 0)), 0) AS pending_value,
                COALESCE(SUM(COALESCE("missed_ltrs", 0)), 0) AS pending_ltrs,
                COALESCE(SUM(COALESCE("missed_qty", 0)), 0) AS pending_qty,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'EXPIRED'
                    THEN "total_order_amt_exclusive" ELSE 0 END), 0) AS expired_value,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'EXPIRED'
                    THEN "total_order_liters" ELSE 0 END), 0) AS expired_ltrs
            FROM filtered
            GROUP BY 1, 2
        )
        SELECT *
        FROM grouped
        ORDER BY
            CASE item_head
                WHEN 'PREMIUM' THEN 1
                WHEN 'COMMODITY' THEN 2
                WHEN 'OTHER' THEN 3
                ELSE 4
            END,
            item
        """,
        filtered_params,
    )
    items = [
        _bigbasket_primary_normalize_row(row, include_cancelled=False)
        for row in item_raw
    ]

    detail_raw = _dict_rows(
        f"""
        {filtered_cte},
        grouped AS (
            SELECT
                COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
                COALESCE(NULLIF(UPPER(TRIM("category"::text)), ''), 'OTHER') AS category,
                COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'OTHER') AS sub_category,
                COALESCE(NULLIF(UPPER(TRIM("per_liter"::text)), ''), '-') AS per_ltr,
                COALESCE(SUM("total_order_amt_inclusive"), 0) AS order_value,
                COALESCE(SUM("total_order_liters"), 0) AS order_ltrs,
                COALESCE(SUM("order_qty"), 0) AS order_qty,
                0 AS projection_ltrs,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "{done_value_col}" ELSE 0 END), 0) AS done_value,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "total_delivered_liters" ELSE 0 END), 0) AS done_ltrs,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "delivered_qty" ELSE 0 END), 0) AS done_qty,
                COALESCE(SUM(COALESCE("missed_qty", 0) * COALESCE("basic_rate", 0)), 0) AS pending_value,
                COALESCE(SUM(COALESCE("missed_ltrs", 0)), 0) AS pending_ltrs,
                COALESCE(SUM(COALESCE("missed_qty", 0)), 0) AS pending_qty,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'EXPIRED'
                    THEN "total_order_amt_exclusive" ELSE 0 END), 0) AS expired_value,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'EXPIRED'
                    THEN "total_order_liters" ELSE 0 END), 0) AS expired_ltrs
            FROM filtered
            GROUP BY 1, 2, 3, 4
        )
        SELECT *
        FROM grouped
        WHERE COALESCE(order_value, 0) <> 0
           OR COALESCE(done_value, 0) <> 0
           OR COALESCE(pending_value, 0) <> 0
           OR COALESCE(order_ltrs, 0) <> 0
           OR COALESCE(done_ltrs, 0) <> 0
           OR COALESCE(pending_ltrs, 0) <> 0
        ORDER BY done_value DESC, done_ltrs DESC, sub_category
        """,
        filtered_params,
    )
    details = [
        _bigbasket_primary_normalize_row(row, include_cancelled=False)
        for row in detail_raw
    ]

    open_vendor_pending = _dict_rows(
        f"""
        {filtered_cte},
        vendor_rows AS (
            SELECT
                COALESCE(
                    NULLIF(UPPER(TRIM("vendor_new"::text)), ''),
                    NULLIF(UPPER(TRIM("vendor_name"::text)), ''),
                    'UNMAPPED'
                ) AS vendor,
                COALESCE("total_order_amt_inclusive", 0) AS order_value_row,
                COALESCE(CASE WHEN UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "{done_value_col}" ELSE 0 END, 0) AS delivered_value_row,
                COALESCE("total_order_liters", 0) AS order_ltrs_row,
                COALESCE(CASE WHEN UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "total_delivered_liters" ELSE 0 END, 0) AS delivered_ltrs_row,
                COALESCE("order_qty", 0) AS order_qty_row,
                COALESCE(CASE WHEN UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "delivered_qty" ELSE 0 END, 0) AS delivered_qty_row,
                COALESCE("missed_qty", 0) * COALESCE("basic_rate", 0) AS pending_value_row,
                COALESCE("missed_ltrs", 0) AS pending_ltrs_row,
                COALESCE("missed_qty", 0) AS pending_qty_row,
                CASE
                    WHEN NULLIF(TRIM("lead_time"::text), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    THEN "lead_time"::numeric
                    ELSE NULL
                END AS lead_time_row
            FROM filtered
        ),
        vendor_agg AS (
            SELECT
                vendor,
                COALESCE(SUM(order_value_row), 0) AS order_value,
                COALESCE(SUM(delivered_value_row), 0) AS delivered_value,
                COALESCE(SUM(pending_value_row), 0) AS pending_value,
                COALESCE(SUM(order_ltrs_row), 0) AS order_ltrs,
                COALESCE(SUM(delivered_ltrs_row), 0) AS delivered_ltrs,
                COALESCE(SUM(pending_ltrs_row), 0) AS pending_ltrs,
                COALESCE(SUM(order_qty_row), 0) AS order_qty,
                COALESCE(SUM(delivered_qty_row), 0) AS delivered_qty,
                COALESCE(SUM(pending_qty_row), 0) AS pending_qty,
                AVG(lead_time_row) FILTER (WHERE lead_time_row IS NOT NULL) AS lead_time_avg
            FROM vendor_rows
            GROUP BY 1
        )
        SELECT *
        FROM vendor_agg
        WHERE order_value <> 0
           OR delivered_value <> 0
           OR pending_value <> 0
           OR order_ltrs <> 0
           OR delivered_ltrs <> 0
           OR pending_ltrs <> 0
           OR order_qty <> 0
           OR delivered_qty <> 0
           OR pending_qty <> 0
        ORDER BY pending_value DESC, order_value DESC, vendor
        """,
        filtered_params,
    )
    open_vendor_pending_value = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("pending_value")),
            _num(row.get("order_value")),
            _num(row.get("delivered_value")),
        ),
        reverse=True,
    )
    open_vendor_pending_ltrs = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("pending_ltrs")),
            _num(row.get("order_ltrs")),
            _num(row.get("delivered_ltrs")),
        ),
        reverse=True,
    )
    open_vendor_pending_qty = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("pending_qty")),
            _num(row.get("order_qty")),
            _num(row.get("delivered_qty")),
        ),
        reverse=True,
    )
    open_vendor_pending_order = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("order_value")),
            _num(row.get("delivered_value")),
            _num(row.get("pending_value")),
        ),
        reverse=True,
    )

    item_total = _bigbasket_primary_total(items, include_cancelled=False)
    detail_total = _bigbasket_primary_total(details, include_cancelled=False)
    kpi_total = _primary_master_po_order_minus_deliver_kpi_total(
        platform_format,
        month_type,
        _MONTH_NAME_TO_NUM.get(month_name),
        year,
    )
    summary_total = _bigbasket_primary_total(summary)
    summary_total.update({
        "projection_value": kpi_total.get("projection_value", 0),
        "projection_ltrs": kpi_total.get("projection_ltrs", 0),
        "projection_qty": kpi_total.get("projection_qty", 0),
    })
    return Response({
        "source": "master_po",
        "format": f"{slug.upper()}_PRIMARY",
        "source_format": platform_format,
        "kpi_source": "master_po_order_minus_deliver",
        "kpi_total": kpi_total,
        "defaulted_to_latest": defaulted_to_latest,
        "mode": month_type,
        "month_type": month_type,
        "month": _MONTH_NAME_TO_NUM.get(month_name),
        "month_name": month_name,
        "year": year,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else None,
        "summary": summary,
        "summary_total": summary_total,
        "items": items,
        "item_total": item_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": items,
        "open_vendor_pending": open_vendor_pending_value,
        "open_vendor_pending_value": open_vendor_pending_value,
        "open_vendor_pending_ltrs": open_vendor_pending_ltrs,
        "open_vendor_pending_qty": open_vendor_pending_qty,
        "open_vendor_pending_order": open_vendor_pending_order,
        "notes": [
            "DONE metrics use COMPLETED for every item head.",
            f"Done value uses {done_value_col}.",
            "Pending metrics use total order amount/litres for all open PENDING and APPOINTMENT DONE rows.",
            "Selected month type controls the date column used for done, expired, cancelled, and max date.",
        ],
    })


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
def inventory_match(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    sku = request.query_params.get("sku", "").strip()
    if not sku or not p.inventory_table:
        return Response({"match": None})
    inv = _safe_ident(p.inventory_table)
    match_col = _safe_col(p.match_column or "sku") or "sku"
    try:
        rows = _dict_rows(
            f'SELECT * FROM "{inv}" WHERE "{match_col}" = %s LIMIT 1',
            [sku],
        )
    except Exception:
        rows = []
    return Response({"match": rows[0] if rows else None})


_PRIMARY_METRIC_SQL = """
    COALESCE(SUM(COALESCE(metric_delivered_value, 0)), 0) AS done_value,
    COALESCE(SUM(COALESCE(metric_delivered_liters, 0)), 0) AS done_ltrs,
    COALESCE(SUM(COALESCE(metric_delivered_qty, 0)), 0) AS done_qty,
    -- Pending = the short-delivered ("missed") balance, not an open_close
    -- filter (the open_close column is empty/CLOSED for every row, which made
    -- Pending read 0). metric_pending_* = missed_ltrs / missed_qty /
    -- (missed_qty x basic_rate) and matches the source DB exactly.
    COALESCE(SUM(COALESCE(metric_pending_liters, 0)), 0) AS missed_ltrs,
    COALESCE(SUM(COALESCE(metric_pending_value, 0)), 0) AS pending_value,
    COALESCE(SUM(COALESCE(metric_pending_liters, 0)), 0) AS pending_ltrs,
    COALESCE(SUM(COALESCE(metric_pending_qty, 0)), 0) AS pending_qty,
    COALESCE(SUM(CASE WHEN status_key = 'EXPIRED'
        THEN COALESCE(metric_order_value, 0) ELSE 0 END), 0) AS expired_value,
    COALESCE(SUM(CASE WHEN status_key = 'EXPIRED'
        THEN COALESCE(metric_order_liters, 0) ELSE 0 END), 0) AS expired_ltrs,
    COALESCE(SUM(CASE WHEN status_key = 'CANCELLED'
        THEN COALESCE(metric_order_value, 0) ELSE 0 END), 0) AS cancelled_value,
    COALESCE(SUM(CASE WHEN status_key = 'CANCELLED'
        THEN COALESCE(metric_order_liters, 0) ELSE 0 END), 0) AS cancelled_ltrs,
    COALESCE(SUM(COALESCE(metric_order_value, 0)), 0) AS order_value,
    COALESCE(SUM(COALESCE(metric_order_liters, 0)), 0) AS order_ltrs,
    COALESCE(SUM(COALESCE(metric_order_qty, 0)), 0) AS order_qty,
    COALESCE(SUM(COALESCE(metric_projection_value, 0)), 0) AS projection_value,
    COALESCE(SUM(COALESCE(metric_projection_ltrs, 0)), 0) AS projection_ltrs,
    COALESCE(SUM(COALESCE(metric_projection_qty, 0)), 0) AS projection_qty
"""


_PRIMARY_TREND_METRIC_SQL = """
    COALESCE(SUM(COALESCE(metric_delivered_value, 0)), 0) AS done_value,
    COALESCE(SUM(COALESCE(metric_delivered_liters, 0)), 0) AS done_ltrs,
    COALESCE(SUM(COALESCE(metric_delivered_qty, 0)), 0) AS done_qty,
    COALESCE(SUM(COALESCE(metric_pending_value, 0)), 0) AS pending_value,
    COALESCE(SUM(COALESCE(metric_pending_liters, 0)), 0) AS pending_ltrs,
    COALESCE(SUM(COALESCE(metric_pending_qty, 0)), 0) AS pending_qty,
    COALESCE(SUM(COALESCE(metric_order_value, 0)), 0) AS order_value,
    COALESCE(SUM(COALESCE(metric_order_liters, 0)), 0) AS order_ltrs,
    COALESCE(SUM(COALESCE(metric_order_qty, 0)), 0) AS order_qty
"""


_AMAZON_PRIMARY_METRIC_SQL = """
    COALESCE(SUM(COALESCE(total_received_cost, 0)), 0) AS done_value,
    COALESCE(SUM(CASE WHEN item_head_key = 'OTHER'
        THEN 0 ELSE COALESCE(total_delivered_liters, 0) END), 0) AS done_ltrs,
    COALESCE(SUM(COALESCE(received_qty, 0)), 0) AS done_qty,
    COALESCE(SUM(CASE WHEN status_key = 'PENDING'
        THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) AS pending_value,
    COALESCE(SUM(CASE WHEN status_key = 'PENDING' AND item_head_key <> 'OTHER'
        THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) AS pending_ltrs,
    COALESCE(SUM(CASE WHEN status_key = 'PENDING'
        THEN COALESCE(order_unit_cl, requested_qty, 0) ELSE 0 END), 0) AS pending_qty,
    COALESCE(SUM(CASE WHEN status_key = 'EXPIRED'
        THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) AS expired_value,
    COALESCE(SUM(CASE WHEN status_key IN ('CANCELLED', 'MOV')
        THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) AS cancelled_value,
    COALESCE(SUM(CASE WHEN status_key = 'EXPIRED' AND item_head_key <> 'OTHER'
        THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) AS expired_ltrs,
    COALESCE(SUM(CASE WHEN status_key IN ('CANCELLED', 'MOV') AND item_head_key <> 'OTHER'
        THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) AS cancelled_ltrs,
    COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
        THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) AS order_value,
    COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV') AND item_head_key <> 'OTHER'
        THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) AS order_ltrs,
    COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
        THEN COALESCE(order_unit_cl, requested_qty, 0) ELSE 0 END), 0) AS order_qty,
    0 AS projection_value,
    0 AS projection_ltrs,
    0 AS projection_qty
"""


def _amazon_primary_po_cte() -> str:
    return """
WITH base AS (
    SELECT
        p.*,
        p.order_date::date AS period_dt
    FROM reporting."Amazon PO" p
),
normalized AS (
    SELECT
        *,
        COALESCE(
            NULLIF(UPPER(TRIM(po_status::text)), ''),
            NULLIF(UPPER(TRIM(status::text)), ''),
            'OTHER'
        ) AS status_key,
        CASE
            WHEN UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
                THEN UPPER(TRIM(item_head::text))
            WHEN UPPER(TRIM(item_head::text)) = 'OTHERS'
                THEN 'OTHER'
            ELSE 'UNMAPPED'
        END AS item_head_key,
        CASE
            WHEN UPPER(TRIM(core_fresh_now::text)) LIKE '%%FRESH%%' THEN 'FRESH'
            WHEN UPPER(TRIM(core_fresh_now::text)) LIKE '%%NOW%%' THEN 'NOW'
            WHEN UPPER(TRIM(core_fresh_now::text)) LIKE '%%CORE%%' THEN 'CORE'
            ELSE COALESCE(NULLIF(UPPER(TRIM(core_fresh_now::text)), ''), 'UNMAPPED')
        END AS channel_key,
        COALESCE(NULLIF(UPPER(TRIM(item::text)), ''), NULLIF(UPPER(TRIM(sku_name::text)), ''), 'OTHER') AS item_key,
        COALESCE(NULLIF(UPPER(TRIM(category::text)), ''), 'OTHER') AS category_key,
        COALESCE(NULLIF(UPPER(TRIM(sub_category::text)), ''), 'OTHER') AS sub_category_key,
        CASE
            WHEN po_month IS NOT NULL
                THEN UPPER(TRIM(TO_CHAR(MAKE_DATE(2000, po_month::integer, 1), 'FMMONTH')))
            WHEN period_dt IS NOT NULL
                THEN UPPER(TRIM(TO_CHAR(period_dt, 'FMMONTH')))
            ELSE NULL
        END AS po_month_key,
        COALESCE("year"::integer, EXTRACT(YEAR FROM period_dt)::integer) AS po_year,
        COALESCE(
            NULLIF(UPPER(TRIM(per_ltr_unit::text)), ''),
            CASE
                WHEN per_liter IS NULL THEN NULL
                WHEN per_liter < 1
                    THEN UPPER(TRIM(TO_CHAR(per_liter * 1000, 'FM999999990.###'))) || ' MLS'
                ELSE UPPER(TRIM(TO_CHAR(per_liter, 'FM999999990.###'))) || ' LTR'
            END,
            '-'
        ) AS per_ltr_key
    FROM base
)
"""


def _parse_amazon_primary_dashboard_params(params) -> tuple[str, int, int, bool]:
    mode = _norm_sec_key(params.get("mode") or params.get("month_type") or "DEL MONTH")
    if mode not in {"DEL MONTH", "PO MONTH"}:
        raise ValidationError("`mode` must be DEL MONTH or PO MONTH.")

    raw_month = str(params.get("month") or "").strip()
    raw_year = str(params.get("year") or "").strip()
    defaulted_to_latest = False

    iso_month = re.fullmatch(r"(\d{4})-(\d{2})", raw_month)
    if iso_month and not raw_year:
        raw_year = iso_month.group(1)
        raw_month = iso_month.group(2)

    if not raw_month or not raw_year:
        latest = _dict_rows(
            """
            SELECT po_month, "year", MAX(order_date::date) AS max_date
            FROM reporting."Amazon PO"
            WHERE po_month IS NOT NULL
              AND "year" IS NOT NULL
            GROUP BY po_month, "year"
            ORDER BY max_date DESC NULLS LAST, "year" DESC, po_month DESC
            LIMIT 1
            """,
            [],
        )
        if latest:
            raw_month = str(latest[0].get("po_month") or "")
            raw_year = str(latest[0].get("year") or "")
            defaulted_to_latest = True

    raw_month = raw_month or str(date.today().month)
    raw_year = raw_year or str(date.today().year)

    if raw_month.isdigit():
        month = int(raw_month)
        if not 1 <= month <= 12:
            raise ValidationError("`month` must be 1-12 or a month name.")
    else:
        month_name = _norm_sec_key(raw_month)
        if month_name not in _MONTH_NAME_TO_NUM:
            raise ValidationError("`month` must be 1-12 or a month name.")
        month = _MONTH_NAME_TO_NUM[month_name]

    try:
        year = int(raw_year)
    except ValueError:
        raise ValidationError("`year` must be numeric.")
    if year < 2000 or year > 2100:
        raise ValidationError("`year` looks out of range.")

    return mode, month, year, defaulted_to_latest


def _amazon_primary_dashboard_response(request):
    mode, month, year, defaulted_to_latest = _parse_amazon_primary_dashboard_params(
        request.query_params
    )
    channel = _norm_sec_key(
        request.query_params.get("channel") or request.query_params.get("core_fresh_now") or "ALL"
    )
    if channel not in {"ALL", "CORE", "FRESH", "NOW"}:
        raise ValidationError("`channel` must be All, Core, Fresh, or Now.")
    month_name = _month_name(month)
    period_filter = "po_month_key = %s AND po_year = %s"
    period_params = [month_name, year]
    channel_filter = "" if channel == "ALL" else " AND channel_key = %s"
    channel_params = [] if channel == "ALL" else [channel]

    # Optional `item_head` filter — only applied to the trend queries below so
    # KPIs / SKUs / sub-categories / pie chart keep the full distribution.
    item_head_raw = (request.query_params.get("item_head") or "").strip().upper()
    if item_head_raw and item_head_raw not in {"PREMIUM", "COMMODITY", "OTHER"}:
        raise ValidationError("`item_head` must be PREMIUM, COMMODITY, or OTHER.")
    trend_head_filter = " AND item_head_key = %s" if item_head_raw else ""
    trend_head_params = [item_head_raw] if item_head_raw else []

    cache_key = (
        f"prim_dash:amazon:{mode}:{channel}:{month}:{year}:"
        f"{item_head_raw or ''}:{int(defaulted_to_latest)}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return Response(cached)

    # Materialize the heavy CTE once for this request. All downstream queries
    # below substitute `amazon_cte = _PRIMARY_CTE_STUB` and read from the
    # `normalized` TEMP TABLE rather than re-running the regex/normalization
    # pipeline 6+ times.
    _materialize_primary_normalized(_amazon_primary_po_cte())
    amazon_cte = _PRIMARY_CTE_STUB

    def with_channel(params: list) -> list:
        return [*params, *channel_params]

    try:
        return _amazon_primary_dashboard_payload(
            request,
            mode=mode,
            month=month,
            month_name=month_name,
            year=year,
            channel=channel,
            channel_filter=channel_filter,
            channel_params=channel_params,
            period_filter=period_filter,
            period_params=period_params,
            trend_head_filter=trend_head_filter,
            trend_head_params=trend_head_params,
            with_channel=with_channel,
            amazon_cte=amazon_cte,
            defaulted_to_latest=defaulted_to_latest,
            cache_key=cache_key,
        )
    finally:
        _drop_primary_normalized()


def _amazon_primary_dashboard_payload(
    request,
    *,
    mode,
    month,
    month_name,
    year,
    channel,
    channel_filter,
    channel_params,
    period_filter,
    period_params,
    trend_head_filter,
    trend_head_params,
    with_channel,
    amazon_cte,
    defaulted_to_latest,
    cache_key,
):
    max_date = _scalar(
        f"""
        {amazon_cte}
        SELECT MAX(period_dt)
        FROM normalized
        WHERE {period_filter}{channel_filter}
        """,
        with_channel(period_params),
    )

    summary_raw = _dict_rows(
        f"""
        {amazon_cte}
        SELECT
            item_head_key AS item_head,
            {_AMAZON_PRIMARY_METRIC_SQL}
        FROM normalized
        WHERE {period_filter}{channel_filter}
          AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY item_head_key
        """,
        with_channel(period_params),
    )
    summary_by_head = {_norm_sec_key(row.get("item_head")): row for row in summary_raw}
    summary = []
    for item_head in _BIGBASKET_PRIMARY_ITEM_HEADS:
        metrics = _primary_metrics(summary_by_head.get(item_head))
        summary.append({"item_head": item_head, **metrics})

    detail_raw = _dict_rows(
        f"""
        {amazon_cte}
        SELECT
            sub_category_key,
            per_ltr_key,
            MIN(item_head_key) AS item_head_key,
            MIN(category_key) AS category_key,
            {_AMAZON_PRIMARY_METRIC_SQL}
        FROM normalized
        WHERE {period_filter}{channel_filter}
          AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY sub_category_key, per_ltr_key
        """,
        with_channel(period_params),
    )
    details = []
    for row in detail_raw:
        metrics = _primary_metrics(row)
        if not any(_num(metrics.get(key)) for key in metrics):
            continue
        details.append({
            "format": "AMAZON",
            "item_head": row.get("item_head_key") or "OTHER",
            "category": row.get("category_key") or row.get("sub_category_key") or "OTHER",
            "sub_category": row.get("sub_category_key") or "OTHER",
            "per_ltr": row.get("per_ltr_key") or "-",
            "value_per_ltr": None if metrics["done_ltrs"] == 0 else metrics["done_value"] / metrics["done_ltrs"],
            **metrics,
        })

    top_item_raw = _dict_rows(
        f"""
        {amazon_cte},
        item_agg AS (
            SELECT
                item_key AS item,
                item_head_key AS item_head,
                {_AMAZON_PRIMARY_METRIC_SQL}
            FROM normalized
            WHERE {period_filter}{channel_filter}
              AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
            GROUP BY item_key, item_head_key
        )
        SELECT *
        FROM item_agg
        WHERE COALESCE(done_value, 0) <> 0
           OR COALESCE(done_ltrs, 0) <> 0
           OR COALESCE(done_qty, 0) <> 0
        ORDER BY done_value DESC, done_ltrs DESC, done_qty DESC
        """,
        with_channel(period_params),
    )
    top_items = [
        {
            "item": row.get("item") or "OTHER",
            "item_head": row.get("item_head") or "OTHER",
            **_primary_metrics(row),
        }
        for row in top_item_raw
    ]

    sku_detail_raw = _dict_rows(
        f"""
        {amazon_cte},
        sku_agg AS (
            SELECT
                COALESCE(NULLIF(UPPER(TRIM(sku_code::text)), ''), '-') AS asin,
                item_key AS item,
                item_head_key AS item_head,
                category_key AS category,
                sub_category_key AS sub_category,
                per_ltr_key AS per_ltr,
                COALESCE(NULLIF(UPPER(TRIM(brand::text)), ''), '-') AS brand,
                {_AMAZON_PRIMARY_METRIC_SQL}
            FROM normalized
            WHERE {period_filter}{channel_filter}
              AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
            GROUP BY
                COALESCE(NULLIF(UPPER(TRIM(sku_code::text)), ''), '-'),
                item_key,
                item_head_key,
                category_key,
                sub_category_key,
                per_ltr_key,
                COALESCE(NULLIF(UPPER(TRIM(brand::text)), ''), '-')
        )
        SELECT *
        FROM sku_agg
        WHERE COALESCE(order_value, 0) <> 0
           OR COALESCE(done_value, 0) <> 0
           OR COALESCE(pending_value, 0) <> 0
           OR COALESCE(cancelled_value, 0) <> 0
        ORDER BY
            CASE item_head
                WHEN 'PREMIUM' THEN 1
                WHEN 'COMMODITY' THEN 2
                WHEN 'OTHER' THEN 3
                ELSE 4
            END,
            category,
            sub_category,
            item,
            asin
        """,
        with_channel(period_params),
    )
    sku_details = [
        {
            "asin": row.get("asin") or "-",
            "item": row.get("item") or "OTHER",
            "item_head": row.get("item_head") or "OTHER",
            "category": row.get("category") or "OTHER",
            "sub_category": row.get("sub_category") or "OTHER",
            "per_ltr": row.get("per_ltr") or "-",
            "brand": row.get("brand") or "-",
            **_primary_metrics(row),
        }
        for row in sku_detail_raw
    ]

    vendor_rows = _dict_rows(
        f"""
        {amazon_cte}
        SELECT
            COALESCE(NULLIF(UPPER(TRIM(vendor::text)), ''), 'UNMAPPED') AS vendor,
            COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
                THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) AS order_value,
            COALESCE(SUM(COALESCE(total_received_cost, 0)), 0) AS delivered_value,
            COALESCE(SUM(CASE WHEN status_key = 'PENDING'
                THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) AS pending_value,
            COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
                THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) AS order_ltrs,
            COALESCE(SUM(COALESCE(total_delivered_liters, 0)), 0) AS delivered_ltrs,
            COALESCE(SUM(CASE WHEN status_key = 'PENDING'
                THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) AS pending_ltrs,
            COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
                THEN COALESCE(order_unit_cl, requested_qty, 0) ELSE 0 END), 0) AS order_qty,
            COALESCE(SUM(COALESCE(received_qty, 0)), 0) AS delivered_qty,
            COALESCE(SUM(CASE WHEN status_key = 'PENDING'
                THEN COALESCE(order_unit_cl, requested_qty, 0) ELSE 0 END), 0) AS pending_qty
        FROM normalized
        WHERE {period_filter}{channel_filter}
          AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY 1
        HAVING
            COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
                THEN COALESCE(total_requested_cost, 0) ELSE 0 END), 0) <> 0
            OR COALESCE(SUM(COALESCE(total_received_cost, 0)), 0) <> 0
            OR COALESCE(SUM(CASE WHEN status_key NOT IN ('CANCELLED', 'CANCELED', 'CANCEL', 'MOV')
                THEN COALESCE(order_ltrs_cl, total_order_liters, 0) ELSE 0 END), 0) <> 0
            OR COALESCE(SUM(COALESCE(received_qty, 0)), 0) <> 0
        ORDER BY pending_value DESC, order_value DESC, vendor
        """,
        with_channel(period_params),
    )
    open_vendor_pending_value = sorted(
        vendor_rows,
        key=lambda row: (
            _num(row.get("pending_value")),
            _num(row.get("order_value")),
            _num(row.get("delivered_value")),
        ),
        reverse=True,
    )
    open_vendor_pending_ltrs = sorted(
        vendor_rows,
        key=lambda row: (
            _num(row.get("pending_ltrs")),
            _num(row.get("order_ltrs")),
            _num(row.get("delivered_ltrs")),
        ),
        reverse=True,
    )
    open_vendor_pending_qty = sorted(
        vendor_rows,
        key=lambda row: (
            _num(row.get("pending_qty")),
            _num(row.get("order_qty")),
            _num(row.get("delivered_qty")),
        ),
        reverse=True,
    )
    open_vendor_pending_order = sorted(
        vendor_rows,
        key=lambda row: (
            _num(row.get("order_value")),
            _num(row.get("order_ltrs")),
            _num(row.get("order_qty")),
        ),
        reverse=True,
    )

    period_start = date(year, month, 1)
    period_end = date(year, month, monthrange(year, month)[1])
    daily_trend = _primary_trend_rows(_dict_rows(
        f"""
        {amazon_cte},
        trend_days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS period
        ),
        agg AS (
            SELECT
                period_dt::date AS period,
                {_AMAZON_PRIMARY_METRIC_SQL}
            FROM normalized
            WHERE {period_filter}{channel_filter}
              AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
              AND period_dt IS NOT NULL
              {trend_head_filter}
            GROUP BY period_dt::date
        )
        SELECT
            d.period,
            TO_CHAR(d.period, 'DD Mon') AS label,
            COALESCE(a.done_value, 0) AS done_value,
            COALESCE(a.done_ltrs, 0) AS done_ltrs,
            COALESCE(a.done_qty, 0) AS done_qty,
            COALESCE(a.pending_value, 0) AS pending_value,
            COALESCE(a.pending_ltrs, 0) AS pending_ltrs,
            COALESCE(a.pending_qty, 0) AS pending_qty,
            COALESCE(a.order_value, 0) AS order_value,
            COALESCE(a.order_ltrs, 0) AS order_ltrs,
            COALESCE(a.order_qty, 0) AS order_qty
        FROM trend_days d
        LEFT JOIN agg a ON a.period = d.period
        ORDER BY d.period
        """,
        [period_start, period_end] + with_channel(period_params) + trend_head_params,
    ))
    # monthly_trend / yearly_trend were removed: the frontend only consumes
    # `trends.day` (PlatformDashboard sparklines). Keys are preserved so any
    # in-flight clients still parse the response shape.
    monthly_trend = []
    yearly_trend = []

    detail_total = _primary_total(details)
    summary_total = _primary_total(summary)
    elapsed_day = _sec_elapsed_day(max_date)
    days_in_month = monthrange(year, month)[1]
    summary_total["projection_value"] = _safe_div(summary_total["done_value"], elapsed_day) * days_in_month
    summary_total["projection_ltrs"] = _safe_div(summary_total["done_ltrs"], elapsed_day) * days_in_month
    summary_total["projection_qty"] = _safe_div(summary_total["done_qty"], elapsed_day) * days_in_month

    payload = {
        "source": 'reporting."Amazon PO"',
        "format": "AMAZON",
        "dashboard_title": "AMAZON Primary Dashboard",
        "mode": mode,
        "month": month,
        "month_name": month_name,
        "year": year,
        "channel": channel,
        "defaulted_to_latest": defaulted_to_latest,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "sku_details": sku_details,
        "open_vendor_pending": open_vendor_pending_value,
        "open_vendor_pending_value": open_vendor_pending_value,
        "open_vendor_pending_ltrs": open_vendor_pending_ltrs,
        "open_vendor_pending_qty": open_vendor_pending_qty,
        "open_vendor_pending_order": open_vendor_pending_order,
        "trends": {
            "day": daily_trend,
            "month": monthly_trend,
            "year": yearly_trend,
        },
        "detail_rows_fixed": False,
        "extra_detail_rows_included": True,
    }
    cache.set(cache_key, payload, _PRIMARY_DASHBOARD_CACHE_TTL)
    return Response(payload)


_PENDENCY_DASHBOARD_FORMATS = {
    "zepto": "ZEPTO",
    "swiggy": "SWIGGY",
    "blinkit": "BLINKIT",
    # master_po stores this as "BIG BASKET" (with a space); the pendency query
    # matches `format` exactly, so the value here must match the data verbatim.
    "bigbasket": "BIG BASKET",
    "flipkart_grocery": "FLIPKART GROCERY",
    "citymall": "CITY MALL",
    "zomato": "ZOMATO",
}


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
def pendency_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    fmt = _PENDENCY_DASHBOARD_FORMATS.get(slug)
    if not fmt:
        raise ValidationError(
            f"Pendency dashboard is not yet enabled for platform '{slug}'."
        )

    raw_year = (request.query_params.get("year") or "").strip()
    raw_po_month = (request.query_params.get("po_month") or "").strip()
    raw_from = (request.query_params.get("from_date") or "").strip()
    raw_to = (request.query_params.get("to_date") or "").strip()

    # No response cache here: the pendency dashboard must reflect uploads
    # immediately. It reads the master_po_mv materialized view (refreshed on
    # every upload), so the query is already fast. A cached payload could be
    # served stale by a worker that didn't handle the upload (the cache is a
    # per-process LocMemCache, so cache.clear() on upload only clears one
    # worker) — which is exactly what made pendency look "not refreshed".

    where_parts = ['UPPER(TRIM("format"::text)) = %s']
    params: list = [fmt]
    resolved_month: str | None = None
    resolved_year: int | None = None
    defaulted_to_latest = False

    has_date_range = bool(
        re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_from)
        and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_to)
    )

    if not raw_year and not raw_po_month and not has_date_range:
        latest = _dict_rows(
            '''
            SELECT
                UPPER(TRIM("po_month"::text)) AS po_month,
                "po_year" AS year,
                MAX(
                    CASE
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                            THEN TO_DATE(TRIM("po_date"::text), 'DD-MM-YYYY')
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                            THEN TRIM("po_date"::text)::date
                    END
                ) AS max_date
            FROM "master_po"
            WHERE UPPER(TRIM("format"::text)) = %s
              AND UPPER(TRIM("open_close"::text)) = 'OPEN'
              AND UPPER(TRIM(COALESCE("po_status", "status", '')::text))
                  NOT IN ('CANCELLED', 'CANCELED', 'CANCEL')
              AND "po_month" IS NOT NULL
              AND "po_year" IS NOT NULL
            GROUP BY 1, 2
            ORDER BY max_date DESC NULLS LAST
            LIMIT 1
            ''',
            [fmt],
        )
        if latest and latest[0].get("po_month") and latest[0].get("year") is not None:
            resolved_month = latest[0]["po_month"]
            resolved_year = int(latest[0]["year"])
            defaulted_to_latest = True

    if raw_po_month:
        resolved_month = raw_po_month.upper()
    if raw_year and raw_year.isdigit():
        resolved_year = int(raw_year)

    # A user-selected date range drives the filter on its own (across any
    # month); the month/year filter only applies when no range is given.
    if not has_date_range:
        if resolved_month:
            where_parts.append('UPPER(TRIM("po_month"::text)) = %s')
            params.append(resolved_month)
        if resolved_year is not None:
            where_parts.append('"po_year" = %s')
            params.append(resolved_year)

    base_where = "WHERE " + " AND ".join(where_parts)
    # Match Primary Dashboard semantics: only OPEN POs, pending = max(order - delivered, 0).
    # Cancelled POs are never pending/open (even with no GRN yet), so drop them —
    # po_status is the normalized status; fall back to the raw status column.
    pending_filter = (
        " AND UPPER(TRIM(\"open_close\"::text)) = 'OPEN'"
        " AND UPPER(TRIM(COALESCE(\"po_status\", \"status\", '')::text))"
        " NOT IN ('CANCELLED', 'CANCELED', 'CANCEL')"
    )
    # User-selected PO-date range (both YYYY-MM-DD) filters POs whose po_date
    # falls between the two dates.
    date_range_filter = ""
    if has_date_range:
        date_range_filter = (
            " AND (CASE "
            "WHEN TRIM(\"po_date\"::text) ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' "
            "THEN TO_DATE(TRIM(\"po_date\"::text), 'DD-MM-YYYY') "
            "WHEN TRIM(\"po_date\"::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "THEN TRIM(\"po_date\"::text)::date END) BETWEEN %s AND %s"
        )
        params.extend([raw_from, raw_to])
    full_where = base_where + pending_filter + date_range_filter

    pending_units_expr = (
        'COALESCE(SUM(GREATEST('
        'COALESCE("order_qty", 0) - COALESCE("delivered_qty", 0), 0'
        ')), 0)'
    )
    pending_ltrs_expr = (
        'COALESCE(SUM(GREATEST('
        'COALESCE("total_order_liters", 0) - COALESCE("total_delivered_liters", 0), 0'
        ')), 0)'
    )

    totals_row = _dict_rows(
        f'''
        SELECT
            {pending_units_expr} AS pending_units,
            {pending_ltrs_expr} AS pending_ltrs,
            COALESCE(SUM("order_qty"), 0) AS open_units,
            COALESCE(SUM("total_order_liters"), 0) AS open_ltrs,
            COUNT(DISTINCT "po_number") AS open_pos,
            COUNT(*) AS rows,
            TO_CHAR(
                MAX(
                    CASE
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
                            THEN TO_DATE(TRIM("po_date"::text), 'DD-MM-YYYY')
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                            THEN TRIM("po_date"::text)::date
                    END
                ),
                'DD-MM-YYYY'
            ) AS max_po_date,
            TO_CHAR(
                MIN(
                    CASE
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
                            THEN TO_DATE(TRIM("po_date"::text), 'DD-MM-YYYY')
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                            THEN TRIM("po_date"::text)::date
                    END
                ),
                'DD-MM-YYYY'
            ) AS min_po_date
        FROM "master_po"
        {full_where}
        ''',
        params,
    )
    totals = totals_row[0] if totals_row else {
        "pending_units": 0,
        "pending_ltrs": 0,
        "open_units": 0,
        "open_ltrs": 0,
        "open_pos": 0,
        "rows": 0,
        "max_po_date": None,
        "min_po_date": None,
    }

    metric_cols = f'''
        {pending_units_expr} AS pending_units,
        {pending_ltrs_expr} AS pending_ltrs,
        COALESCE(SUM("order_qty"), 0) AS open_units,
        COALESCE(SUM("total_order_liters"), 0) AS open_ltrs,
        COALESCE(SUM("total_order_amt_exclusive"), 0) AS order_value,
        COUNT(DISTINCT "po_number") AS open_pos
    '''
    order_clause = "ORDER BY pending_ltrs DESC, pending_units DESC"

    by_city = _dict_rows(
        f'''
        SELECT
            COALESCE(NULLIF(TRIM("city"::text), ''), 'UNMAPPED') AS city,
            {metric_cols}
        FROM "master_po"
        {full_where}
        GROUP BY 1
        {order_clause}
        ''',
        params,
    )

    by_sku = _dict_rows(
        f'''
        SELECT
            COALESCE(NULLIF(TRIM("sku_code"::text), ''), 'UNMAPPED') AS sku_code,
            COALESCE(NULLIF(TRIM("sku_name"::text), ''), '-') AS sku_name,
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS item,
            {metric_cols}
        FROM "master_po"
        {full_where}
        GROUP BY 1, 2, 3
        {order_clause}
        ''',
        params,
    )

    by_warehouse = _dict_rows(
        f'''
        SELECT
            COALESCE(NULLIF(TRIM("location"::text), ''), 'UNMAPPED') AS warehouse,
            {metric_cols}
        FROM "master_po"
        {full_where}
        GROUP BY 1
        {order_clause}
        ''',
        params,
    )

    by_distributor = _dict_rows(
        f'''
        SELECT
            COALESCE(NULLIF(TRIM("vendor_new"::text), ''), 'UNMAPPED') AS distributor,
            {metric_cols}
        FROM "master_po"
        {full_where}
        GROUP BY 1
        {order_clause}
        ''',
        params,
    )

    by_po = _dict_rows(
        f'''
        SELECT
            COALESCE(NULLIF(TRIM("po_number"::text), ''), 'UNMAPPED') AS po_number,
            MAX(NULLIF(TRIM("vendor_new"::text), '')) AS distributor,
            MAX(NULLIF(TRIM("location"::text), '')) AS location,
            TO_CHAR(
                MAX(
                    CASE
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
                            THEN TO_DATE(TRIM("po_date"::text), 'DD-MM-YYYY')
                        WHEN TRIM("po_date"::text) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                            THEN TRIM("po_date"::text)::date
                    END
                ),
                'DD-MM-YYYY'
            ) AS po_date,
            TO_CHAR(
                MAX(
                    CASE
                        WHEN TRIM("po_expiry_date"::text) ~ '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
                            THEN TO_DATE(TRIM("po_expiry_date"::text), 'DD-MM-YYYY')
                        WHEN TRIM("po_expiry_date"::text) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                            THEN TRIM("po_expiry_date"::text)::date
                    END
                ),
                'DD-MM-YYYY'
            ) AS po_expiry_date,
            {metric_cols}
        FROM "master_po"
        {full_where}
        GROUP BY 1
        {order_clause}
        ''',
        params,
    )

    _payload = {
        "platform": slug,
        "format": fmt,
        "po_month": resolved_month,
        "year": resolved_year,
        "defaulted_to_latest": defaulted_to_latest,
        "totals": {
            "pending_units": _num(totals.get("pending_units")),
            "pending_ltrs": _num(totals.get("pending_ltrs")),
            "open_units": _num(totals.get("open_units")),
            "open_ltrs": _num(totals.get("open_ltrs")),
            "open_pos": int(totals.get("open_pos") or 0),
            "rows": int(totals.get("rows") or 0),
        },
        "max_po_date": totals.get("max_po_date"),
        "min_po_date": totals.get("min_po_date"),
        "by_city": by_city,
        "by_sku": by_sku,
        "by_warehouse": by_warehouse,
        "by_distributor": by_distributor,
        "by_po": by_po,
    }
    return Response(_payload)


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.primary")
def primary_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "amazon":
        return _amazon_primary_dashboard_response(request)
    if slug == "bigbasket":
        return _bigbasket_primary_dashboard_response(request, slug)

    platform_format = _PRIMARY_DASHBOARD_FORMATS.get(slug)
    if not platform_format:
        raise ValidationError(
            "Primary Dashboard is available only for primary sales platforms."
        )

    mode, month, year, defaulted_to_latest = _parse_primary_dashboard_params(
        request.query_params,
        platform_format,
    )
    month_name = _month_name(month)
    period_end_cap = min(
        date(year, month, monthrange(year, month)[1]),
        date.today(),
    )
    period_date_col = "po_dt" if mode == "PO MONTH" else "delivery_dt"
    period_filter = f"{_primary_period_filter(mode)} AND {period_date_col} <= %s"
    period_params = [month_name, year, period_end_cap]
    vendor_metric_filter = f"{_primary_vendor_metric_filter(mode)} AND {period_date_col} <= %s"
    vendor_pending_filter = f"{_primary_vendor_pending_filter(mode)} AND {period_date_col} <= %s"
    vendor_period_params = [
        month_name,
        year,
        period_end_cap,
        month_name,
        year,
        period_end_cap,
    ]

    # Optional `item_head` filter — only applied to the trend queries, since
    # the front-end already filters KPIs / SKUs / sub-categories client-side
    # and the pie chart needs all three heads to render the split.
    item_head_raw = (request.query_params.get("item_head") or "").strip().upper()
    if item_head_raw and item_head_raw not in {"PREMIUM", "COMMODITY", "OTHER"}:
        raise ValidationError("`item_head` must be PREMIUM, COMMODITY, or OTHER.")
    trend_head_filter = " AND item_head_key = %s" if item_head_raw else ""
    trend_head_params = [item_head_raw] if item_head_raw else []

    cache_key = (
        f"prim_dash:v{_PRIMARY_DASHBOARD_CACHE_VERSION}:"
        f"{slug}:{platform_format}:{mode}:{month}:{year}:"
        f"{period_end_cap.isoformat()}:{item_head_raw or ''}:{int(defaulted_to_latest)}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return Response(cached)

    # Materialize the heavy CTE once for this request — all downstream queries
    # below substitute `primary_cte = _PRIMARY_CTE_STUB` and read from the
    # `normalized` TEMP TABLE rather than re-running the regex pack parsing,
    # text-date parsing and COALESCE normalization 6+ times.
    _materialize_primary_normalized(_primary_master_po_cte(platform_format))
    primary_cte = _PRIMARY_CTE_STUB

    try:
        return _primary_dashboard_payload(
            slug=slug,
            platform_format=platform_format,
            primary_cte=primary_cte,
            mode=mode,
            month=month,
            month_name=month_name,
            year=year,
            period_filter=period_filter,
            period_params=period_params,
            vendor_metric_filter=vendor_metric_filter,
            vendor_pending_filter=vendor_pending_filter,
            vendor_period_params=vendor_period_params,
            trend_head_filter=trend_head_filter,
            trend_head_params=trend_head_params,
            period_end_cap=period_end_cap,
            defaulted_to_latest=defaulted_to_latest,
            cache_key=cache_key,
        )
    finally:
        _drop_primary_normalized()


def _primary_dashboard_payload(
    *,
    slug,
    platform_format,
    primary_cte,
    mode,
    month,
    month_name,
    year,
    period_filter,
    period_params,
    vendor_metric_filter,
    vendor_pending_filter,
    vendor_period_params,
    trend_head_filter,
    trend_head_params,
    period_end_cap,
    defaulted_to_latest,
    cache_key,
):
    max_date_col = "po_dt" if mode == "PO MONTH" else "delivery_dt"
    max_date_month_key = "po_month_key" if mode == "PO MONTH" else "delivery_month_key"
    max_date_year_key = "po_year" if mode == "PO MONTH" else "delivery_year"
    max_date = _scalar(
        f"""
        {primary_cte}
        SELECT MAX({max_date_col})
        FROM normalized
        WHERE {max_date_month_key} = %s
          AND {max_date_year_key} = %s
          AND {max_date_col} <= %s
        """,
        [month_name, year, period_end_cap],
    )

    summary_raw = _dict_rows(
        f"""
        {primary_cte}
        SELECT
            item_head_key AS item_head,
            {_PRIMARY_METRIC_SQL}
        FROM normalized
        WHERE {period_filter}
          AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY item_head_key
        """,
        period_params,
    )
    summary_by_head = {_norm_sec_key(row.get("item_head")): row for row in summary_raw}
    summary = []
    for item_head in _ZEPTO_PRIMARY_ITEM_HEADS:
        metrics = _primary_metrics(summary_by_head.get(item_head))
        summary.append({"item_head": item_head, **metrics})

    fill_rate_date_col = "po_dt" if mode == "PO MONTH" else "delivery_dt"
    fill_rate_month_key = "po_month_key" if mode == "PO MONTH" else "delivery_month_key"
    fill_rate_year_key = "po_year" if mode == "PO MONTH" else "delivery_year"
    fill_rate_max_date = _scalar(
        f"""
        {primary_cte}
        SELECT MAX({fill_rate_date_col})
        FROM normalized
        WHERE {fill_rate_month_key} = %s
          AND {fill_rate_year_key} = %s
          AND {fill_rate_date_col} <= %s
        """,
        [month_name, year, period_end_cap],
    )
    fill_rate_cutoff = None
    if hasattr(fill_rate_max_date, "isoformat"):
        fill_rate_cutoff = fill_rate_max_date - timedelta(days=7)
    fill_rate_start = (
        date(year - 1, 12, 1)
        if month == 1
        else date(year, month - 1, 1)
    )
    fill_rate_raw = []
    if fill_rate_cutoff and fill_rate_cutoff >= fill_rate_start:
        fill_rate_raw = _dict_rows(
            f"""
            {primary_cte}
            SELECT
                item_head_key AS item_head,
                {_PRIMARY_METRIC_SQL}
            FROM normalized
            WHERE {fill_rate_date_col} BETWEEN %s AND %s
              AND item_head_key IN ('PREMIUM', 'COMMODITY', 'OTHER')
            GROUP BY item_head_key
            """,
            [fill_rate_start, fill_rate_cutoff],
        )
    fill_rate_by_head = {_norm_sec_key(row.get("item_head")): row for row in fill_rate_raw}
    fill_rate_summary = []
    for item_head in _ZEPTO_PRIMARY_ITEM_HEADS:
        metrics = _primary_metrics(fill_rate_by_head.get(item_head))
        fill_rate_summary.append({"item_head": item_head, **metrics})
    fill_rate_total = _primary_total(fill_rate_summary)
    lead_time_days = 0.0
    if fill_rate_cutoff and fill_rate_cutoff >= fill_rate_start:
        lead_time_days = _num(_scalar(
            f"""
            {primary_cte}
            SELECT AVG(
                CASE
                    WHEN NULLIF(TRIM(lead_time::text), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    THEN lead_time::numeric
                    ELSE NULL
                END
            )
            FROM normalized
            WHERE {fill_rate_date_col} BETWEEN %s AND %s
            """,
            [fill_rate_start, fill_rate_cutoff],
        ))

    detail_raw = _dict_rows(
        f"""
        {primary_cte}
        SELECT
            sub_category_key,
            per_ltr_key,
            item_head_key,
            category_key,
            {_PRIMARY_METRIC_SQL}
        FROM normalized
        WHERE {period_filter}
        GROUP BY item_head_key, category_key, sub_category_key, per_ltr_key
        """,
        period_params,
    )
    detail_by_key = {
        (
            _norm_sec_key(row.get("item_head_key")),
            _norm_sec_key(row.get("sub_category_key")),
            _norm_sec_key(row.get("per_ltr_key")),
        ): row
        for row in detail_raw
    }

    details = []
    fixed_detail_keys = set()
    if slug == "zepto":
        for fmt, item_head, category, sub_category, per_ltr in _ZEPTO_PRIMARY_DETAIL_ROWS:
            detail_key = (
                _norm_sec_key(item_head),
                _norm_sec_key(sub_category),
                _norm_sec_key(per_ltr),
            )
            fixed_detail_keys.add(detail_key)
            metrics = _primary_metrics(
                detail_by_key.get(detail_key)
            )
            details.append({
                "format": fmt,
                "item_head": item_head,
                "category": category,
                "sub_category": sub_category,
                "per_ltr": per_ltr,
                "value_per_ltr": None if metrics["done_ltrs"] == 0 else metrics["done_value"] / metrics["done_ltrs"],
                **metrics,
            })

    # The workbook has a fixed display list, but the database can receive new
    # pack sizes before the sheet template is updated. Include those live rows
    # so sub-category dashboard totals do not silently miss valid sales.
    for detail_key, row in detail_by_key.items():
        if detail_key in fixed_detail_keys:
            continue
        metrics = _primary_metrics(row)
        if not any(_num(metrics.get(key)) for key in metrics):
            continue
        details.append({
            "format": platform_format,
            "item_head": row.get("item_head_key") or "OTHER",
            "category": row.get("category_key") or row.get("sub_category_key") or "OTHER",
            "sub_category": row.get("sub_category_key") or "OTHER",
            "per_ltr": row.get("per_ltr_key") or "-",
            "value_per_ltr": None if metrics["done_ltrs"] == 0 else metrics["done_value"] / metrics["done_ltrs"],
            **metrics,
        })

    top_item_raw = _dict_rows(
        f"""
        {primary_cte},
        item_agg AS (
            SELECT
                item_key AS item,
                item_head_key AS item_head,
                {_PRIMARY_METRIC_SQL}
            FROM normalized
            WHERE {period_filter}
            GROUP BY item_key, item_head_key
        )
        SELECT *
        FROM item_agg
        -- Keep an item if it moved on ANY metric. Delivered is the usual signal,
        -- but a month with orders and no deliveries yet (e.g. the current month
        -- early on) has done_* = 0 — fall back to ordered so the SKU list is not
        -- empty. Ordered first in the sort so those months rank sensibly; the
        -- frontend re-sorts by whichever metric the user has toggled.
        WHERE COALESCE(done_value, 0) <> 0
           OR COALESCE(done_ltrs, 0) <> 0
           OR COALESCE(done_qty, 0) <> 0
           OR COALESCE(order_value, 0) <> 0
           OR COALESCE(order_ltrs, 0) <> 0
           OR COALESCE(order_qty, 0) <> 0
        ORDER BY done_value DESC, done_ltrs DESC, done_qty DESC,
                 order_value DESC, order_ltrs DESC, order_qty DESC
        """,
        period_params,
    )
    top_items = [
        {
            "item": row.get("item") or "OTHER",
            "item_head": row.get("item_head") or "OTHER",
            **_primary_metrics(row),
        }
        for row in top_item_raw
    ]
    open_vendor_pending = _dict_rows(
        f"""
        {primary_cte}
        , vendor_rows AS (
            SELECT
                COALESCE(
                    NULLIF(UPPER(TRIM(vendor_new::text)), ''),
                    NULLIF(UPPER(TRIM(vendor_name::text)), ''),
                    'UNMAPPED'
                ) AS vendor,
                ({vendor_metric_filter}) AS in_metric_period,
                ({vendor_pending_filter}) AS in_pending_period,
                COALESCE(metric_order_value, 0) AS order_value_row,
                COALESCE(metric_delivered_value, 0) AS delivered_value_row,
                COALESCE(metric_order_liters, 0) AS order_ltrs_row,
                COALESCE(metric_delivered_liters, 0) AS delivered_ltrs_row,
                COALESCE(metric_order_qty, 0) AS order_qty_row,
                COALESCE(metric_delivered_qty, 0) AS delivered_qty_row,
                COALESCE(metric_order_value, 0) AS pending_value_row,
                COALESCE(metric_order_liters, 0) AS pending_ltrs_row,
                COALESCE(metric_order_qty, 0) AS pending_qty_row,
                lead_time AS lead_time_row,
                COALESCE(NULLIF(UPPER(TRIM(open_close::text)), ''), 'CLOSED') AS open_close_key
            FROM normalized
        ),
        vendor_agg AS (
            SELECT
                vendor,
                COALESCE(SUM(CASE WHEN in_metric_period THEN order_value_row ELSE 0 END), 0) AS order_value,
                COALESCE(SUM(CASE WHEN in_metric_period THEN delivered_value_row ELSE 0 END), 0) AS delivered_value,
                COALESCE(SUM(CASE WHEN open_close_key = 'OPEN' AND in_pending_period
                    THEN pending_value_row
                    ELSE 0 END), 0) AS pending_value,
                COALESCE(SUM(CASE WHEN in_metric_period THEN order_ltrs_row ELSE 0 END), 0) AS order_ltrs,
                COALESCE(SUM(CASE WHEN in_metric_period THEN delivered_ltrs_row ELSE 0 END), 0) AS delivered_ltrs,
                COALESCE(SUM(CASE WHEN open_close_key = 'OPEN' AND in_pending_period
                    THEN pending_ltrs_row
                    ELSE 0 END), 0) AS pending_ltrs,
                COALESCE(SUM(CASE WHEN in_metric_period THEN order_qty_row ELSE 0 END), 0) AS order_qty,
                COALESCE(SUM(CASE WHEN in_metric_period THEN delivered_qty_row ELSE 0 END), 0) AS delivered_qty,
                COALESCE(SUM(CASE WHEN open_close_key = 'OPEN' AND in_pending_period
                    THEN pending_qty_row
                    ELSE 0 END), 0) AS pending_qty,
                AVG(lead_time_row) FILTER (
                    WHERE in_metric_period AND lead_time_row IS NOT NULL
                ) AS lead_time_avg
            FROM vendor_rows
            GROUP BY 1
        )
        SELECT *
        FROM vendor_agg
        WHERE order_value <> 0
           OR delivered_value <> 0
           OR pending_value <> 0
           OR order_ltrs <> 0
           OR delivered_ltrs <> 0
           OR pending_ltrs <> 0
           OR order_qty <> 0
           OR delivered_qty <> 0
           OR pending_qty <> 0
        ORDER BY pending_value DESC, order_value DESC, vendor
        """,
        vendor_period_params,
    )
    open_vendor_pending_value = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("pending_value")),
            _num(row.get("order_value")),
            _num(row.get("delivered_value")),
        ),
        reverse=True,
    )
    open_vendor_pending_ltrs = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("pending_ltrs")),
            _num(row.get("order_ltrs")),
            _num(row.get("delivered_ltrs")),
        ),
        reverse=True,
    )
    open_vendor_pending_qty = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("pending_qty")),
            _num(row.get("order_qty")),
            _num(row.get("delivered_qty")),
        ),
        reverse=True,
    )
    open_vendor_pending_order = sorted(
        open_vendor_pending,
        key=lambda row: (
            _num(row.get("order_value")),
            _num(row.get("order_ltrs")),
            _num(row.get("order_qty")),
        ),
        reverse=True,
    )

    detail_total = _primary_total(details)
    summary_total = _primary_total(summary)
    elapsed_day = _sec_elapsed_day(max_date)
    days_in_month = monthrange(year, month)[1]
    summary_total["projection_value"] = _safe_div(summary_total["done_value"], elapsed_day) * days_in_month
    summary_total["projection_ltrs"] = _safe_div(summary_total["done_ltrs"], elapsed_day) * days_in_month
    summary_total["projection_qty"] = _safe_div(summary_total["done_qty"], elapsed_day) * days_in_month
    trend_date_col = "delivery_dt" if mode == "DEL MONTH" else "po_dt"
    period_start = date(year, month, 1)
    period_end = min(date(year, month, monthrange(year, month)[1]), period_end_cap)

    daily_trend = _primary_trend_rows(_dict_rows(
        f"""
        {primary_cte},
        trend_days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS period
        ),
        agg AS (
            SELECT
                {trend_date_col}::date AS period,
                {_PRIMARY_TREND_METRIC_SQL}
            FROM normalized
            WHERE {period_filter}
              AND {trend_date_col} IS NOT NULL
              {trend_head_filter}
            GROUP BY {trend_date_col}::date
        )
        SELECT
            d.period,
            TO_CHAR(d.period, 'DD Mon') AS label,
            COALESCE(a.done_value, 0) AS done_value,
            COALESCE(a.done_ltrs, 0) AS done_ltrs,
            COALESCE(a.done_qty, 0) AS done_qty,
            COALESCE(a.pending_value, 0) AS pending_value,
            COALESCE(a.pending_ltrs, 0) AS pending_ltrs,
            COALESCE(a.pending_qty, 0) AS pending_qty,
            COALESCE(a.order_value, 0) AS order_value,
            COALESCE(a.order_ltrs, 0) AS order_ltrs,
            COALESCE(a.order_qty, 0) AS order_qty
        FROM trend_days d
        LEFT JOIN agg a ON a.period = d.period
        ORDER BY d.period
        """,
        [period_start, period_end] + period_params + trend_head_params,
    ))
    # monthly_trend / yearly_trend were removed: the frontend only consumes
    # `trends.day` (PlatformDashboard sparklines). Keys are preserved so any
    # in-flight clients still parse the response shape.
    monthly_trend = []
    yearly_trend = []

    kpi_source = (
        "total_po"
        if slug in _TOTAL_PO_KPI_DASHBOARD_SLUGS
        else "master_po_order_minus_deliver"
        if slug in _MASTER_PO_ORDER_MINUS_DELIVER_KPI_SLUGS
        else "master_po"
    )
    kpi_total = (
        _primary_total_po_kpi_total(
            platform_format,
            mode,
            month,
            year,
            period_end_cap,
        )
        if slug in _TOTAL_PO_KPI_DASHBOARD_SLUGS
        else _primary_master_po_order_minus_deliver_kpi_total(
            platform_format,
            mode,
            month,
            year,
        )
        if slug in _MASTER_PO_ORDER_MINUS_DELIVER_KPI_SLUGS
        else None
    )
    card_total = kpi_total or summary_total

    payload = {
        "source": "master_po",
        "format": platform_format,
        "dashboard_title": f"{platform_format} Primary Dashboard",
        "mode": mode,
        "month": month,
        "month_name": month_name,
        "year": year,
        "defaulted_to_latest": defaulted_to_latest,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "summary": summary,
        "summary_total": card_total,
        "kpi_source": kpi_source,
        "kpi_total": kpi_total,
        "fill_rate_summary": fill_rate_summary,
        "fill_rate_total": fill_rate_total,
        "lead_time_days": lead_time_days,
        "fill_rate_date_from": fill_rate_start.isoformat(),
        "fill_rate_date_to": fill_rate_cutoff.isoformat() if fill_rate_cutoff else None,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "open_vendor_pending": open_vendor_pending_value,
        "open_vendor_pending_value": open_vendor_pending_value,
        "open_vendor_pending_ltrs": open_vendor_pending_ltrs,
        "open_vendor_pending_qty": open_vendor_pending_qty,
        "open_vendor_pending_order": open_vendor_pending_order,
        "trends": {
            "day": daily_trend,
            "month": monthly_trend,
            "year": yearly_trend,
        },
        "detail_rows_fixed": slug == "zepto",
        "extra_detail_rows_included": True,
    }
    cache.set(cache_key, payload, _PRIMARY_DASHBOARD_CACHE_TTL)
    return Response(payload)


@api_view(["GET"])
@permission_classes([require("platform.po.view")])
@cached_get(timeout=60, prefix="plat.primary_total")
def primary_overview_total(request):
    """Fast aggregate used by the home dashboard Primary card."""
    raw_month = str(request.query_params.get("month") or date.today().month).strip()
    raw_year = str(request.query_params.get("year") or date.today().year).strip()
    try:
        month = int(raw_month)
        year = int(raw_year)
    except ValueError:
        raise ValidationError("`month` and `year` must be numeric.")
    if not 1 <= month <= 12:
        raise ValidationError("`month` must be 1-12.")
    if year < 2000 or year > 2100:
        raise ValidationError("`year` looks out of range.")

    requested_slugs = [
        slug.strip().lower()
        for slug in str(request.query_params.get("slugs") or "").split(",")
        if slug.strip()
    ]
    if not requested_slugs:
        requested_slugs = ["amazon", *_PRIMARY_DASHBOARD_FORMATS.keys()]

    allowed_slugs = [slug for slug in requested_slugs if can_access_platform(request.user, slug)]
    month_name = _month_name(month)
    period_end_cap = min(date(year, month, monthrange(year, month)[1]), date.today())
    cache_key = (
        f"primary_overview:v{_PRIMARY_DASHBOARD_CACHE_VERSION}:"
        f"{month}:{year}:{period_end_cap.isoformat()}:{','.join(allowed_slugs)}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return Response(cached)

    master_format_keys = []
    slug_format_keys = []  # (slug, format_key) — lets us split the totals per platform
    include_amazon = False
    for slug in allowed_slugs:
        if slug == "amazon":
            include_amazon = True
            continue
        platform_format = _PRIMARY_DASHBOARD_FORMATS.get(slug)
        if not platform_format:
            continue
        fk = re.sub(r"[^a-z0-9]+", "", str(platform_format).strip().lower())
        if fk not in master_format_keys:
            master_format_keys.append(fk)
        slug_format_keys.append((slug, fk))

    total_ltrs = Decimal("0")
    total_value = Decimal("0")
    item_heads = {
        "PREMIUM": {"done_ltrs": Decimal("0"), "done_value": Decimal("0")},
        "COMMODITY": {"done_ltrs": Decimal("0"), "done_value": Decimal("0")},
        "OTHER": {"done_ltrs": Decimal("0"), "done_value": Decimal("0")},
    }

    def add_item_head_total(row):
        nonlocal total_ltrs, total_value
        head = str(row.get("item_head") or "OTHER").strip().upper()
        if head not in item_heads:
            head = "OTHER"
        done_ltrs = Decimal(str(row.get("done_ltrs") or 0))
        done_value = Decimal(str(row.get("done_value") or 0))
        total_ltrs += done_ltrs
        total_value += done_value
        item_heads[head]["done_ltrs"] += done_ltrs
        item_heads[head]["done_value"] += done_value

    by_platform = {}

    def _empty_heads():
        return {h: {"done_ltrs": Decimal("0"), "done_value": Decimal("0")}
                for h in ("PREMIUM", "COMMODITY", "OTHER")}

    def _heads_payload(heads):
        return {
            "item_heads": {h: {"done_ltrs": float(v["done_ltrs"]),
                               "done_value": float(v["done_value"])}
                           for h, v in heads.items()},
            "done_ltrs": float(sum(v["done_ltrs"] for v in heads.values())),
            "done_value": float(sum(v["done_value"] for v in heads.values())),
        }

    if master_format_keys:
        values_sql = ", ".join(["(%s)"] * len(master_format_keys))
        delivery_expr = _prim_safe_date_expr("delivery_date", "p")
        rows = _dict_rows(
            f"""
            WITH requested(format_key) AS (
                VALUES {values_sql}
            ),
            base AS (
                SELECT
                    r.format_key,
                    p.total_delivered_liters,
                    p.total_delivered_amt_exclusive,
                    {delivery_expr} AS delivery_dt,
                    CASE
                        WHEN UPPER(TRIM(p.item_head::text)) = 'PREMIUM' THEN 'PREMIUM'
                        WHEN UPPER(TRIM(p.item_head::text)) = 'COMMODITY' THEN 'COMMODITY'
                        ELSE 'OTHER'
                    END AS item_head,
                    COALESCE(
                        NULLIF(UPPER(TRIM(p.delivery_month::text)), ''),
                        UPPER(TRIM(TO_CHAR({delivery_expr}, 'FMMONTH')))
                    ) AS delivery_month_key
                FROM public.master_po p
                JOIN requested r
                  ON REGEXP_REPLACE(LOWER(TRIM(p.format::text)), '[^a-z0-9]+', '', 'g') = r.format_key
            )
            SELECT
                format_key,
                item_head,
                COALESCE(SUM(COALESCE(total_delivered_liters, 0)), 0) AS done_ltrs,
                COALESCE(SUM(COALESCE(total_delivered_amt_exclusive, 0)), 0) AS done_value
            FROM base
            WHERE delivery_month_key = %s
              AND EXTRACT(YEAR FROM delivery_dt)::integer = %s
              AND delivery_dt <= %s
            GROUP BY format_key, item_head
            """,
            [*master_format_keys, month_name, year, period_end_cap],
        )
        heads_by_fk = {}
        for row in rows:
            add_item_head_total(row)
            head = str(row.get("item_head") or "OTHER").strip().upper()
            if head not in ("PREMIUM", "COMMODITY", "OTHER"):
                head = "OTHER"
            bucket = heads_by_fk.setdefault(row.get("format_key"), _empty_heads())
            bucket[head]["done_ltrs"] += Decimal(str(row.get("done_ltrs") or 0))
            bucket[head]["done_value"] += Decimal(str(row.get("done_value") or 0))
        for slug, fk in slug_format_keys:
            by_platform[slug] = _heads_payload(heads_by_fk.get(fk, _empty_heads()))

    if include_amazon:
        amazon_rows = _dict_rows(
            f"""
            {_amazon_primary_po_cte()}
            SELECT
                CASE
                    WHEN item_head_key = 'PREMIUM' THEN 'PREMIUM'
                    WHEN item_head_key = 'COMMODITY' THEN 'COMMODITY'
                    ELSE 'OTHER'
                END AS item_head,
                COALESCE(SUM(COALESCE(total_delivered_liters, 0)), 0) AS done_ltrs,
                COALESCE(SUM(COALESCE(total_received_cost, 0)), 0) AS done_value
            FROM normalized
            WHERE po_month_key = %s
              AND po_year = %s
            GROUP BY 1
            """,
            [month_name, year],
        )
        amazon_heads = _empty_heads()
        for row in amazon_rows:
            add_item_head_total(row)
            head = str(row.get("item_head") or "OTHER").strip().upper()
            if head not in ("PREMIUM", "COMMODITY", "OTHER"):
                head = "OTHER"
            amazon_heads[head]["done_ltrs"] += Decimal(str(row.get("done_ltrs") or 0))
            amazon_heads[head]["done_value"] += Decimal(str(row.get("done_value") or 0))
        by_platform["amazon"] = _heads_payload(amazon_heads)

    payload = {
        "done_ltrs": float(total_ltrs),
        "done_value": float(total_value),
        "item_heads": {
            key: {
                "done_ltrs": float(value["done_ltrs"]),
                "done_value": float(value["done_value"]),
            }
            for key, value in item_heads.items()
        },
        "by_platform": by_platform,
        "month": month,
        "month_name": month_name,
        "year": year,
        "platforms": allowed_slugs,
    }
    cache.set(cache_key, payload, _PRIMARY_DASHBOARD_CACHE_TTL)
    return Response(payload)


def _parse_price_upload_date(value: str) -> date | None:
    value = str(value or "").strip()
    if not value:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValidationError("`date` must be YYYY-MM-DD.")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValidationError("`date` must be a valid calendar date.")


_INVENTORY_DASHBOARD_PLATFORMS = {
    "blinkit": {
        "label": "Blinkit",
        "format": "BLINKIT",
        "sales_format": "blinkit",
        "latest_source": "secmaster_blinkit",
    },
    "zepto": {
        "label": "Zepto",
        "format": "ZEPTO",
        "sales_format": "zepto",
        "latest_source": "secmaster_zepto",
    },
    "swiggy": {
        "label": "Swiggy",
        "format": "SWIGGY",
        "sales_format": "swiggy",
        "latest_source": "secmaster_swiggy",
    },
    "bigbasket": {
        "label": "BigBasket",
        "format": "BIG BASKET",
        "sales_format": "bigbasket",
        "latest_source": "secmaster_bigbasket",
    },
}


def _inventory_dashboard_platform(slug: str, dashboard_name: str) -> dict:
    config = _INVENTORY_DASHBOARD_PLATFORMS.get(slug)
    if not config:
        raise ValidationError(
            f"{dashboard_name} is available only for Blinkit, Zepto, Swiggy and BigBasket."
        )
    return config


def _secmaster_inventory_date_expr(slug: str) -> str:
    if slug == "zepto":
        return _secmaster_zepto_date_expr()
    return '"date"::date'


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.soh_doh")
def blinkit_soh_doh_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "amazon":
        return _amazon_soh_doh_dashboard(request)

    platform = _inventory_dashboard_platform(slug, "SOH/DOH Dashboard")
    inventory_format = platform["format"]
    sales_format = platform["sales_format"]
    sale_date_expr = _secmaster_inventory_date_expr(slug)

    requested_date = _parse_price_upload_date(request.query_params.get("date", ""))
    defaulted_to_latest = False
    if requested_date is None:
        requested_date = _scalar(
            """
            SELECT MAX(inventory_date)
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
            """,
            [inventory_format],
        )
        defaulted_to_latest = True

    available_dates = _dict_rows(
        """
        SELECT inventory_date, COUNT(*) AS rows
        FROM all_platform_inventory
        WHERE UPPER(TRIM(format::text)) = %s
        GROUP BY inventory_date
        ORDER BY inventory_date DESC
        LIMIT 30
        """,
        [inventory_format],
    )

    if requested_date is None:
        return Response({
            "source": {
                "sales": "SecMaster",
                "inventory": "all_platform_inventory",
            },
            "format": inventory_format,
            "platform": slug,
            "dashboard_title": f"{platform['label']} SOH/DOH Dashboard",
            "requested_date": None,
            "effective_date": None,
            "max_sales_date": None,
            "sales_max_date": None,
            "month_start": None,
            "defaulted_to_latest": defaulted_to_latest,
            "available_dates": [],
            "rows": [],
            "total": _blinkit_soh_doh_empty_total(),
        })

    effective_date = _scalar(
        """
        SELECT MAX(inventory_date)
        FROM all_platform_inventory
        WHERE UPPER(TRIM(format::text)) = %s
          AND inventory_date <= %s
        """,
        [inventory_format, requested_date],
    )

    if effective_date is None:
        return Response({
            "source": {
                "sales": "SecMaster",
                "inventory": "all_platform_inventory",
            },
            "format": inventory_format,
            "platform": slug,
            "dashboard_title": f"{platform['label']} SOH/DOH Dashboard",
            "requested_date": requested_date.isoformat(),
            "effective_date": None,
            "max_sales_date": None,
            "sales_max_date": None,
            "month_start": None,
            "defaulted_to_latest": defaulted_to_latest,
            "available_dates": _inventory_date_options(available_dates),
            "rows": [],
            "total": _blinkit_soh_doh_empty_total(),
        })

    month_start = effective_date.replace(day=1)
    max_sales_date = _scalar(
        f"""
        SELECT MAX({sale_date_expr})
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
          AND ({sale_date_expr}) >= %s
          AND ({sale_date_expr}) <= %s
          AND ({sale_date_expr}) IS NOT NULL
        """,
        [sales_format, month_start, effective_date],
    )
    sales_end_date = max_sales_date or effective_date
    sales_max_date_value = (
        max_sales_date.isoformat()
        if hasattr(max_sales_date, "isoformat")
        else max_sales_date
    )
    elapsed_day = _sec_elapsed_day(max_sales_date)
    rows = _dict_rows(
        f"""
        WITH sales AS (
            SELECT
                UPPER(TRIM(COALESCE("item"::text, ''))) AS item_key,
                MIN(NULLIF(TRIM("item"::text), '')) AS item,
                MIN(NULLIF(TRIM("sku_code"::text), '')) AS sku_code,
                COALESCE(SUM("quantity"), 0)::numeric AS quantity,
                COALESCE(SUM("ltr_sold"), 0)::numeric AS ltr_sold
            FROM secmaster_mv
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
              AND ({sale_date_expr}) >= %s
              AND ({sale_date_expr}) <= %s
            GROUP BY UPPER(TRIM(COALESCE("item"::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item::text, ''))) AS item_key,
                MIN(NULLIF(TRIM(item::text), '')) AS inventory_item,
                MIN(NULLIF(TRIM(sku_code::text), '')) AS inv_sku_code,
                COALESCE(SUM(soh_unit), 0)::numeric AS soh_units,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(item::text, '')))
        )
        SELECT
            COALESCE(NULLIF(s.item, ''), NULLIF(i.inventory_item, '')) AS item,
            COALESCE(NULLIF(s.sku_code, ''), NULLIF(i.inv_sku_code, '')) AS sku_code,
            COALESCE(s.quantity, 0) AS quantity,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            i.inventory_item,
            COALESCE(i.soh_units, 0) AS soh_units,
            COALESCE(i.soh_ltr, 0) AS soh_ltr
        FROM sales s
        FULL OUTER JOIN inventory i
          ON s.item_key = i.item_key
        WHERE COALESCE(s.item_key, i.item_key) <> ''
        ORDER BY COALESCE(NULLIF(s.item, ''), NULLIF(i.inventory_item, '')) ASC NULLS LAST
        """,
        [sales_format, month_start, sales_end_date, inventory_format, effective_date],
    )

    normalized_rows = []
    for row in rows:
        quantity = _num(row.get("quantity"))
        ltr_sold = _num(row.get("ltr_sold"))
        soh_units = _num(row.get("soh_units"))
        soh_ltr = _num(row.get("soh_ltr"))
        drr_units = _safe_div(quantity, elapsed_day)
        drr_ltr = _safe_div(ltr_sold, elapsed_day)
        normalized_rows.append({
            "item": row.get("item") or row.get("inventory_item") or "",
            "sku_code": row.get("sku_code") or "",
            "quantity": quantity,
            "ltr_sold": ltr_sold,
            "inventory_item": row.get("inventory_item") or "",
            "soh_units": soh_units,
            "soh_ltr": soh_ltr,
            "drr_ltr": drr_ltr,
            "drr_units": drr_units,
            "doh": _safe_div(soh_units, drr_units),
        })

    total = _blinkit_soh_doh_total(normalized_rows, elapsed_day)

    return Response({
        "source": {
                "sales": "SecMaster",
                "inventory": "all_platform_inventory",
            },
        "format": inventory_format,
        "platform": slug,
        "dashboard_title": f"{platform['label']} SOH/DOH Dashboard",
        "requested_date": requested_date.isoformat(),
        "effective_date": effective_date.isoformat(),
        "max_sales_date": sales_max_date_value,
        "sales_max_date": sales_max_date_value,
        "month_start": month_start.isoformat(),
        "defaulted_to_latest": defaulted_to_latest,
        "elapsed_day": elapsed_day,
        "available_dates": _inventory_date_options(available_dates),
        "rows": normalized_rows,
        "total": total,
    })


def _inventory_date_options(rows: list[dict]) -> list[dict]:
    options = []
    for row in rows:
        inventory_date = row.get("inventory_date")
        options.append({
            "date": inventory_date.isoformat()
            if hasattr(inventory_date, "isoformat")
            else inventory_date,
            "rows": int(row.get("rows") or 0),
        })
    return options


def _blinkit_soh_doh_empty_total() -> dict:
    return {
        "quantity": 0.0,
        "ltr_sold": 0.0,
        "soh_units": 0.0,
        "soh_ltr": 0.0,
        "drr_ltr": 0.0,
        "drr_units": 0.0,
        "doh": 0.0,
    }


def _blinkit_soh_doh_total(rows: list[dict], elapsed_day: int) -> dict:
    total = _blinkit_soh_doh_empty_total()
    total["quantity"] = sum(_num(row.get("quantity")) for row in rows)
    total["ltr_sold"] = sum(_num(row.get("ltr_sold")) for row in rows)
    total["soh_units"] = sum(_num(row.get("soh_units")) for row in rows)
    total["soh_ltr"] = sum(_num(row.get("soh_ltr")) for row in rows)
    if elapsed_day > 0:
        total["drr_units"] = total["quantity"] / elapsed_day
        total["drr_ltr"] = total["ltr_sold"] / elapsed_day
    total["doh"] = _safe_div(total["soh_units"], total["drr_units"])
    return total


# ── Region (city-wise) DOH ───────────────────────────────────────────────────
# Same SOH/DOH math as ``blinkit_soh_doh_dashboard`` but grouped by **city**
# instead of by item. Stock comes from ``all_platform_inventory`` (its
# ``location`` column is the source table's city for these formats, and it
# already carries ``soh_ltr`` via ``master_sheet.per_unit_value``). Sales come
# from the raw per-platform secondary table, which has a matching city column;
# litres reuse the same per-unit conversion so SOH and DRR litres share a basis.
# DOH = SOH units / DRR units, and is 0 (never ∞) when there are no sales.
#
# Only the source table + its column names differ per platform, so everything
# below is parametrized by this config. Identifiers are hardcoded constants
# (never request input), so f-string interpolation into SQL is safe.
_REGION_DOH_PLATFORMS = {
    "swiggy": {
        "label": "Swiggy",
        "inventory_format": "SWIGGY",
        "master_format_key": "swiggy",
        "sales_table": '"swiggySec"',
        "sales_source": "swiggySec",
        "city_col": '"CITY"',
        "sku_col": '"ITEM_CODE"',
        "date_col": '"ORDERED_DATE"',
        # Units count combos to match the SOH/DOH dashboard quantity; litres
        # convert only the base units (same basis as the inventory view).
        "units_expr": 'COALESCE(s."COMBO_UNITS_SOLD", 0) + COALESCE(s."UNITS_SOLD", 0)',
        "base_units_col": 's."UNITS_SOLD"',
    },
    "zepto": {
        "label": "Zepto",
        "inventory_format": "ZEPTO",
        "master_format_key": "zepto",
        "sales_table": '"zeptoSec"',
        "sales_source": "zeptoSec",
        "city_col": '"City"',
        "sku_col": '"SKU Number"',
        "date_col": '"Date"',
        # Zepto has no combo concept — units = base sold units.
        "units_expr": 'COALESCE(s."Sales (Qty) - Units", 0)',
        "base_units_col": 's."Sales (Qty) - Units"',
    },
}


def _region_doh_empty_total() -> dict:
    return {
        "soh_units": 0.0,
        "soh_ltr": 0.0,
        "units_sold": 0.0,
        "ltr_sold": 0.0,
        "drr_units": 0.0,
        "drr_ltr": 0.0,
        "doh": 0.0,
    }


def _region_doh_total(rows: list[dict], elapsed_day: int) -> dict:
    total = _region_doh_empty_total()
    total["soh_units"] = sum(_num(row.get("soh_units")) for row in rows)
    total["soh_ltr"] = sum(_num(row.get("soh_ltr")) for row in rows)
    total["units_sold"] = sum(_num(row.get("units_sold")) for row in rows)
    total["ltr_sold"] = sum(_num(row.get("ltr_sold")) for row in rows)
    if elapsed_day > 0:
        total["drr_units"] = total["units_sold"] / elapsed_day
        total["drr_ltr"] = total["ltr_sold"] / elapsed_day
    total["doh"] = _safe_div(total["soh_units"], total["drr_units"])
    return total


def _region_doh_payload(
    slug: str,
    cfg: dict,
    *,
    requested_date,
    effective_date=None,
    sales_max_date=None,
    month_start=None,
    elapsed_day: int = 0,
    defaulted_to_latest: bool = False,
    rows: list[dict] | None = None,
    total: dict | None = None,
) -> dict:
    def _iso(value):
        return value.isoformat() if hasattr(value, "isoformat") else value

    return {
        "source": {"sales": cfg["sales_source"], "inventory": "all_platform_inventory"},
        "format": cfg["inventory_format"],
        "platform": slug,
        "dashboard_title": f"{cfg['label']} Region DOH Dashboard",
        "requested_date": _iso(requested_date),
        "effective_date": _iso(effective_date),
        "sales_max_date": _iso(sales_max_date),
        "max_sales_date": _iso(sales_max_date),
        "month_start": _iso(month_start),
        "elapsed_day": elapsed_day,
        "defaulted_to_latest": defaulted_to_latest,
        "rows": rows or [],
        "total": total or _region_doh_empty_total(),
    }


def _region_doh_dashboard_response(request, slug: str):
    cfg = _REGION_DOH_PLATFORMS[slug]
    _ensure_scope(request.user, slug)
    inventory_format = cfg["inventory_format"]

    requested_date = _parse_price_upload_date(request.query_params.get("date", ""))
    defaulted_to_latest = False
    if requested_date is None:
        requested_date = _scalar(
            """
            SELECT MAX(inventory_date)
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
            """,
            [inventory_format],
        )
        defaulted_to_latest = True

    if requested_date is None:
        return Response(
            _region_doh_payload(
                slug, cfg,
                requested_date=None,
                defaulted_to_latest=defaulted_to_latest,
            )
        )

    effective_date = _scalar(
        """
        SELECT MAX(inventory_date)
        FROM all_platform_inventory
        WHERE UPPER(TRIM(format::text)) = %s
          AND inventory_date <= %s
        """,
        [inventory_format, requested_date],
    )
    if effective_date is None:
        return Response(
            _region_doh_payload(
                slug, cfg,
                requested_date=requested_date,
                defaulted_to_latest=defaulted_to_latest,
            )
        )

    month_start = effective_date.replace(day=1)
    max_sales_date = _scalar(
        f"""
        SELECT MAX(s.{cfg['date_col']}::date)
        FROM {cfg['sales_table']} s
        WHERE s.{cfg['date_col']}::date >= %s
          AND s.{cfg['date_col']}::date <= %s
        """,
        [month_start, effective_date],
    )
    sales_end_date = max_sales_date or effective_date
    elapsed_day = _sec_elapsed_day(max_sales_date)

    rows = _dict_rows(
        f"""
        WITH master_lookup AS (
            SELECT sku_key, per_unit_value
            FROM (
                SELECT
                    UPPER(TRIM(ms.format_sku_code::text)) AS sku_key,
                    ms.per_unit_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(ms.format_sku_code::text))
                        ORDER BY
                            CASE WHEN ms.per_unit_value IS NULL THEN 1 ELSE 0 END,
                            ms.ctid
                    ) AS rn
                FROM public.master_sheet ms
                WHERE NULLIF(TRIM(ms.format_sku_code::text), '') IS NOT NULL
                  AND REGEXP_REPLACE(LOWER(TRIM(COALESCE(ms.format, '')::text)), '[^a-z0-9]+', '', 'g') = %s
            ) ranked
            WHERE rn = 1
        ),
        sales AS (
            SELECT
                UPPER(TRIM(COALESCE(s.{cfg['city_col']}::text, ''))) AS city_key,
                MIN(NULLIF(TRIM(s.{cfg['city_col']}::text), '')) AS city,
                COALESCE(SUM({cfg['units_expr']}), 0)::numeric AS units_sold,
                COALESCE(
                    SUM(COALESCE({cfg['base_units_col']}, 0)::double precision
                        * COALESCE(ml.per_unit_value, 0)),
                    0
                )::numeric AS ltr_sold
            FROM {cfg['sales_table']} s
            LEFT JOIN master_lookup ml
              ON ml.sku_key = UPPER(TRIM(s.{cfg['sku_col']}::text))
            WHERE s.{cfg['date_col']}::date >= %s
              AND s.{cfg['date_col']}::date <= %s
            GROUP BY UPPER(TRIM(COALESCE(s.{cfg['city_col']}::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(location::text, ''))) AS city_key,
                MIN(NULLIF(TRIM(location::text), '')) AS city,
                COALESCE(SUM(soh_unit), 0)::numeric AS soh_units,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(location::text, '')))
        )
        SELECT
            COALESCE(NULLIF(i.city, ''), NULLIF(s.city, '')) AS city,
            COALESCE(i.soh_units, 0) AS soh_units,
            COALESCE(i.soh_ltr, 0) AS soh_ltr,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold
        FROM inventory i
        FULL OUTER JOIN sales s
          ON i.city_key = s.city_key
        WHERE COALESCE(i.city_key, s.city_key) <> ''
        ORDER BY COALESCE(NULLIF(i.city, ''), NULLIF(s.city, '')) ASC NULLS LAST
        """,
        [
            cfg["master_format_key"],
            month_start,
            sales_end_date,
            inventory_format,
            effective_date,
        ],
    )

    normalized_rows = []
    for row in rows:
        units_sold = _num(row.get("units_sold"))
        ltr_sold = _num(row.get("ltr_sold"))
        soh_units = _num(row.get("soh_units"))
        soh_ltr = _num(row.get("soh_ltr"))
        drr_units = _safe_div(units_sold, elapsed_day)
        drr_ltr = _safe_div(ltr_sold, elapsed_day)
        normalized_rows.append({
            "city": row.get("city") or "",
            "soh_units": soh_units,
            "soh_ltr": soh_ltr,
            "units_sold": units_sold,
            "ltr_sold": ltr_sold,
            "drr_units": drr_units,
            "drr_ltr": drr_ltr,
            "doh": _safe_div(soh_units, drr_units),
        })

    total = _region_doh_total(normalized_rows, elapsed_day)

    return Response(
        _region_doh_payload(
            slug, cfg,
            requested_date=requested_date,
            effective_date=effective_date,
            sales_max_date=max_sales_date,
            month_start=month_start,
            elapsed_day=elapsed_day,
            defaulted_to_latest=defaulted_to_latest,
            rows=normalized_rows,
            total=total,
        )
    )


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.region_doh")
def swiggy_region_doh_dashboard(request):
    return _region_doh_dashboard_response(request, "swiggy")


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.region_doh")
def zepto_region_doh_dashboard(request):
    return _region_doh_dashboard_response(request, "zepto")


def _amazon_soh_month_name(raw_value) -> str | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    if value.isdigit():
        month_num = int(value)
        if month_num < 1 or month_num > 12:
            raise ValidationError("`month` must be between 1 and 12.")
        return date(2000, month_num, 1).strftime("%B").upper()

    key = _norm_sec_key(value)
    for month_num in range(1, 13):
        full = date(2000, month_num, 1).strftime("%B").upper()
        short = date(2000, month_num, 1).strftime("%b").upper()
        if key in {full, short}:
            return full
    raise ValidationError("`month` must be a valid month name or number.")


def _amazon_soh_year(raw_value) -> int | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        year = int(value)
    except ValueError as exc:
        raise ValidationError("`year` must be a number.") from exc
    if year < 2000 or year > 2100:
        raise ValidationError("`year` must be between 2000 and 2100.")
    return year


def _amazon_soh_empty_total() -> dict:
    return {
        "units_sold": 0.0,
        "ltr_sold": 0.0,
        "soh_unit": 0.0,
        "soh_ltr": 0.0,
        "drr_unit": 0.0,
        "drr_ltr": 0.0,
        "doh": 0.0,
    }


def _amazon_soh_total(rows: list[dict], elapsed_day: int) -> dict:
    total = _amazon_soh_empty_total()
    total["units_sold"] = sum(_num(row.get("units_sold")) for row in rows)
    total["ltr_sold"] = sum(_num(row.get("ltr_sold")) for row in rows)
    total["soh_unit"] = sum(_num(row.get("soh_unit")) for row in rows)
    total["soh_ltr"] = sum(_num(row.get("soh_ltr")) for row in rows)
    if elapsed_day > 0:
        total["drr_unit"] = total["units_sold"] / elapsed_day
        total["drr_ltr"] = total["ltr_sold"] / elapsed_day
    total["doh"] = (
        (total["soh_unit"] / total["drr_unit"]) - 2
        if total["drr_unit"]
        else 0.0
    )
    return total


def _amazon_soh_normalize_rows(rows: list[dict], elapsed_day: int) -> list[dict]:
    normalized = []
    for row in rows:
        units_sold = _num(row.get("units_sold"))
        ltr_sold = _num(row.get("ltr_sold"))
        soh_unit = _num(row.get("soh_unit"))
        soh_ltr = _num(row.get("soh_ltr"))
        drr_unit = units_sold / elapsed_day if elapsed_day > 0 else 0.0
        drr_ltr = ltr_sold / elapsed_day if elapsed_day > 0 else 0.0
        normalized_row = dict(row)
        normalized_row.update({
            "units_sold": units_sold,
            "ltr_sold": ltr_sold,
            "soh_unit": soh_unit,
            "soh_ltr": soh_ltr,
            "drr_unit": drr_unit,
            "drr_ltr": drr_ltr,
            "doh": ((soh_unit / drr_unit) - 2) if drr_unit else 0.0,
        })
        normalized.append(normalized_row)
    return normalized


def _amazon_soh_empty_payload(
    *,
    requested_date,
    month_name: str | None,
    year: int | None,
    defaulted_to_latest: bool,
    available_dates: list[dict],
    notes: list[str] | None = None,
) -> dict:
    empty_total = _amazon_soh_empty_total()
    return {
        "source": {
            "sales": "amazon_sec_range_master_view",
            "inventory": "amazon_master_inventory",
        },
        "format": "AMAZON",
        "platform": "amazon",
        "dashboard_title": "Amazon SOH/DOH Dashboard",
        "requested_date": requested_date.isoformat()
        if hasattr(requested_date, "isoformat")
        else requested_date,
        "effective_date": None,
        "effective_inventory_date": None,
        "month_start": None,
        "month": month_name,
        "year": year,
        "month_day": None,
        "defaulted_to_latest": defaulted_to_latest,
        "elapsed_day": 0,
        "available_dates": _inventory_date_options(available_dates),
        "dashboards": {
            "item_head": [],
            "asin": [],
            "category": [],
            "category_sub_category": [],
        },
        "totals": {
            "item_head": empty_total,
            "asin": empty_total,
            "category": empty_total,
            "category_sub_category": empty_total,
        },
        "notes": notes or [],
    }


def _amazon_soh_doh_dashboard(request):
    raw_month = request.query_params.get("month", "")
    raw_year = request.query_params.get("year", "")
    raw_date = request.query_params.get("date", "")
    month_name = _amazon_soh_month_name(raw_month)
    year = _amazon_soh_year(raw_year)
    requested_date = _parse_price_upload_date(raw_date)
    defaulted_to_latest = not raw_month and not raw_year and not raw_date

    if requested_date is not None:
        month_name = month_name or requested_date.strftime("%B").upper()
        year = year or requested_date.year

    available_dates = _dict_rows(
        """
        SELECT inventory_date, COUNT(*) AS rows
        FROM amazon_master_inventory
        WHERE inventory_date IS NOT NULL
        GROUP BY inventory_date
        ORDER BY inventory_date DESC
        LIMIT 30
        """,
        [],
    )

    period_where = ["inventory_date IS NOT NULL"]
    period_params: list = []
    if month_name:
        period_where.append("UPPER(TRIM(\"month\"::text)) = %s")
        period_params.append(month_name)
    if year is not None:
        period_where.append("\"year\" = %s")
        period_params.append(year)
    if requested_date is not None:
        period_where.append("inventory_date <= %s")
        period_params.append(requested_date)

    effective_date = _scalar(
        f"""
        SELECT MAX(inventory_date)
        FROM amazon_master_inventory
        WHERE {" AND ".join(period_where)}
        """,
        period_params,
    )

    if effective_date is None:
        return Response(_amazon_soh_empty_payload(
            requested_date=requested_date,
            month_name=month_name,
            year=year,
            defaulted_to_latest=defaulted_to_latest,
            available_dates=available_dates,
            notes=["No Amazon inventory rows found for the selected period."],
        ))

    month_name = month_name or effective_date.strftime("%B").upper()
    year = year or effective_date.year
    month_start = effective_date.replace(day=1)
    elapsed_day = max(1, effective_date.day)
    month_day = f"{effective_date.day:02d}-{month_name}"

    common_params = [year, month_name, month_day, year, month_name, effective_date]

    item_head_rows = _dict_rows(
        """
        WITH expected(item_key, item_head, sort_order) AS (
            VALUES
                ('PREMIUM', 'PREMIUM', 1),
                ('COMMODITY', 'COMMODITY', 2),
                ('OTHER', 'OTHER', 3)
        ),
        sales AS (
            SELECT
                UPPER(TRIM(COALESCE(item_head::text, ''))) AS item_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS item_head,
                COALESCE(SUM(shipped_units), 0)::numeric AS units_sold,
                COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
            FROM amazon_sec_range_master_view
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND UPPER(TRIM(month_day::text)) = %s
            GROUP BY UPPER(TRIM(COALESCE(item_head::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item_head::text, ''))) AS item_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS item_head,
                COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(item_head::text, '')))
        ),
        keys AS (
            SELECT
                item_key,
                MIN(item_head) AS item_head,
                MIN(sort_order) AS sort_order
            FROM (
                SELECT item_key, item_head, sort_order FROM expected
                UNION ALL
                SELECT item_key, item_head, 99 FROM sales WHERE item_key <> ''
                UNION ALL
                SELECT item_key, item_head, 99 FROM inventory WHERE item_key <> ''
            ) k
            GROUP BY item_key
        )
        SELECT
            %s::date AS max_updated_date,
            COALESCE(NULLIF(k.item_head, ''), NULLIF(s.item_head, ''), NULLIF(i.item_head, '')) AS item_head,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            COALESCE(i.soh_unit, 0) AS soh_unit,
            COALESCE(i.soh_ltr, 0) AS soh_ltr
        FROM keys k
        LEFT JOIN sales s ON s.item_key = k.item_key
        LEFT JOIN inventory i ON i.item_key = k.item_key
        ORDER BY k.sort_order, COALESCE(NULLIF(k.item_head, ''), NULLIF(s.item_head, ''), NULLIF(i.item_head, ''))
        """,
        common_params + [effective_date],
    )

    asin_rows = _dict_rows(
        """
        WITH row_list AS (
            SELECT
                UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS type,
                MIN(NULLIF(TRIM(category::text), '')) AS category,
                MIN(NULLIF(TRIM(sub_category::text), '')) AS sub_category,
                MIN(NULLIF(TRIM(brand_2::text), '')) AS brand,
                MIN(NULLIF(TRIM(per_unit::text), '')) AS per_unit,
                MIN(NULLIF(TRIM(asin::text), '')) AS asin
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND NULLIF(TRIM(COALESCE(asin::text, '')), '') IS NOT NULL
            GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
        ),
        sales AS (
            SELECT
                UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                COALESCE(SUM(shipped_units), 0)::numeric AS units_sold,
                COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
            FROM amazon_sec_range_master_view
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND UPPER(TRIM(month_day::text)) = %s
            GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
        )
        SELECT
            r.type,
            r.category,
            r.sub_category,
            r.brand,
            r.per_unit,
            r.asin,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            COALESCE(i.soh_unit, 0) AS soh_unit,
            COALESCE(i.soh_ltr, 0) AS soh_ltr
        FROM row_list r
        LEFT JOIN sales s ON s.asin_key = r.asin_key
        LEFT JOIN inventory i ON i.asin_key = r.asin_key
        ORDER BY r.type DESC NULLS LAST, r.category ASC NULLS LAST, r.sub_category ASC NULLS LAST, r.asin ASC NULLS LAST
        """,
        [year, month_name, year, month_name, month_day, year, month_name, effective_date],
    )

    category_rows = _dict_rows(
        """
        WITH sales AS (
            SELECT
                UPPER(TRIM(COALESCE(item_head::text, ''))) AS type_key,
                UPPER(TRIM(COALESCE(category::text, ''))) AS category_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS type,
                MIN(NULLIF(TRIM(category::text), '')) AS category,
                COALESCE(SUM(shipped_units), 0)::numeric AS units_sold,
                COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
            FROM amazon_sec_range_master_view
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND UPPER(TRIM(month_day::text)) = %s
            GROUP BY
                UPPER(TRIM(COALESCE(item_head::text, ''))),
                UPPER(TRIM(COALESCE(category::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item_head::text, ''))) AS type_key,
                UPPER(TRIM(COALESCE(category::text, ''))) AS category_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS type,
                MIN(NULLIF(TRIM(category::text), '')) AS category,
                COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND inventory_date = %s
            GROUP BY
                UPPER(TRIM(COALESCE(item_head::text, ''))),
                UPPER(TRIM(COALESCE(category::text, '')))
        ),
        keys AS (
            SELECT type_key, category_key FROM sales
            UNION
            SELECT type_key, category_key FROM inventory
        )
        SELECT
            'AMAZON' AS format,
            COALESCE(NULLIF(s.type, ''), NULLIF(i.type, '')) AS type,
            COALESCE(NULLIF(s.category, ''), NULLIF(i.category, '')) AS category,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            COALESCE(i.soh_unit, 0) AS soh_unit,
            COALESCE(i.soh_ltr, 0) AS soh_ltr
        FROM keys k
        LEFT JOIN sales s ON s.type_key = k.type_key AND s.category_key = k.category_key
        LEFT JOIN inventory i ON i.type_key = k.type_key AND i.category_key = k.category_key
        WHERE COALESCE(k.type_key, '') <> '' OR COALESCE(k.category_key, '') <> ''
        ORDER BY type DESC NULLS LAST, category ASC NULLS LAST
        """,
        common_params,
    )

    category_sub_category_rows = _dict_rows(
        """
        WITH sales AS (
            SELECT
                UPPER(TRIM(COALESCE(item_head::text, ''))) AS type_key,
                UPPER(TRIM(COALESCE(category::text, ''))) AS category_key,
                UPPER(TRIM(COALESCE(sub_category::text, ''))) AS sub_category_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS type,
                MIN(NULLIF(TRIM(category::text), '')) AS category,
                MIN(NULLIF(TRIM(sub_category::text), '')) AS sub_category,
                COALESCE(SUM(shipped_units), 0)::numeric AS units_sold,
                COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
            FROM amazon_sec_range_master_view
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND UPPER(TRIM(month_day::text)) = %s
            GROUP BY
                UPPER(TRIM(COALESCE(item_head::text, ''))),
                UPPER(TRIM(COALESCE(category::text, ''))),
                UPPER(TRIM(COALESCE(sub_category::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item_head::text, ''))) AS type_key,
                UPPER(TRIM(COALESCE(category::text, ''))) AS category_key,
                UPPER(TRIM(COALESCE(sub_category::text, ''))) AS sub_category_key,
                MIN(NULLIF(TRIM(item_head::text), '')) AS type,
                MIN(NULLIF(TRIM(category::text), '')) AS category,
                MIN(NULLIF(TRIM(sub_category::text), '')) AS sub_category,
                COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND inventory_date = %s
            GROUP BY
                UPPER(TRIM(COALESCE(item_head::text, ''))),
                UPPER(TRIM(COALESCE(category::text, ''))),
                UPPER(TRIM(COALESCE(sub_category::text, '')))
        ),
        keys AS (
            SELECT type_key, category_key, sub_category_key FROM sales
            UNION
            SELECT type_key, category_key, sub_category_key FROM inventory
        )
        SELECT
            'AMAZON' AS format,
            COALESCE(NULLIF(s.type, ''), NULLIF(i.type, '')) AS type,
            COALESCE(NULLIF(s.category, ''), NULLIF(i.category, '')) AS category,
            COALESCE(NULLIF(s.sub_category, ''), NULLIF(i.sub_category, '')) AS sub_category,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            COALESCE(i.soh_unit, 0) AS soh_unit,
            COALESCE(i.soh_ltr, 0) AS soh_ltr
        FROM keys k
        LEFT JOIN sales s
          ON s.type_key = k.type_key
         AND s.category_key = k.category_key
         AND s.sub_category_key = k.sub_category_key
        LEFT JOIN inventory i
          ON i.type_key = k.type_key
         AND i.category_key = k.category_key
         AND i.sub_category_key = k.sub_category_key
        WHERE COALESCE(k.type_key, '') <> ''
           OR COALESCE(k.category_key, '') <> ''
           OR COALESCE(k.sub_category_key, '') <> ''
        ORDER BY type DESC NULLS LAST, category ASC NULLS LAST, sub_category ASC NULLS LAST
        """,
        common_params,
    )

    dashboards = {
        "item_head": _amazon_soh_normalize_rows(item_head_rows, elapsed_day),
        "asin": _amazon_soh_normalize_rows(asin_rows, elapsed_day),
        "category": _amazon_soh_normalize_rows(category_rows, elapsed_day),
        "category_sub_category": _amazon_soh_normalize_rows(category_sub_category_rows, elapsed_day),
    }
    totals = {
        key: _amazon_soh_total(rows, elapsed_day)
        for key, rows in dashboards.items()
    }

    notes = []
    if totals["asin"]["units_sold"] == 0 and totals["asin"]["soh_unit"] > 0:
        notes.append(
            "Inventory is available for the selected snapshot, but no SEC range sales rows match the derived month-day."
        )

    return Response({
        "source": {
            "sales": "amazon_sec_range_master_view",
            "inventory": "amazon_master_inventory",
        },
        "format": "AMAZON",
        "platform": "amazon",
        "dashboard_title": "Amazon SOH/DOH Dashboard",
        "requested_date": requested_date.isoformat()
        if hasattr(requested_date, "isoformat")
        else None,
        "effective_date": effective_date.isoformat(),
        "effective_inventory_date": effective_date.isoformat(),
        "month_start": month_start.isoformat(),
        "month": month_name,
        "year": year,
        "month_day": month_day,
        "defaulted_to_latest": defaulted_to_latest,
        "elapsed_day": elapsed_day,
        "available_dates": _inventory_date_options(available_dates),
        "dashboards": dashboards,
        "totals": totals,
        "notes": notes,
    })


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.amazon_price")
def amazon_price_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "amazon":
        raise ValidationError("Amazon Price Dashboard is available only for Amazon.")

    selected_date = _parse_price_upload_date(request.query_params.get("date", ""))
    defaulted_to_latest = False
    if selected_date is None:
        latest = _scalar(
            'SELECT MAX(upload_date) FROM amazon_price_data',
            [],
        )
        selected_date = latest
        defaulted_to_latest = True

    upload_dates = _dict_rows(
        """
        SELECT upload_date, COUNT(*) AS rows
        FROM amazon_price_data
        GROUP BY upload_date
        ORDER BY upload_date DESC
        LIMIT 30
        """,
        [],
    )

    if selected_date is None:
        return Response({
            "source": "amazon_price_data",
            "dashboard_title": "Amazon Price Dashboard",
            "selected_date": None,
            "defaulted_to_latest": defaulted_to_latest,
            "upload_dates": [],
            "summary": {
                "total_rows": 0,
                "in_stock": 0,
                "out_of_stock": 0,
                "missing_url_price": 0,
                "seller_count": 0,
                "avg_url_price": None,
            },
            "rows": [],
        })

    summary_rows = _dict_rows(
        """
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(CASE WHEN stock_status ILIKE '%%in stock%%' THEN 1 ELSE 0 END), 0) AS in_stock,
            COALESCE(SUM(CASE WHEN stock_status ILIKE '%%out%%' THEN 1 ELSE 0 END), 0) AS out_of_stock,
            COALESCE(SUM(CASE WHEN url_price IS NULL THEN 1 ELSE 0 END), 0) AS missing_url_price,
            COUNT(DISTINCT NULLIF(TRIM(seller), '')) AS seller_count,
            AVG(url_price) AS avg_url_price
        FROM amazon_price_data
        WHERE upload_date = %s
        """,
        [selected_date],
    )
    rows = _dict_rows(
        """
        SELECT
            upload_date,
            url,
            asin,
            product,
            margin_basis,
            mrp,
            asp,
            margin_pct,
            tax_pct,
            cost_without_tax,
            url_price,
            stock_status,
            seller,
            rk_price,
            jm_price,
            svd_price,
            bau_price,
            art_price,
            created_at,
            updated_at
        FROM amazon_price_data
        WHERE upload_date = %s
        ORDER BY product ASC NULLS LAST, asin ASC
        """,
        [selected_date],
    )

    return Response({
        "source": "amazon_price_data",
        "dashboard_title": "Amazon Price Dashboard",
        "selected_date": selected_date.isoformat(),
        "defaulted_to_latest": defaulted_to_latest,
        "upload_dates": [
            {
                "date": row["upload_date"].isoformat()
                if hasattr(row["upload_date"], "isoformat")
                else row["upload_date"],
                "rows": int(row["rows"] or 0),
            }
            for row in upload_dates
        ],
        "summary": summary_rows[0] if summary_rows else {},
        "rows": rows,
    })


# ─── Ads dashboard (unified payload for all 5 platforms) ─────────────────────
#
# Each platform's endpoint is a thin wrapper that calls _ads_dashboard_payload()
# with platform-specific config. Output shape is identical across platforms so a
# single frontend component renders all of them:
#
#   {
#     "source": <view name>,
#     "dashboard_title": <string>,
#     "dimension_label": "Portfolios" | "Items",
#     "summary": {<metric_key>: <number>, ...},
#     "available_metrics": [{key, label, format, agg}, ...],
#     "default_metric_keys": [...],
#     "trend_axes": {"spend": {label, format}, "revenue": {label, format}},
#     "trend_rows": [{date, spend, revenue}, ...],
#     "breakdown_columns": [{key, label, format, agg, default_visible}, ...],
#     "breakdown_rows": [{dimension, <metric_key>: <number>, ...}, ...],
#     "max_date": <iso date>,
#     "filter_options": {years, months, dates},
#     "filters": {year, month, date},
#   }
#
# metric_specs is a list of dicts with:
#   {key, label, format, agg, expr}
# where `expr` is a SQL aggregate expression (e.g. "COALESCE(SUM(total_cost), 0)"
# or "CASE WHEN SUM(total_cost) > 0 THEN SUM(sales)/SUM(total_cost) ELSE 0 END").
# Derived ratios are weighted (SUM-of-numerator / SUM-of-denominator), labelled
# AVERAGE in the UI.

def _ads_build_where(request, *, allow_date: bool = True):
    """Parse year / month / date query params and build TWO where clauses:
      * `where_sql` / `params`         — full filter (year + month + date)
      * `trend_where_sql` / `trend_params` — same filter without `date`

    The trend chart uses the trend version: a specific date narrows the trend
    to everything UP TO that date (inclusive), so the line still spans multiple
    days and visibly changes as the user moves the date — rather than
    collapsing to a single point.

    Returns (where_sql, params, trend_where_sql, trend_params, filters_dict).
    """
    year_param = (request.query_params.get("year") or "").strip()
    month_param = (request.query_params.get("month") or "").strip().upper()
    date_param = (request.query_params.get("date") or "").strip()
    from_param = (request.query_params.get("from_date") or "").strip()
    to_param = (request.query_params.get("to_date") or "").strip()

    _iso = r"^\d{4}-\d{2}-\d{2}$"

    base_clauses: list[str] = []
    base_params: list = []
    if year_param:
        try:
            base_clauses.append("year = %s")
            base_params.append(int(year_param))
        except ValueError:
            raise ValidationError(f"Invalid year value: {year_param!r}")
    if month_param:
        # The `month` column stores uppercase names ('JUNE'). Also accept a
        # numeric month (1-12) — the sales/secondary endpoints take numbers, so
        # a client following that convention would otherwise get silent zeros.
        if month_param.isdigit():
            m = int(month_param)
            if not 1 <= m <= 12:
                raise ValidationError(
                    f"Invalid month value: {month_param!r} (expected 1-12 or a month name)"
                )
            month_param = _month_name(m)
        base_clauses.append("month = %s")
        base_params.append(month_param)

    # Trend version starts from the base (year/month) clauses; a date filter is
    # added below.
    trend_clauses = list(base_clauses)
    trend_params = list(base_params)

    # Full version (summary / breakdown) narrows to the date filter too.
    full_clauses = list(base_clauses)
    full_params = list(base_params)

    if allow_date and (from_param or to_param):
        # Calendar From/To range — both summary/breakdown and trend are scoped to
        # [from, to]; either bound may be omitted (open-ended on that side).
        if from_param:
            if not re.match(_iso, from_param):
                raise ValidationError(
                    f"Invalid from_date value: {from_param!r} (expected YYYY-MM-DD)"
                )
            for clauses, prms in ((full_clauses, full_params), (trend_clauses, trend_params)):
                clauses.append("date >= %s::date")
                prms.append(from_param)
        if to_param:
            if not re.match(_iso, to_param):
                raise ValidationError(
                    f"Invalid to_date value: {to_param!r} (expected YYYY-MM-DD)"
                )
            for clauses, prms in ((full_clauses, full_params), (trend_clauses, trend_params)):
                clauses.append("date <= %s::date")
                prms.append(to_param)
    elif allow_date and date_param:
        # Backward-compatible single exact date; the trend follows it as an
        # inclusive upper bound (up-to-that-date) so the line still spans days.
        if not re.match(_iso, date_param):
            raise ValidationError(f"Invalid date value: {date_param!r} (expected YYYY-MM-DD)")
        full_clauses.append("date = %s::date")
        full_params.append(date_param)
        trend_clauses.append("date <= %s::date")
        trend_params.append(date_param)

    trend_where_sql = ("WHERE " + " AND ".join(trend_clauses)) if trend_clauses else ""
    where_sql = ("WHERE " + " AND ".join(full_clauses)) if full_clauses else ""

    return where_sql, full_params, trend_where_sql, trend_params, {
        "year": year_param or None,
        "month": month_param or None,
        "date": date_param or None,
        "from_date": from_param or None,
        "to_date": to_param or None,
    }


def _ads_dashboard_payload(
    *,
    source: str,
    title: str,
    dimension_key: str,            # e.g. "portfolio_name" or "item"
    dimension_label: str,          # e.g. "Portfolios" or "Items"
    dimension_unmapped: str,       # e.g. "(Unassigned)" or "(Unmapped)"
    metric_specs: list[dict],
    default_metric_keys: list[str],
    default_visible_columns: list[str],
    spend_metric: str,             # which metric drives the trend chart's spend axis
    revenue_metric: str,           # which metric drives the trend chart's revenue axis
    where_sql: str,
    params: list,
    trend_where_sql: str,
    trend_params: list,
    filters: dict,
    allow_date_filter: bool = True,
    summary_max_date_keys: list | None = None,
    summary_use_max_date: bool = False,
) -> dict:
    # Comma-separated "expr AS \"key\"" for the SELECT list.
    metric_select_sql = ", ".join(
        f'{spec["expr"]} AS "{spec["key"]}"' for spec in metric_specs
    )

    # When the source stores cumulative range snapshots (each date is a running
    # month-to-date total), summing across dates over-counts every metric.
    # `summary_use_max_date` makes the summary/KPI totals reflect the latest
    # (max) date only — for EVERY metric, including the ratios: recomputing
    # e.g. SUM(gmv)/SUM(spend) at `date = max_date` yields the ratio of the
    # max-date totals, so ROAS/ACOS stay consistent with the spend/GMV shown.
    # Breakdown + trend are intentionally left as range data.
    if summary_use_max_date:
        summary_max_date_keys = [spec["key"] for spec in metric_specs]

    # Inline the unmapped-placeholder literal so we don't have to manage
    # param ordering with the GROUP BY.
    unmapped_lit = "'" + dimension_unmapped.replace("'", "''") + "'"
    dim_expr = f"COALESCE(NULLIF(TRIM({dimension_key}::text), ''), {unmapped_lit})"

    # 1) Summary (single row of aggregates)
    summary_rows = _dict_rows(
        f"""
        SELECT {metric_select_sql}, MAX(date) AS max_date
        FROM {source}
        {where_sql}
        """,
        params,
    )
    summary = dict(summary_rows[0]) if summary_rows else {s["key"]: 0 for s in metric_specs}
    max_date = summary.pop("max_date", None)
    if hasattr(max_date, "isoformat"):
        max_date = max_date.isoformat()

    # 1b) Max-date-only summary override. For range-snapshot sources (e.g. Zepto
    # ads), each date is a cumulative month-to-date snapshot, so SUMming across
    # dates over-counts. For the listed metric keys, replace the period sum with
    # the value from the latest (max) date only. Breakdown/trend are untouched.
    if summary_max_date_keys and max_date:
        md_specs = [s for s in metric_specs if s["key"] in summary_max_date_keys]
        if md_specs:
            md_select = ", ".join(f'{s["expr"]} AS "{s["key"]}"' for s in md_specs)
            md_where = (where_sql + " AND " if where_sql else "WHERE ") + "date = %s::date"
            md_rows = _dict_rows(
                f"SELECT {md_select} FROM {source} {md_where}",
                list(params) + [max_date],
            )
            if md_rows:
                for s in md_specs:
                    if s["key"] in md_rows[0]:
                        summary[s["key"]] = md_rows[0][s["key"]]

    # 2) Breakdown by dimension
    spend_alias = f'"{spend_metric}"'
    breakdown_rows = _dict_rows(
        f"""
        SELECT {dim_expr} AS dimension, {metric_select_sql}
        FROM {source}
        {where_sql}
        GROUP BY {dim_expr}
        ORDER BY {spend_alias} DESC NULLS LAST
        """,
        params,
    )

    # 3) Trend by date. Includes a value-per-metric column so the frontend can
    # render BOTH the dual-axis spend-vs-revenue chart AND a tiny sparkline
    # inside each KPI card without a second round-trip.
    #
    # Trend follows the date filter as an inclusive UPPER BOUND (up-to-that-date)
    # via `trend_where_sql`, so picking a date shrinks/grows the line instead of
    # collapsing it to a single point. Year / month filters still apply.
    spend_spec = next(s for s in metric_specs if s["key"] == spend_metric)
    revenue_spec = next(s for s in metric_specs if s["key"] == revenue_metric)
    trend_metric_select_sql = ", ".join(
        f'{spec["expr"]} AS "{spec["key"]}"' for spec in metric_specs
    )
    trend_rows = _dict_rows(
        f"""
        SELECT date,
               {spend_spec["expr"]}   AS spend,
               {revenue_spec["expr"]} AS revenue,
               {trend_metric_select_sql}
        FROM {source}
        {trend_where_sql}
        GROUP BY date
        ORDER BY date
        """,
        trend_params,
    )
    for r in trend_rows:
        if hasattr(r.get("date"), "isoformat"):
            r["date"] = r["date"].isoformat()

    # 4) Filter options — always global (ignore current filters so dropdowns
    #    always show every available choice).
    years = [
        int(r["year"])
        for r in _dict_rows(
            f"SELECT DISTINCT year FROM {source} WHERE year IS NOT NULL ORDER BY year DESC",
            [],
        )
    ]
    months = [
        r["month"]
        for r in _dict_rows(
            f"""
            SELECT DISTINCT month, MIN(date) AS sort_date
            FROM {source}
            WHERE month IS NOT NULL AND month <> ''
            GROUP BY month
            ORDER BY sort_date
            """,
            [],
        )
    ]
    dates = [
        r["date"].isoformat() if hasattr(r["date"], "isoformat") else r["date"]
        for r in _dict_rows(
            f"SELECT DISTINCT date FROM {source} WHERE date IS NOT NULL ORDER BY date DESC",
            [],
        )
    ]

    return {
        "source": source,
        "dashboard_title": title,
        "dimension_label": dimension_label,
        "dimension_key": dimension_key,
        "summary": summary,
        "available_metrics": [
            {"key": s["key"], "label": s["label"], "format": s["format"], "agg": s["agg"]}
            for s in metric_specs
        ],
        "default_metric_keys": default_metric_keys,
        "trend_axes": {
            "spend":   {"label": spend_spec["label"],   "format": spend_spec["format"]},
            "revenue": {"label": revenue_spec["label"], "format": revenue_spec["format"]},
        },
        "trend_rows": trend_rows,
        "breakdown_columns": [
            {
                "key": s["key"],
                "label": s["label"],
                "format": s["format"],
                "agg": s["agg"],
                "default_visible": s["key"] in default_visible_columns,
            }
            for s in metric_specs
        ],
        "breakdown_rows": breakdown_rows,
        "max_date": max_date,
        "filter_options": {"years": years, "months": months, "dates": dates},
        "filters": filters,
    }


# ─── Meta (Facebook / Instagram) ─────────────────────────────────────────────
# Source: meta_data. `date` is TEXT 'DD-MM-YYYY', so the max-date and month sort
# use to_date(date,'DD-MM-YYYY'); the year/month filter reuses _ads_build_where
# with allow_date=False (year/month are generated text columns). CPC/CPR/CPM/CTR
# are recomputed at the aggregate level (SUM/SUM) rather than summing the per-row
# generated columns.
_META_METRICS_SQL = """
    COALESCE(SUM(reach), 0)          AS reach,
    COALESCE(SUM(impressions), 0)    AS impressions,
    COALESCE(SUM(unique_clicks), 0)  AS link_clicks,
    COALESCE(SUM(amount_spent), 0)   AS amount_spent,
    CASE WHEN COALESCE(SUM(unique_clicks), 0) > 0
         THEN SUM(amount_spent)::numeric / SUM(unique_clicks) ELSE 0 END AS cpc,
    CASE WHEN COALESCE(SUM(reach), 0) > 0
         THEN SUM(amount_spent)::numeric / SUM(reach) * 1000 ELSE 0 END AS cpr,
    CASE WHEN COALESCE(SUM(impressions), 0) > 0
         THEN SUM(amount_spent)::numeric / SUM(impressions) * 1000 ELSE 0 END AS cpm,
    CASE WHEN COALESCE(SUM(impressions), 0) > 0
         THEN SUM(unique_clicks)::numeric / SUM(impressions) * 100 ELSE 0 END AS ctr
"""


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.meta")
def meta_dashboard(request):
    """Meta ads campaign dashboard (grand-total KPIs + per-campaign breakdown).

    Filters: ?year= & ?month= (name or 1-12). Returns the same metrics the sheet
    shows — Reach, Impressions, Link clicks, Amount spent, CPC, CPR, CPM, CTR —
    plus the max date and the year/month dropdown options.
    """
    # meta_data.year / .month are TEXT generated columns, so compare them as text
    # (the shared _ads_build_where casts year to int, which errors on a text column).
    _months = [
        "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
        "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
    ]
    year_param = (request.query_params.get("year") or "").strip()
    month_param = (request.query_params.get("month") or "").strip().upper()
    if month_param.isdigit():
        m = int(month_param)
        month_param = _months[m - 1] if 1 <= m <= 12 else ""
    clauses, params = [], []
    if year_param:
        clauses.append("year = %s")
        params.append(year_param)
    if month_param:
        clauses.append("month = %s")
        params.append(month_param)
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    filters = {"year": year_param or None, "month": month_param or None}

    summary_rows = _dict_rows(
        f"""
        SELECT {_META_METRICS_SQL},
               COUNT(DISTINCT campaign_name) AS campaigns,
               to_char(MAX(to_date(NULLIF(date, ''), 'DD-MM-YYYY')), 'DD-MM-YYYY') AS max_date
        FROM meta_data
        {where_sql}
        """,
        params,
    )
    summary = dict(summary_rows[0]) if summary_rows else {}
    max_date = summary.pop("max_date", None)

    rows = _dict_rows(
        f"""
        SELECT COALESCE(NULLIF(TRIM(campaign_name), ''), '(Unnamed)') AS campaign_name,
               MAX(campaign_status) AS campaign_status,
               {_META_METRICS_SQL}
        FROM meta_data
        {where_sql}
        GROUP BY 1
        ORDER BY amount_spent DESC NULLS LAST
        """,
        params,
    )

    years = [
        int(r["year"])
        for r in _dict_rows(
            "SELECT DISTINCT year FROM meta_data WHERE year ~ '^[0-9]+$' ORDER BY year DESC",
            [],
        )
    ]
    months = [
        r["month"]
        for r in _dict_rows(
            """
            SELECT DISTINCT month, MIN(to_date(NULLIF(date, ''), 'DD-MM-YYYY')) AS sort_date
            FROM meta_data
            WHERE month IS NOT NULL AND month <> ''
            GROUP BY month
            ORDER BY sort_date
            """,
            [],
        )
    ]

    return Response(
        {
            "summary": summary,
            "rows": rows,
            "max_date": max_date,
            "filter_options": {"years": years, "months": months},
            "filters": filters,
        }
    )


# ─── Amazon ──────────────────────────────────────────────────────────────────
# Source: amazon_ads_master (rich column set — total_cost, sales, impressions,
# clicks, purchases, units_sold, NTB variants, …). Dimension: portfolio_name.

_AMAZON_METRIC_SPECS = [
    {"key": "total_cost",     "label": "Total cost",   "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(total_cost), 0)"},
    {"key": "sales",          "label": "Sales",        "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(sales), 0)"},
    {"key": "roas",           "label": "ROAS",         "format": "ratio",   "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(total_cost), 0) > 0 "
             "THEN COALESCE(SUM(sales), 0)::numeric / SUM(total_cost) "
             "ELSE 0 END"},
    {"key": "acos",           "label": "ACOS",         "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(sales), 0) > 0 "
             "THEN COALESCE(SUM(total_cost), 0)::numeric / SUM(sales) * 100 "
             "ELSE 0 END"},
    {"key": "impressions",    "label": "Impressions",  "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(impressions), 0)"},
    {"key": "clicks",         "label": "Clicks",       "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(clicks), 0)"},
    {"key": "ctr",            "label": "CTR",          "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(impressions), 0) > 0 "
             "THEN COALESCE(SUM(clicks), 0)::numeric / SUM(impressions) * 100 "
             "ELSE 0 END"},
    {"key": "cpc",            "label": "CPC",          "format": "inr",     "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(clicks), 0) > 0 "
             "THEN COALESCE(SUM(total_cost), 0)::numeric / SUM(clicks) "
             "ELSE 0 END"},
    {"key": "purchases",      "label": "Purchases",    "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(purchases), 0)"},
    {"key": "units_sold",     "label": "Units sold",   "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(units_sold), 0)"},
    {"key": "ntb_orders",     "label": "NTB orders",   "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(purchases_ntb), 0)"},
    {"key": "ntb_sales",      "label": "NTB sales",    "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(sales_ntb), 0)"},
    {"key": "ntb_orders_pct", "label": "% orders NTB", "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(purchases), 0) > 0 "
             "THEN COALESCE(SUM(purchases_ntb), 0)::numeric / SUM(purchases) * 100 "
             "ELSE 0 END"},
    {"key": "ntb_sales_pct",  "label": "% sales NTB",  "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(sales), 0) > 0 "
             "THEN COALESCE(SUM(sales_ntb), 0)::numeric / SUM(sales) * 100 "
             "ELSE 0 END"},
    {"key": "detail_page_views", "label": "Detail page views", "format": "count", "agg": "sum",
     "expr": "COALESCE(SUM(detail_page_views), 0)"},
]


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.amazon_ads")
def amazon_ads_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "amazon":
        raise ValidationError("Amazon Ads Dashboard is available only for Amazon.")

    # Dimension switch — re-groups the whole sheet by Portfolio / Campaign / ASIN.
    # (ASIN lives in advertised_product_id; advertised_product_sku mixes ASINs and
    # free-text SKUs, so the product id is the reliable ASIN column.)
    dimension = str(request.query_params.get("dimension") or "portfolio").strip().lower()
    dim_map = {
        "portfolio": ("portfolio_name", "Portfolios"),
        "campaign": ("campaign_name", "Campaigns"),
        "asin": ("advertised_product_id", "ASINs"),
    }
    dimension_key, dimension_label = dim_map.get(dimension, dim_map["portfolio"])

    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="amazon_ads_master",
        # Range summary (sum across the selected period), not max-date snapshot.
        summary_use_max_date=False,
        title="AMS ADS Dashboard",
        dimension_key=dimension_key,
        dimension_label=dimension_label,
        dimension_unmapped="(Unassigned)",
        metric_specs=_AMAZON_METRIC_SPECS,
        default_metric_keys=["total_cost", "sales", "roas", "acos"],
        default_visible_columns=[
            "impressions", "clicks", "ctr", "total_cost", "cpc",
            "purchases", "sales", "acos", "roas",
        ],
        spend_metric="total_cost",
        revenue_metric="sales",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


# ─── Swiggy / Zepto / BigBasket / Blinkit ────────────────────────────────────
# Source views: swiggy_ads_master / zepto_ads_master / bigbasket_ads_master /
# blinkit_ads_master. Dimension: item (from master_sheet via the views).
#
# Swiggy / Zepto / BigBasket share a single GMV column. Blinkit keeps direct
# and indirect GMV separate. All share ad_spent, direct_qty_sold, ads_ltr_sold,
# impressions.

def _quick_commerce_metrics(*, gmv_field: str, include_indirect_qty: bool, include_indirect_gmv: bool):
    """Build metric_specs for Swiggy/Zepto/BigBasket/Blinkit. Single source of
    truth so the schema stays in lockstep across the four platforms."""
    gmv_label = "GMV" if gmv_field == "gmv" else "Direct GMV"
    # When indirect GMV is tracked separately (Blinkit only), the ROAS
    # numerator sums direct + indirect — both are attributed revenue and
    # excluding the halo understates ad efficiency. ACOS still uses
    # `gmv_field` alone (= direct), so ACOS is no longer the exact inverse
    # of ROAS for Blinkit — this is intentional. The other QC platforms
    # don't expose an indirect column, so their ROAS is unchanged.
    roas_numerator = (
        f"(COALESCE(SUM({gmv_field}), 0) + COALESCE(SUM(indirect_gmv), 0))"
        if include_indirect_gmv
        else f"COALESCE(SUM({gmv_field}), 0)"
    )
    specs = [
        {"key": "ad_spent",        "label": "Ad spent",        "format": "inr",     "agg": "sum",
         "expr": "COALESCE(SUM(ad_spent), 0)"},
        {"key": "gmv",             "label": gmv_label,         "format": "inr",     "agg": "sum",
         "expr": f"COALESCE(SUM({gmv_field}), 0)"},
        {"key": "roas",            "label": "ROAS",            "format": "ratio",   "agg": "avg",
         "expr": f"CASE WHEN COALESCE(SUM(ad_spent), 0) > 0 "
                 f"THEN {roas_numerator}::numeric / SUM(ad_spent) "
                 f"ELSE 0 END"},
        {"key": "acos",            "label": "ACOS",            "format": "percent", "agg": "avg",
         "expr": f"CASE WHEN COALESCE(SUM({gmv_field}), 0) > 0 "
                 f"THEN COALESCE(SUM(ad_spent), 0)::numeric / SUM({gmv_field}) * 100 "
                 f"ELSE 0 END"},
        {"key": "impressions",     "label": "Impressions",     "format": "count",   "agg": "sum",
         "expr": "COALESCE(SUM(impressions), 0)"},
        {"key": "direct_qty_sold", "label": "Direct qty sold", "format": "count",   "agg": "sum",
         "expr": "COALESCE(SUM(direct_qty_sold), 0)"},
    ]
    if include_indirect_qty:
        specs.append({"key": "indirect_qty_sold", "label": "Indirect qty sold", "format": "count", "agg": "sum",
                      "expr": "COALESCE(SUM(indirect_qty_sold), 0)"})
    if include_indirect_gmv:
        specs.append({"key": "indirect_gmv", "label": "Indirect GMV", "format": "inr", "agg": "sum",
                      "expr": "COALESCE(SUM(indirect_gmv), 0)"})
    specs.append({"key": "ads_ltr_sold", "label": "Ads litres sold", "format": "litres", "agg": "sum",
                  "expr": "COALESCE(SUM(ads_ltr_sold), 0)"})
    return specs


_QC_DEFAULT_METRIC_KEYS = ["ad_spent", "gmv", "roas", "acos"]
_QC_DEFAULT_VISIBLE_COLUMNS = [
    "impressions", "ad_spent", "direct_qty_sold", "gmv", "ads_ltr_sold", "roas", "acos",
]


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.swiggy_ads")
def swiggy_ads_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "swiggy":
        raise ValidationError("Swiggy Ads Dashboard is available only for Swiggy.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="swiggy_ads_master",
        summary_use_max_date=True,
        title="Swiggy ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        # swiggy_ads_master uses `direct_gmv` (not `gmv`) — alias it via the gmv_field arg.
        metric_specs=_quick_commerce_metrics(gmv_field="direct_gmv", include_indirect_qty=False, include_indirect_gmv=False),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=_QC_DEFAULT_VISIBLE_COLUMNS,
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.zepto_ads")
def zepto_ads_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "zepto":
        raise ValidationError("Zepto Ads Dashboard is available only for Zepto.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="zepto_ads_master",
        title="Zepto ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        metric_specs=_quick_commerce_metrics(gmv_field="gmv", include_indirect_qty=True, include_indirect_gmv=False),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=_QC_DEFAULT_VISIBLE_COLUMNS,
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
        # zepto_ads_master holds cumulative range snapshots — the summary/KPI
        # totals reflect the latest date only across ALL additive metrics, not
        # the sum across snapshots.
        summary_use_max_date=True,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.bb_ads")
def bigbasket_ads_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "bigbasket":
        raise ValidationError("BigBasket Ads Dashboard is available only for BigBasket.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="bigbasket_ads_master",
        summary_use_max_date=True,
        title="BigBasket ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        metric_specs=_quick_commerce_metrics(gmv_field="gmv", include_indirect_qty=True, include_indirect_gmv=False),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=_QC_DEFAULT_VISIBLE_COLUMNS,
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


# ── Daily ads dashboards (Swiggy / Zepto / BigBasket) ─────────────────────────
# Same payload as the range dashboards above, sourced from the *ads_daily copy
# tables' master views. Daily rows are per-day (non-cumulative), so the summary
# SUMs are correct as-is — no max-date override (unlike zepto_ads_master).

@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.swiggy_ads_daily")
def swiggy_ads_daily_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "swiggy":
        raise ValidationError("Swiggy Daily Ads Dashboard is available only for Swiggy.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="swiggyads_daily_master",
        title="Swiggy Daily ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        metric_specs=_quick_commerce_metrics(gmv_field="direct_gmv", include_indirect_qty=False, include_indirect_gmv=False),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=_QC_DEFAULT_VISIBLE_COLUMNS,
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.zepto_ads_daily")
def zepto_ads_daily_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "zepto":
        raise ValidationError("Zepto Daily Ads Dashboard is available only for Zepto.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="zeptoads_daily_master",
        title="Zepto Daily ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        metric_specs=_quick_commerce_metrics(gmv_field="gmv", include_indirect_qty=True, include_indirect_gmv=False),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=_QC_DEFAULT_VISIBLE_COLUMNS,
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.bb_ads_daily")
def bigbasket_ads_daily_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "bigbasket":
        raise ValidationError("BigBasket Daily Ads Dashboard is available only for BigBasket.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="bigbasketads_daily_master",
        title="BigBasket Daily ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        metric_specs=_quick_commerce_metrics(gmv_field="gmv", include_indirect_qty=True, include_indirect_gmv=False),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=_QC_DEFAULT_VISIBLE_COLUMNS,
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.blinkit_ads")
def blinkit_ads_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "blinkit":
        raise ValidationError("Blinkit Ads Dashboard is available only for Blinkit.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="blinkit_ads_master",
        # Range summary (sum across the selected period), not max-date snapshot.
        summary_use_max_date=False,
        title="Blinkit ADS Dashboard",
        dimension_key="item",
        dimension_label="Items",
        dimension_unmapped="(Unmapped)",
        # blinkit_ads_master has both `direct_gmv` and `indirect_gmv` — keep them separate.
        metric_specs=_quick_commerce_metrics(gmv_field="direct_gmv", include_indirect_qty=True, include_indirect_gmv=True),
        default_metric_keys=_QC_DEFAULT_METRIC_KEYS,
        default_visible_columns=[*_QC_DEFAULT_VISIBLE_COLUMNS, "indirect_gmv", "indirect_qty_sold"],
        spend_metric="ad_spent",
        revenue_metric="gmv",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


# ─── Marketing Ads Summary (cross-platform) ──────────────────────────────────
# Combines every platform's RANGE ads view into one normalised set so the
# Marketing → Ads → Summary dashboard can show grand totals plus a breakdown
# that regroups by item head / category / sub category / item / platform.
#
# Per-platform quirks folded in here so callers see one uniform shape:
#   * Swiggy has no indirect_qty_sold — only direct counts toward qty.
#   * Amazon uses units_sold / total_cost and has no `item`.
#   * Flipkart uses total_converted_units / ad_spend / views and has no
#     item_head / category / sub_category / item.
# Every source view carries year / month / date, so the shared _ads_build_where
# filter applies uniformly to the union.
_ADS_SUMMARY_UNION = """
    -- Ads sale = DIRECT ads qty × the SKU's basic_rate from monthly_landing_rate
    -- (matched on platform format + sku_code + the row's month). Indirect/halo
    -- qty is deliberately excluded from ads_sale for the QC platforms (it still
    -- counts in the qty column). The landing table has at most one rate per
    -- (format, sku, month) so the LEFT JOIN can't fan out; a missing rate →
    -- ads_sale 0.
    -- `use_max_date` mirrors each platform ads dashboard's summary method:
    -- TRUE  → cumulative month-to-date snapshot (Swiggy/Zepto/BigBasket/Flipkart)
    --         so the summary keeps ONLY the latest (max) date's rows;
    -- FALSE → per-day/range (Amazon/Blinkit) + brand fund + SecMaster → summed.
    -- Keep this in sync with summary_use_max_date on the per-platform dashboards.
    SELECT 'Blinkit'::text AS platform, b.item_head, b.category, b.sub_category, b.item,
           (COALESCE(b.direct_qty_sold, 0) + COALESCE(b.indirect_qty_sold, 0))::numeric AS qty,
           COALESCE(b.impressions, 0)::numeric AS impressions,
           COALESCE(b.ad_spent, 0)::numeric AS ad_spent,
           0::numeric AS brand_fund, 0::numeric AS sec_qty, 0::numeric AS sec_value,
           (COALESCE(b.direct_qty_sold, 0)
             * COALESCE(lr.basic_rate, 0))::numeric AS ads_sale,
           b.year, b.month, b.date, FALSE AS use_max_date, 'other'::text AS src
      FROM blinkit_ads_master b
      LEFT JOIN monthly_landing_rate lr
        ON REGEXP_REPLACE(LOWER(lr.format), '[^a-z0-9]+', '', 'g') = 'blinkit'
       AND UPPER(TRIM(lr.sku_code)) = UPPER(TRIM(b.format_sku_code))
       AND lr.month = to_char(date_trunc('month', b.date::date), 'YYYY-MM-DD')
    UNION ALL
    SELECT 'Zepto', z.item_head, z.category, z.sub_category, z.item,
           (COALESCE(z.direct_qty_sold, 0) + COALESCE(z.indirect_qty_sold, 0))::numeric,
           COALESCE(z.impressions, 0)::numeric, COALESCE(z.ad_spent, 0)::numeric,
           0::numeric, 0::numeric, 0::numeric,
           (COALESCE(z.direct_qty_sold, 0)
             * COALESCE(lr.basic_rate, 0))::numeric,
           z.year, z.month, z.date, TRUE, 'other'::text
      FROM zepto_ads_master z
      LEFT JOIN monthly_landing_rate lr
        ON REGEXP_REPLACE(LOWER(lr.format), '[^a-z0-9]+', '', 'g') = 'zepto'
       AND UPPER(TRIM(lr.sku_code)) = UPPER(TRIM(z.sku_id))
       AND lr.month = to_char(date_trunc('month', z.date::date), 'YYYY-MM-DD')
    UNION ALL
    SELECT 'BigBasket', bb.item_head, bb.category, bb.sub_category, bb.item,
           (COALESCE(bb.direct_qty_sold, 0) + COALESCE(bb.indirect_qty_sold, 0))::numeric,
           COALESCE(bb.impressions, 0)::numeric, COALESCE(bb.ad_spent, 0)::numeric,
           0::numeric, 0::numeric, 0::numeric,
           (COALESCE(bb.direct_qty_sold, 0)
             * COALESCE(lr.basic_rate, 0))::numeric,
           bb.year, bb.month, bb.date, TRUE, 'other'::text
      FROM bigbasket_ads_master bb
      LEFT JOIN monthly_landing_rate lr
        ON REGEXP_REPLACE(LOWER(lr.format), '[^a-z0-9]+', '', 'g') = 'bigbasket'
       AND UPPER(TRIM(lr.sku_code)) = UPPER(TRIM(bb.sku_id))
       AND lr.month = to_char(date_trunc('month', bb.date::date), 'YYYY-MM-DD')
    UNION ALL
    SELECT 'Swiggy', s.item_head, s.category, s.sub_category, s.item,
           COALESCE(s.direct_qty_sold, 0)::numeric,
           COALESCE(s.impressions, 0)::numeric, COALESCE(s.ad_spent, 0)::numeric,
           0::numeric, 0::numeric, 0::numeric,
           (COALESCE(s.direct_qty_sold, 0) * COALESCE(lr.basic_rate, 0))::numeric,
           s.year, s.month, s.date, TRUE, 'other'::text
      FROM swiggy_ads_master s
      LEFT JOIN monthly_landing_rate lr
        ON REGEXP_REPLACE(LOWER(lr.format), '[^a-z0-9]+', '', 'g') = 'swiggy'
       AND UPPER(TRIM(lr.sku_code)) = UPPER(TRIM(s.format_sku_code))
       AND lr.month = to_char(date_trunc('month', s.date::date), 'YYYY-MM-DD')
    UNION ALL
    SELECT 'Amazon', item_head, category, sub_category, NULL::text,
           COALESCE(units_sold, 0)::numeric,
           COALESCE(impressions, 0)::numeric, COALESCE(total_cost, 0)::numeric,
           -- Ads Sale for Amazon = the ads `sales` figure (Ads Dashboard "Sales" KPI).
           0::numeric, 0::numeric, 0::numeric, COALESCE(sales, 0)::numeric, year, month, date, FALSE, 'other'::text
      FROM amazon_ads_master
    UNION ALL
    SELECT 'Flipkart', NULL::text, NULL::text, NULL::text, NULL::text,
           COALESCE(total_converted_units, 0)::numeric,
           COALESCE(views, 0)::numeric, COALESCE(ad_spend, 0)::numeric,
           0::numeric, 0::numeric, 0::numeric,
           -- Flipkart has no SKU for the landing-rate ads_sale, but it reports a
           -- real revenue figure — use it directly as the Ads Sale value.
           -- use_max_date=TRUE: flipkart_ads_master is a CUMULATIVE month-to-date
           -- snapshot (each date is a running total), so summing across dates
           -- over-counts ~N×. Keep only the latest (max) date — the true month
           -- total — matching the Flipkart Ads Dashboard (summary_use_max_date).
           COALESCE(total_revenue, 0)::numeric, year, month, date, TRUE, 'flipkart_ads'::text
      FROM flipkart_ads_master
    UNION ALL
    -- Flipkart SKU-level ads from the FSN report — carries the item_head /
    -- category / sub_category / item mapping that flipkart_ads_master lacks.
    -- Tagged src='fsn' so it feeds ONLY the item/category/sub_category/item
    -- breakdowns; the Platform section keeps using flipkart_ads_master
    -- (src='flipkart_ads'). The FSN report has no period column, so it is
    -- attributed to the LATEST flipkart_ads period (single row → no fan-out) so
    -- it responds to the month filter in step with the Flipkart ads data.
    SELECT 'Flipkart', f.item_head, f.category, f.sub_category, f.item,
           (COALESCE(f.direct_units_sold, 0) + COALESCE(f.indirect_units_sold, 0))::numeric,
           COALESCE(f.views, 0)::numeric, COALESCE(f.ad_spend, 0)::numeric,
           0::numeric, 0::numeric, 0::numeric,
           COALESCE(f.total_revenue, 0)::numeric,
           fp.year, fp.month, fp.date, FALSE, 'fsn'::text
      FROM consolidated_fsn_report f
      CROSS JOIN (SELECT year, month, MAX(date) AS date
                    FROM flipkart_ads_master
                   GROUP BY year, month
                   ORDER BY MAX(date) DESC
                   LIMIT 1) fp
    UNION ALL
    -- Brand fund spend (no ad qty / impressions / ad spend) — folded in so the
    -- breakdown can show a Brand Fund column per dimension.
    SELECT 'Blinkit', item_head, category, sub_category, item,
           0::numeric, 0::numeric, 0::numeric,
           COALESCE(brand_fund_spent, 0)::numeric, 0::numeric, 0::numeric, 0::numeric, year, month, date, FALSE, 'other'::text
      FROM blinkit_brandfund_master
    UNION ALL
    SELECT 'Swiggy', item_head, category, sub_category, item,
           0::numeric, 0::numeric, 0::numeric,
           COALESCE(brand_fund_spent, 0)::numeric, 0::numeric, 0::numeric, 0::numeric, year, month, date, FALSE, 'other'::text
      FROM swiggy_brandfund_master
    UNION ALL
    SELECT 'Zepto', item_head, category, sub_category, item,
           0::numeric, 0::numeric, 0::numeric,
           COALESCE(brand_fund_spent, 0)::numeric, 0::numeric, 0::numeric, 0::numeric, year, month, date, FALSE, 'other'::text
      FROM zepto_brandfund_master
    UNION ALL
    SELECT 'Amazon', item_head, category, sub_category, NULL::text,
           0::numeric, 0::numeric, 0::numeric,
           COALESCE(budget_spent, 0)::numeric, 0::numeric, 0::numeric, 0::numeric, year, month, date, FALSE, 'other'::text
      FROM amazon_coupon_master
    UNION ALL
    -- Amazon delivered quantity + delivered sale from the DAILY master view
    -- (per-day rows → summed across the range). sec_qty = shipped_units (matches
    -- the Secondary Dashboard's "Deliver Quantity"); sec_value = shipped_revenue_2
    -- feeds the Total Sale column.
    SELECT 'Amazon', item_head, category, sub_category, item,
           0::numeric, 0::numeric, 0::numeric, 0::numeric,
           COALESCE(shipped_units, 0)::numeric,
           COALESCE(shipped_revenue_2, 0)::numeric, 0::numeric,
           year, month, to_date::date, FALSE, 'other'::text
      FROM amazon_sec_daily_master_view
    UNION ALL
    -- Secondary sell-out from SecMaster (no ad metrics) — folded in for the
    -- "Total Qty Delivered" (quantity) and "Total Sale" (amount) columns.
    -- `format` is mapped to the same platform labels so Platform group-by lines up.
    SELECT CASE UPPER(TRIM(format::text))
                WHEN 'BLINKIT' THEN 'Blinkit'
                WHEN 'ZEPTO' THEN 'Zepto'
                WHEN 'BIG BASKET' THEN 'BigBasket'
                WHEN 'SWIGGY' THEN 'Swiggy'
                WHEN 'FLIPKART' THEN 'Flipkart'
                WHEN 'JIO MART' THEN 'Jio Mart'
                ELSE INITCAP(TRIM(format::text))
           END,
           item_head, category, sub_category, item,
           0::numeric, 0::numeric, 0::numeric, 0::numeric,
           -- Total Qty Delivered = quantity. Total Sale = sales_amt_exc
           -- (tax-exclusive) for every QC platform EXCEPT Flipkart, which uses
           -- `amount` (its sales_amt_exc is 0 in SecMaster).
           COALESCE(quantity, 0)::numeric,
           CASE WHEN UPPER(TRIM(format::text)) = 'FLIPKART'
                THEN COALESCE(amount, 0)
                ELSE COALESCE(sales_amt_exc, 0) END::numeric,
           0::numeric, year, month, date, FALSE, 'other'::text
      FROM secmaster_mv
"""

# group_by key -> (column expression, display label). 'platform' groups by the
# union's synthetic platform label; the rest group by the named dimension column.
_ADS_SUMMARY_DIMENSIONS = [
    ("item_head", "Item Head"),
    ("category", "Category"),
    ("sub_category", "Sub Category"),
    ("item", "Item"),
    ("platform", "Platform"),
]
_ADS_SUMMARY_DIMENSION_KEYS = {key for key, _ in _ADS_SUMMARY_DIMENSIONS}


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=600, prefix="plat.ads_summary")
def marketing_ads_summary(request):
    """Cross-platform ads summary over the range ads views.

    Returns KPI grand totals (qty sold, impressions, ad spent) plus a breakdown
    table grouped by the `group_by` dimension (item_head | category |
    sub_category | item | platform), filtered by the shared year/month/date
    params. qty sold already folds direct + indirect per platform.
    """
    group_by = (request.query_params.get("group_by") or "item_head").strip().lower()
    if group_by not in _ADS_SUMMARY_DIMENSION_KEYS:
        raise ValidationError(
            f"Invalid group_by: {group_by!r} (expected one of "
            f"{sorted(_ADS_SUMMARY_DIMENSION_KEYS)})"
        )
    where_sql, params, _t_sql, _t_params, filters = _ads_build_where(request, allow_date=True)
    params = list(params)

    # Optional platform filter. The frontend sends a platform slug (e.g.
    # 'bigbasket'); the union exposes a synthetic label ('BigBasket'). Compare on
    # a normalized form (lowercase, alphanumerics only) so 'bigbasket' matches
    # 'BigBasket' and 'big basket' alike. Unknown / empty → no platform filter
    # (all platforms combined, the default).
    platform_param = (request.query_params.get("platform") or "").strip()
    if platform_param:
        norm = re.sub(r"[^a-z0-9]+", "", platform_param.lower())
        clause = "REGEXP_REPLACE(LOWER(platform), '[^a-z0-9]+', '', 'g') = %s"
        where_sql = f"{where_sql} AND {clause}" if where_sql else f"WHERE {clause}"
        params.append(norm)
    filters["platform"] = platform_param or None

    # Compute the breakdown for EVERY dimension in ONE request so the frontend
    # can switch group-by tabs instantly (client-side, no refetch). The 11-view
    # union is the only expensive part, so it runs ONCE via a MATERIALIZED CTE;
    # each dimension is then a cheap GROUP BY over the cached CTE rows.
    metric_sums = (
        "COALESCE(SUM(qty), 0), COALESCE(SUM(impressions), 0), "
        "COALESCE(SUM(ad_spent), 0), COALESCE(SUM(brand_fund), 0), "
        "COALESCE(SUM(sec_qty), 0), COALESCE(SUM(sec_value), 0), "
        "COALESCE(SUM(ads_sale), 0)"
    )
    dim_selects = []
    for key, _label in _ADS_SUMMARY_DIMENSIONS:
        grp = (
            "platform"
            if key == "platform"
            else f"COALESCE(NULLIF(TRIM({key}::text), ''), '(Unmapped)')"
        )
        # Flipkart source split: the Platform section keeps the campaign-level
        # flipkart_ads_master rows (exclude the FSN rows); the item/category/etc.
        # breakdowns use the SKU-mapped FSN rows instead (exclude the dimensionless
        # flipkart_ads rows). All non-Flipkart sources are tagged 'other' and pass
        # both filters unchanged.
        src_filter = "src <> 'fsn'" if key == "platform" else "src <> 'flipkart_ads'"
        dim_selects.append(
            f"SELECT '{key}' AS dim, {grp} AS grp, {metric_sums} "
            f"FROM adscte WHERE {src_filter} GROUP BY {grp}"
        )
    # Pre-aggregate the union to the dashboard's grain (platform + the 4 SKU
    # dimensions) inside the CTE. This collapses secmaster's unused city/date
    # grain (~830k rows → a few hundred) ONCE, so the 5 per-dimension GROUP BYs
    # below run over a tiny set instead of re-scanning the full union 5×.
    # `scoped` keeps each platform's summary grain: the max-date platforms
    # (use_max_date=TRUE — cumulative month-to-date snapshots) contribute ONLY
    # their latest date's rows, matching those platforms' ads dashboards; the
    # range platforms + brand fund + SecMaster keep every row and are summed.
    sql = (
        f"WITH scoped AS (SELECT u.*, "
        f"MAX(CASE WHEN u.use_max_date THEN u.date END) "
        f"OVER (PARTITION BY u.platform) AS __pmd "
        f"FROM ({_ADS_SUMMARY_UNION}) u {where_sql}), "
        f"adscte AS MATERIALIZED (SELECT platform, item_head, category, "
        f"sub_category, item, src, SUM(qty) AS qty, SUM(impressions) AS impressions, "
        f"SUM(ad_spent) AS ad_spent, SUM(brand_fund) AS brand_fund, "
        f"SUM(sec_qty) AS sec_qty, SUM(sec_value) AS sec_value, "
        f"SUM(ads_sale) AS ads_sale FROM scoped "
        f"WHERE NOT use_max_date OR date = __pmd "
        f"GROUP BY platform, item_head, category, sub_category, item, src) "
        + " UNION ALL ".join(dim_selects)
    )

    breakdowns = {key: [] for key, _ in _ADS_SUMMARY_DIMENSIONS}
    with connection.cursor() as cur:
        cur.execute(sql, list(params))
        for r in cur.fetchall():
            breakdowns[r[0]].append({
                "group": r[1],
                "qty_sold": float(r[2]),
                "impressions": float(r[3]),
                "ad_spent": float(r[4]),
                "brand_fund": float(r[5]),
                "sec_qty": float(r[6]),
                "sec_value": float(r[7]),
                "ads_sale": float(r[8]),
            })

    # Default display order: highest ad spend first (the table re-sorts on click).
    for lst in breakdowns.values():
        lst.sort(key=lambda d: (d["ad_spent"], d["qty_sold"]), reverse=True)

    metric_keys = (
        "qty_sold", "impressions", "ad_spent",
        "brand_fund", "sec_qty", "sec_value", "ads_sale",
    )
    # Grand totals: sum the Platform breakdown. It keeps Flipkart on the
    # campaign-level flipkart_ads_master rows (the item/category/etc. breakdowns
    # swap Flipkart to the FSN source, which has different totals), so the KPI
    # cards stay in step with the Platform section and the per-platform dashboards.
    any_rows = breakdowns["platform"]
    totals = {k: sum(r[k] for r in any_rows) for k in metric_keys}

    return Response({
        "totals": totals,
        "group_by": group_by,
        "dimensions": [{"key": k, "label": l} for k, l in _ADS_SUMMARY_DIMENSIONS],
        "breakdowns": breakdowns,
        "rows": breakdowns.get(group_by, any_rows),
        "filters": filters,
    })


# ─── Flipkart ────────────────────────────────────────────────────────────────
# Source: flipkart_ads_master (= flipkart_ads + derived year/month). Dimension:
# campaign_name — Flipkart ads are campaign-level, no SKU dimension.

_FLIPKART_METRIC_SPECS = [
    {"key": "ad_spend",        "label": "Ad spend",       "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(ad_spend), 0)"},
    {"key": "revenue",         "label": "Revenue",        "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(total_revenue), 0)"},
    {"key": "roi",             "label": "ROI",            "format": "ratio",   "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(ad_spend), 0) > 0 "
             "THEN COALESCE(SUM(total_revenue), 0)::numeric / SUM(ad_spend) "
             "ELSE 0 END"},
    {"key": "acos",            "label": "ACOS",           "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(total_revenue), 0) > 0 "
             "THEN COALESCE(SUM(ad_spend), 0)::numeric / SUM(total_revenue) * 100 "
             "ELSE 0 END"},
    {"key": "views",           "label": "Views",          "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(views), 0)"},
    {"key": "clicks",          "label": "Clicks",         "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(clicks), 0)"},
    {"key": "ctr",             "label": "CTR",            "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(views), 0) > 0 "
             "THEN COALESCE(SUM(clicks), 0)::numeric / SUM(views) * 100 "
             "ELSE 0 END"},
    {"key": "cpc",             "label": "CPC",            "format": "inr",     "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(clicks), 0) > 0 "
             "THEN COALESCE(SUM(ad_spend), 0)::numeric / SUM(clicks) "
             "ELSE 0 END"},
    {"key": "units_sold",      "label": "Units sold",     "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(total_converted_units), 0)"},
    {"key": "cvr",             "label": "CVR",            "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(clicks), 0) > 0 "
             "THEN COALESCE(SUM(total_converted_units), 0)::numeric / SUM(clicks) * 100 "
             "ELSE 0 END"},
    {"key": "campaign_budget", "label": "Campaign budget","format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(campaign_budget), 0)"},
]


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.fk_ads")
def flipkart_ads_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "flipkart":
        raise ValidationError("Flipkart Ads Dashboard is available only for Flipkart.")
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(request, allow_date=True)
    return Response(_ads_dashboard_payload(
        source="flipkart_ads_master",
        # Range summary (sum across the selected period), not max-date snapshot.
        summary_use_max_date=False,
        title="Flipkart ADS Dashboard",
        dimension_key="campaign_name",
        dimension_label="Campaigns",
        dimension_unmapped="(Unnamed)",
        metric_specs=_FLIPKART_METRIC_SPECS,
        default_metric_keys=["ad_spend", "revenue", "roi", "acos"],
        default_visible_columns=[
            "ad_spend", "views", "clicks", "ctr", "cpc",
            "units_sold", "cvr", "revenue", "roi",
        ],
        spend_metric="ad_spend",
        revenue_metric="revenue",
        where_sql=where_sql,
        params=params,
        trend_where_sql=trend_where_sql,
        trend_params=trend_params,
        filters=filters,
    ))


# ─── Flipkart FSN Dashboard ──────────────────────────────────────────────────
# Source: consolidated_fsn_report (the Flipkart Consolidated FSN Report upload).
# It has NO date column, so this dashboard has no date filters / time trend; the
# "Ad spend vs Revenue" chart is a per-sub-category bar comparison instead, and
# the breakdown table can be re-grouped by item / sub_category / category /
# item_head / campaign via the `dimension` query param.

_FSN_METRIC_SPECS = [
    {"key": "ad_spend",   "label": "Ad spend",     "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(ad_spend), 0)"},
    {"key": "revenue",    "label": "Revenue",      "format": "inr",     "agg": "sum",
     "expr": "COALESCE(SUM(total_revenue), 0)"},
    {"key": "roi",        "label": "ROI",          "format": "ratio",   "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(ad_spend), 0) > 0 "
             "THEN COALESCE(SUM(total_revenue), 0)::numeric / SUM(ad_spend) ELSE 0 END"},
    {"key": "acos",       "label": "ACOS",         "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(total_revenue), 0) > 0 "
             "THEN COALESCE(SUM(ad_spend), 0)::numeric / SUM(total_revenue) * 100 ELSE 0 END"},
    {"key": "views",      "label": "Views",        "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(views), 0)"},
    {"key": "clicks",     "label": "Clicks",       "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(clicks), 0)"},
    {"key": "ctr",        "label": "CTR",          "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(views), 0) > 0 "
             "THEN COALESCE(SUM(clicks), 0)::numeric / SUM(views) * 100 ELSE 0 END"},
    {"key": "cpc",        "label": "CPC",          "format": "inr",     "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(clicks), 0) > 0 "
             "THEN COALESCE(SUM(ad_spend), 0)::numeric / SUM(clicks) ELSE 0 END"},
    {"key": "units_sold", "label": "Units sold",   "format": "count",   "agg": "sum",
     "expr": "COALESCE(SUM(direct_units_sold), 0) + COALESCE(SUM(indirect_units_sold), 0)"},
    {"key": "direct_units", "label": "Direct units", "format": "count", "agg": "sum",
     "expr": "COALESCE(SUM(direct_units_sold), 0)"},
    {"key": "cvr",        "label": "CVR",          "format": "percent", "agg": "avg",
     "expr": "CASE WHEN COALESCE(SUM(clicks), 0) > 0 "
             "THEN COALESCE(SUM(direct_units_sold), 0)::numeric / SUM(clicks) * 100 ELSE 0 END"},
]

# dimension query value -> (table column, human label)
_FSN_DIMENSIONS = {
    "item":         ("item",          "Items"),
    "sub_category": ("sub_category",  "Sub Categories"),
    "category":     ("category",      "Categories"),
    "item_head":    ("item_head",     "Item Heads"),
    "campaign_name": ("campaign_name", "Campaigns"),
}


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.fk_fsn")
def flipkart_fsn_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "flipkart":
        raise ValidationError("Flipkart FSN Dashboard is available only for Flipkart.")

    dim_param = (request.GET.get("dimension") or "item").strip()
    dim_col, dim_label = _FSN_DIMENSIONS.get(dim_param, _FSN_DIMENSIONS["item"])

    source = "consolidated_fsn_report"
    where_sql = "WHERE UPPER(TRIM(format::text)) = 'FLIPKART'"
    metric_select_sql = ", ".join(
        f'{s["expr"]} AS "{s["key"]}"' for s in _FSN_METRIC_SPECS
    )

    # 1) Summary (single row of totals).
    summary_rows = _dict_rows(
        f"SELECT {metric_select_sql} FROM {source} {where_sql}", []
    )
    summary = dict(summary_rows[0]) if summary_rows else {s["key"]: 0 for s in _FSN_METRIC_SPECS}

    # 2) Breakdown by the chosen dimension.
    dim_expr = f"COALESCE(NULLIF(TRIM({dim_col}::text), ''), '(Unmapped)')"
    breakdown_rows = _dict_rows(
        f"SELECT {dim_expr} AS dimension, {metric_select_sql} "
        f"FROM {source} {where_sql} GROUP BY {dim_expr} "
        f"ORDER BY COALESCE(SUM(ad_spend), 0) DESC",
        [],
    )

    # 3) Chart — Ad spend vs Revenue across sub-categories (top 12 by spend).
    subcat_expr = "COALESCE(NULLIF(TRIM(sub_category::text), ''), '(Unmapped)')"
    trend_rows = _dict_rows(
        f"SELECT {subcat_expr} AS label, "
        f"COALESCE(SUM(ad_spend), 0) AS spend, "
        f"COALESCE(SUM(total_revenue), 0) AS revenue, {metric_select_sql} "
        f"FROM {source} {where_sql} GROUP BY {subcat_expr} "
        f"ORDER BY COALESCE(SUM(ad_spend), 0) DESC LIMIT 12",
        [],
    )

    # The FSN report now carries a user-entered `date` (captured at upload —
    # the file itself has no date). Surface it as MONTH / YEAR / MAX DATE so the
    # dashboard shows which period the report is for. FSN is a single consolidated
    # snapshot (the uploader wipes + reloads), so all rows share one date; take
    # the latest.
    try:
        fsn_period_rows = _dict_rows(
            "SELECT to_char(date, 'YYYY-MM-DD') AS date, "
            "UPPER(TO_CHAR(date, 'FMMonth')) AS month, "
            "EXTRACT(YEAR FROM date)::int AS year "
            "FROM consolidated_fsn_report "
            "WHERE UPPER(TRIM(format::text)) = 'FLIPKART' AND date IS NOT NULL "
            "ORDER BY date DESC LIMIT 1",
            [],
        )
    except Exception:
        # `date` column may not exist yet (migration 0068 not applied) — degrade
        # gracefully to "no period" instead of failing the whole dashboard.
        fsn_period_rows = []
    fsn_period = fsn_period_rows[0] if fsn_period_rows else {}
    fsn_date = fsn_period.get("date")
    fsn_month = str(fsn_period["month"]).upper() if fsn_period.get("month") else None
    fsn_year = str(fsn_period["year"]) if fsn_period.get("year") is not None else None

    default_visible = {
        "ad_spend", "revenue", "roi", "views", "clicks",
        "ctr", "cpc", "units_sold", "cvr",
    }
    return Response({
        "source": source,
        "dashboard_title": "Flipkart FSN Dashboard",
        "dimension_label": dim_label,
        "dimension_key": dim_param,
        "summary": summary,
        "available_metrics": [
            {"key": s["key"], "label": s["label"], "format": s["format"], "agg": s["agg"]}
            for s in _FSN_METRIC_SPECS
        ],
        "default_metric_keys": ["ad_spend", "revenue", "roi", "acos"],
        "trend_axes": {
            "spend":   {"label": "Ad spend", "format": "inr"},
            "revenue": {"label": "Revenue",  "format": "inr"},
        },
        "trend_rows": trend_rows,
        "breakdown_columns": [
            {"key": s["key"], "label": s["label"], "format": s["format"], "agg": s["agg"],
             "default_visible": s["key"] in default_visible}
            for s in _FSN_METRIC_SPECS
        ],
        "breakdown_rows": breakdown_rows,
        "max_date": fsn_date,
        "filter_options": {
            "years": [fsn_year] if fsn_year else [],
            "months": [fsn_month] if fsn_month else [],
            "dates": [fsn_date] if fsn_date else [],
        },
        "filters": (
            {"month": fsn_month, "year": fsn_year} if fsn_month and fsn_year else {}
        ),
    })


# ─── Brand Fund Dashboards (Blinkit / Swiggy / Zepto) ────────────────────────
# Source views: blinkit_brandfund_master / swiggy_brandfund_master /
# zepto_brandfund_master. All three share the same column shape:
#   date, sku_id, sku_name, format, brand_fund_spent,
#   category, sub_category, item, item_head, month, year, month_day
#
# Payload shape (one shell renders all three):
#   {
#     "source": <view name>,
#     "dashboard_title": <string>,
#     "summary": {"total_brand_fund": <number>},
#     "item_rows":        [{dimension, total}, ...],   # grouped by `item`
#     "subcategory_rows": [{dimension, total}, ...],   # grouped by `sub_category`
#     "trend_axes": {"spend": {label, format}},        # single-line chart axis
#     "trend_rows": [{date, spend}, ...],              # brand fund spent per day
#     "max_date": <iso date>,
#     "filter_options": {years, months, dates},
#     "filters": {year, month, date},
#   }

def _brandfund_dashboard_payload(*, source: str, title: str, request) -> dict:
    where_sql, params, trend_where_sql, trend_params, filters = _ads_build_where(
        request, allow_date=True,
    )

    # 1) Summary — single number, plus max_date for the header chip.
    summary_rows = _dict_rows(
        f"""
        SELECT COALESCE(SUM(brand_fund_spent), 0) AS total_brand_fund,
               MAX(date) AS max_date
        FROM {source}
        {where_sql}
        """,
        params,
    )
    summary = dict(summary_rows[0]) if summary_rows else {"total_brand_fund": 0, "max_date": None}
    max_date = summary.pop("max_date", None)
    if hasattr(max_date, "isoformat"):
        max_date = max_date.isoformat()

    # 2) Breakdown rows for each grouping. Unmapped/null dimensions collapse
    #    into a single "(Unmapped)" bucket so the row count stays meaningful.
    def _grouped(dim_col: str):
        dim_expr = f"COALESCE(NULLIF(TRIM({dim_col}::text), ''), '(Unmapped)')"
        return _dict_rows(
            f"""
            SELECT {dim_expr} AS dimension,
                   COALESCE(SUM(brand_fund_spent), 0) AS total
            FROM {source}
            {where_sql}
            GROUP BY {dim_expr}
            ORDER BY total DESC NULLS LAST
            """,
            params,
        )

    item_rows = _grouped("item")
    subcategory_rows = _grouped("sub_category")

    # 2b) Day-by-day trend — total brand fund spent per date (single line).
    #     Mirrors the ADS dashboards: the date filter acts as an inclusive UPPER
    #     BOUND (up-to-that-date) via `trend_where_sql`, so picking a date shows
    #     the series up to that day instead of collapsing it to a single point.
    #     Year / month filters still apply.
    trend_rows = _dict_rows(
        f"""
        SELECT date,
               COALESCE(SUM(brand_fund_spent), 0) AS spend
        FROM {source}
        {trend_where_sql}
        GROUP BY date
        ORDER BY date
        """,
        trend_params,
    )
    for r in trend_rows:
        if hasattr(r.get("date"), "isoformat"):
            r["date"] = r["date"].isoformat()

    # 3) Filter options — global (ignore current filters so dropdowns always
    #    show every available choice).
    years = [
        int(r["year"])
        for r in _dict_rows(
            f"SELECT DISTINCT year FROM {source} WHERE year IS NOT NULL ORDER BY year DESC",
            [],
        )
    ]
    months = [
        r["month"]
        for r in _dict_rows(
            f"""
            SELECT DISTINCT month, MIN(date) AS sort_date
            FROM {source}
            WHERE month IS NOT NULL AND month <> ''
            GROUP BY month
            ORDER BY sort_date
            """,
            [],
        )
    ]
    dates = [
        r["date"].isoformat() if hasattr(r["date"], "isoformat") else r["date"]
        for r in _dict_rows(
            f"SELECT DISTINCT date FROM {source} WHERE date IS NOT NULL ORDER BY date DESC",
            [],
        )
    ]

    return {
        "source": source,
        "dashboard_title": title,
        "summary": summary,
        "item_rows": item_rows,
        "subcategory_rows": subcategory_rows,
        "trend_axes": {"spend": {"label": "Brand Fund Spent", "format": "inr"}},
        "trend_rows": trend_rows,
        "max_date": max_date,
        "filter_options": {"years": years, "months": months, "dates": dates},
        "filters": filters,
    }


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.blinkit_bf")
def blinkit_brandfund_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "blinkit":
        raise ValidationError("Blinkit Brand Fund Dashboard is available only for Blinkit.")
    return Response(_brandfund_dashboard_payload(
        source="blinkit_brandfund_master",
        title="Blinkit Brand Fund Dashboard",
        request=request,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.swiggy_bf")
def swiggy_brandfund_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "swiggy":
        raise ValidationError("Swiggy Brand Fund Dashboard is available only for Swiggy.")
    return Response(_brandfund_dashboard_payload(
        source="swiggy_brandfund_master",
        title="Swiggy Brand Fund Dashboard",
        request=request,
    ))


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.zepto_bf")
def zepto_brandfund_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "zepto":
        raise ValidationError("Zepto Brand Fund Dashboard is available only for Zepto.")
    return Response(_brandfund_dashboard_payload(
        source="zepto_brandfund_master",
        title="Zepto Brand Fund Dashboard",
        request=request,
    ))


# ─── Monthly Landing Rate ───
# Single shared table `monthly_landing_rate` with columns:
#   sku_code, sku_name, landing_rate, basic_rate, format, month
# `format` partitions rows per-platform.
# Only INSERTs are performed — prior rows are kept as history.

_LANDING_PLATFORMS = {
    "blinkit",
    "zepto",
    "swiggy",
    "bigbasket",
    "flipkart_grocery",
}

_LANDING_PLATFORM_LABELS = "blinkit, zepto, swiggy, bigbasket, flipkart_grocery"


def _format_for(p: PlatformConfig) -> str:
    # Store canonical platform formats in uppercase, matching the source
    # sheets/tables convention: BLINKIT, BIG BASKET, FLIPKART GROCERY, etc.
    return (p.po_filter_value or p.slug).strip().upper()


def _format_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _format_match_clause(p: PlatformConfig) -> tuple[str, list]:
    aliases = {
        _format_key(p.po_filter_value),
        _format_key(p.slug),
        _format_key(p.name),
    }
    aliases.discard("")
    placeholders = ", ".join(["%s"] * len(aliases))
    return (
        "REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g') "
        f"IN ({placeholders})",
        sorted(aliases),
    )


def _parse_month(val: str) -> str | None:
    """Accept `YYYY-MM` or `YYYY-MM-DD`, normalize to first-of-month `YYYY-MM-01`."""
    if not val:
        return None
    val = val.strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}", val):
            y, m = val.split("-")
            return f"{y}-{m}-01"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", val):
            y, m, _ = val.split("-")
            return f"{y}-{m}-01"
    except Exception:
        return None
    return None


def _decimal_input(value, field: str) -> Decimal:
    if value is None or str(value).strip() == "":
        raise ValidationError(f"{field} must be numeric.")
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        raise ValidationError(f"{field} must be numeric.")


def _landing_basic_rate(body, landing_rate: Decimal) -> Decimal:
    manual = body.get("manual_basic_rate") in (True, "true", "True", "1", 1)
    if manual:
        return _decimal_input(body.get("basic_rate"), "basic_rate")
    return landing_rate / _LANDING_BASIC_DIVISOR


# --- Secondary Dashboards ---

_FK_GROCERY_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_FK_GROCERY_SEC_DETAIL_ROWS = (
    ("PREMIUM", "CANOLA", "CANOLA", "1 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "5 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "1 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "5 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "4 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "1 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "4 LTR"),
    ("OTHER", "DRINKS", "APPLE SF", "200 MLS"),
    ("OTHER", "DRINKS", "BLUEBERRY", "200 MLS"),
    ("OTHER", "DRINKS", "GINGER ALE SF", "200 MLS"),
    ("OTHER", "DRINKS", "JEERA", "160 MLS"),
    ("OTHER", "DRINKS", "JEERA SF", "200 MLS"),
    ("OTHER", "DRINKS", "MANGO", "500 MLS"),
    ("OTHER", "DRINKS", "MINERAL WATER", "1 LTR"),
    ("OTHER", "DRINKS", "MOJITO", "200 MLS"),
)

_BLINKIT_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_BLINKIT_SEC_DETAIL_ROWS = (
    ("PREMIUM", "CANOLA", "CANOLA", "1 LTR", 12644),
    ("PREMIUM", "CANOLA", "CANOLA", "5 LTR", 6255),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "1 LTR", 3948),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR", 5232),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR", 12774),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR", 3740),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR", 15382),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR", 13310),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "1 LTR", 2914),
)

_SWIGGY_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_SWIGGY_SEC_DETAIL_ROWS = (
    ("PREMIUM", "GROUNDNUT", "GROUNDNUT", "1 LTR"),
    ("PREMIUM", "GROUNDNUT", "GROUNDNUT", "5 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "1 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "6 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "5 LTR"),
    ("PREMIUM", "BLENDED", "SO OLIVE", "1 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "250 MLS"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "1 LTR"),
    ("PREMIUM", "BLENDED", "SO OLIVE", "5 LTR"),
    ("PREMIUM", "GHEE", "DESI GHEE", "1 LTR"),
    ("OTHER", "DRINKS", "BLUEBERRY", "200 MLS"),
    ("OTHER", "DRINKS", "JEERA", "160 MLS"),
    ("OTHER", "DRINKS", "MINERAL WATER", "1 LTR"),
    ("OTHER", "DRINKS", "MOJITO", "200 MLS"),
    ("OTHER", "DRINKS", "SODA", "750 MLS"),
    ("OTHER", "DRINKS", "TONIC WATER", "200 MLS"),
    ("COMMODITY", "BLENDED", "GOLD", "1 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "1 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "5 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "5 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR"),
    ("COMMODITY", "RICE BRAN", "RICE BRAN", "1 LTR"),
    ("COMMODITY", "RICE BRAN", "RICE BRAN", "5 LTR"),
)

_ZEPTO_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_ZEPTO_SEC_DETAIL_ROWS = (
    ("PREMIUM", "GROUNDNUT", "GROUNDNUT", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "2 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "5 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "5 LTR"),
    ("PREMIUM", "BLENDED", "SO OLIVE", "1 LTR"),
    ("PREMIUM", "BLENDED", "SO OLIVE", "5 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "1 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "15 LTR"),
    ("PREMIUM", "GHEE", "A2 GHEE", "1 LTR"),
    ("PREMIUM", "GHEE", "A2 GHEE", "500 MLS"),
    ("PREMIUM", "GROUNDNUT", "GROUNDNUT", "5 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "5 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "1 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR"),
    ("COMMODITY", "RICE BRAN", "RICE BRAN", "1 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "1 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "5 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "15 LTR"),
    ("COMMODITY", "RICE BRAN", "RICE BRAN", "5 LTR"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "15 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "15 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "5 LTR"),
    ("OTHER", "DRINKS", "ENERGY DRINK SF", "200 MLS"),
    ("OTHER", "DRINKS", "JEERA", "160 MLS"),
    ("OTHER", "DRINKS", "MANGO", "500 MLS"),
    ("OTHER", "DRINKS", "MOJITO", "200 MLS"),
    ("OTHER", "DRINKS", "SODA", "750 MLS"),
)

_ZEPTO_PRIMARY_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_ZEPTO_PRIMARY_DETAIL_ROWS = (
    ("ZEPTO", "PREMIUM", "BLENDED", "SO OLIVE", "1 LTR"),
    ("ZEPTO", "PREMIUM", "BLENDED", "SO OLIVE", "5 LTR"),
    ("ZEPTO", "PREMIUM", "CANOLA", "CANOLA", "1 LTR"),
    ("ZEPTO", "PREMIUM", "CANOLA", "CANOLA", "15 LTR"),
    ("ZEPTO", "PREMIUM", "CANOLA", "CANOLA", "2 LTR"),
    ("ZEPTO", "PREMIUM", "CANOLA", "CANOLA", "5 LTR"),
    ("ZEPTO", "PREMIUM", "GHEE", "A2 GHEE", "1 LTR"),
    ("ZEPTO", "PREMIUM", "GHEE", "A2 GHEE", "500 MLS"),
    ("ZEPTO", "PREMIUM", "GROUNDNUT", "GROUNDNUT", "1 LTR"),
    ("ZEPTO", "PREMIUM", "GROUNDNUT", "GROUNDNUT", "5 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "EXTRA LIGHT", "1 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "EXTRA LIGHT", "5 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "EXTRA VIRGIN", "1 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "EXTRA VIRGIN", "5 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("ZEPTO", "PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("ZEPTO", "COMMODITY", "BLENDED", "GOLD", "1 LTR"),
    ("ZEPTO", "COMMODITY", "BLENDED", "GOLD", "5 LTR"),
    ("ZEPTO", "COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR"),
    ("ZEPTO", "COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "15 LTR"),
    ("ZEPTO", "COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR"),
    ("ZEPTO", "COMMODITY", "RICE BRAN", "RICE BRAN", "1 LTR"),
    ("ZEPTO", "COMMODITY", "RICE BRAN", "RICE BRAN", "5 LTR"),
    ("ZEPTO", "COMMODITY", "SOYABEAN", "SOYABEAN", "15 LTR"),
    ("ZEPTO", "COMMODITY", "SUNFLOWER", "SUNFLOWER", "1 LTR"),
    ("ZEPTO", "COMMODITY", "SUNFLOWER", "SUNFLOWER", "15 LTR"),
    ("ZEPTO", "COMMODITY", "SUNFLOWER", "SUNFLOWER", "5 LTR"),
    ("ZEPTO", "OTHER", "DRINKS", "ENERGY DRINK SF", "200 MLS"),
    ("ZEPTO", "OTHER", "DRINKS", "JEERA", "160 MLS"),
    ("ZEPTO", "OTHER", "DRINKS", "MANGO", "500 MLS"),
    ("ZEPTO", "OTHER", "DRINKS", "MOJITO", "200 MLS"),
    ("ZEPTO", "OTHER", "DRINKS", "SODA", "750 MLS"),
)

_BIGBASKET_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_BIGBASKET_SEC_TARGETS = {
    "PREMIUM": 5000,
    "COMMODITY": 12000,
    "OTHER": 0,
}

_BIGBASKET_SEC_DETAIL_ROWS = (
    ("PREMIUM", "CANOLA", "CANOLA", "1 LTR"),
    ("PREMIUM", "CANOLA", "CANOLA", "5 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "1 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "5 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "1 LTR"),
    ("PREMIUM", "COCONUT", "COCONUT", "1 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "5 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "1 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "5 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "5 LTR"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "1 LTR"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "5 LTR"),
    ("OTHER", "DRINKS", "APPLE", "200 MLS"),
    ("OTHER", "DRINKS", "APPLE SF", "200 MLS"),
    ("OTHER", "DRINKS", "BLUEBERRY", "200 MLS"),
    ("OTHER", "DRINKS", "GINGER ALE SF", "200 MLS"),
    ("OTHER", "DRINKS", "MANGO", "200 MLS"),
    ("OTHER", "DRINKS", "MANGO", "500 MLS"),
    ("OTHER", "DRINKS", "MOJITO", "200 MLS"),
    ("OTHER", "DRINKS", "MOJITO SF", "200 MLS"),
    ("OTHER", "DRINKS", "ROSE", "200 MLS"),
    ("OTHER", "DRINKS", "SODA", "750 MLS"),
    ("OTHER", "DRINKS", "TONIC WATER", "200 MLS"),
)

_FLIPKART_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_FLIPKART_SEC_DETAIL_ROWS = (
    ("PREMIUM", "OLIVE", "EXTRA LIGHT"),
    ("PREMIUM", "CANOLA", "CANOLA"),
    ("PREMIUM", "OLIVE", "JIVO POMACE"),
    ("PREMIUM", "GROUNDNUT", "GROUNDNUT"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN"),
    ("PREMIUM", "OLIVE", "SANO POMACE"),
    ("COMMODITY", "RICE BRAN", "RICE BRAN"),
    ("COMMODITY", "SUNFLOWER OIL", "SUNFLOWER OIL"),
    ("PREMIUM", "COCONUT", "COCONUT"),
    ("COMMODITY", "BLENDED", "GOLD"),
    ("PREMIUM", "BLENDED", "SO OLIVE"),
    ("PREMIUM", "YELLOW MUSTARD", "YELLOW MUSTARD"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN"),
    ("PREMIUM", "GHEE", "A2 GHEE"),
    ("OTHER", "SPICES", "SAFFRON"),
    ("PREMIUM", "SESAME", "SESAME"),
    ("OTHER", "SEEDS", "FLAX SEEDS"),
    ("OTHER", "SEEDS", "ALL SEEDS"),
    ("OTHER", "SEEDS", "BAASIL SEEDS"),
    ("OTHER", "SEEDS", "CHIA SEEDS"),
    ("OTHER", "HONEY", "HONEY"),
    ("PREMIUM", "GHEE", "DESI GHEE"),
    ("PREMIUM", "OLIVE", "CLASSIC"),
    ("PREMIUM", "OLIVE", "POMACE"),
    ("OTHER", "Casserole", "Casserole"),
    ("OTHER", "COFFEE", "COFFEE"),
    ("OTHER", "Crypto", "Crypto"),
    ("OTHER", "ELEGANCE", "ELEGANCE"),
    ("OTHER", "Ferrero", "Ferrero"),
    ("OTHER", "FlipPRo", "FlipPRo"),
    ("OTHER", "GIFT PACK", "DRY FRUITS"),
    ("OTHER", "LUNCH BOX", "LUNCH BOX"),
    ("OTHER", "RICE", "RICE"),
    ("OTHER", "SEEDS", "BASIL SEEDS"),
    ("OTHER", "SEEDS", "CHIA SEED"),
    ("OTHER", "SEEDS", "FLAX SEED"),
    ("OTHER", "SEEDS", "PUMPKIN SEED"),
    ("OTHER", "SEEDS", "PUMPKIN SEEDS"),
    ("OTHER", "SEEDS", "Seeds"),
    ("OTHER", "SEEDS", "SUNFLOWER SEEDS"),
    ("OTHER", "SPICES", "BLACK CARDAMOM"),
    ("OTHER", "SPICES", "BLACK PEPPER"),
    ("OTHER", "SPICES", "CINNAMON"),
    ("OTHER", "SPICES", "CUMIN"),
    ("OTHER", "SPICES", "GREEN CARDAMOM"),
    ("OTHER", "WHEATGRASS", "PUNJABI JEERA"),
    ("OTHER", "WHEATGRASS", "WHEATGRASS APPLE"),
    ("OTHER", "WHEATGRASS", "WHEATGRASS BLUEBERRY"),
    ("OTHER", "WHEATGRASS", "WHEATGRASS GIGNGER ALE"),
    ("OTHER", "WHEATGRASS", "WHEATGRASS MANGO"),
    ("OTHER", "WHEATGRASS", "WHEATGRASS MOJITO"),
)

_FLIPKART_SEC_MONTHLY_CATEGORY_ROWS = (
    ("PREMIUM", "BLENDED", "SO OLIVE"),
    ("", "CANOLA", "CANOLA"),
    ("", "COCONUT", "COCONUT"),
    ("", "GHEE", "A2 GHEE"),
    ("", "GHEE", "DESI GHEE"),
    ("", "GROUNDNUT", "GROUNDNUT"),
    ("", "OLIVE", "CLASSIC"),
    ("", "OLIVE", "EXTRA LIGHT"),
    ("", "OLIVE", "EXTRA VIRGIN"),
    ("", "OLIVE", "JIVO POMACE"),
    ("", "OLIVE", "POMACE"),
    ("", "OLIVE", "SANO POMACE"),
    ("", "SESAME", "SESAME"),
    ("", "YELLOW MUSTARD", "YELLOW MUSTARD"),
    ("COMMODITY", "BLENDED", "GOLD"),
    ("", "MUSTARD", "MUSTARD KACCHI GHANI"),
    ("", "RICE BRAN", "RICE BRAN"),
    ("", "SOYABEAN", "SOYABEAN"),
    ("", "SUNFLOWER OIL", "SUNFLOWER OIL"),
)

_FLIPKART_SEC_MONTHLY_ITEM_HEADS = ("PREMIUM", "COMMODITY")

_BLINKIT_DRR_SALES_OF = ("ALL", "PREMIUM", "COMMODITY", "OTHER")
_FLIPKART_MP_DRR_SALES_OF = ("ALL", "PREMIUM", "COMMODITY", "OTHER")
_AMAZON_DRR_ITEM_HEADS = ("ALL", "PREMIUM", "COMMODITY", "OTHER")
_AMAZON_DRR_SALES_MODES = ("ORDERED", "SHIPPED")
_BIGBASKET_PRIMARY_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")
_BIGBASKET_PRIMARY_MONTH_TYPES = ("DEL MONTH", "PO MONTH")

_MONTH_NAME_TO_NUM = {
    date(2000, month, 1).strftime("%B").upper(): month
    for month in range(1, 13)
}


def _norm_sec_key(value) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().upper())


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _per_liter_shpd(units, litres):
    litres = _num(litres)
    if litres == 0:
        return None
    return _num(units) / litres


def _value_per_unit(value, units):
    units = _num(units)
    if units == 0:
        return None
    return _num(value) / units


def _value_per_ltr_zero(value, litres):
    litres = _num(litres)
    if litres == 0:
        return 0.0
    return _num(value) / litres


def _sec_total(rows: list[dict], *, include_ratio: bool = True) -> dict:
    shipped_units = sum(_num(r.get("shipped_units")) for r in rows)
    shipped_ltr = sum(_num(r.get("shipped_ltr")) for r in rows)
    shipped_value = sum(_num(r.get("shipped_value")) for r in rows)
    total = {
        "shipped_units": shipped_units,
        "shipped_ltr": shipped_ltr,
        "shipped_value": shipped_value,
    }
    if include_ratio:
        total["per_liter_shpd"] = _per_liter_shpd(shipped_units, shipped_ltr)
    return total


def _sec_total_with_order_value(rows: list[dict]) -> dict:
    total = _sec_total(rows)
    total["order_value"] = sum(_num(r.get("order_value")) for r in rows)
    return total


def _safe_div(numerator, denominator) -> float:
    denominator = _num(denominator)
    if denominator == 0:
        return 0.0
    return _num(numerator) / denominator


def _sec_elapsed_day(max_date) -> int:
    if hasattr(max_date, "day"):
        return max_date.day or 0
    if isinstance(max_date, str):
        match = re.match(r"^\d{4}-\d{2}-(\d{2})", max_date)
        if match:
            return int(match.group(1))
    return 0


def _amazon_sec_month_day_keys(max_date, month_name: str) -> list[str]:
    day = _sec_elapsed_day(max_date)
    if not day or not month_name:
        return []
    month_name = _norm_sec_key(month_name)
    keys = [f"{day}-{month_name}", f"{day:02d}-{month_name}"]
    return list(dict.fromkeys(keys))


_FK_GROCERY_DRR_ITEM_ORDER = (
    "CANOLA 1L",
    "EXTRA LIGHT 2L",
    "GOLD 1L",
    "JIVO POMACE 1L",
    "JIVO POMACE 2L",
    "JIVO POMACE 5L",
    "MUSTARD 1L",
    "MUSTARD 4L",
    "MUSTARD 5L",
    "PUNJABI JEERA 160ML",
    "SOYABEAN 1L POUCH",
    "SUNFLOWER 4L",
    "WATER 1L",
    "WG APPLE JUICE 200 ML",
    "WG BLUEBERRY JUICE 200ML",
    "WG GINGER ALE 200ML",
    "WG JEERA 200ML",
    "WG MANGO JUICE 500ML",
    "WG MOJITO 200ML",
)

_FK_GROCERY_MOM_TARGETS = {
    "PREMIUM": 2000,
    "COMMODITY": 52000,
}

_FK_GROCERY_MOM_TEMPLATE = (
    ("CANOLA", "CANOLA 1L", "PREMIUM", 1000),
    ("EXTRA LIGHT", "EXTRA LIGHT 2L", "PREMIUM", 200),
    ("GOLD", "GOLD 5L", "COMMODITY", 0),
    ("JIVO POMACE", "JIVO POMACE 1L", "PREMIUM", 400),
    ("JIVO POMACE", "JIVO POMACE 5L", "PREMIUM", 400),
    ("MUSTARD KACHI GHANI", "MUSTARD 1L", "COMMODITY", 45000),
    ("MUSTARD KACHI GHANI", "MUSTARD 4L", "COMMODITY", 4500),
    ("MUSTARD KACHI GHANI", "MUSTARD 5L", "COMMODITY", 1000),
    ("SOYABEAN", "SOYABEAN 1L POUCH", "COMMODITY", 1000),
    ("SUNFLOWER", "SUNFLOWER 4L", "COMMODITY", 500),
)

_BIGBASKET_MOM_TARGETS = {
    "PREMIUM": 5000,
    "COMMODITY": 12000,
}

_BIGBASKET_MOM_TEMPLATE = (
    ("CANOLA", "CANOLA 1L", "PREMIUM", 1000),
    ("CANOLA", "CANOLA 1L POUCH", "PREMIUM", 500),
    ("CANOLA", "CANOLA 5L", "PREMIUM", 1000),
    ("EXTRA LIGHT", "EXTRA LIGHT 1L", "PREMIUM", 800),
    ("EXTRA LIGHT", "EXTRA LIGHT 2L", "PREMIUM", 500),
    ("EXTRA LIGHT", "EXTRA LIGHT 5L", "PREMIUM", 100),
    ("EXTRA VIRGIN", "EXTRA VIRGIN 1L", "PREMIUM", 100),
    ("EXTRA VIRGIN", "EXTRA VIRGIN 5L", "PREMIUM", 0),
    ("JIVO POMACE", "JIVO POMACE 1L", "PREMIUM", 800),
    ("JIVO POMACE", "JIVO POMACE 2L", "PREMIUM", 100),
    ("JIVO POMACE", "JIVO POMACE 5L", "PREMIUM", 100),
    ("MUSTARD KACCHI GHANI", "MUSTARD 1L", "COMMODITY", 1000),
    ("MUSTARD KACCHI GHANI", "MUSTARD 5L", "COMMODITY", 1500),
    ("SOYABEAN", "SOYABEAN 1L", "COMMODITY", 0),
    ("SOYABEAN", "SOYABEAN 5L", "COMMODITY", 0),
    ("SUNFLOWER", "SUNFLOWER 1L", "COMMODITY", 6500),
    ("SUNFLOWER", "SUNFLOWER 5L", "COMMODITY", 3000),
)


def _parse_sec_month_year(params, *, latest_source: str = "flipkart_grocery") -> tuple[int, int, bool]:
    raw_date = str(params.get("date") or "").strip()
    raw_month = str(params.get("month") or "").strip()
    raw_year = str(params.get("year") or "").strip()

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
        try:
            selected = date.fromisoformat(raw_date)
        except ValueError:
            raise ValidationError("`date` must be a valid calendar date.")
        return selected.month, selected.year, False
    if re.fullmatch(r"\d{4}-\d{2}", raw_month):
        year, month = raw_month.split("-")
        return int(month), int(year), False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_month):
        try:
            selected = date.fromisoformat(raw_month)
        except ValueError:
            raise ValidationError("`month` date must be a valid calendar date.")
        return selected.month, selected.year, False

    if raw_month and raw_year:
        try:
            month = int(raw_month)
            year = int(raw_year)
        except ValueError:
            raise ValidationError("`month` and `year` must be numeric or month must be YYYY-MM.")
        if not 1 <= month <= 12:
            raise ValidationError("`month` must be 1-12.")
        if year < 2000 or year > 2100:
            raise ValidationError("`year` looks out of range.")
        return month, year, False

    if latest_source == "flipkart_secondary_all":
        latest = _dict_rows(
            """
            SELECT "month", "year"
            FROM "flipkart_secondary_all"
            WHERE "Order Date" IS NOT NULL
            ORDER BY "Order Date" DESC
            LIMIT 1
            """,
            [],
        )
    elif latest_source == "amazon_sec_range_master_view":
        latest = _dict_rows(
            """
            SELECT "month", "year"
            FROM "amazon_sec_range_master_view"
            WHERE "to_date" IS NOT NULL
            ORDER BY "to_date" DESC
            LIMIT 1
            """,
            [],
        )
    elif latest_source == "amazon_sec_daily_master_view":
        latest = _dict_rows(
            """
            SELECT "month", "year"
            FROM "amazon_sec_daily_master_view"
            WHERE "to_date" IS NOT NULL
            ORDER BY "to_date" DESC
            LIMIT 1
            """,
            [],
        )
    elif latest_source.startswith("secmaster_"):
        source_format = latest_source.replace("secmaster_", "", 1)
        if source_format == "swiggy":
            latest = _dict_rows(
                """
                SELECT
                    TRIM(TO_CHAR("ORDERED_DATE"::timestamp, 'MONTH')) AS "month",
                    EXTRACT(year FROM "ORDERED_DATE") AS "year"
                FROM "swiggySec"
                WHERE "ORDERED_DATE" IS NOT NULL
                ORDER BY "ORDERED_DATE" DESC
                LIMIT 1
                """,
                [],
            )
        else:
            date_expr = (
                _secmaster_zepto_date_expr()
                if source_format == "zepto"
                else '"date"'
            )
            latest = _dict_rows(
                f"""
                SELECT "month", "year"
                FROM secmaster_mv
                WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
                  AND ({date_expr}) IS NOT NULL
                ORDER BY ({date_expr}) DESC
                LIMIT 1
                """,
                [source_format],
            )
    else:
        latest = _dict_rows(
            """
            SELECT "month", "year"
            FROM "flipkart_grocery_master"
            WHERE "real_date" IS NOT NULL
            ORDER BY "real_date" DESC
            LIMIT 1
            """,
            [],
        )
    if latest:
        month_value = latest[0]["month"]
        if isinstance(month_value, str) and not month_value.strip().isdigit():
            month = _MONTH_NAME_TO_NUM.get(_norm_sec_key(month_value))
            if month is None:
                month = date.today().month
        else:
            month = int(month_value)
        return month, int(latest[0]["year"]), True

    today = date.today()
    return today.month, today.year, True


def _parse_flipkart_secondary_monthly_year(params) -> tuple[int, bool]:
    raw_year = str(params.get("year") or "").strip()
    if raw_year:
        try:
            year = int(raw_year)
        except ValueError:
            raise ValidationError("`year` must be numeric.")
        if year < 2000 or year > 2100:
            raise ValidationError("`year` looks out of range.")
        return year, False

    latest_year = _scalar(
        'SELECT "year" FROM "flipkart_secondary_all" WHERE "year" IS NOT NULL ORDER BY "year" DESC LIMIT 1',
        [],
    )
    return int(latest_year) if latest_year else date.today().year, True


def _parse_amazon_secondary_monthly_year(params) -> tuple[int, bool]:
    raw_year = str(params.get("year") or "").strip()
    if raw_year:
        try:
            year = int(raw_year)
        except ValueError:
            raise ValidationError("`year` must be numeric.")
        if year < 2000 or year > 2100:
            raise ValidationError("`year` looks out of range.")
        return year, False

    latest_year = _scalar(
        """
        SELECT "year"
        FROM "amazon_sec_range_master_view"
        WHERE "year" IS NOT NULL
        ORDER BY "to_date" DESC NULLS LAST, "year" DESC
        LIMIT 1
        """,
        [],
    )
    return int(latest_year) if latest_year else date.today().year, True


def _parse_amazon_comparison_params(params) -> tuple[str, int, int, bool]:
    raw_month = str(params.get("month") or "").strip().upper()
    raw_year = str(params.get("year") or "").strip()
    defaulted_to_latest = False

    if not raw_month or not raw_year:
        latest = _dict_rows(
            """
            SELECT "month", "year"
            FROM "amazon_sec_range_master_view"
            WHERE "to_date" IS NOT NULL
            ORDER BY "to_date" DESC
            LIMIT 1
            """,
            [],
        )
        if latest:
            raw_month = str(latest[0].get("month") or "").strip().upper()
            raw_year = str(latest[0].get("year") or "").strip()
            defaulted_to_latest = True
        else:
            raw_month = raw_month or "MAY"
            raw_year = raw_year or "2026"

    raw_history_year = str(params.get("history_year") or raw_year).strip()

    if raw_month.isdigit():
        month_number = int(raw_month)
        if not 1 <= month_number <= 12:
            raise ValidationError("`month` must be 1-12 or a month name.")
        month_name = _month_name(month_number)
    else:
        month_name = _norm_sec_key(raw_month)
        if month_name not in _MONTH_NAME_TO_NUM:
            raise ValidationError("`month` must be 1-12 or a month name.")

    try:
        year = int(raw_year)
        history_year = int(raw_history_year)
    except ValueError:
        raise ValidationError("`year` and `history_year` must be numeric.")
    if not 2000 <= year <= 2100 or not 2000 <= history_year <= 2100:
        raise ValidationError("`year` or `history_year` looks out of range.")
    return month_name, year, history_year, defaulted_to_latest


def _parse_sec_as_of_date(params) -> date | None:
    """The `as_of_date` the Ads Dashboard sends to scope the Total Sales / TACOS
    columns to the picked calendar date. A dedicated param (not `date`) so the
    standalone SEC dashboard pages — which never send it — are unaffected."""
    raw = str(params.get("as_of_date") or "").strip()
    if not raw:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        raise ValidationError("`as_of_date` must be YYYY-MM-DD.")
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise ValidationError("`as_of_date` must be a valid calendar date.")


def _parse_sec_selected_date(params) -> date | None:
    raw_date = str(params.get("date") or "").strip()
    raw_month = str(params.get("month") or "").strip()
    # `as_of_date` (from the Ads Dashboard) narrows the per-day SEC platforms to
    # the picked day, exactly like an explicit `date` would.
    raw_as_of = str(params.get("as_of_date") or "").strip()
    candidate = raw_date or raw_as_of or (
        raw_month if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_month) else ""
    )
    if not candidate:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        raise ValidationError("`date` must be YYYY-MM-DD.")
    try:
        return date.fromisoformat(candidate)
    except ValueError:
        raise ValidationError("`date` must be a valid calendar date.")


def _sec_date_filter(selected_date: date | None, date_expr: str = '"date"') -> tuple[str, list]:
    if not selected_date:
        return "", []
    return f" AND ({date_expr})::date = %s", [selected_date]


def _secmaster_zepto_date_expr(alias: str | None = None) -> str:
    prefix = f'{alias}.' if alias else ""
    return f"""
        CASE
            WHEN TRIM({prefix}"real_date"::text) ~ '^\\d{{2}}-\\d{{2}}-\\d{{4}}$'
                THEN TO_DATE(TRIM({prefix}"real_date"::text), 'DD-MM-YYYY')
            WHEN TRIM({prefix}"real_date"::text) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'
                THEN TRIM({prefix}"real_date"::text)::date
            ELSE {prefix}"date"
        END
    """


def _date_span(month: int, year: int, max_date: date | None) -> list[date]:
    if not max_date:
        return []
    end_day = min(max_date.day, monthrange(year, month)[1])
    return [date(year, month, day) for day in range(1, end_day + 1)]


def _shift_month(month: int, year: int, offset: int) -> tuple[int, int]:
    zero_based = (year * 12) + (month - 1) + offset
    shifted_year, shifted_month_zero = divmod(zero_based, 12)
    return shifted_month_zero + 1, shifted_year


def _month_name(month: int) -> str:
    return date(2000, month, 1).strftime("%B").upper()


def _parse_bigbasket_primary_month_type(raw_value) -> tuple[str, str, str]:
    month_type = _norm_sec_key(raw_value or "DEL MONTH")
    if month_type in {"DEL", "DELIVERY", "DELIVERY MONTH"}:
        month_type = "DEL MONTH"
    if month_type not in _BIGBASKET_PRIMARY_MONTH_TYPES:
        raise ValidationError("`month_type` must be DEL MONTH or PO MONTH.")
    if month_type == "PO MONTH":
        return month_type, "po_month", "po_date"
    return month_type, "delivery_month", "delivery_date"


def _parse_month_name_param(raw_value, *, param_name: str = "month") -> str | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    if re.fullmatch(r"\d{4}-\d{2}", value):
        _, raw_month = value.split("-")
        month_num = int(raw_month)
        if not 1 <= month_num <= 12:
            raise ValidationError(f"`{param_name}` must contain a valid month.")
        return _month_name(month_num)
    if value.isdigit():
        month_num = int(value)
        if not 1 <= month_num <= 12:
            raise ValidationError(f"`{param_name}` must be 1-12 or a month name.")
        return _month_name(month_num)
    month_name = _norm_sec_key(value)
    if month_name not in _MONTH_NAME_TO_NUM:
        raise ValidationError(f"`{param_name}` must be 1-12 or a month name.")
    return month_name


def _prim_safe_date_expr(column: str, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    expr = f'{prefix}"{column}"'
    return f"""
        CASE
            WHEN TRIM({expr}::text) ~ '^\\d{{2}}-\\d{{2}}-\\d{{4}}$'
                THEN TO_DATE(TRIM({expr}::text), 'DD-MM-YYYY')
            WHEN TRIM({expr}::text) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'
                THEN TRIM({expr}::text)::date
            ELSE NULL
        END
    """


def _primary_text_date_expr(column_name: str) -> str:
    return _prim_safe_date_expr(column_name)


def _parse_bigbasket_primary_period(params, platform_format: str) -> tuple[str, str, str, str, int, bool]:
    month_type, month_col, date_col = _parse_bigbasket_primary_month_type(
        params.get("month_type") or params.get("mode")
    )
    raw_year = str(params.get("year") or "").strip()
    month_name = _parse_month_name_param(params.get("month"))
    defaulted_to_latest = False

    if month_name and raw_year:
        try:
            year = int(raw_year)
        except ValueError:
            raise ValidationError("`year` must be numeric.")
        if not 2000 <= year <= 2100:
            raise ValidationError("`year` looks out of range.")
        return month_type, month_col, date_col, month_name, year, defaulted_to_latest

    date_expr = _primary_text_date_expr(date_col)
    latest = _dict_rows(
        f"""
        SELECT ({date_expr}) AS latest_date
        FROM "master_po"
        WHERE UPPER(TRIM("format"::text)) = %s
          AND ({date_expr}) IS NOT NULL
        ORDER BY ({date_expr}) DESC
        LIMIT 1
        """,
        [platform_format],
    )
    if latest:
        latest_date = latest[0].get("latest_date")
        if hasattr(latest_date, "month") and hasattr(latest_date, "year"):
            return (
                month_type,
                month_col,
                date_col,
                _month_name(latest_date.month),
                int(latest_date.year),
                True,
            )

    today = date.today()
    return month_type, month_col, date_col, _month_name(today.month), today.year, True


_PRIM_PO_DATE_EXPR = _prim_safe_date_expr("po_date")
_PRIM_PO_EXPIRY_DATE_EXPR = _prim_safe_date_expr("po_expiry_date")
_PRIM_DELIVERY_DATE_EXPR = _prim_safe_date_expr("delivery_date")

def _primary_master_po_cte(platform_format: str = "ZEPTO") -> str:
    format_key = re.sub(r"[^a-z0-9]+", "", str(platform_format or "ZEPTO").strip().lower())
    format_key = format_key.replace("'", "''")
    return f"""
WITH base AS (
    SELECT
        p.*,
        {_PRIM_PO_DATE_EXPR} AS po_dt,
        {_PRIM_PO_EXPIRY_DATE_EXPR} AS expiry_dt,
        {_PRIM_DELIVERY_DATE_EXPR} AS delivery_dt
    FROM public.master_po p
    WHERE REGEXP_REPLACE(LOWER(TRIM(p.format::text)), '[^a-z0-9]+', '', 'g') = '{format_key}'
),
with_pack_text AS (
    SELECT
        *,
        UPPER(CONCAT_WS(
            ' ',
            item::text,
            sap_sku_name::text,
            sku_name::text,
            unit_of_measure::text
        )) AS pack_text
    FROM base
),
with_pack_matches AS (
    SELECT
        *,
        regexp_match(
            pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'
        ) AS combo_full_match,
        regexp_match(
            pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'
        ) AS combo_compact_match,
        regexp_match(
            pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*(?:ML|MLS|M)(?:[^A-Z0-9]|$)'
        ) AS ml_match,
        regexp_match(
            pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER)(?:[^A-Z0-9]|$)'
        ) AS ltr_match,
        regexp_match(
            pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*L(?:[^A-Z0-9]|$)'
        ) AS l_match
    FROM with_pack_text
),
metric_base AS (
    SELECT
        *,
        COALESCE(
            CASE
                WHEN combo_full_match IS NOT NULL
                    THEN combo_full_match[1]::numeric + combo_full_match[2]::numeric
                WHEN combo_compact_match IS NOT NULL
                    THEN combo_compact_match[1]::numeric + combo_compact_match[2]::numeric
                WHEN ml_match IS NOT NULL
                    THEN ml_match[1]::numeric / 1000
                WHEN ltr_match IS NOT NULL
                    THEN ltr_match[1]::numeric
                WHEN l_match IS NOT NULL
                    THEN l_match[1]::numeric
                ELSE NULL
            END,
            NULLIF(per_liter, 0),
            1
        ) AS effective_per_liter
    FROM with_pack_matches
),
normalized AS (
    SELECT
        *,
        COALESCE(NULLIF(UPPER(TRIM(po_status::text)), ''), 'OTHER') AS status_key,
        CASE
            WHEN UPPER(TRIM(item_head::text)) = 'PREMIUM' THEN 'PREMIUM'
            WHEN UPPER(TRIM(item_head::text)) = 'COMMODITY' THEN 'COMMODITY'
            ELSE 'OTHER'
        END AS item_head_key,
        COALESCE(NULLIF(UPPER(TRIM(item::text)), ''), NULLIF(UPPER(TRIM(sku_name::text)), ''), 'OTHER') AS item_key,
        COALESCE(NULLIF(UPPER(TRIM(category::text)), ''), 'OTHER') AS category_key,
        COALESCE(NULLIF(UPPER(TRIM(sub_category::text)), ''), 'OTHER') AS sub_category_key,
        COALESCE(NULLIF(UPPER(TRIM(open_close::text)), ''), 'CLOSED') AS open_close_key,
        COALESCE(
            NULLIF(UPPER(TRIM(po_month::text)), ''),
            UPPER(TRIM(TO_CHAR(po_dt, 'FMMONTH')))
        ) AS po_month_key,
        COALESCE(
            NULLIF(UPPER(TRIM(delivery_month::text)), ''),
            UPPER(TRIM(TO_CHAR(delivery_dt, 'FMMONTH')))
        ) AS delivery_month_key,
        UPPER(TRIM(TO_CHAR(expiry_dt, 'FMMONTH'))) AS expiry_month_key,
        EXTRACT(YEAR FROM delivery_dt)::integer AS delivery_year,
        EXTRACT(YEAR FROM expiry_dt)::integer AS expiry_year,
        CASE
            WHEN effective_per_liter IS NULL THEN UPPER(TRIM(unit_of_measure::text))
            WHEN effective_per_liter < 1
                THEN UPPER(TRIM(TO_CHAR(effective_per_liter * 1000, 'FM999999990.###'))) || ' MLS'
            ELSE UPPER(TRIM(TO_CHAR(effective_per_liter, 'FM999999990.###'))) || ' LTR'
        END AS per_ltr_key,
        -- Direct mapping per user spec: each KPI card reads exactly one
        -- canonical column from master_po — no qty x rate fallbacks.
        -- Value cards use the tax/margin-INCLUSIVE amounts (these match the
        -- source DB; the *_exclusive columns under-report Order/Deliver value).
        COALESCE(total_order_liters, 0) AS metric_order_liters,
        COALESCE(total_delivered_liters, 0) AS metric_delivered_liters,
        COALESCE(total_order_amt_inclusive, 0) AS metric_order_value,
        COALESCE(total_deliver_amt_inclusive, 0) AS metric_delivered_value,
        COALESCE(order_qty, 0) AS metric_order_qty,
        COALESCE(delivered_qty, 0) AS metric_delivered_qty,
        0 AS metric_projection_value,
        0 AS metric_projection_ltrs,
        0 AS metric_projection_qty,
        COALESCE(missed_ltrs, 0) AS metric_pending_liters,
        COALESCE(missed_qty, 0) AS metric_pending_qty,
        COALESCE(
            COALESCE(missed_qty, 0) * CASE
                WHEN NULLIF(TRIM(basic_rate::text), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    THEN NULLIF(TRIM(basic_rate::text), '')::numeric
                ELSE 0
            END,
            0
        ) AS metric_pending_value
    FROM metric_base
)
"""


_PRIM_MASTER_PO_CTE = _primary_master_po_cte("ZEPTO")


def _parse_primary_dashboard_params(params, platform_format: str = "ZEPTO") -> tuple[str, int, int, bool]:
    mode = _norm_sec_key(params.get("mode") or params.get("month_type") or "DEL MONTH")
    if mode not in {"DEL MONTH", "PO MONTH"}:
        raise ValidationError("`mode` must be DEL MONTH or PO MONTH.")

    raw_month = str(params.get("month") or "").strip()
    raw_year = str(params.get("year") or "").strip()
    defaulted_to_latest = False

    iso_month = re.fullmatch(r"(\d{4})-(\d{2})", raw_month)
    if iso_month and not raw_year:
        raw_year = iso_month.group(1)
        raw_month = iso_month.group(2)

    if not raw_month or not raw_year:
        order_date = "delivery_dt" if mode == "DEL MONTH" else "po_dt"
        primary_cte = _primary_master_po_cte(platform_format)
        latest = _dict_rows(
            f"""
            {primary_cte}
            SELECT
                {order_date} AS period_date,
                delivery_year AS del_year,
                po_year
            FROM normalized
            WHERE {order_date} IS NOT NULL
              AND {order_date} <= %s
            ORDER BY {order_date} DESC
            LIMIT 1
            """,
            [date.today()],
        )
        if latest:
            period_date = latest[0].get("period_date")
            if hasattr(period_date, "month"):
                raw_month = str(period_date.month)
                raw_year = str(latest[0].get("del_year" if mode == "DEL MONTH" else "po_year") or period_date.year)
                defaulted_to_latest = True

    raw_month = raw_month or str(date.today().month)
    raw_year = raw_year or str(date.today().year)

    if raw_month.isdigit():
        month = int(raw_month)
        if not 1 <= month <= 12:
            raise ValidationError("`month` must be 1-12 or a month name.")
    else:
        month_name = _norm_sec_key(raw_month)
        if month_name not in _MONTH_NAME_TO_NUM:
            raise ValidationError("`month` must be 1-12 or a month name.")
        month = _MONTH_NAME_TO_NUM[month_name]

    try:
        year = int(raw_year)
    except ValueError:
        raise ValidationError("`year` must be numeric.")
    if year < 2000 or year > 2100:
        raise ValidationError("`year` looks out of range.")

    return mode, month, year, defaulted_to_latest


def _primary_period_filter(mode: str) -> str:
    if mode == "PO MONTH":
        return "po_month_key = %s AND po_year = %s"
    return "delivery_month_key = %s AND delivery_year = %s"


def _primary_vendor_metric_filter(mode: str) -> str:
    if mode == "PO MONTH":
        return "po_month_key = %s AND po_year = %s"
    return "delivery_month_key = %s AND delivery_year = %s"


def _primary_vendor_pending_filter(mode: str) -> str:
    if mode == "PO MONTH":
        return "po_month_key = %s AND po_year = %s"
    return "delivery_month_key = %s AND delivery_year = %s"


def _primary_zero_metrics() -> dict:
    return {
        "done_value": 0.0,
        "done_ltrs": 0.0,
        "done_qty": 0.0,
        "missed_ltrs": 0.0,
        "pending_value": 0.0,
        "pending_ltrs": 0.0,
        "pending_qty": 0.0,
        "dp_value": 0.0,
        "dp_ltrs": 0.0,
        "expired_value": 0.0,
        "expired_ltrs": 0.0,
        "cancelled_value": 0.0,
        "cancelled_ltrs": 0.0,
        "order_value": 0.0,
        "order_ltrs": 0.0,
        "order_qty": 0.0,
        "projection_value": 0.0,
        "projection_ltrs": 0.0,
        "projection_qty": 0.0,
    }


def _primary_metrics(row: dict | None) -> dict:
    metrics = _primary_zero_metrics()
    if row:
        for key in (
            "done_value",
            "done_ltrs",
            "done_qty",
            "missed_ltrs",
            "pending_value",
            "pending_ltrs",
            "pending_qty",
            "expired_value",
            "expired_ltrs",
            "cancelled_value",
            "cancelled_ltrs",
            "order_value",
            "order_ltrs",
            "order_qty",
            "projection_value",
            "projection_ltrs",
            "projection_qty",
        ):
            metrics[key] = _num(row.get(key))
    metrics["dp_value"] = metrics["done_value"] + metrics["pending_value"]
    metrics["dp_ltrs"] = metrics["done_ltrs"] + metrics["pending_ltrs"]
    return metrics


def _primary_total(rows: list[dict]) -> dict:
    total = _primary_zero_metrics()
    for row in rows:
        for key in total:
            total[key] += _num(row.get(key))
    return total


def _primary_trend_rows(rows: list[dict]) -> list[dict]:
    trend_rows = []
    for row in rows:
        period = row.get("period")
        trend_rows.append({
            "period": period.isoformat() if hasattr(period, "isoformat") else period,
            "label": row.get("label") or str(period or ""),
            "done_value": _num(row.get("done_value")),
            "done_ltrs": _num(row.get("done_ltrs")),
            "done_qty": _num(row.get("done_qty")),
            "pending_value": _num(row.get("pending_value")),
            "pending_ltrs": _num(row.get("pending_ltrs")),
            "pending_qty": _num(row.get("pending_qty")),
            "order_value": _num(row.get("order_value")),
            "order_ltrs": _num(row.get("order_ltrs")),
            "order_qty": _num(row.get("order_qty")),
        })
    return trend_rows


_AMAZON_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_AMAZON_SEC_CATEGORY_ROWS = (
    ("AMAZON", "PREMIUM", "BLENDED", "SO OLIVE"),
    ("AMAZON", "PREMIUM", "CANOLA", "CANOLA"),
    ("AMAZON", "PREMIUM", "COCONUT", "COCONUT"),
    ("AMAZON", "PREMIUM", "GHEE", "A2 GHEE"),
    ("AMAZON", "PREMIUM", "GHEE", "DESI GHEE"),
    ("AMAZON", "PREMIUM", "GROUNDNUT", "GROUNDNUT"),
    ("AMAZON", "PREMIUM", "MUSTARD", "YELLOW MUSTARD"),
    ("AMAZON", "PREMIUM", "OLIVE", "CLASSIC"),
    ("AMAZON", "PREMIUM", "OLIVE", "EXTRA LIGHT"),
    ("AMAZON", "PREMIUM", "OLIVE", "EXTRA VIRGIN"),
    ("AMAZON", "PREMIUM", "OLIVE", "JIVO POMACE"),
    ("AMAZON", "PREMIUM", "OLIVE", "SANO POMACE"),
    ("AMAZON", "PREMIUM", "SESAME OIL", "SESAME OIL"),
    ("AMAZON", "COMMODITY", "BLENDED", "GOLD"),
    ("AMAZON", "COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI"),
    ("AMAZON", "COMMODITY", "RICE BRAN", "RICE BRAN"),
    ("AMAZON", "COMMODITY", "SOYABEAN", "SOYABEAN"),
    ("AMAZON", "COMMODITY", "SUNFLOWER", "SUNFLOWER"),
    ("AMAZON", "OTHER", "COFFEE", "COFFEE"),
    ("AMAZON", "OTHER", "DRINKS", "APPLE"),
    ("AMAZON", "OTHER", "DRINKS", "COLA"),
    ("AMAZON", "OTHER", "DRINKS", "GINGER ALE SF"),
    ("AMAZON", "OTHER", "DRINKS", "JEERA"),
    ("AMAZON", "OTHER", "DRINKS", "LEMON"),
    ("AMAZON", "OTHER", "DRINKS", "MANGO"),
    ("AMAZON", "OTHER", "DRINKS", "MINERAL WATER"),
    ("AMAZON", "OTHER", "DRINKS", "MOJITO"),
    ("AMAZON", "OTHER", "DRINKS", "ORANGE"),
    ("AMAZON", "OTHER", "DRINKS", "ROSE"),
    ("AMAZON", "OTHER", "DRINKS", "SODA"),
    ("AMAZON", "OTHER", "DRINKS", "TONIC WATER"),
    ("AMAZON", "OTHER", "GIFT PACK", "DRY FRUITS"),
    ("AMAZON", "OTHER", "HONEY", "NATURAL HONEY"),
    ("AMAZON", "OTHER", "RICE", "BASMATI"),
    ("AMAZON", "OTHER", "ROSEMARY LEAVES", "ROSEMARY LEAVES"),
    ("AMAZON", "OTHER", "SEEDS", "BASIL"),
    ("AMAZON", "OTHER", "SEEDS", "CHIA"),
    ("AMAZON", "OTHER", "SEEDS", "FLAX"),
    ("AMAZON", "OTHER", "SEEDS", "PUMPKIN"),
    ("AMAZON", "OTHER", "SEEDS", "QUINOA SEEDS"),
    ("AMAZON", "OTHER", "SEEDS", "SUNFLOWER SEEDS"),
    ("AMAZON", "OTHER", "SLICED OLIVE", "BLACK OLIVE"),
    ("AMAZON", "OTHER", "SPICES", "BLACK PEPPER"),
    ("AMAZON", "OTHER", "SPICES", "CARDAMOM"),
    ("AMAZON", "OTHER", "SPICES", "CINNAMON"),
    ("AMAZON", "OTHER", "SPICES", "CLOVE"),
    ("AMAZON", "OTHER", "SPICES", "CUMIN SEEDS"),
    ("AMAZON", "OTHER", "SPICES", "GREEN CARDAMOM"),
    ("AMAZON", "OTHER", "SPICES", "SAFFRON"),
)

_AMAZON_SEC_MONTHLY_CATEGORY_ROWS = (
    ("PREMIUM", "BLENDED", "SO OLIVE"),
    ("", "CANOLA", "CANOLA"),
    ("", "COCONUT", "COCONUT"),
    ("", "GHEE", "A2 GHEE"),
    ("", "GHEE", "DESI GHEE"),
    ("", "GROUNDNUT", "GROUNDNUT"),
    ("", "MUSTARD", "YELLOW MUSTARD"),
    ("", "OLIVE", "CLASSIC"),
    ("", "OLIVE", "EXTRA LIGHT"),
    ("", "OLIVE", "EXTRA VIRGIN"),
    ("", "OLIVE", "JIVO POMACE"),
    ("", "OLIVE", "SANO POMACE"),
    ("COMMODITY", "BLENDED", "GOLD"),
    ("", "MUSTARD", "MUSTARD KACCHI GHANI"),
    ("", "RICE BRAN", "RICE BRAN"),
    ("", "SOYABEAN", "SOYABEAN"),
    ("", "SUNFLOWER", "SUNFLOWER"),
)

_AMAZON_SEC_MONTHLY_ITEM_HEADS = ("PREMIUM", "COMMODITY")

_AMAZON_COMPARISON_ROWS = (
    ("PREMIUM", "BLENDED", "SO OLIVE", "JIVO"),
    ("PREMIUM", "CANOLA", "CANOLA", "JIVO"),
    ("PREMIUM", "CANOLA", "CANOLA", "SANO"),
    ("PREMIUM", "COCONUT", "COCONUT", "JIVO"),
    ("PREMIUM", "GHEE", "A2 GHEE", "JIVO"),
    ("PREMIUM", "GHEE", "DESI GHEE", "JIVO"),
    ("PREMIUM", "GROUNDNUT", "GROUNDNUT", "JIVO"),
    ("PREMIUM", "MUSTARD", "YELLOW MUSTARD", "JIVO"),
    ("PREMIUM", "OLIVE", "CLASSIC", "SANO"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "JIVO"),
    ("PREMIUM", "OLIVE", "EXTRA VIRGIN", "JIVO"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "JIVO"),
    ("PREMIUM", "OLIVE", "SANO POMACE", "SANO"),
    ("PREMIUM", "SEASAME OIL", "SEASAME OIL", "JIVO"),
    ("COMMODITY", "BLENDED", "GOLD", "JIVO"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "JIVO"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "SANO"),
    ("COMMODITY", "RICE BRAN", "RICE BRAN", "JIVO"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "JIVO"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "SANO"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "JIVO"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "SANO"),
)


def _sum_mom_rows(rows: list[dict]) -> dict:
    keys = (
        "target",
        "current_done_ltr",
        "estimated_ltr",
        "previous_1_ltr",
        "previous_2_ltr",
        "previous_3_ltr",
        "previous_4_ltr",
    )
    return {key: sum(_num(row.get(key)) for row in rows) for key in keys}


def _top_ltr_items_from_secmaster(
    format_key: str,
    month_name: str,
    year: int,
    date_filter: str = "",
    date_params: list | None = None,
    value_column: str = '"sales_amt_exc"',
) -> list[dict]:
    return _dict_rows(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM({value_column}), 0) AS shipped_value
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
          AND NULLIF(TRIM("item"::text), '') IS NOT NULL
        GROUP BY 1, 2
        ORDER BY COALESCE(SUM("ltr_sold"), 0) DESC
        LIMIT 8
        """,
        [format_key, month_name, year, *(date_params or [])],
    )


def _top_ltr_items_from_table(
    table_name: str,
    month_column: str,
    year_column: str,
    item_column: str,
    units_column: str,
    ltr_column: str,
    value_column: str,
    month_value,
    year_value,
    date_filter: str = "",
    date_params: list | None = None,
) -> list[dict]:
    return _dict_rows(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM({item_column}::text), ''), '-') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM({units_column}), 0) AS shipped_units,
            COALESCE(SUM({ltr_column}), 0) AS shipped_ltr,
            COALESCE(SUM({value_column}), 0) AS shipped_value
        FROM "{table_name}"
        WHERE {month_column} = %s
          AND {year_column} = %s
          {date_filter}
          AND NULLIF(TRIM({item_column}::text), '') IS NOT NULL
        GROUP BY 1, 2
        ORDER BY COALESCE(SUM({ltr_column}), 0) DESC
        LIMIT 10
        """,
        [month_value, year_value, *(date_params or [])],
    )


# Format-slug filter shared by every SecMaster-backed platform.
_SECMASTER_FORMAT_WHERE = (
    " AND REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g') = %s"
)

_SEC_TREND_FIELDS = (
    "order_value", "order_ltr", "order_units",
    "shipped_value", "shipped_ltr", "shipped_units",
    "return_value", "return_ltr", "return_units",
)


def _build_sec_keyed_trend(
    *,
    table,
    date_col,
    month,
    year,
    max_date,
    days_in_month,
    value_expr,
    ltr_expr,
    qty_expr,
    where_sql="",
    where_params=None,
    order_value_expr=None,
    order_ltr_expr=None,
    order_units_expr=None,
    return_value_expr=None,
    return_ltr_expr=None,
    return_units_expr=None,
):
    """Per-item-head trend (day / month / year) keyed by {all, premium, commodity}.

    Mirrors the Amazon-secondary `visual_dashboard.trend` shape so the dashboard's
    All / Premium / Commodity toggle (and the Excel export) work for every
    secondary platform. The whole platform's history is aggregated by date and
    item head in SQL, then bucketed in Python: the Day view uses the selected
    month, Month uses the selected year, Year spans every year present.

    Platforms whose source has only delivered metrics pass just the shipped
    expressions; Order/Return then come back as 0 and the chart hides those lines.
    """
    where_params = list(where_params or [])
    has_order = order_value_expr is not None
    has_return = return_value_expr is not None

    def _z(expr):
        return f"COALESCE(SUM({expr}), 0)"

    cols = [
        f"{date_col}::date AS d",
        'UPPER(TRIM("item_head"::text)) AS item_head',
        f"{_z(value_expr)} AS shipped_value",
        f"{_z(ltr_expr)} AS shipped_ltr",
        f"{_z(qty_expr)} AS shipped_units",
    ]
    if has_order:
        cols += [
            f"{_z(order_value_expr)} AS order_value",
            f"{_z(order_ltr_expr or '0')} AS order_ltr",
            f"{_z(order_units_expr or '0')} AS order_units",
        ]
    if has_return:
        cols += [
            f"{_z(return_value_expr)} AS return_value",
            f"{_z(return_ltr_expr or '0')} AS return_ltr",
            f"{_z(return_units_expr or '0')} AS return_units",
        ]

    rows = _dict_rows(
        f"""
        SELECT {", ".join(cols)}
        FROM "{table}"
        WHERE {date_col} IS NOT NULL{where_sql}
        GROUP BY {date_col}::date, UPPER(TRIM("item_head"::text))
        """,
        where_params,
    )

    def _empty():
        return {field: 0.0 for field in _SEC_TREND_FIELDS}

    def _bundle(agg):
        return {
            "values": {
                "order": agg["order_value"],
                "deliver": agg["shipped_value"],
                "return": agg["return_value"],
            },
            "ltrs": {
                "order": agg["order_ltr"],
                "deliver": agg["shipped_ltr"],
                "return": agg["return_ltr"],
            },
            "quantity": {
                "order": agg["order_units"],
                "deliver": agg["shipped_units"],
                "return": agg["return_units"],
            },
        }

    def _matches(row_head, item_head):
        if item_head == "all":
            return True
        return _norm_sec_key(row_head) == item_head.upper()

    heads = ("all", "premium", "commodity")
    dated = [row for row in rows if row.get("d")]
    latest_month = max(
        (row["d"].month for row in dated if row["d"].year == year),
        default=month,
    )
    years_present = sorted({row["d"].year for row in dated}) or [year]
    year_start, year_end = years_present[0], years_present[-1]

    def _accumulate(predicate, item_head, key_fn):
        buckets = {}
        for row in dated:
            if not predicate(row["d"]):
                continue
            if not _matches(row.get("item_head"), item_head):
                continue
            agg = buckets.setdefault(key_fn(row["d"]), _empty())
            for field in _SEC_TREND_FIELDS:
                agg[field] += _num(row.get(field))
        return buckets

    def day_series(item_head):
        buckets = _accumulate(
            lambda d: d.year == year and d.month == month,
            item_head,
            lambda d: d.day,
        )
        out = []
        for day in range(1, days_in_month + 1):
            current = date(year, month, day)
            agg = (
                buckets.get(day, _empty())
                if (max_date and current <= max_date)
                else _empty()
            )
            out.append({
                "date": current.isoformat(),
                "period": current.isoformat(),
                "label": f"{day:02d}",
                "day": day,
                **_bundle(agg),
            })
        return out

    def month_series(item_head):
        buckets = _accumulate(
            lambda d: d.year == year,
            item_head,
            lambda d: d.month,
        )
        out = []
        for month_number in range(1, latest_month + 1):
            month_period = date(year, month_number, 1)
            out.append({
                "period": month_period.isoformat(),
                "label": month_period.strftime("%b").upper(),
                "month": month_number,
                **_bundle(buckets.get(month_number, _empty())),
            })
        return out

    def year_series(item_head):
        buckets = _accumulate(lambda d: True, item_head, lambda d: d.year)
        out = []
        for period_year in range(year_start, year_end + 1):
            out.append({
                "period": str(period_year),
                "label": str(period_year),
                "year": period_year,
                **_bundle(buckets.get(period_year, _empty())),
            })
        return out

    return {
        "day": {head: day_series(head) for head in heads},
        "month": {head: month_series(head) for head in heads},
        "year": {head: year_series(head) for head in heads},
    }


# Per-platform sources for the Sec Dashboard "year" filter. Mirrors the table
# each *_sec_dashboard_response reads from, so the dropdown only offers years
# that actually have data. SecMaster-backed platforms filter on their format.
# slug -> tuple of (table, year_sql_expression, format_filter_or_None). The year
# expression yields the year for each row; Swiggy derives it from ORDERED_DATE
# since swiggySec has no year column.
_SEC_DASHBOARD_YEAR_SOURCES = {
    "amazon": (
        ("amazon_sec_range_master_view", '"year"', None),
        ("amazon_sec_daily_master_view", '"year"', None),
    ),
    "blinkit": (("secmaster_mv", '"year"', "blinkit"),),
    "zepto": (("secmaster_mv", '"year"', "zepto"),),
    "bigbasket": (("secmaster_mv", '"year"', "bigbasket"),),
    "swiggy": (("swiggySec", 'EXTRACT(YEAR FROM "ORDERED_DATE")::int', None),),
    "flipkart": (("flipkart_secondary_all", '"year"', None),),
    "flipkart_grocery": (("flipkart_grocery_master", '"year"', None),),
}


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=300, prefix="plat.sec_years")
def sec_dashboard_years(request, slug: str):
    """Distinct years that actually have secondary data for the platform, so the
    Sec Dashboard year filter only lists years present in the database."""
    slug = (slug or "").strip().lower()
    _ensure_scope(request.user, slug)
    sources = _SEC_DASHBOARD_YEAR_SOURCES.get(slug)
    years: set[int] = set()
    errors = []
    if sources:
        with connection.cursor() as cur:
            for table, year_expr, fmt in sources:
                try:
                    if fmt:
                        cur.execute(
                            f'SELECT DISTINCT ({year_expr})::text FROM "{table}" '
                            'WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '
                            "'[^a-z0-9]+', '', 'g') = %s",
                            [fmt],
                        )
                    else:
                        cur.execute(
                            f'SELECT DISTINCT ({year_expr})::text FROM "{table}"'
                        )
                    for (raw,) in cur.fetchall():
                        digits = re.sub(r"\D", "", str(raw or ""))
                        if len(digits) == 4:
                            value = int(digits)
                            if 2000 <= value <= 2100:
                                years.add(value)
                except Exception as exc:  # noqa: BLE001
                    errors.append({"table": table, "error": str(exc)})
    return Response({"years": sorted(years, reverse=True), "errors": errors})


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.fk_sec")
def flipkart_grocery_sec_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "blinkit":
        return _blinkit_sec_dashboard_response(request)
    if slug == "swiggy":
        return _swiggy_sec_dashboard_response(request)
    if slug == "zepto":
        return _zepto_sec_dashboard_response(request)
    if slug == "bigbasket":
        return _bigbasket_sec_dashboard_response(request)
    if slug == "flipkart":
        return _flipkart_sec_dashboard_response(request)
    if slug == "amazon":
        return _amazon_sec_dashboard_response(request)
    if slug != "flipkart_grocery":
        raise ValidationError(
            "Sec Dashboard is available only for Amazon, Big Basket, Blinkit, Swiggy, Zepto, Flipkart and Flipkart Grocery."
        )

    month, year, defaulted_to_latest = _parse_sec_month_year(request.query_params)
    selected_date = _parse_sec_selected_date(request.query_params)
    date_filter, date_params = _sec_date_filter(selected_date, '"real_date"')

    max_date = _scalar(
        f"""
        SELECT MAX("real_date")
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
          {date_filter}
        """,
        [month, year, *date_params],
    )

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("qty"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sale_amt_exclusive"), 0) AS shipped_value
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
          {date_filter}
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY UPPER(TRIM("item_head"::text))
        """,
        [month, year, *date_params],
    )
    summary_by_head = {_norm_sec_key(r.get("item_head")): r for r in summary_raw}
    summary = []
    for item_head in _FK_GROCERY_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_units = _num(row.get("shipped_units"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        summary.append({
            "item_head": item_head,
            "shipped_units": shipped_units,
            "shipped_ltr": shipped_ltr,
            "shipped_value": _num(row.get("shipped_value")),
            "per_liter_shpd": _per_liter_shpd(shipped_units, shipped_ltr),
        })

    detail_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("sale_amt_exclusive"), 0) AS shipped_value,
            COALESCE(SUM("qty"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
          {date_filter}
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [month, year, *date_params],
    )
    detail_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))): r
        for r in detail_raw
    }

    details = []
    for item_head, category, sub_category, per_ltr in _FK_GROCERY_SEC_DETAIL_ROWS:
        row = detail_by_key.get((_norm_sec_key(sub_category), _norm_sec_key(per_ltr)), {})
        shipped_units = _num(row.get("shipped_units"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        details.append({
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": per_ltr,
            "shipped_value": _num(row.get("shipped_value")),
            "shipped_units": shipped_units,
            "shipped_ltr": shipped_ltr,
            "per_liter_shpd": _per_liter_shpd(shipped_units, shipped_ltr),
        })

    top_items = _top_ltr_items_from_table(
        table_name="flipkart_grocery_master",
        month_column='"month"',
        year_column='"year"',
        item_column='"item"',
        units_column='"qty"',
        ltr_column='"ltr_sold"',
        value_column='"sale_amt_exclusive"',
        month_value=month,
        year_value=year,
        date_filter=date_filter,
        date_params=date_params,
    )

    sec_trend = _build_sec_keyed_trend(
        table="flipkart_grocery_master",
        date_col='"real_date"',
        month=month,
        year=year,
        max_date=max_date,
        days_in_month=monthrange(year, month)[1],
        value_expr='"sale_amt_exclusive"',
        ltr_expr='"ltr_sold"',
        qty_expr='"qty"',
    )

    return Response({
        "source": "flipkart_grocery_master",
        "sec_trend": sec_trend,
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if max_date else None,
        "summary": summary,
        "summary_total": _sec_total(summary),
        "details": details,
        "detail_total": _sec_total(details),
        "top_items": top_items,
    })


def _amazon_effective_margin(row: dict) -> float | None:
    shipped_value = _num(row.get("shipped_value"))
    margin_value = _num(row.get("margin_value"))
    if shipped_value == 0:
        return None
    return margin_value / shipped_value


def _amazon_projection(shipped_ltr, elapsed_day: int, days_in_month: int) -> float:
    if elapsed_day <= 0:
        return 0.0
    return _num(shipped_ltr) / elapsed_day * days_in_month


def _amazon_sec_totals(rows: list[dict], *, include_projection: bool = True) -> dict:
    total = {
        "order_value": sum(_num(row.get("order_value")) for row in rows),
        "order_ltr": sum(_num(row.get("order_ltr")) for row in rows),
        "order_units": sum(_num(row.get("order_units")) for row in rows),
        "shipped_value": sum(_num(row.get("shipped_value")) for row in rows),
        "shipped_ltr": sum(_num(row.get("shipped_ltr")) for row in rows),
        "return_value": sum(_num(row.get("return_value")) for row in rows),
        "return_ltr": sum(_num(row.get("return_ltr")) for row in rows),
        "shipped_units": sum(_num(row.get("shipped_units")) for row in rows),
        "return_units": sum(_num(row.get("return_units")) for row in rows),
    }
    total["per_liter_shpd"] = _value_per_ltr_zero(
        total["shipped_value"],
        total["shipped_ltr"],
    )
    if include_projection:
        total["projection_ltr"] = sum(_num(row.get("projection_ltr")) for row in rows)
    margin_value = sum(_num(row.get("margin_value")) for row in rows)
    margin_tax_value = sum(_num(row.get("margin_tax_value")) for row in rows)
    total["margin_value"] = margin_value
    total["margin_tax_value"] = margin_tax_value
    total["margin_pct"] = (
        margin_value / total["shipped_value"]
        if total["shipped_value"]
        else None
    )
    total["net_realise_shpd"] = _value_per_ltr_zero(
        total["shipped_value"] - margin_tax_value,
        total["shipped_ltr"],
    )
    return total


def _amazon_secondary_monthly_dashboard_response(request):
    year, defaulted_to_latest = _parse_amazon_secondary_monthly_year(request.query_params)

    # amazon_sec_range_master_view keys each row by month_day =
    # TO_CHAR(to_date,'DD') || '-' || <MONTH> — a zero-padded day tied to the
    # range's latest to_date. For a COMPLETED month that is the last calendar
    # day; for the CURRENT, in-progress month it is the latest upload day. The
    # old code keyed every month to monthrange()'s last day, so the current
    # month never matched its own rows and always read 0 (audit finding #13).
    # Resolve each month's real latest day from the data instead; months with no
    # data fall back to the last calendar day (which harmlessly matches nothing).
    max_day_rows = _dict_rows(
        """
        SELECT UPPER(TRIM("month"::text)) AS mon_name,
               MAX(EXTRACT(DAY FROM "to_date"))::int AS max_day
        FROM "amazon_sec_range_master_view"
        WHERE "year" = %s
        GROUP BY UPPER(TRIM("month"::text))
        """,
        [year],
    )
    max_day_by_month_name = {
        _norm_sec_key(r.get("mon_name")): int(r.get("max_day"))
        for r in max_day_rows
        if r.get("mon_name") and r.get("max_day") is not None
    }

    months = []
    for month in range(1, 13):
        month_key = _month_name(month)
        day = max_day_by_month_name.get(_norm_sec_key(month_key), monthrange(year, month)[1])
        months.append({
            "month": month,
            "key": month_key,
            "label": "FEBURARY" if month == 2 else month_key,
            "day": day,
            # Zero-padded to match the stored "DD-MONTH" form exactly.
            "month_day": f"{int(day):02d}-{month_key}",
        })
    month_keys = [month["key"] for month in months]
    month_days = [month["month_day"] for month in months]
    month_day_by_key = {month["key"]: month["month_day"] for month in months}
    placeholders = ", ".join(["%s"] * len(month_days))

    def empty_month_values(fields: tuple[str, ...]) -> dict:
        return {month_key: {field: 0.0 for field in fields} for month_key in month_keys}

    def sum_month_rows(rows: list[dict], fields: tuple[str, ...]) -> dict:
        total = {"months": empty_month_values(fields)}
        for month_key in month_keys:
            for field in fields:
                total["months"][month_key][field] = sum(
                    _num(row.get("months", {}).get(month_key, {}).get(field))
                    for row in rows
                )
        return total

    max_date = _scalar(
        """
        SELECT MAX("to_date")
        FROM "amazon_sec_range_master_view"
        WHERE "year" = %s
        """,
        [year],
    )

    period_row_count = int(_scalar(
        f"""
        SELECT COUNT(*)
        FROM "amazon_sec_range_master_view"
        WHERE "year" = %s
          AND UPPER(TRIM("month_day"::text)) IN ({placeholders})
        """,
        [year, *month_days],
    ) or 0)

    monthly_summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("month_day"::text)) AS month_day_key,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr
        FROM "amazon_sec_range_master_view"
        WHERE "year" = %s
          AND UPPER(TRIM("month_day"::text)) IN ({placeholders})
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY')
        GROUP BY
            UPPER(TRIM("month_day"::text)),
            UPPER(TRIM("item_head"::text))
        """,
        [year, *month_days],
    )
    summary_by_key = {
        (_norm_sec_key(row.get("item_head")), _norm_sec_key(row.get("month_day_key"))): row
        for row in monthly_summary_raw
    }

    sales_liters = []
    sales_values = []
    for item_head in _AMAZON_SEC_MONTHLY_ITEM_HEADS:
        litre_months = empty_month_values(("order_ltr", "shipped_ltr"))
        value_months = empty_month_values(("order_value", "shipped_value"))
        for month_key in month_keys:
            month_day = month_day_by_key[month_key]
            row = summary_by_key.get((item_head, month_day), {})
            litre_months[month_key]["order_ltr"] = _num(row.get("order_ltr"))
            litre_months[month_key]["shipped_ltr"] = _num(row.get("shipped_ltr"))
            value_months[month_key]["order_value"] = _num(row.get("order_value"))
            value_months[month_key]["shipped_value"] = _num(row.get("shipped_value"))
        sales_liters.append({
            "type": item_head,
            "months": litre_months,
        })
        sales_values.append({
            "type": item_head,
            "months": value_months,
        })

    sales_values_total_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("month_day"::text)) AS month_day_key,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value
        FROM "amazon_sec_range_master_view"
        WHERE "year" = %s
          AND UPPER(TRIM("month_day"::text)) IN ({placeholders})
        GROUP BY UPPER(TRIM("month_day"::text))
        """,
        [year, *month_days],
    )
    sales_values_total_by_key = {
        _norm_sec_key(row.get("month_day_key")): row
        for row in sales_values_total_raw
    }
    sales_values_total = {"months": empty_month_values(("order_value", "shipped_value"))}
    for month_key in month_keys:
        month_day = month_day_by_key[month_key]
        row = sales_values_total_by_key.get(month_day, {})
        sales_values_total["months"][month_key]["order_value"] = _num(row.get("order_value"))
        sales_values_total["months"][month_key]["shipped_value"] = _num(row.get("shipped_value"))

    category_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("month_day"::text)) AS month_day_key,
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr
        FROM "amazon_sec_range_master_view"
        WHERE "year" = %s
          AND UPPER(TRIM("month_day"::text)) IN ({placeholders})
        GROUP BY
            UPPER(TRIM("month_day"::text)),
            UPPER(TRIM("sub_category"::text))
        """,
        [year, *month_days],
    )
    category_by_key = {
        (_norm_sec_key(row.get("sub_category_key")), _norm_sec_key(row.get("month_day_key"))): row
        for row in category_raw
    }

    category_liters = []
    category_values = []
    for item_head, category, sub_category in _AMAZON_SEC_MONTHLY_CATEGORY_ROWS:
        litre_months = empty_month_values(("order_ltr", "shipped_ltr"))
        value_months = empty_month_values(("order_value", "shipped_value"))
        for month_key in month_keys:
            month_day = month_day_by_key[month_key]
            row = category_by_key.get((_norm_sec_key(sub_category), month_day), {})
            litre_months[month_key]["order_ltr"] = _num(row.get("order_ltr"))
            litre_months[month_key]["shipped_ltr"] = _num(row.get("shipped_ltr"))
            value_months[month_key]["order_value"] = _num(row.get("order_value"))
            value_months[month_key]["shipped_value"] = _num(row.get("shipped_value"))
        category_liters.append({
            "type": item_head,
            "category": category,
            "sub_category": sub_category,
            "months": litre_months,
        })
        category_values.append({
            "type": item_head,
            "category": category,
            "sub_category": sub_category,
            "months": value_months,
        })

    mom_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("month_day"::text)) AS month_day_key,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr
        FROM "amazon_sec_range_master_view"
        WHERE UPPER(TRIM("month_day"::text)) IN ({placeholders})
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY')
        GROUP BY
            UPPER(TRIM("month_day"::text)),
            UPPER(TRIM("item_head"::text))
        """,
        month_days,
    )
    mom_by_key = {
        (_norm_sec_key(row.get("item_head")), _norm_sec_key(row.get("month_day_key"))): _num(row.get("shipped_ltr"))
        for row in mom_raw
    }

    mom_growth = []
    previous_premium = 0.0
    previous_commodity = 0.0
    for index, month_info in enumerate(months):
        month_day = month_info["month_day"]
        premium_ltr = mom_by_key.get(("PREMIUM", month_day), 0.0)
        commodity_ltr = mom_by_key.get(("COMMODITY", month_day), 0.0)
        if index == 0:
            premium_growth = 0.0
            commodity_growth = 0.0
        else:
            premium_growth = _safe_div(premium_ltr - previous_premium, previous_premium)
            commodity_growth = _safe_div(commodity_ltr - previous_commodity, previous_commodity)
        mom_growth.append({
            "month": month_info["key"],
            "label": month_info["label"],
            "month_day": month_day,
            "premium_ltr": premium_ltr,
            "commodity_ltr": commodity_ltr,
            "premium_growth": premium_growth,
            "commodity_growth": commodity_growth,
        })
        previous_premium = premium_ltr
        previous_commodity = commodity_ltr

    notes = []
    if period_row_count == 0:
        notes.append("No Amazon SEC range month-end data found for the selected year.")
    notes.append("Uses exact Excel month-end filters against amazon_sec_range_master_view.")
    notes.append("MOM growth follows Excel and does not filter by year.")

    return Response({
        "source": "amazon_sec_range_master_view",
        "format": "AMAZON",
        "dashboard_title": "Amazon Secondary Monthly Dashboard",
        "defaulted_to_latest": defaulted_to_latest,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "month_strategy": "excel_month_end",
        "period_row_count": period_row_count,
        "months": months,
        "sales_liters": sales_liters,
        "sales_liters_total": sum_month_rows(
            sales_liters,
            ("order_ltr", "shipped_ltr"),
        ),
        "sales_values": sales_values,
        "sales_values_total": sales_values_total,
        "category_liters": category_liters,
        "category_liters_total": sum_month_rows(
            category_liters,
            ("order_ltr", "shipped_ltr"),
        ),
        "category_values": category_values,
        "category_values_total": sum_month_rows(
            category_values,
            ("order_value", "shipped_value"),
        ),
        "mom_growth": mom_growth,
        "notes": {
            "messages": notes,
            "mom_growth_year_filter": False,
            "month_strategy": "excel_month_end",
            "source_view_only": True,
            "value_total_includes_all_item_heads": True,
            "litre_total_item_heads": list(_AMAZON_SEC_MONTHLY_ITEM_HEADS),
            "category_template_fixed": True,
        },
    })


def _amazon_comparison_dashboard_response(request):
    (
        selected_month,
        selected_year,
        history_year,
        defaulted_to_latest,
    ) = _parse_amazon_comparison_params(request.query_params)
    selected_to_date = _scalar(
        """
        SELECT MAX("to_date")
        FROM "amazon_sec_range_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
        """,
        [selected_month],
    )
    selected_month_day = (
        f"{selected_to_date.day:02d}-{selected_month}"
        if hasattr(selected_to_date, "day")
        else None
    )

    current_by_key = {}
    if selected_month_day:
        current_raw = _dict_rows(
            """
            SELECT
                UPPER(TRIM("sub_category"::text)) AS sub_category_key,
                UPPER(TRIM("brand_2"::text)) AS brand_key,
                COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
                COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_rev,
                COALESCE(SUM("shipped_revenue_after_margin"), 0) AS rev_after_margin
            FROM "amazon_sec_range_master_view"
            WHERE "year" = %s
              AND UPPER(TRIM("month_day"::text)) = %s
            GROUP BY
                UPPER(TRIM("sub_category"::text)),
                UPPER(TRIM("brand_2"::text))
            """,
            [selected_year, selected_month_day],
        )
        current_by_key = {
            (
                _norm_sec_key(row.get("sub_category_key")),
                _norm_sec_key(row.get("brand_key")),
            ): row
            for row in current_raw
        }

    highest_raw = _dict_rows(
        """
        WITH monthly AS (
            SELECT
                UPPER(TRIM("sub_category"::text)) AS sub_category_key,
                UPPER(TRIM("brand_2"::text)) AS brand_key,
                UPPER(TRIM("month"::text)) AS month_key,
                COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
                COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_rev,
                COALESCE(SUM("shipped_revenue_after_margin"), 0) AS rev_after_margin
            FROM "amazon_sec_range_master_view"
            WHERE "year" = %s
            GROUP BY
                UPPER(TRIM("sub_category"::text)),
                UPPER(TRIM("brand_2"::text)),
                UPPER(TRIM("month"::text))
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY sub_category_key, brand_key
                    ORDER BY shipped_ltr DESC, month_key ASC
                ) AS rn
            FROM monthly
        )
        SELECT
            sub_category_key,
            brand_key,
            month_key,
            shipped_ltr,
            shipped_rev,
            rev_after_margin
        FROM ranked
        WHERE rn = 1
        """,
        [history_year],
    )
    highest_by_key = {
        (
            _norm_sec_key(row.get("sub_category_key")),
            _norm_sec_key(row.get("brand_key")),
        ): row
        for row in highest_raw
    }

    rows = []
    for item_head, category, sub_category, brand in _AMAZON_COMPARISON_ROWS:
        highest = highest_by_key.get(
            (_norm_sec_key(sub_category), _norm_sec_key(brand)),
            {},
        )
        highest_ltr = _num(highest.get("shipped_ltr"))
        highest_rev = _num(highest.get("shipped_rev"))
        highest_rev_after_margin = _num(highest.get("rev_after_margin"))
        current = current_by_key.get(
            (_norm_sec_key(sub_category), _norm_sec_key(brand)),
            {},
        )
        shipped_ltr = _num(current.get("shipped_ltr"))
        shipped_rev = _num(current.get("shipped_rev"))
        rev_after_margin = _num(current.get("rev_after_margin"))
        rows.append({
            "type": item_head,
            "category": category,
            "sub_category": sub_category,
            "brand": brand,
            "highest": {
                "month": highest.get("month_key") or None,
                "shipped_ltr": highest_ltr,
                "shipped_rev": highest_rev,
                "rev_after_margin": highest_rev_after_margin,
                "price_per_ltr": _value_per_ltr_zero(highest_rev, highest_ltr) * 1.05,
                "net_realise": (
                    _value_per_ltr_zero(highest_rev_after_margin, highest_ltr) * 0.95
                ),
            },
            "current": {
                "shipped_ltr": shipped_ltr,
                "shipped_rev": shipped_rev,
                "rev_after_margin": rev_after_margin,
                "price_per_ltr": _value_per_ltr_zero(shipped_rev, shipped_ltr) * 1.05,
                "net_realise": _value_per_ltr_zero(rev_after_margin, shipped_ltr) * 0.95,
            },
        })

    totals = {
        "highest": {
            "shipped_ltr": sum(_num(row["highest"].get("shipped_ltr")) for row in rows),
            "shipped_rev": sum(_num(row["highest"].get("shipped_rev")) for row in rows),
            "rev_after_margin": sum(
                _num(row["highest"].get("rev_after_margin"))
                for row in rows
            ),
            "price_per_ltr": None,
            "net_realise": None,
        },
        "current": {
            "shipped_ltr": sum(_num(row["current"].get("shipped_ltr")) for row in rows),
            "shipped_rev": sum(_num(row["current"].get("shipped_rev")) for row in rows),
            "rev_after_margin": None,
            "price_per_ltr": None,
            "net_realise": None,
        },
    }

    return Response({
        "source": "amazon_sec_range_master_view",
        "dashboard_title": "Amazon Comparison Dashboard",
        "format": "AMAZON",
        "selected_month": selected_month,
        "selected_year": selected_year,
        "history_year": history_year,
        "defaulted_to_latest": defaulted_to_latest,
        "selected_to_date": (
            selected_to_date.isoformat()
            if hasattr(selected_to_date, "isoformat")
            else selected_to_date
        ),
        "selected_month_day": selected_month_day,
        "rows": rows,
        "totals": totals,
        "notes": {
            "excel_visible_match": False,
            "highest_block_calculated": True,
            "numeric_empty_values": 0,
            "text_empty_display": "-",
        },
    })


def _amazon_sec_dashboard_response(request):
    month_params = request.query_params.copy()
    month_params.pop("date", None)
    month, year, defaulted_to_latest = _parse_sec_month_year(
        month_params,
        latest_source="amazon_sec_range_master_view",
    )
    selected_date = None
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]

    # Ads Dashboard: `as_of_date` scopes Total Sales to the picked calendar date.
    # Amazon SEC rows are cumulative range reports keyed by their to_date, so we
    # cap the effective max_date at the selected day — the whole downstream
    # cutoff (cutoff_month_day_keys, elapsed_day, projections) then reflects the
    # latest report on or before that date, instead of the month's latest.
    as_of_date = _parse_sec_as_of_date(request.query_params)
    as_of_filter, as_of_params = ("", [])
    if as_of_date:
        as_of_filter = ' AND "to_date" <= %s'
        as_of_params = [as_of_date]

    max_date = _scalar(
        f"""
        SELECT MAX("to_date")
        FROM "amazon_sec_range_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          AND "to_date" IS NOT NULL
          {as_of_filter}
        """,
        [month_name, year, *as_of_params],
    )
    elapsed_day = _sec_elapsed_day(max_date)
    cutoff_month_day_keys = _amazon_sec_month_day_keys(max_date, month_name)
    cutoff_month_day = cutoff_month_day_keys[0] if cutoff_month_day_keys else None

    base_params = [year]
    base_where = 'WHERE "year" = %s'
    if cutoff_month_day_keys:
        month_day_placeholders = ", ".join(["%s"] * len(cutoff_month_day_keys))
        base_where += f' AND UPPER(TRIM("month_day"::text)) IN ({month_day_placeholders})'
        base_params.extend(cutoff_month_day_keys)
    else:
        base_where += " AND 1 = 0"

    period_row_count = int(_scalar(
        f'SELECT COUNT(*) FROM "amazon_sec_range_master_view" {base_where}',
        base_params,
    ) or 0)
    has_period_data = period_row_count > 0
    sub_category_key_expr = """
        CASE
            WHEN UPPER(TRIM("sub_category"::text)) = 'SEASAME OIL'
                THEN 'SESAME OIL'
            ELSE UPPER(TRIM("sub_category"::text))
        END
    """

    category_raw = _dict_rows(
        f"""
        SELECT
            {sub_category_key_expr} AS sub_category_key,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_units"), 0) AS return_units,
            COALESCE(
                SUM(
                    "calculated_shipped_revenue"
                    * (COALESCE("margin_pct", 0) / 100.0)
                ),
                0
            ) AS margin_value,
            COALESCE(
                SUM(
                    "calculated_shipped_revenue"
                    * ((COALESCE("margin_pct", 0) + 5) / 100.0)
                ),
                0
            ) AS margin_tax_value
        FROM "amazon_sec_range_master_view"
        {base_where}
        GROUP BY {sub_category_key_expr}
        """,
        base_params,
    )
    category_by_key = {
        _norm_sec_key(row.get("sub_category_key")): row
        for row in category_raw
    }

    category_summary = []
    for fmt, item_head, category, sub_category in _AMAZON_SEC_CATEGORY_ROWS:
        row = category_by_key.get(_norm_sec_key(sub_category), {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        margin_pct = _amazon_effective_margin(row)
        margin_tax_value = _num(row.get("margin_tax_value"))
        category_summary.append({
            "format": fmt,
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "order_value": _num(row.get("order_value")),
            "order_ltr": _num(row.get("order_ltr")),
            "order_units": _num(row.get("order_units")),
            "shipped_value": shipped_value,
            "shipped_ltr": shipped_ltr,
            "return_value": _num(row.get("return_value")),
            "return_ltr": _num(row.get("return_ltr")),
            "shipped_units": _num(row.get("shipped_units")),
            "return_units": _num(row.get("return_units")),
            "margin_pct": margin_pct,
            "margin_value": _num(row.get("margin_value")),
            "margin_tax_value": margin_tax_value,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
            "net_realise_shpd": _value_per_ltr_zero(
                shipped_value - margin_tax_value,
                shipped_ltr,
            ),
            "projection_ltr": _amazon_projection(
                shipped_ltr,
                elapsed_day,
                days_in_month,
            ),
        })

    rk_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_units"), 0) AS return_units,
            COALESCE(
                SUM(
                    "calculated_shipped_revenue"
                    * (COALESCE("margin_pct", 0) / 100.0)
                ),
                0
            ) AS margin_value,
            COALESCE(
                SUM(
                    "calculated_shipped_revenue"
                    * ((COALESCE("margin_pct", 0) + 5) / 100.0)
                ),
                0
            ) AS margin_tax_value
        FROM "amazon_sec_range_master_view"
        {base_where}
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY UPPER(TRIM("item_head"::text))
        """,
        base_params,
    )
    rk_by_key = {_norm_sec_key(row.get("item_head")): row for row in rk_raw}

    rk_world_summary = []
    rk_world_returns = []
    for item_head in _AMAZON_SEC_ITEM_HEADS:
        row = rk_by_key.get(item_head, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        margin_tax_value = _num(row.get("margin_tax_value"))
        rk_world_summary.append({
            "item_head": item_head,
            "order_value": _num(row.get("order_value")),
            "order_ltr": _num(row.get("order_ltr")),
            "order_units": _num(row.get("order_units")),
            "shipped_value": shipped_value,
            "shipped_ltr": shipped_ltr,
            "shipped_units": _num(row.get("shipped_units")),
            "return_value": _num(row.get("return_value")),
            "return_ltr": _num(row.get("return_ltr")),
            "return_units": _num(row.get("return_units")),
            "margin_pct": _amazon_effective_margin(row),
            "margin_value": _num(row.get("margin_value")),
            "margin_tax_value": margin_tax_value,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
            "net_realise_shpd": _value_per_ltr_zero(
                shipped_value - margin_tax_value,
                shipped_ltr,
            ),
            "projection_ltr": _amazon_projection(
                shipped_ltr,
                elapsed_day,
                days_in_month,
            ),
        })
        rk_world_returns.append({
            "item_head": item_head,
            "return_value": _num(row.get("return_value")),
            "return_ltr": _num(row.get("return_ltr")),
            "return_units": _num(row.get("return_units")),
        })

    sku_details = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            UPPER(TRIM("category"::text)) AS category,
            UPPER(TRIM("sub_category"::text)) AS sub_category,
            COALESCE(NULLIF(TRIM("brand_2"::text), ''), '-') AS brand,
            COALESCE(NULLIF(TRIM("per_unit"::text), ''), '-') AS per_ltr,
            TRIM("asin"::text) AS asin,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_range_master_view"
        {base_where}
          AND NULLIF(TRIM("asin"::text), '') IS NOT NULL
        GROUP BY
            UPPER(TRIM("item_head"::text)),
            UPPER(TRIM("category"::text)),
            UPPER(TRIM("sub_category"::text)),
            COALESCE(NULLIF(TRIM("brand_2"::text), ''), '-'),
            COALESCE(NULLIF(TRIM("per_unit"::text), ''), '-'),
            TRIM("asin"::text)
        ORDER BY
            UPPER(TRIM("item_head"::text)) DESC,
            UPPER(TRIM("category"::text)) ASC,
            UPPER(TRIM("sub_category"::text)) ASC,
            COALESCE(NULLIF(TRIM("per_unit"::text), ''), '-') ASC,
            TRIM("asin"::text) ASC
        """,
        base_params,
    )
    for row in sku_details:
        row["order_value"] = _num(row.get("order_value"))
        row["order_ltr"] = _num(row.get("order_ltr"))
        row["order_units"] = _num(row.get("order_units"))
        row["shipped_value"] = _num(row.get("shipped_value"))
        row["shipped_ltr"] = _num(row.get("shipped_ltr"))
        row["return_value"] = _num(row.get("return_value"))
        row["return_ltr"] = _num(row.get("return_ltr"))
        row["shipped_units"] = _num(row.get("shipped_units"))
        row["return_units"] = _num(row.get("return_units"))
        row["per_liter_shpd"] = _value_per_ltr_zero(
            row["shipped_value"],
            row["shipped_ltr"],
        )

    sku_total = _amazon_sec_totals(sku_details, include_projection=False)
    category_total = _amazon_sec_totals(category_summary)
    rk_world_total = _amazon_sec_totals(rk_world_summary)
    rk_return_total = {
        "return_value": sum(_num(row.get("return_value")) for row in rk_world_returns),
        "return_ltr": sum(_num(row.get("return_ltr")) for row in rk_world_returns),
        "return_units": sum(_num(row.get("return_units")) for row in rk_world_returns),
    }

    visual_total_rows = _dict_rows(
        f"""
        SELECT
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_range_master_view"
        {base_where}
        """,
        base_params,
    )
    visual_total = visual_total_rows[0] if visual_total_rows else {}

    def visual_metric_bundle(row: dict) -> dict:
        return {
            "values": {
                "order": _num(row.get("order_value")),
                "deliver": _num(row.get("shipped_value")),
                "return": _num(row.get("return_value")),
            },
            "ltrs": {
                "order": _num(row.get("order_ltr")),
                "deliver": _num(row.get("shipped_ltr")),
                "return": _num(row.get("return_ltr")),
            },
            "quantity": {
                "order": _num(row.get("order_units")),
                "deliver": _num(row.get("shipped_units")),
                "return": _num(row.get("return_units")),
            },
        }

    item_head_split = [
        {
            "item_head": row.get("item_head"),
            "label": row.get("item_head"),
            **visual_metric_bundle(row),
        }
        for row in rk_world_summary
    ]

    top_sku_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            TRIM("asin"::text) AS asin,
            COALESCE(
                NULLIF(
                    TRIM(
                        CONCAT_WS(
                            ' ',
                            NULLIF(TRIM("sub_category"::text), ''),
                            NULLIF(TRIM("per_unit"::text), '')
                        )
                    ),
                    ''
                ),
                NULLIF(TRIM("asin"::text), ''),
                'UNMAPPED'
            ) AS label,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_range_master_view"
        {base_where}
          AND NULLIF(TRIM("asin"::text), '') IS NOT NULL
        GROUP BY
            UPPER(TRIM("item_head"::text)),
            TRIM("asin"::text),
            COALESCE(
                NULLIF(
                    TRIM(
                        CONCAT_WS(
                            ' ',
                            NULLIF(TRIM("sub_category"::text), ''),
                            NULLIF(TRIM("per_unit"::text), '')
                        )
                    ),
                    ''
                ),
                NULLIF(TRIM("asin"::text), ''),
                'UNMAPPED'
            )
        """,
        base_params,
    )

    metric_fields = (
        "order_value",
        "order_ltr",
        "order_units",
        "shipped_value",
        "shipped_ltr",
        "shipped_units",
        "return_value",
        "return_ltr",
        "return_units",
    )

    def aggregate_metric_rows(rows, key_fields):
        aggregated = {}
        for row in rows:
            key = tuple(row.get(field) or "" for field in key_fields)
            if key not in aggregated:
                aggregated[key] = {field: row.get(field) for field in key_fields}
                for metric_field in metric_fields:
                    aggregated[key][metric_field] = 0.0
            for metric_field in metric_fields:
                aggregated[key][metric_field] += _num(row.get(metric_field))
        return list(aggregated.values())

    def rows_for_item_head(rows, item_head):
        if item_head == "all":
            return rows
        item_head_key = item_head.upper()
        return [
            row
            for row in rows
            if _norm_sec_key(row.get("item_head")) == item_head_key
        ]

    def build_top_sku_rows(metric_field, item_head="all"):
        source_rows = aggregate_metric_rows(
            rows_for_item_head(top_sku_raw, item_head),
            ("asin", "label"),
        )
        sorted_rows = sorted(
            source_rows,
            key=lambda row: (
                -_num(row.get(metric_field)),
                str(row.get("label") or row.get("asin") or ""),
            ),
        )[:10]
        return [
            {
                "asin": row.get("asin"),
                "label": row.get("label"),
                **visual_metric_bundle(row),
            }
            for row in sorted_rows
        ]

    top_10_sku = {
        item_head: {
            "values": build_top_sku_rows("shipped_value", item_head),
            "ltrs": build_top_sku_rows("shipped_ltr", item_head),
            "quantity": build_top_sku_rows("shipped_units", item_head),
        }
        for item_head in ("all", "premium", "commodity")
    }

    sub_category_mix_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'UNMAPPED') AS label,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("calculated_shipped_revenue"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_range_master_view"
        {base_where}
        GROUP BY
            UPPER(TRIM("item_head"::text)),
            COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'UNMAPPED')
        """,
        base_params,
    )

    def build_sub_category_mix_rows(metric_field, item_head="all"):
        source_rows = aggregate_metric_rows(
            rows_for_item_head(sub_category_mix_raw, item_head),
            ("label",),
        )
        sorted_rows = sorted(
            source_rows,
            key=lambda row: (
                -_num(row.get(metric_field)),
                str(row.get("label") or ""),
            ),
        )
        return [
            {
                "label": row.get("label"),
                "sub_category": row.get("label"),
                **visual_metric_bundle(row),
            }
            for row in sorted_rows
        ]

    sub_category_mix = {
        item_head: {
            "values": build_sub_category_mix_rows("shipped_value", item_head),
            "ltrs": build_sub_category_mix_rows("shipped_ltr", item_head),
            "quantity": build_sub_category_mix_rows("shipped_units", item_head),
        }
        for item_head in ("all", "premium", "commodity")
    }

    # Trend (day / month / year) is built per item-head — "all", "premium" and
    # "commodity" — so the dashboard can filter the trend lines the same way the
    # Top-SKU and sub-category charts already do. The "all" series sums every
    # item head per period and reproduces the previous item-head-agnostic trend
    # exactly. rows_for_item_head("all") returns every row; "premium"/"commodity"
    # keep only matching item heads.
    def _sum_trend_by_period(raw_rows, period_key, item_head):
        by_period = {}
        for row in rows_for_item_head(raw_rows, item_head):
            key = row.get(period_key)
            agg = by_period.get(key)
            if agg is None:
                agg = {field: 0.0 for field in metric_fields}
                by_period[key] = agg
            for field in metric_fields:
                agg[field] += _num(row.get(field))
        return by_period

    _ITEM_HEAD_KEYS = ("all", "premium", "commodity")

    daily_raw = _dict_rows(
        """
        SELECT
            "to_date"::date AS sale_date,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("shipped_revenue_2"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_daily_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          AND "to_date" IS NOT NULL
        GROUP BY "to_date"::date, UPPER(TRIM("item_head"::text))
        ORDER BY "to_date"::date
        """,
        [month_name, year],
    )

    def build_daily_trend(item_head):
        by_date = _sum_trend_by_period(daily_raw, "sale_date", item_head)
        rows = []
        for day in range(1, days_in_month + 1):
            current_date = date(year, month, day)
            row = (
                by_date.get(current_date, {})
                if max_date and current_date <= max_date
                else {}
            )
            rows.append({
                "date": current_date.isoformat(),
                "period": current_date.isoformat(),
                "label": f"{day:02d}",
                "day": day,
                **visual_metric_bundle(row),
            })
        return rows

    trend_day = {ih: build_daily_trend(ih) for ih in _ITEM_HEAD_KEYS}

    monthly_raw = _dict_rows(
        """
        SELECT
            DATE_TRUNC('month', "to_date")::date AS period,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("shipped_revenue_2"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_daily_master_view"
        WHERE "to_date" IS NOT NULL
          AND EXTRACT(YEAR FROM "to_date")::integer = %s
        GROUP BY DATE_TRUNC('month', "to_date")::date, UPPER(TRIM("item_head"::text))
        ORDER BY DATE_TRUNC('month', "to_date")::date
        """,
        [year],
    )
    monthly_periods = {row["period"] for row in monthly_raw if row.get("period")}
    latest_month_period = max(monthly_periods, default=date(year, month, 1))

    def build_monthly_trend(item_head):
        by_period = _sum_trend_by_period(monthly_raw, "period", item_head)
        rows = []
        for month_number in range(1, latest_month_period.month + 1):
            month_period = date(year, month_number, 1)
            row = by_period.get(month_period, {})
            rows.append({
                "period": month_period.isoformat(),
                "label": month_period.strftime("%b").upper(),
                "month": month_number,
                **visual_metric_bundle(row),
            })
        return rows

    trend_month = {ih: build_monthly_trend(ih) for ih in _ITEM_HEAD_KEYS}

    yearly_raw = _dict_rows(
        """
        SELECT
            EXTRACT(YEAR FROM "to_date")::integer AS period_year,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ordered_revenue"), 0) AS order_value,
            COALESCE(SUM("ordered_litres"), 0) AS order_ltr,
            COALESCE(SUM("ordered_units"), 0) AS order_units,
            COALESCE(SUM("shipped_revenue_2"), 0) AS shipped_value,
            COALESCE(SUM("shipped_litres"), 0) AS shipped_ltr,
            COALESCE(SUM("shipped_units"), 0) AS shipped_units,
            COALESCE(SUM("return_value"), 0) AS return_value,
            COALESCE(SUM("return_litres"), 0) AS return_ltr,
            COALESCE(SUM("return_units"), 0) AS return_units
        FROM "amazon_sec_daily_master_view"
        WHERE "to_date" IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM "to_date")::integer, UPPER(TRIM("item_head"::text))
        ORDER BY EXTRACT(YEAR FROM "to_date")::integer
        """,
        [],
    )
    yearly_years = {
        int(row["period_year"]) for row in yearly_raw if row.get("period_year")
    }
    if yearly_years:
        year_start = min(yearly_years)
        year_end = max(yearly_years)
    else:
        year_start = year_end = year

    def build_yearly_trend(item_head):
        by_year = {}
        for row in rows_for_item_head(yearly_raw, item_head):
            key = int(row["period_year"]) if row.get("period_year") else None
            if key is None:
                continue
            agg = by_year.get(key)
            if agg is None:
                agg = {field: 0.0 for field in metric_fields}
                by_year[key] = agg
            for field in metric_fields:
                agg[field] += _num(row.get(field))
        rows = []
        for period_year in range(year_start, year_end + 1):
            row = by_year.get(period_year, {})
            rows.append({
                "period": str(period_year),
                "label": str(period_year),
                "year": period_year,
                **visual_metric_bundle(row),
            })
        return rows

    trend_year = {ih: build_yearly_trend(ih) for ih in _ITEM_HEAD_KEYS}

    visual_dashboard = {
        "show_by_options": ["values", "ltrs", "quantity"],
        "cards": visual_metric_bundle(visual_total),
        "trend": {
            "day": trend_day,
            "month": trend_month,
            "year": trend_year,
        },
        "item_head_split": item_head_split,
        "top_10_sku": top_10_sku,
        "sub_category_mix": sub_category_mix,
    }

    notes = []
    if not has_period_data:
        notes.append("No Amazon SEC range data found for the selected period.")
    notes.append("AMAZON MP block is excluded because this dashboard uses only amazon_sec_range_master_view.")
    notes.append("Margins are sourced from amazon_sec_range_margins through amazon_sec_range_master_view.")

    return Response({
        "source": "amazon_sec_range_master_view",
        "dashboard_title": "Amazon Secondary Dashboard",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "cutoff_month_day": cutoff_month_day,
        "cutoff_month_day_keys": cutoff_month_day_keys,
        "elapsed_day": elapsed_day,
        "period_row_count": period_row_count,
        "days_in_month": days_in_month,
        "amazon_mp_available": False,
        "category_summary": category_summary,
        "category_total": category_total,
        "rk_world_summary": rk_world_summary,
        "rk_world_total": rk_world_total,
        "rk_world_returns": rk_world_returns,
        "rk_world_return_total": rk_return_total,
        "sku_details": sku_details,
        "sku_total": sku_total,
        "visual_dashboard": visual_dashboard,
        "notes": notes,
        "show_amazon_excel_columns": True,
        "summary": rk_world_summary,
        "summary_total": rk_world_total,
        "details": sku_details,
        "detail_total": sku_total,
        "summary_note": "Uses amazon_sec_range_master_view filtered by year and month_day built from the selected month's max date.",
        "detail_subtitle": "ASIN-level detail from amazon_sec_range_master_view",
    })


# Amazon MP (Marketplace GST MTR) dashboard. Aggregates the amazon_mp_master
# view, filtered by shipment_month (NAME) + shipment_year. Show By toggle picks
# the metric: Values -> invoice_amount (inclusive) & tax_exclusive_gross
# (exclusive), LTRS -> delivered_ltr, Quantity -> quantity.
_MONTH_NUM_BY_NAME = {
    date(2000, _m, 1).strftime("%B").upper(): _m for _m in range(1, 13)
}


def _amazon_mp_dashboard_latest(default_month: int, default_year: int):
    rows = _dict_rows(
        """
        SELECT DISTINCT shipment_year AS year,
                        UPPER(TRIM(shipment_month)) AS month_name
        FROM amazon_mp_master
        WHERE shipment_year IS NOT NULL
          AND NULLIF(TRIM(shipment_month), '') IS NOT NULL
        """,
        [],
    )
    best = None
    for row in rows:
        try:
            year = int(row.get("year"))
        except (TypeError, ValueError):
            continue
        month = _MONTH_NUM_BY_NAME.get(str(row.get("month_name") or "").strip().upper())
        if not month:
            continue
        if best is None or (year, month) > best:
            best = (year, month)
    return (best[1], best[0]) if best else (default_month, default_year)


def _amazon_mp_dashboard_response(request):
    today = date.today()
    month_raw = request.query_params.get("month")
    year_raw = request.query_params.get("year")
    defaulted_to_latest = False
    if month_raw and year_raw:
        try:
            month = int(month_raw)
            year = int(year_raw)
        except (TypeError, ValueError):
            raise ValidationError("`month` and `year` must be integers.")
        if not 1 <= month <= 12:
            raise ValidationError("`month` must be between 1 and 12.")
        if not 2000 <= year <= 2100:
            raise ValidationError("`year` must be between 2000 and 2100.")
    else:
        month, year = _amazon_mp_dashboard_latest(today.month, today.year)
        defaulted_to_latest = True

    month_name = _month_name(month)
    where = "WHERE shipment_year = %s AND UPPER(TRIM(shipment_month)) = %s"
    params = [year, month_name]

    # Litres and quantity are summed GROSS (ABS) so refunded/returned units count
    # as positive volume — matching the Amazon MP sheet's Done Ltr / Done Unit
    # (e.g. June: net 4,820 L vs the sheet's 5,524 L, the difference being 2x the
    # refund litres). Revenue (invoice_amount / tax_exclusive_gross) stays NET.
    kpi_rows = _dict_rows(
        f"""
        SELECT
            COALESCE(SUM(invoice_amount), 0) AS inclusive,
            COALESCE(SUM(tax_exclusive_gross), 0) AS exclusive,
            COALESCE(SUM(ABS(delivered_ltr)), 0) AS ltrs,
            COALESCE(SUM(ABS(quantity)), 0) AS quantity,
            COUNT(*) AS row_count,
            -- Latest shipment date in the selected month. shipment_date is text
            -- ('DD/MM/YY HH:MM'), so parse the DD/MM/YY prefix; only rows matching
            -- that shape are parsed to avoid errors on stray values.
            MAX(CASE WHEN TRIM(shipment_date) ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{2}}'
                     THEN TO_DATE(LEFT(TRIM(shipment_date), 8), 'DD/MM/YY') END) AS max_date
        FROM amazon_mp_master
        {where}
        """,
        params,
    )
    kpi_row = kpi_rows[0] if kpi_rows else {}
    _max_date = kpi_row.get("max_date")
    max_date_iso = _max_date.isoformat() if hasattr(_max_date, "isoformat") else (str(_max_date) if _max_date else None)

    def _group(col_expr: str, *, limit: int | None = None) -> list[dict]:
        sql = f"""
            SELECT
                {col_expr} AS label,
                COALESCE(SUM(invoice_amount), 0) AS value,
                COALESCE(SUM(ABS(delivered_ltr)), 0) AS ltrs,
                COALESCE(SUM(ABS(quantity)), 0) AS quantity
            FROM amazon_mp_master
            {where}
            GROUP BY {col_expr}
            ORDER BY value DESC
        """
        if limit:
            sql += f"\nLIMIT {int(limit)}"
        return [
            {
                "label": row.get("label") or "-",
                "value": _num(row.get("value")),
                "ltrs": _num(row.get("ltrs")),
                "quantity": _num(row.get("quantity")),
            }
            for row in _dict_rows(sql, params)
        ]

    item_head = _group("COALESCE(NULLIF(UPPER(TRIM(item_head)), ''), 'OTHER')")
    sub_category = _group("COALESCE(NULLIF(UPPER(TRIM(sub_category)), ''), '-')", limit=10)
    brand = _group("COALESCE(NULLIF(UPPER(TRIM(brand)), ''), '-')", limit=10)
    state = _group("COALESCE(NULLIF(UPPER(TRIM(ship_to_state)), ''), '-')", limit=10)

    trend_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM(shipment_month)) AS month_name,
            COALESCE(SUM(invoice_amount), 0) AS value,
            COALESCE(SUM(ABS(delivered_ltr)), 0) AS ltrs,
            COALESCE(SUM(ABS(quantity)), 0) AS quantity
        FROM amazon_mp_master
        WHERE shipment_year = %s
          AND NULLIF(TRIM(shipment_month), '') IS NOT NULL
        GROUP BY UPPER(TRIM(shipment_month))
        """,
        [year],
    )
    trend = []
    for row in trend_raw:
        num_month = _MONTH_NUM_BY_NAME.get(str(row.get("month_name") or "").strip().upper())
        if not num_month:
            continue
        trend.append(
            {
                "month": num_month,
                "label": date(2000, num_month, 1).strftime("%b").upper(),
                "value": _num(row.get("value")),
                "ltrs": _num(row.get("ltrs")),
                "quantity": _num(row.get("quantity")),
            }
        )
    trend.sort(key=lambda item: item["month"])

    # ASINs present in this month's marketplace data but missing from master_sheet
    # (the asin = format_sku_code join found no row, so every master attribute —
    # brand, item_head — is NULL). Surfaced so the user can add them to the master
    # sheet and get them mapped to a brand / item head / category.
    unmapped_rows = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM(asin)) AS asin,
            MAX(item_description) AS item,
            COALESCE(SUM(invoice_amount), 0) AS value,
            COALESCE(SUM(ABS(quantity)), 0) AS quantity
        FROM amazon_mp_master
        {where}
          AND NULLIF(TRIM(asin), '') IS NOT NULL
          AND brand IS NULL
          AND item_head IS NULL
        GROUP BY UPPER(TRIM(asin))
        ORDER BY SUM(quantity) DESC NULLS LAST, UPPER(TRIM(asin))
        LIMIT 500
        """,
        params,
    )
    unmapped_asins = [
        {
            "asin": row.get("asin"),
            "item": row.get("item"),
            "value": _num(row.get("value")),
            "quantity": _num(row.get("quantity")),
        }
        for row in unmapped_rows
        if row.get("asin")
    ]

    return Response(
        {
            "dashboard_title": "Amazon MP Dashboard",
            "available": True,
            "defaulted_to_latest": defaulted_to_latest,
            "month": month,
            "month_name": month_name,
            "year": year,
            "max_date": max_date_iso,
            "row_count": int(_num(kpi_row.get("row_count"))),
            "kpi": {
                "inclusive": _num(kpi_row.get("inclusive")),
                "exclusive": _num(kpi_row.get("exclusive")),
                "ltrs": _num(kpi_row.get("ltrs")),
                "quantity": _num(kpi_row.get("quantity")),
            },
            "item_head": item_head,
            "sub_category": sub_category,
            "brand": brand,
            "state": state,
            "trend": trend,
            "unmapped_asins": unmapped_asins,
        }
    )


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.amazon_mp")
def amazon_mp_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "amazon":
        raise ValidationError("MP Dashboard is available only for Amazon.")
    return _amazon_mp_dashboard_response(request)


# ── Amazon Coupon Dashboard ──────────────────────────────────────────────────
# Sourced from amazon_coupon_master. KPIs are grand-total columns; the table is
# coupon-name-wise; the item_head split powers a Premium/Commodity donut.
@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.amazon_coupon")
def amazon_coupon_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "amazon":
        raise ValidationError("Coupon Dashboard is available only for Amazon.")
    return _amazon_coupon_dashboard_response(request)


def _coupon_empty_kpi() -> dict:
    return {
        "clips": 0.0,
        "redemptions": 0.0,
        "budget_spent": 0.0,
        "budget_remaining": 0.0,
        "total_budget": 0.0,
    }


def _amazon_coupon_dashboard_response(request):
    # The `date` column is a daily snapshot; values are cumulative-as-of-date
    # within a campaign. So we report ONE snapshot date at a time (latest by
    # default) instead of summing across dates, which would double-count.
    requested_date = _parse_price_upload_date(request.query_params.get("date", ""))
    defaulted_to_latest = False
    if requested_date is None:
        requested_date = _scalar("SELECT MAX(date) FROM amazon_coupon_master", [])
        defaulted_to_latest = True

    available_dates = [
        {
            "date": row["date"].isoformat()
            if hasattr(row["date"], "isoformat")
            else row["date"],
            "rows": int(row["rows"] or 0),
        }
        for row in _dict_rows(
            """
            SELECT date, COUNT(*) AS rows
            FROM amazon_coupon_master
            WHERE date IS NOT NULL
            GROUP BY date
            ORDER BY date DESC
            LIMIT 90
            """,
            [],
        )
    ]

    effective_date = None
    if requested_date is not None:
        effective_date = _scalar(
            "SELECT MAX(date) FROM amazon_coupon_master WHERE date <= %s",
            [requested_date],
        )

    base = {
        "platform": "amazon",
        "dashboard_title": "Amazon Coupon Dashboard",
        "source": "amazon_coupon_master",
        "requested_date": requested_date.isoformat()
        if hasattr(requested_date, "isoformat")
        else requested_date,
        "effective_date": effective_date.isoformat()
        if hasattr(effective_date, "isoformat")
        else effective_date,
        "defaulted_to_latest": defaulted_to_latest,
        "available_dates": available_dates,
    }

    if effective_date is None:
        return Response({**base, "kpi": _coupon_empty_kpi(), "coupons": [], "item_head": []})

    kpi_rows = _dict_rows(
        """
        SELECT
            COALESCE(SUM(clips), 0)            AS clips,
            COALESCE(SUM(redemptions), 0)      AS redemptions,
            COALESCE(SUM(budget_spent), 0)     AS budget_spent,
            COALESCE(SUM(budget_remaining), 0) AS budget_remaining,
            COALESCE(SUM(total_budget), 0)     AS total_budget
        FROM amazon_coupon_master
        WHERE date = %s
        """,
        [effective_date],
    )
    kpi = kpi_rows[0] if kpi_rows else {}

    # Coupon-name-wise rows for the snapshot date. budget_used is a derived
    # "% of budget consumed" (spent / total) since the raw column isn't additive.
    coupons = _dict_rows(
        """
        SELECT
            coupon_name,
            MAX(item_head)                     AS item_head,
            MAX(brand)                         AS brand,
            COALESCE(SUM(clips), 0)            AS clips,
            COALESCE(SUM(redemptions), 0)      AS redemptions,
            COALESCE(SUM(budget_spent), 0)     AS budget_spent,
            COALESCE(SUM(budget_remaining), 0) AS budget_remaining,
            COALESCE(SUM(total_budget), 0)     AS total_budget,
            CASE
                WHEN COALESCE(SUM(total_budget), 0) > 0
                    THEN 100.0 * SUM(budget_spent) / SUM(total_budget)
                ELSE 0
            END                                AS budget_used
        FROM amazon_coupon_master
        WHERE date = %s
          AND NULLIF(TRIM(coupon_name::text), '') IS NOT NULL
        GROUP BY coupon_name
        ORDER BY COALESCE(SUM(budget_spent), 0) DESC
        """,
        [effective_date],
    )

    item_head = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(UPPER(TRIM(item_head::text)), ''), 'OTHER') AS label,
            COALESCE(SUM(budget_spent), 0)     AS budget_spent,
            COALESCE(SUM(redemptions), 0)      AS redemptions,
            COALESCE(SUM(total_budget), 0)     AS total_budget,
            COUNT(DISTINCT coupon_name)        AS coupons
        FROM amazon_coupon_master
        WHERE date = %s
        GROUP BY COALESCE(NULLIF(UPPER(TRIM(item_head::text)), ''), 'OTHER')
        ORDER BY COALESCE(SUM(budget_spent), 0) DESC
        """,
        [effective_date],
    )

    def _f(value):
        return float(value or 0)

    return Response({
        **base,
        "kpi": {
            "clips": _f(kpi.get("clips")),
            "redemptions": _f(kpi.get("redemptions")),
            "budget_spent": _f(kpi.get("budget_spent")),
            "budget_remaining": _f(kpi.get("budget_remaining")),
            "total_budget": _f(kpi.get("total_budget")),
        },
        "coupons": [
            {
                "coupon_name": row.get("coupon_name") or "",
                "item_head": row.get("item_head") or "",
                "brand": row.get("brand") or "",
                "clips": _f(row.get("clips")),
                "redemptions": _f(row.get("redemptions")),
                "budget_spent": _f(row.get("budget_spent")),
                "budget_remaining": _f(row.get("budget_remaining")),
                "budget_used": _f(row.get("budget_used")),
                "total_budget": _f(row.get("total_budget")),
            }
            for row in coupons
        ],
        "item_head": [
            {
                "label": row.get("label") or "OTHER",
                "budget_spent": _f(row.get("budget_spent")),
                "redemptions": _f(row.get("redemptions")),
                "total_budget": _f(row.get("total_budget")),
                "coupons": int(row.get("coupons") or 0),
            }
            for row in item_head
        ],
    })


def _bigbasket_sec_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_bigbasket",
    )
    selected_date = _parse_sec_selected_date(request.query_params)
    date_filter, date_params = _sec_date_filter(selected_date)
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]

    max_date = _scalar(
        f"""
        SELECT MAX("date")
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )
    elapsed_day = _sec_elapsed_day(max_date)

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY UPPER(TRIM("item_head"::text))
        """,
        [month_name, year, *date_params],
    )
    summary_by_head = {_norm_sec_key(r.get("item_head")): r for r in summary_raw}
    summary = []
    for item_head in _BIGBASKET_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_units = _num(row.get("shipped_units"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        target = _BIGBASKET_SEC_TARGETS[item_head]
        drr_base = shipped_units if item_head == "OTHER" else shipped_ltr
        drr = _safe_div(drr_base, elapsed_day)
        estimated_ltr = None if item_head == "OTHER" else drr * days_in_month
        summary.append({
            "item_head": item_head,
            "shipped_units": shipped_units,
            "shipped_ltr": shipped_ltr,
            "shipped_value": shipped_value,
            "estimated_ltr": estimated_ltr,
            "target": target,
            "drr": drr,
            "target_drr": _safe_div(target, days_in_month),
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
        })

    detail_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [month_name, year, *date_params],
    )
    detail_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))): r
        for r in detail_raw
    }

    details = []
    for item_head, category, sub_category, per_ltr in _BIGBASKET_SEC_DETAIL_ROWS:
        row = detail_by_key.get((_norm_sec_key(sub_category), _norm_sec_key(per_ltr)), {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        details.append({
            "format": "BIG BASKET",
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": per_ltr,
            "shipped_value": shipped_value,
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
        })

    summary_total = _sec_total(summary)
    summary_total["estimated_ltr"] = sum(
        _num(row.get("estimated_ltr")) for row in summary
    )
    summary_total["target"] = sum(_num(row.get("target")) for row in summary)
    summary_total["drr"] = None
    summary_total["target_drr"] = None
    summary_total["per_liter_shpd"] = _value_per_ltr_zero(
        summary_total["shipped_value"],
        summary_total["shipped_ltr"],
    )

    detail_total = _sec_total(details)
    detail_total["per_liter_shpd"] = _value_per_ltr_zero(
        detail_total["shipped_value"],
        detail_total["shipped_ltr"],
    )
    top_items = _top_ltr_items_from_secmaster(
        "bigbasket",
        month_name,
        year,
        date_filter,
        date_params,
    )

    sec_trend = _build_sec_keyed_trend(
        table="secmaster_mv",
        date_col='"date"',
        month=month,
        year=year,
        max_date=max_date,
        days_in_month=days_in_month,
        value_expr='"sales_amt_exc"',
        ltr_expr='"ltr_sold"',
        qty_expr='"quantity"',
        where_sql=_SECMASTER_FORMAT_WHERE,
        where_params=["bigbasket"],
    )

    return Response({
        "source": "SecMaster",
        "format": "BIG BASKET",
        "sec_trend": sec_trend,
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "elapsed_day": elapsed_day,
        "days_in_month": days_in_month,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "show_format_column": True,
        "show_sec_planning_columns": True,
        "dashboard_title": "Big Basket SEC Dashboard",
        "detail_subtitle": "Excel rows 15-43 from SEC DASHBOARD",
        "ratio_label": "PER LTR(SHPD)",
        "summary_note": "OTHER DRR uses sale units to match the workbook formula.",
    })


def _flipkart_sec_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="flipkart_secondary_all",
    )
    selected_date = _parse_sec_selected_date(request.query_params)
    date_filter, date_params = _sec_date_filter(selected_date, '"Order Date"')
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]

    max_date = _scalar(
        f"""
        SELECT MAX("Order Date")
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )
    elapsed_day = _sec_elapsed_day(max_date)

    summary_raw = _dict_rows(
        f"""
        SELECT
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("GMV"), 0) AS order_value,
            COALESCE(SUM("ltr_ordered"), 0) AS order_ltr,
            COALESCE(SUM("Final Sale Units"), 0) AS shipped_units,
            COALESCE(SUM("Final Sale Amount"), 0) AS shipped_value,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("Cancellation Amount"), 0) AS cancelled_value,
            COALESCE(SUM("cancellation_ltr"), 0) AS cancelled_ltr,
            COALESCE(SUM("Return Amount"), 0) AS return_value,
            COALESCE(SUM("return_ltr"), 0) AS return_ltr,
            COALESCE(SUM("Return Units"), 0) AS return_units
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          {date_filter}
          AND COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        """,
        [month_name, year, *date_params],
    )
    summary_by_head = {_norm_sec_key(r.get("item_head")): r for r in summary_raw}
    summary = []
    for item_head in _FLIPKART_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        drr = _safe_div(shipped_ltr, elapsed_day)
        summary.append({
            "item_head": item_head,
            "order_value": _num(row.get("order_value")),
            "order_ltr": _num(row.get("order_ltr")),
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "shipped_value": shipped_value,
            "cancelled_value": _num(row.get("cancelled_value")),
            "cancelled_ltr": _num(row.get("cancelled_ltr")),
            "return_value": _num(row.get("return_value")),
            "return_ltr": _num(row.get("return_ltr")),
            "return_units": _num(row.get("return_units")),
            "drr": drr,
            "projection": drr * 31,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
        })

    detail_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            COALESCE(SUM("GMV"), 0) AS order_value,
            COALESCE(SUM("ltr_ordered"), 0) AS order_ltr,
            COALESCE(SUM("Final Sale Amount"), 0) AS shipped_value,
            COALESCE(SUM("Final Sale Units"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("Return Units"), 0) AS return_units
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          {date_filter}
        GROUP BY UPPER(TRIM("sub_category"::text))
        """,
        [month_name, year, *date_params],
    )
    detail_by_key = {_norm_sec_key(r.get("sub_category_key")): r for r in detail_raw}

    details = []
    for item_head, category, sub_category in _FLIPKART_SEC_DETAIL_ROWS:
        row = detail_by_key.get(_norm_sec_key(sub_category), {})
        order_value = _num(row.get("order_value"))
        order_ltr = _num(row.get("order_ltr"))
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        drr_value = _safe_div(order_value, elapsed_day)
        drr_ltr = _safe_div(order_ltr, elapsed_day)
        details.append({
            "format": "FLIPKART",
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": "",
            "order_value": order_value,
            "order_ltr": order_ltr,
            "shipped_value": shipped_value,
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "drr_value": drr_value,
            "drr_ltr": drr_ltr,
            "projection": drr_value * days_in_month,
            "return_units": _num(row.get("return_units")),
            "return_units_percent": 0,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
        })

    total_return_units = sum(_num(row.get("return_units")) for row in details)
    for row in details:
        row["return_units_percent"] = _safe_div(row.get("return_units"), total_return_units)

    summary_total = _sec_total_with_order_value(summary)
    summary_total["order_ltr"] = sum(_num(row.get("order_ltr")) for row in summary)
    summary_total["cancelled_value"] = sum(_num(row.get("cancelled_value")) for row in summary)
    summary_total["cancelled_ltr"] = sum(_num(row.get("cancelled_ltr")) for row in summary)
    summary_total["return_value"] = sum(_num(row.get("return_value")) for row in summary)
    summary_total["return_ltr"] = sum(_num(row.get("return_ltr")) for row in summary)
    summary_total["return_units"] = sum(_num(row.get("return_units")) for row in summary)
    summary_total["drr"] = _safe_div(summary_total["shipped_ltr"], elapsed_day)
    summary_total["projection"] = summary_total["drr"] * 31
    summary_total["per_liter_shpd"] = _value_per_ltr_zero(
        summary_total["shipped_value"],
        summary_total["shipped_ltr"],
    )

    detail_total = _sec_total_with_order_value(details)
    detail_total["order_ltr"] = sum(_num(row.get("order_ltr")) for row in details)
    detail_total["drr_value"] = sum(_num(row.get("drr_value")) for row in details)
    detail_total["drr_ltr"] = _safe_div(detail_total["shipped_ltr"], elapsed_day)
    detail_total["projection"] = sum(_num(row.get("projection")) for row in details)
    detail_total["return_units"] = total_return_units
    detail_total["return_units_percent"] = 1 if total_return_units else 0
    detail_total["per_liter_shpd"] = _value_per_ltr_zero(
        detail_total["shipped_value"],
        detail_total["shipped_ltr"],
    )
    top_items = _top_ltr_items_from_table(
        table_name="flipkart_secondary_all",
        month_column='UPPER(TRIM("month"::text))',
        year_column='"year"',
        item_column='"item"',
        units_column='"Final Sale Units"',
        ltr_column='"ltr_sold"',
        value_column='"Final Sale Amount"',
        month_value=month_name,
        year_value=year,
        date_filter=date_filter,
        date_params=date_params,
    )

    # Flipkart's source carries Order + Deliver + Return, so all three trend
    # lines are populated (unlike the SecMaster / grocery platforms).
    sec_trend = _build_sec_keyed_trend(
        table="flipkart_secondary_all",
        date_col='"Order Date"',
        month=month,
        year=year,
        max_date=max_date,
        days_in_month=days_in_month,
        value_expr='"Final Sale Amount"',
        ltr_expr='"ltr_sold"',
        qty_expr='"Final Sale Units"',
        order_value_expr='"GMV"',
        order_ltr_expr='"ltr_ordered"',
        order_units_expr='0',
        return_value_expr='"Return Amount"',
        return_ltr_expr='"return_ltr"',
        return_units_expr='"Return Units"',
    )

    return Response({
        "source": "flipkart_secondary_all",
        "format": "FLIPKART",
        "sec_trend": sec_trend,
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "elapsed_day": elapsed_day,
        "days_in_month": days_in_month,
        "show_flipkart_excel_columns": True,
        "dashboard_title": "Flipkart Secondary Dashboard",
        "detail_subtitle": "Excel rows 12-63 from SECONDARY DASHBOARD",
        "summary_note": "Uses flipkart_secondary_all to match the Excel SECONDARY formulas.",
        "value_source_note": "Source changed from SecMaster to flipkart_secondary_all.",
        "kpi_labels": {
            "units": "Shipped Units",
            "litres": "Shipped LTR",
            "value": "Shipped Value",
        },
        "summary_labels": {
            "item_head": "Item Head",
            "order_value": "Order Value",
            "order_ltr": "Order LTR",
            "value": "Shipped Value",
            "units": "Shipped Units",
            "litres": "Shipped LTR",
        },
        "detail_labels": {
            "per_ltr": "Formula Level",
            "order_value": "Order Value",
            "order_ltr": "Order LTR",
            "value": "Shipped Value",
            "units": "Shipped Units",
            "litres": "Shipped LTR",
        },
    })


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.amazon_cmp")
def amazon_comparison_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "amazon":
        raise ValidationError("Comparison Dashboard is available only for Amazon.")
    return _amazon_comparison_dashboard_response(request)


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.fk_sec_monthly")
def flipkart_secondary_monthly_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "amazon":
        return _amazon_secondary_monthly_dashboard_response(request)
    if slug != "flipkart":
        raise ValidationError("Month Sale is available only for Amazon and Flipkart.")

    year, defaulted_to_latest = _parse_flipkart_secondary_monthly_year(request.query_params)
    months = [
        {
            "month": month,
            "key": _month_name(month),
            "label": date(2000, month, 1).strftime("%B"),
        }
        for month in range(1, 13)
    ]
    month_keys = [month["key"] for month in months]

    def empty_month_values(fields: tuple[str, ...]) -> dict:
        return {month_key: {field: 0.0 for field in fields} for month_key in month_keys}

    def sum_month_rows(rows: list[dict], fields: tuple[str, ...]) -> dict:
        total = {"months": empty_month_values(fields)}
        for month_key in month_keys:
            for field in fields:
                total["months"][month_key][field] = sum(
                    _num(row.get("months", {}).get(month_key, {}).get(field))
                    for row in rows
                )
        return total

    max_date = _scalar(
        """
        SELECT MAX("Order Date")
        FROM "flipkart_secondary_all"
        WHERE "year" = %s
        """,
        [year],
    )

    monthly_summary_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("month"::text)) AS month_key,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("GMV"), 0) AS order_value,
            COALESCE(SUM("Final Sale Amount"), 0) AS shipped_value,
            COALESCE(SUM("ltr_ordered"), 0) AS order_ltr,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM "flipkart_secondary_all"
        WHERE "year" = %s
          AND COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') IN ('PREMIUM', 'COMMODITY')
        GROUP BY
            UPPER(TRIM("month"::text)),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        """,
        [year],
    )
    summary_by_key = {
        (_norm_sec_key(row.get("item_head")), _norm_sec_key(row.get("month_key"))): row
        for row in monthly_summary_raw
    }

    sales_liters = []
    sales_values = []
    for item_head in _FLIPKART_SEC_MONTHLY_ITEM_HEADS:
        litre_months = empty_month_values(("order_ltr", "shipped_ltr"))
        value_months = empty_month_values(("order_value", "shipped_value"))
        for month_key in month_keys:
            row = summary_by_key.get((item_head, month_key), {})
            litre_months[month_key]["order_ltr"] = _num(row.get("order_ltr"))
            litre_months[month_key]["shipped_ltr"] = _num(row.get("shipped_ltr"))
            value_months[month_key]["order_value"] = _num(row.get("order_value"))
            value_months[month_key]["shipped_value"] = _num(row.get("shipped_value"))
        sales_liters.append({
            "type": item_head,
            "months": litre_months,
        })
        sales_values.append({
            "type": item_head,
            "months": value_months,
        })

    category_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("month"::text)) AS month_key,
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            COALESCE(SUM("ltr_ordered"), 0) AS order_ltr,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM "flipkart_secondary_all"
        WHERE "year" = %s
        GROUP BY
            UPPER(TRIM("month"::text)),
            UPPER(TRIM("sub_category"::text))
        """,
        [year],
    )
    category_by_key = {
        (_norm_sec_key(row.get("sub_category_key")), _norm_sec_key(row.get("month_key"))): row
        for row in category_raw
    }

    category_liters = []
    for item_head, category, sub_category in _FLIPKART_SEC_MONTHLY_CATEGORY_ROWS:
        row_months = empty_month_values(("order_ltr", "shipped_ltr"))
        for month_key in month_keys:
            row = category_by_key.get((_norm_sec_key(sub_category), month_key), {})
            row_months[month_key]["order_ltr"] = _num(row.get("order_ltr"))
            row_months[month_key]["shipped_ltr"] = _num(row.get("shipped_ltr"))
        category_liters.append({
            "type": item_head,
            "category": category,
            "sub_category": sub_category,
            "months": row_months,
        })

    mom_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("month"::text)) AS month_key,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM "flipkart_secondary_all"
        WHERE COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') IN ('PREMIUM', 'COMMODITY')
        GROUP BY
            UPPER(TRIM("month"::text)),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        """,
        [],
    )
    mom_by_key = {
        (_norm_sec_key(row.get("item_head")), _norm_sec_key(row.get("month_key"))): _num(row.get("shipped_ltr"))
        for row in mom_raw
    }

    mom_growth = []
    previous_premium = 0.0
    previous_commodity = 0.0
    for index, month_info in enumerate(months):
        month_key = month_info["key"]
        premium_ltr = mom_by_key.get(("PREMIUM", month_key), 0.0)
        commodity_ltr = mom_by_key.get(("COMMODITY", month_key), 0.0)
        if index == 0:
            premium_growth = 0.0
            commodity_growth = 0.0
        else:
            premium_growth = _safe_div(premium_ltr - previous_premium, previous_premium)
            commodity_growth = _safe_div(commodity_ltr - previous_commodity, previous_commodity)
        mom_growth.append({
            "month": month_key,
            "label": month_info["label"],
            "premium_ltr": premium_ltr,
            "commodity_ltr": commodity_ltr,
            "premium_growth": premium_growth,
            "commodity_growth": commodity_growth,
        })
        previous_premium = premium_ltr
        previous_commodity = commodity_ltr

    return Response({
        "source": "flipkart_secondary_all",
        "format": "FLIPKART",
        "defaulted_to_latest": defaulted_to_latest,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "months": months,
        "sales_liters": sales_liters,
        "sales_values": sales_values,
        "sales_values_total": sum_month_rows(
            sales_values,
            ("order_value", "shipped_value"),
        ),
        "category_liters": category_liters,
        "category_liters_total": sum_month_rows(
            category_liters,
            ("order_ltr", "shipped_ltr"),
        ),
        "mom_growth": mom_growth,
        "dashboard_title": "Flipkart Month Sale",
        "notes": {
            "mom_growth_year_filter": False,
            "other_excluded_from_totals": True,
            "category_template_fixed": True,
        },
    })


def _blinkit_sec_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_blinkit",
    )
    selected_date = _parse_sec_selected_date(request.query_params)
    date_filter, date_params = _sec_date_filter(selected_date)
    month_name = _month_name(month)

    cache_key = (
        f"sec_dash:blinkit:{month}:{year}:"
        f"{selected_date.isoformat() if selected_date else ''}:"
        f"{int(defaulted_to_latest)}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return Response(cached)

    max_date = _scalar(
        f"""
        SELECT MAX("date")
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )
    elapsed_day = _sec_elapsed_day(max_date)
    days_in_month = monthrange(year, month)[1]

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY UPPER(TRIM("item_head"::text))
        """,
        [month_name, year, *date_params],
    )
    summary_by_head = {_norm_sec_key(r.get("item_head")): r for r in summary_raw}
    summary = []
    for item_head in _BLINKIT_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_units = _num(row.get("shipped_units"))
        summary.append({
            "item_head": item_head,
            "shipped_units": shipped_units,
            "shipped_ltr": _num(row.get("shipped_ltr")),
            "shipped_value": shipped_value,
            "per_liter_shpd": _value_per_unit(shipped_value, shipped_units),
        })

    detail_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [month_name, year, *date_params],
    )
    detail_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))): r
        for r in detail_raw
    }

    # "Last month" = the FULL previous calendar month's litres per
    # (sub_category, per_ltr), computed live relative to the SELECTED month —
    # not the hardcoded snapshot that _BLINKIT_SEC_DETAIL_ROWS used to carry
    # (audit finding #7: that column was frozen to one month regardless of the
    # period picked). The prior month is a completed month, so it is NOT scoped
    # by the intra-month `date_filter`.
    prev_month, prev_year = (12, year - 1) if month == 1 else (month - 1, year)
    last_month_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("ltr_sold"), 0) AS last_month_ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [_month_name(prev_month), prev_year],
    )
    last_month_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))):
            _num(r.get("last_month_ltr"))
        for r in last_month_raw
    }

    details = []
    for item_head, category, sub_category, per_ltr, _static_last_month in _BLINKIT_SEC_DETAIL_ROWS:
        detail_key = (_norm_sec_key(sub_category), _norm_sec_key(per_ltr))
        row = detail_by_key.get(detail_key, {})
        last_month = last_month_by_key.get(detail_key, 0)
        shipped_value = _num(row.get("shipped_value"))
        shipped_units = _num(row.get("shipped_units"))
        details.append({
            "format": "BLINKIT",
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": per_ltr,
            "shipped_value": shipped_value,
            "shipped_units": shipped_units,
            "shipped_ltr": _num(row.get("shipped_ltr")),
            "per_liter_shpd": _value_per_unit(shipped_value, shipped_units),
            "last_month": last_month,
        })

    summary_total = _sec_total(summary)
    summary_total["per_liter_shpd"] = _value_per_unit(
        summary_total["shipped_value"],
        summary_total["shipped_units"],
    )
    detail_total = _sec_total(details)
    detail_total["per_liter_shpd"] = _value_per_unit(
        detail_total["shipped_value"],
        detail_total["shipped_units"],
    )
    detail_total["last_month"] = sum(_num(r.get("last_month")) for r in details)
    top_items = _top_ltr_items_from_secmaster(
        "blinkit",
        month_name,
        year,
        date_filter,
        date_params,
    )
    # daily/monthly/yearly trend queries removed: no frontend file consumes
    # `data.trends.*` for the Blinkit sec dashboard. The yearly query scanned
    # every Blinkit row across all years (no date scope) and was the dominant
    # cost of this endpoint. Keys are preserved in the response so any
    # in-flight client still parses the shape.
    daily_trend = []
    monthly_trend = []
    yearly_trend = []

    sec_trend = _build_sec_keyed_trend(
        table="secmaster_mv",
        date_col='"date"',
        month=month,
        year=year,
        max_date=max_date,
        days_in_month=days_in_month,
        value_expr='"sales_amt_exc"',
        ltr_expr='"ltr_sold"',
        qty_expr='"quantity"',
        where_sql=_SECMASTER_FORMAT_WHERE,
        where_params=["blinkit"],
    )

    payload = {
        "source": "SecMaster",
        "format": "BLINKIT",
        "sec_trend": sec_trend,
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "elapsed_day": elapsed_day,
        "days_in_month": days_in_month,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "show_format_column": True,
        "show_last_month": True,
        "dashboard_title": "Blinkit Secondary Dashboard",
        "detail_subtitle": "Excel rows 12-20 from SECONDARY DASHBOARD",
        "trends": {
            "day": daily_trend,
            "month": monthly_trend,
            "year": yearly_trend,
        },
    }
    cache.set(cache_key, payload, _PRIMARY_DASHBOARD_CACHE_TTL)
    return Response(payload)


def _swiggy_sec_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_swiggy",
    )
    selected_date = _parse_sec_selected_date(request.query_params)
    prev_month, prev_year = _shift_month(month, year, -1)

    month_start = date(year, month, 1)
    next_month, next_year = _shift_month(month, year, 1)
    month_end = date(next_year, next_month, 1)
    prev_month_start = date(prev_year, prev_month, 1)
    current_start = selected_date or month_start
    current_end = (selected_date + timedelta(days=1)) if selected_date else month_end
    current_rate_month = month_start.isoformat()
    prev_rate_month = prev_month_start.isoformat()

    aggregate_raw = _dict_rows(
        """
        WITH target_months AS (
            -- The current and previous month buckets the sales below fall into.
            SELECT %s::date AS target_month
            UNION ALL SELECT %s::date
        ),
        rates AS (
            -- Effective landing rate per SKU as of each bucket = the newest row
            -- with month <= that bucket. So a month whose rate isn't set yet
            -- carries the previous month's rate forward (display/calc only —
            -- nothing is written), instead of falling to 0.
            SELECT DISTINCT ON (UPPER(TRIM(lr.sku_code::text)), tm.target_month)
                   UPPER(TRIM(lr.sku_code::text)) AS sku_key,
                   tm.target_month AS target_month,
                   lr.landing_rate
              FROM target_months tm
              JOIN monthly_landing_rate lr
                ON REGEXP_REPLACE(LOWER(TRIM(lr.format::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
               AND lr.month::date <= tm.target_month
             ORDER BY UPPER(TRIM(lr.sku_code::text)), tm.target_month,
                      lr.month::date DESC, lr.created_at DESC
        ),
        base AS (
            SELECT
                s."ORDERED_DATE" AS ordered_date,
                COALESCE(NULLIF(UPPER(TRIM(m.item_head::text)), ''), 'OTHER') AS item_head,
                UPPER(TRIM(m.sub_category::text)) AS sub_category_key,
                UPPER(TRIM(m.per_unit::text)) AS per_ltr_key,
                COALESCE(s."COMBO_UNITS_SOLD", 0) + COALESCE(s."UNITS_SOLD", 0) AS quantity,
                CASE
                    WHEN m.is_litre = 'Y'::text
                        THEN COALESCE(s."UNITS_SOLD", 0)::double precision * m.per_unit_value
                    ELSE NULL::double precision
                END AS ltr_sold,
                COALESCE(
                    r.landing_rate
                    * (COALESCE(s."COMBO_UNITS_SOLD", 0) + COALESCE(s."UNITS_SOLD", 0))::numeric,
                    0::numeric
                ) AS sales_amt,
                DATE_TRUNC('month', s."ORDERED_DATE"::timestamp)::date AS month_start
            FROM "swiggySec" s
            LEFT JOIN master_sheet m
                   ON m.format_sku_code::text = s."ITEM_CODE"
            LEFT JOIN rates r
                   ON r.sku_key = UPPER(TRIM(s."ITEM_CODE"::text))
                  AND r.target_month = DATE_TRUNC('month', s."ORDERED_DATE"::timestamp)::date
            WHERE (
                    s."ORDERED_DATE" >= %s
                AND s."ORDERED_DATE" < %s
            ) OR (
                    s."ORDERED_DATE" >= %s
                AND s."ORDERED_DATE" < %s
            )
        )
        SELECT
            item_head,
            sub_category_key,
            per_ltr_key,
            COALESCE(SUM(quantity) FILTER (WHERE month_start = %s), 0) AS shipped_units,
            COALESCE(SUM(ltr_sold) FILTER (WHERE month_start = %s), 0) AS shipped_ltr,
            COALESCE(SUM(sales_amt) FILTER (WHERE month_start = %s), 0) AS shipped_value,
            COALESCE(SUM(ltr_sold) FILTER (WHERE month_start = %s), 0) AS last_month,
            MAX(ordered_date) FILTER (WHERE month_start = %s) AS max_date
        FROM base
        GROUP BY item_head, sub_category_key, per_ltr_key
        """,
        [
            current_rate_month,
            prev_rate_month,
            current_start,
            current_end,
            prev_month_start,
            month_start,
            month_start,
            month_start,
            month_start,
            prev_month_start,
            month_start,
        ],
    )

    max_date = None
    detail_by_key = {}
    summary_by_head = {}
    for row in aggregate_raw:
        row_max_date = row.get("max_date")
        if row_max_date and (max_date is None or row_max_date > max_date):
            max_date = row_max_date

        item_head = _norm_sec_key(row.get("item_head")) or "OTHER"
        summary_row = summary_by_head.setdefault(
            item_head,
            {"shipped_units": 0, "shipped_ltr": 0, "shipped_value": 0},
        )
        summary_row["shipped_units"] += _num(row.get("shipped_units"))
        summary_row["shipped_ltr"] += _num(row.get("shipped_ltr"))
        summary_row["shipped_value"] += _num(row.get("shipped_value"))

        key = (_norm_sec_key(row.get("sub_category_key")), _norm_sec_key(row.get("per_ltr_key")))
        detail_row = detail_by_key.setdefault(
            key,
            {"shipped_value": 0, "shipped_units": 0, "shipped_ltr": 0, "last_month": 0},
        )
        detail_row["shipped_value"] += _num(row.get("shipped_value"))
        detail_row["shipped_units"] += _num(row.get("shipped_units"))
        detail_row["shipped_ltr"] += _num(row.get("shipped_ltr"))
        detail_row["last_month"] += _num(row.get("last_month"))

    summary = []
    for item_head in _SWIGGY_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        summary.append({
            "item_head": item_head,
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "shipped_value": shipped_value,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
        })

    details = []
    for item_head, category, sub_category, per_ltr in _SWIGGY_SEC_DETAIL_ROWS:
        key = (_norm_sec_key(sub_category), _norm_sec_key(per_ltr))
        row = detail_by_key.get(key, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        details.append({
            "format": "SWIGGY",
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": per_ltr,
            "shipped_value": shipped_value,
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
            "last_month": _num(row.get("last_month")),
        })

    summary_total = _sec_total(summary)
    summary_total["per_liter_shpd"] = _value_per_ltr_zero(
        summary_total["shipped_value"],
        summary_total["shipped_ltr"],
    )
    detail_total = _sec_total(details)
    detail_total["per_liter_shpd"] = _value_per_ltr_zero(
        detail_total["shipped_value"],
        detail_total["shipped_ltr"],
    )
    detail_total["last_month"] = sum(_num(r.get("last_month")) for r in details)
    top_items = _dict_rows(
        """
        WITH rates AS (
            -- Effective rate per SKU as of the selected month = newest row with
            -- month <= it, so an unset month carries the previous month's rate
            -- forward (calc only, nothing stored) instead of dropping to 0.
            SELECT DISTINCT ON (UPPER(TRIM(sku_code::text)))
                   UPPER(TRIM(sku_code::text)) AS sku_key,
                   landing_rate
              FROM monthly_landing_rate
             WHERE REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
               AND month::date <= %s::date
             ORDER BY UPPER(TRIM(sku_code::text)), month::date DESC, created_at DESC
        )
        SELECT
            COALESCE(
                NULLIF(TRIM(m.item::text), ''),
                COALESCE(NULLIF(TRIM(s."PRODUCT_NAME"::text), ''), '-')
            ) AS item,
            COALESCE(NULLIF(UPPER(TRIM(m.item_head::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM(COALESCE(s."COMBO_UNITS_SOLD", 0) + COALESCE(s."UNITS_SOLD", 0)), 0) AS shipped_units,
            COALESCE(
                SUM(
                    CASE
                        WHEN m.is_litre = 'Y'::text
                            THEN COALESCE(s."UNITS_SOLD", 0)::double precision * m.per_unit_value
                        ELSE NULL::double precision
                    END
                ),
                0
            ) AS shipped_ltr,
            COALESCE(
                SUM(
                    COALESCE(
                        r.landing_rate
                        * (COALESCE(s."COMBO_UNITS_SOLD", 0) + COALESCE(s."UNITS_SOLD", 0))::numeric,
                        0::numeric
                    )
                ),
                0
            ) AS shipped_value
        FROM "swiggySec" s
        LEFT JOIN master_sheet m
               ON m.format_sku_code::text = s."ITEM_CODE"
        LEFT JOIN rates r
               ON r.sku_key = UPPER(TRIM(s."ITEM_CODE"::text))
        WHERE s."ORDERED_DATE" >= %s
          AND s."ORDERED_DATE" < %s
          AND NULLIF(TRIM(COALESCE(m.item::text, s."PRODUCT_NAME"::text)), '') IS NOT NULL
        GROUP BY 1, 2
        ORDER BY
            COALESCE(
                SUM(
                    CASE
                        WHEN m.is_litre = 'Y'::text
                            THEN COALESCE(s."UNITS_SOLD", 0)::double precision * m.per_unit_value
                        ELSE NULL::double precision
                    END
                ),
                0
            ) DESC
        LIMIT 8
        """,
        [current_rate_month, current_start, current_end],
    )

    return Response({
        "source": "SecMaster",
        "format": "SWIGGY",
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "previous_month": prev_month,
        "previous_year": prev_year,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "show_format_column": False,
        "show_last_month": True,
        "show_ratio_column": False,
        "dashboard_title": "Swiggy Secondary Dashboard",
        "detail_subtitle": "Excel rows 8-36 from SECONDARY DASHBOARD",
        "summary_note": "VALUE uses SecMaster.sales_amt to match workbook DATABASE column P.",
        "value_source_note": "VALUE and DONE VALUE use SecMaster.sales_amt to match workbook DATABASE column P.",
        "kpi_labels": {
            "units": "Qty",
            "litres": "Liter",
            "value": "Value",
        },
        "summary_labels": {
            "item_head": "Category",
            "value": "Value",
            "units": "Qty",
            "litres": "Liter",
        },
        "detail_labels": {
            "per_ltr": "Per Unit",
            "value": "Done Value",
            "units": "Done Qty",
            "litres": "Done Liters",
            "last_month": "Last Month",
        },
    })


def _zepto_sec_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_zepto",
    )
    selected_date = _parse_sec_selected_date(request.query_params)
    date_expr = _secmaster_zepto_date_expr()
    date_filter, date_params = _sec_date_filter(selected_date, date_expr)
    month_name = _month_name(month)

    max_date = _scalar(
        f"""
        SELECT MAX({date_expr})
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )
    elapsed_day = _sec_elapsed_day(max_date)
    days_in_month = monthrange(year, month)[1]

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY UPPER(TRIM("item_head"::text))
        """,
        [month_name, year, *date_params],
    )
    summary_by_head = {_norm_sec_key(r.get("item_head")): r for r in summary_raw}
    summary = []
    for item_head in _ZEPTO_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        summary.append({
            "item_head": item_head,
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "shipped_value": shipped_value,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
        })

    detail_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [month_name, year, *date_params],
    )
    detail_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))): r
        for r in detail_raw
    }

    details = []
    for item_head, category, sub_category, per_ltr in _ZEPTO_SEC_DETAIL_ROWS:
        row = detail_by_key.get((_norm_sec_key(sub_category), _norm_sec_key(per_ltr)), {})
        shipped_value = _num(row.get("shipped_value"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        details.append({
            "format": "ZEPTO",
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": per_ltr,
            "shipped_value": shipped_value,
            "shipped_units": _num(row.get("shipped_units")),
            "shipped_ltr": shipped_ltr,
            "per_liter_shpd": _value_per_ltr_zero(shipped_value, shipped_ltr),
            "include_in_excel_total": item_head != "OTHER",
        })

    summary_total = _sec_total(summary)
    summary_total["per_liter_shpd"] = _value_per_ltr_zero(
        summary_total["shipped_value"],
        summary_total["shipped_ltr"],
    )
    excel_total_rows = [row for row in details if row["include_in_excel_total"]]
    detail_total = _sec_total(excel_total_rows)
    detail_total["per_liter_shpd"] = sum(
        _num(row.get("per_liter_shpd")) for row in excel_total_rows
    )
    top_items = _top_ltr_items_from_secmaster(
        "zepto",
        month_name,
        year,
        date_filter,
        date_params,
    )

    sec_trend = _build_sec_keyed_trend(
        table="secmaster_mv",
        date_col='"date"',
        month=month,
        year=year,
        max_date=max_date,
        days_in_month=days_in_month,
        value_expr='"sales_amt_exc"',
        ltr_expr='"ltr_sold"',
        qty_expr='"quantity"',
        where_sql=_SECMASTER_FORMAT_WHERE,
        where_params=["zepto"],
    )

    return Response({
        "source": "SecMaster",
        "format": "ZEPTO",
        "sec_trend": sec_trend,
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "selected_date": selected_date.isoformat() if selected_date else None,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "elapsed_day": elapsed_day,
        "days_in_month": days_in_month,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "show_format_column": True,
        "dashboard_title": "Zepto SEC Dashboard",
        "detail_subtitle": "Excel rows 14-47; grand total follows rows 14-42",
        "ratio_label": "PER LTR(SHPD)",
        "detail_total_note": "Detail grand total excludes OTHER rows to match Excel F48:I48.",
    })


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.sku_analysis")
def sku_analysis_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "zepto":
        return _zepto_sku_analysis_dashboard_response(request)
    if slug == "bigbasket":
        return _bigbasket_sku_analysis_dashboard_response(request)
    if slug != "blinkit":
        raise ValidationError(
            "SKU Analysis Dashboard is available only for Big Basket, Blinkit and Zepto."
        )

    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_blinkit",
    )
    month_name = _month_name(month)
    selected_item = str(request.query_params.get("item") or "").strip()

    max_date = _scalar(
        """
        SELECT MAX("date")
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        """,
        [month_name, year],
    )

    item_head = None
    if selected_item:
        item_head = _scalar(
            """
            SELECT UPPER(TRIM("item_head"::text))
            FROM secmaster_mv
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
              AND UPPER(TRIM("item"::text)) = UPPER(TRIM(%s))
              AND NULLIF(TRIM("item_head"::text), '') IS NOT NULL
            LIMIT 1
            """,
            [selected_item],
        )

    daily_where = [
        "REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'",
        'UPPER(TRIM("month"::text)) = %s',
        '"year"::numeric = %s',
    ]
    daily_params: list = [month_name, year]
    if selected_item:
        daily_where.append('UPPER(TRIM("item"::text)) = UPPER(TRIM(%s))')
        daily_params.append(selected_item)
    daily_where_sql = " AND ".join(daily_where)

    daily_raw = _dict_rows(
        f"""
        SELECT
            "date" AS sale_date,
            COALESCE(SUM("quantity"), 0) AS qty_sold,
            COALESCE(SUM("ltr_sold"), 0) AS liter_sold,
            COALESCE(SUM("sales_amt_exc"), 0) AS sales_amount
        FROM secmaster_mv
        WHERE {daily_where_sql}
          AND "date" IS NOT NULL
        GROUP BY "date"
        ORDER BY "date" ASC
        """,
        daily_params,
    )
    daily_by_date = {row["sale_date"]: row for row in daily_raw}
    days_in_month = monthrange(year, month)[1]
    daily_rows = []
    for day in range(1, days_in_month + 1):
        row_date = date(year, month, day)
        row = daily_by_date.get(row_date, {})
        daily_rows.append({
            "date": row_date.isoformat(),
            "display_date": row_date.strftime("%d-%m-%Y"),
            "qty_sold": _num(row.get("qty_sold")),
            "liter_sold": _num(row.get("liter_sold")),
            "sales_amount": _num(row.get("sales_amount")),
        })

    top_skus = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS sku,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("ltr_sold"), 0) AS ltrs_sold
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), '-'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        ORDER BY COALESCE(SUM("ltr_sold"), 0) DESC
        LIMIT 10
        """,
        [month_name, year],
    )

    item_options = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), '-'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        ORDER BY item ASC
        """,
        [month_name, year],
    )

    daily_total = {
        "qty_sold": sum(_num(row.get("qty_sold")) for row in daily_rows),
        "liter_sold": sum(_num(row.get("liter_sold")) for row in daily_rows),
        "sales_amount": sum(_num(row.get("sales_amount")) for row in daily_rows),
    }

    return Response({
        "source": "SecMaster",
        "format": "BLINKIT",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "selected_item": selected_item or None,
        "selected_item_head": item_head,
        "daily_rows": daily_rows,
        "daily_total": daily_total,
        "top_skus": top_skus,
        "item_options": item_options,
    })


def _bigbasket_sku_analysis_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_bigbasket",
    )
    month_name = _month_name(month)
    selected_item = str(request.query_params.get("item") or "").strip()
    sale_date_expr = _secmaster_zepto_date_expr("sm")
    sale_date_expr_plain = _secmaster_zepto_date_expr()

    max_date = _scalar(
        f"""
        SELECT MAX({sale_date_expr_plain})
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        """,
        [month_name, year],
    )

    item_head = None
    if selected_item:
        item_head = _scalar(
            f"""
            SELECT UPPER(TRIM(sm."item_head"::text))
            FROM secmaster_mv sm
            WHERE REGEXP_REPLACE(LOWER(TRIM(sm."format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
              AND UPPER(TRIM(sm."item"::text)) = UPPER(TRIM(%s))
              AND UPPER(TRIM(sm."month"::text)) = %s
              AND sm."year"::numeric = %s
              AND NULLIF(TRIM(sm."item_head"::text), '') IS NOT NULL
            ORDER BY ({sale_date_expr}) DESC NULLS LAST
            LIMIT 1
            """,
            [selected_item, month_name, year],
        )

    daily_where = [
        "REGEXP_REPLACE(LOWER(TRIM(sm.\"format\"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'",
        'UPPER(TRIM(sm."month"::text)) = %s',
        'sm."year"::numeric = %s',
    ]
    daily_params: list = [month_name, year]
    if selected_item:
        daily_where.append('UPPER(TRIM(sm."item"::text)) = UPPER(TRIM(%s))')
        daily_params.append(selected_item)
    daily_where_sql = " AND ".join(daily_where)

    daily_raw = _dict_rows(
        f"""
        SELECT
            {sale_date_expr} AS sale_date,
            COALESCE(SUM(sm."quantity"), 0) AS qty_sold,
            COALESCE(SUM(sm."ltr_sold"), 0) AS liter_sold,
            COALESCE(SUM(sm."sales_amt_exc"), 0) AS sales_amount
        FROM secmaster_mv sm
        WHERE {daily_where_sql}
          AND ({sale_date_expr}) IS NOT NULL
        GROUP BY {sale_date_expr}
        ORDER BY {sale_date_expr} ASC
        """,
        daily_params,
    )
    daily_by_date = {row["sale_date"]: row for row in daily_raw}
    days_in_month = monthrange(year, month)[1]
    daily_rows = []
    for day in range(1, days_in_month + 1):
        row_date = date(year, month, day)
        row = daily_by_date.get(row_date, {})
        daily_rows.append({
            "date": row_date.isoformat(),
            "display_date": row_date.strftime("%d-%m-%Y"),
            "qty_sold": _num(row.get("qty_sold")),
            "liter_sold": _num(row.get("liter_sold")),
            "sales_amount": _num(row.get("sales_amount")),
        })

    top_skus = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS sku,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("quantity"), 0) AS ltrs_sold
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), '-'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        ORDER BY COALESCE(SUM("quantity"), 0) DESC
        LIMIT 10
        """,
        [month_name, year],
    )

    item_options = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), '-'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        ORDER BY item ASC
        """,
        [month_name, year],
    )

    daily_total = {
        "qty_sold": sum(_num(row.get("qty_sold")) for row in daily_rows),
        "liter_sold": sum(_num(row.get("liter_sold")) for row in daily_rows),
        "sales_amount": sum(_num(row.get("sales_amount")) for row in daily_rows),
    }
    top_sku_total = {
        "ltrs_sold": sum(_num(row.get("ltrs_sold")) for row in top_skus),
    }

    return Response({
        "source": "SecMaster",
        "format": "BIG BASKET",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "selected_item": selected_item or None,
        "selected_item_head": item_head,
        "daily_rows": daily_rows,
        "daily_total": daily_total,
        "top_skus": top_skus,
        "top_sku_total": top_sku_total,
        "top_metric_basis": "quantity",
        "item_options": item_options,
    })


def _zepto_sku_analysis_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_zepto",
    )
    month_name = _month_name(month)
    selected_item = str(request.query_params.get("item") or "").strip()
    zepto_sale_date_expr = _secmaster_zepto_date_expr("sm")
    zepto_sale_date_expr_plain = _secmaster_zepto_date_expr()

    max_date = _scalar(
        f"""
        SELECT MAX({zepto_sale_date_expr_plain})
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        """,
        [month_name, year],
    )

    item_head = None
    if selected_item:
        item_head = _scalar(
            """
            SELECT UPPER(TRIM("item_head"::text))
            FROM secmaster_mv
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
              AND UPPER(TRIM("item"::text)) = UPPER(TRIM(%s))
              AND UPPER(TRIM("month"::text)) = %s
              AND "year"::numeric = %s
              AND NULLIF(TRIM("item_head"::text), '') IS NOT NULL
            ORDER BY
                CASE
                    WHEN TRIM("real_date"::text) ~ '^\\d{2}-\\d{2}-\\d{4}$'
                        THEN TO_DATE(TRIM("real_date"::text), 'DD-MM-YYYY')
                    WHEN TRIM("real_date"::text) ~ '^\\d{4}-\\d{2}-\\d{2}$'
                        THEN TRIM("real_date"::text)::date
                    ELSE "date"
                END DESC NULLS LAST
            LIMIT 1
            """,
            [selected_item, month_name, year],
        )

    daily_where = [
        "REGEXP_REPLACE(LOWER(TRIM(sm.\"format\"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'",
        'UPPER(TRIM(sm."month"::text)) = %s',
        'sm."year"::numeric = %s',
    ]
    daily_params: list = [month_name, year]
    if selected_item:
        daily_where.append('UPPER(TRIM(sm."item"::text)) = UPPER(TRIM(%s))')
        daily_params.append(selected_item)
    daily_where_sql = " AND ".join(daily_where)

    daily_raw = _dict_rows(
        f"""
        SELECT
            {zepto_sale_date_expr} AS sale_date,
            COALESCE(SUM(sm."quantity"), 0) AS qty_sold,
            COALESCE(SUM(sm."ltr_sold"), 0) AS liter_sold,
            COALESCE(SUM(sm."sales_amt_exc"), 0) AS sales_amount
        FROM secmaster_mv sm
        WHERE {daily_where_sql}
          AND ({zepto_sale_date_expr}) IS NOT NULL
        GROUP BY {zepto_sale_date_expr}
        ORDER BY {zepto_sale_date_expr} ASC
        """,
        daily_params,
    )
    daily_by_date = {row["sale_date"]: row for row in daily_raw}
    days_in_month = monthrange(year, month)[1]
    daily_rows = []
    for day in range(1, days_in_month + 1):
        row_date = date(year, month, day)
        row = daily_by_date.get(row_date, {})
        daily_rows.append({
            "date": row_date.isoformat(),
            "display_date": row_date.strftime("%d-%m-%Y"),
            "qty_sold": _num(row.get("qty_sold")),
            "liter_sold": _num(row.get("liter_sold")),
            "sales_amount": _num(row.get("sales_amount")),
        })

    top_skus = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS sku,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM(sm."sales_amt_exc"), 0) AS sales
        FROM secmaster_mv sm
        WHERE REGEXP_REPLACE(LOWER(TRIM(sm."format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM(sm."month"::text)) = %s
          AND sm."year"::numeric = %s
        GROUP BY
            COALESCE(NULLIF(TRIM(sm."item"::text), ''), '-'),
            COALESCE(NULLIF(UPPER(TRIM(sm."item_head"::text)), ''), 'OTHER')
        ORDER BY COALESCE(SUM(
            sm."sales_amt_exc"
        ), 0) DESC
        LIMIT 10
        """,
        [month_name, year],
    )

    item_options = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), '-'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        ORDER BY item ASC
        """,
        [month_name, year],
    )

    daily_total = {
        "qty_sold": sum(_num(row.get("qty_sold")) for row in daily_rows),
        "liter_sold": sum(_num(row.get("liter_sold")) for row in daily_rows),
        "sales_amount": sum(_num(row.get("sales_amount")) for row in daily_rows),
    }
    top_sku_total = {
        "sales": sum(_num(row.get("sales")) for row in top_skus),
    }

    return Response({
        "source": "SecMaster",
        "format": "ZEPTO",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "selected_item": selected_item or None,
        "selected_item_head": item_head,
        "daily_rows": daily_rows,
        "daily_total": daily_total,
        "top_skus": top_skus,
        "top_sku_total": top_sku_total,
        "item_options": item_options,
    })


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.blinkit_drr")
def blinkit_drr_dashboard(request):
    _ensure_scope(request.user, "blinkit")
    return _blinkit_drr_dashboard_response(request)


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.zepto_drr")
def zepto_drr_dashboard(request):
    _ensure_scope(request.user, "zepto")
    return _zepto_drr_dashboard_response(request)


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.swiggy_drr")
def swiggy_drr_dashboard(request):
    _ensure_scope(request.user, "swiggy")
    return _swiggy_drr_dashboard_response(request)


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
@cached_get(timeout=60, prefix="plat.bb_drr")
def bigbasket_drr_dashboard(request):
    _ensure_scope(request.user, "bigbasket")
    return _bigbasket_drr_dashboard_response(request)


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.fk_drr")
def flipkart_grocery_drr_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "amazon":
        return _amazon_drr_dashboard_response(request)
    if slug == "blinkit":
        return _blinkit_drr_dashboard_response(request)
    if slug == "zepto":
        return _zepto_drr_dashboard_response(request)
    if slug == "flipkart":
        return _flipkart_mp_drr_dashboard_response(request)
    if slug != "flipkart_grocery":
        raise ValidationError("DRR Dashboard is available only for Amazon, Blinkit, Zepto, Flipkart and Flipkart Grocery.")

    month, year, defaulted_to_latest = _parse_sec_month_year(request.query_params)
    sales_of = str(request.query_params.get("sales_of") or "ALL").strip().upper() or "ALL"
    if sales_of != "ALL":
        raise ValidationError("DRR Dashboard currently supports SALES OF = ALL only.")

    max_date = _scalar(
        """
        SELECT MAX("real_date")
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        """,
        [month, year],
    )

    daily_raw = _dict_rows(
        """
        SELECT
            "real_date",
            COALESCE(SUM("sale_amt_exclusive"), 0) AS ops,
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        GROUP BY "real_date"
        ORDER BY "real_date"
        """,
        [month, year],
    )
    daily_by_date = {r["real_date"]: r for r in daily_raw}
    daily = []
    for current_date in _date_span(month, year, max_date):
        row = daily_by_date.get(current_date, {})
        daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "ops": _num(row.get("ops")),
            "ltr": _num(row.get("ltr")),
        })

    item_raw = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("qty"), 0) AS qty,
            COALESCE(SUM("ltr_sold"), 0) AS liters,
            COALESCE(SUM("sale_amt_exclusive"), 0) AS landing_amt
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        """,
        [month, year],
    )

    elapsed_days = max_date.day if max_date else 0
    days_in_month = monthrange(year, month)[1]
    order = {item: idx for idx, item in enumerate(_FK_GROCERY_DRR_ITEM_ORDER)}
    items = []
    for row in sorted(
        item_raw,
        key=lambda r: (order.get(str(r.get("item") or "").upper(), 999), str(r.get("item") or "")),
    ):
        qty = _num(row.get("qty"))
        liters = _num(row.get("liters"))
        landing_amt = _num(row.get("landing_amt"))
        drr_qty = _safe_div(qty, elapsed_days)
        drr_liters = _safe_div(liters, elapsed_days)
        drr_value = _safe_div(landing_amt, elapsed_days)
        items.append({
            "item": row.get("item"),
            "item_head": row.get("item_head"),
            "qty": qty,
            "liters": liters,
            "landing_amt": landing_amt,
            "drr_qty": drr_qty,
            "drr_liters": drr_liters,
            "drr_value": drr_value,
            "estimated_liters": drr_liters * days_in_month,
        })

    total_qty = sum(_num(r.get("qty")) for r in items)
    total_liters = sum(_num(r.get("liters")) for r in items)
    total_landing_amt = sum(_num(r.get("landing_amt")) for r in items)
    total_drr_qty = _safe_div(total_qty, elapsed_days)
    total_drr_liters = _safe_div(total_liters, elapsed_days)
    total_drr_value = _safe_div(total_landing_amt, elapsed_days)
    totals = {
        "qty": total_qty,
        "liters": total_liters,
        "landing_amt": total_landing_amt,
        "drr_qty": total_drr_qty,
        "drr_liters": total_drr_liters,
        "drr_value": total_drr_value,
        "estimated_liters": total_drr_liters * days_in_month,
    }

    return Response({
        "source": "flipkart_grocery_master",
        "defaulted_to_latest": defaulted_to_latest,
        "sales_of": sales_of,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "daily": daily,
        "daily_groups": [daily[i:i + 9] for i in range(0, len(daily), 9)],
        "items": items,
        "totals": totals,
    })


def _blinkit_drr_empty_total() -> dict:
    return {
        "qty": 0.0,
        "ltr": 0.0,
        "liters": 0.0,
        "value": 0.0,
        "landing_amt": 0.0,
        "drr_qty": 0.0,
        "drr_ltr": 0.0,
        "drr_liters": 0.0,
        "drr_value": 0.0,
        "cur_day_soh_units": 0.0,
        "cur_day_soh_ltr": 0.0,
        "doh": None,
    }


def _blinkit_drr_total(rows: list[dict], elapsed_days: int) -> dict:
    total = _blinkit_drr_empty_total()
    total["qty"] = sum(_num(row.get("qty")) for row in rows)
    total["ltr"] = sum(_num(row.get("ltr")) for row in rows)
    total["liters"] = total["ltr"]
    total["value"] = sum(_num(row.get("value")) for row in rows)
    total["landing_amt"] = total["value"]
    total["cur_day_soh_units"] = sum(_num(row.get("cur_day_soh_units")) for row in rows)
    total["cur_day_soh_ltr"] = sum(_num(row.get("cur_day_soh_ltr")) for row in rows)
    if elapsed_days > 0:
        total["drr_qty"] = total["qty"] / elapsed_days
        total["drr_ltr"] = total["ltr"] / elapsed_days
        total["drr_liters"] = total["drr_ltr"]
        total["drr_value"] = total["value"] / elapsed_days
    return total


def _blinkit_drr_daily_groups(daily: list[dict]) -> list[dict]:
    ranges = ((1, 9), (10, 18), (19, 27), (28, 31))
    groups = []
    for start, end in ranges:
        days = daily[start - 1:end]
        if not days:
            continue
        groups.append({
            "label": f"{start}-{end}",
            "days": days,
        })
    return groups


def _blinkit_drr_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_blinkit",
    )
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]
    month_start = date(year, month, 1)
    month_end = date(year, month, days_in_month)

    sales_of = str(request.query_params.get("sales_of") or "ALL").strip().upper() or "ALL"
    if sales_of not in _BLINKIT_DRR_SALES_OF:
        raise ValidationError(
            "`sales_of` must be one of ALL, PREMIUM, COMMODITY or OTHER."
        )

    max_date = _scalar(
        """
        SELECT MAX("date"::date)
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        """,
        [month_name, year],
    )
    elapsed_days = _sec_elapsed_day(max_date)

    empty_response = {
        "source": {
            "sales": "SecMaster",
            "inventory": "all_platform_inventory",
        },
        "format": "BLINKIT",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "sales_of": sales_of,
        "sales_of_options": list(_BLINKIT_DRR_SALES_OF),
        "max_date": None,
        "sales_max_date": None,
        "inventory_effective_date": None,
        "elapsed_day": 0,
        "elapsed_days": 0,
        "days_in_month": days_in_month,
        "daily": [],
        "daily_groups": [],
        "rows": [],
        "items": [],
        "total": _blinkit_drr_empty_total(),
        "totals": _blinkit_drr_empty_total(),
        "show_blinkit_drr": True,
        "show_value_column": False,
    }
    if max_date is None:
        return Response(empty_response)

    inventory_effective_date = _scalar(
        """
        SELECT MAX(inventory_date)
        FROM all_platform_inventory
        WHERE UPPER(TRIM(format::text)) = 'BLINKIT'
          AND inventory_date >= %s
          AND inventory_date <= %s
        """,
        [month_start, month_end],
    )

    daily_sales_of_filter = ""
    daily_params = [month_name, year, max_date]
    if sales_of != "ALL":
        daily_sales_of_filter = 'AND UPPER(TRIM("item_head"::text)) = %s'
        daily_params.append(sales_of)

    daily_raw = _dict_rows(
        f"""
        SELECT
            "date"::date AS sale_date,
            COALESCE(SUM("sales_amt_exc"), 0) AS ops,
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          AND "date"::date <= %s
          {daily_sales_of_filter}
        GROUP BY "date"::date
        ORDER BY "date"::date
        """,
        daily_params,
    )
    daily_by_date = {row["sale_date"]: row for row in daily_raw}
    daily = []
    for day in range(1, days_in_month + 1):
        current_date = date(year, month, day)
        row = daily_by_date.get(current_date, {}) if current_date <= max_date else {}
        daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "day": day,
            "ops": _num(row.get("ops")),
            "ltr": _num(row.get("ltr")),
        })

    item_rows = _dict_rows(
        """
        WITH sales AS (
            SELECT
                UPPER(TRIM(COALESCE("item"::text, ''))) AS item_key,
                MIN(NULLIF(TRIM("item"::text), '')) AS product,
                COALESCE(MIN(NULLIF(UPPER(TRIM("item_head"::text)), '')), 'OTHER') AS item_head,
                COALESCE(SUM("quantity"), 0)::numeric AS qty,
                COALESCE(SUM("ltr_sold"), 0)::numeric AS ltr,
                COALESCE(SUM("sales_amt_exc"), 0)::numeric AS value
            FROM secmaster_mv
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
              AND "date"::date >= %s
              AND "date"::date <= %s
            GROUP BY UPPER(TRIM(COALESCE("item"::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item::text, ''))) AS item_key,
                MIN(NULLIF(TRIM(item::text), '')) AS inventory_item,
                COALESCE(MIN(NULLIF(UPPER(TRIM(item_head::text)), '')), 'OTHER') AS inventory_item_head,
                COALESCE(SUM(soh_unit), 0)::numeric AS cur_day_soh_units,
                COALESCE(SUM(soh_ltr), 0)::numeric AS cur_day_soh_ltr
            FROM all_platform_inventory
            WHERE format = 'BLINKIT'
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(item::text, '')))
        )
        SELECT
            COALESCE(NULLIF(s.item_head, ''), NULLIF(i.inventory_item_head, ''), 'OTHER') AS item_head,
            COALESCE(NULLIF(s.product, ''), NULLIF(i.inventory_item, '')) AS product,
            i.inventory_item,
            COALESCE(s.qty, 0) AS qty,
            COALESCE(s.ltr, 0) AS ltr,
            COALESCE(s.value, 0) AS value,
            COALESCE(i.cur_day_soh_units, 0) AS cur_day_soh_units,
            COALESCE(i.cur_day_soh_ltr, 0) AS cur_day_soh_ltr
        FROM sales s
        FULL OUTER JOIN inventory i
          ON s.item_key = i.item_key
        WHERE COALESCE(s.item_key, i.item_key) <> ''
        ORDER BY
            CASE COALESCE(NULLIF(s.item_head, ''), NULLIF(i.inventory_item_head, ''), 'OTHER')
                WHEN 'PREMIUM' THEN 1
                WHEN 'COMMODITY' THEN 2
                WHEN 'OTHER' THEN 3
                ELSE 4
            END,
            COALESCE(NULLIF(s.product, ''), NULLIF(i.inventory_item, '')) ASC NULLS LAST
        """,
        [month_start, max_date, inventory_effective_date],
    )

    items = []
    for row in item_rows:
        row_item_head = (row.get("item_head") or "OTHER").upper()
        # Mirror the daily filter — KPI totals + the per-SKU table must
        # only reflect rows matching the selected item-head bucket.
        if sales_of != "ALL" and row_item_head != sales_of:
            continue
        qty = _num(row.get("qty"))
        ltr = _num(row.get("ltr"))
        value = _num(row.get("value"))
        soh_units = _num(row.get("cur_day_soh_units"))
        soh_ltr = _num(row.get("cur_day_soh_ltr"))
        drr_qty = _safe_div(qty, elapsed_days)
        drr_ltr = _safe_div(ltr, elapsed_days)
        drr_value = _safe_div(value, elapsed_days)
        product = row.get("product") or row.get("inventory_item") or ""
        items.append({
            "item_head": row_item_head,
            "product": product,
            "item": product,
            "inventory_item": row.get("inventory_item") or "",
            "qty": qty,
            "ltr": ltr,
            "liters": ltr,
            "value": value,
            "landing_amt": value,
            "drr_qty": drr_qty,
            "drr_ltr": drr_ltr,
            "drr_liters": drr_ltr,
            "drr_value": drr_value,
            "cur_day_soh_units": soh_units,
            "cur_day_soh_ltr": soh_ltr,
            "doh": _safe_div(soh_units, drr_qty),
        })

    total = _blinkit_drr_total(items, elapsed_days)
    daily_total = {
        "ops": sum(_num(row.get("ops")) for row in daily),
        "ltr": sum(_num(row.get("ltr")) for row in daily),
    }

    return Response({
        "source": {
            "sales": "SecMaster",
            "inventory": "all_platform_inventory",
        },
        "format": "BLINKIT",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "sales_of": sales_of,
        "sales_of_options": list(_BLINKIT_DRR_SALES_OF),
        "max_date": max_date.isoformat(),
        "sales_max_date": max_date.isoformat(),
        "inventory_effective_date": (
            inventory_effective_date.isoformat()
            if hasattr(inventory_effective_date, "isoformat")
            else inventory_effective_date
        ),
        "elapsed_day": elapsed_days,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "daily": daily,
        "daily_groups": _blinkit_drr_daily_groups(daily),
        "daily_total": daily_total,
        "rows": items,
        "items": items,
        "total": total,
        "totals": total,
        "show_blinkit_drr": True,
        "show_value_column": False,
        "value_source_note": "VALUE and OPS use SecMaster.sales_amt_exc to match DRR DATABASE column S.",
        "doh_note": "DOH follows the DRR sheet: current SOH units divided by DRR qty.",
    })


def _zepto_drr_dashboard_response(request):
    return _inventory_drr_dashboard_response(request, "zepto")


def _swiggy_drr_dashboard_response(request):
    return _inventory_drr_dashboard_response(request, "swiggy")


def _bigbasket_drr_dashboard_response(request):
    return _inventory_drr_dashboard_response(request, "bigbasket")


def _inventory_drr_dashboard_response(request, slug: str):
    platform = _inventory_dashboard_platform(slug, "DRR Dashboard")
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source=platform["latest_source"],
    )
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]
    month_start = date(year, month, 1)
    month_end = date(year, month, days_in_month)
    sale_date_expr = _secmaster_inventory_date_expr(slug)
    sales_format = platform["sales_format"]
    inventory_format = platform["format"]
    dashboard_title = f"{platform['label']} DRR Dashboard"

    sales_of = str(request.query_params.get("sales_of") or "ALL").strip().upper() or "ALL"
    if sales_of not in _BLINKIT_DRR_SALES_OF:
        raise ValidationError(
            "`sales_of` must be one of ALL, PREMIUM, COMMODITY or OTHER."
        )

    max_date = _scalar(
        f"""
        SELECT MAX({sale_date_expr})
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          AND ({sale_date_expr}) IS NOT NULL
        """,
        [sales_format, month_name, year],
    )
    elapsed_days = _sec_elapsed_day(max_date)

    empty_response = {
        "source": {
            "sales": "SecMaster",
            "inventory": "all_platform_inventory",
        },
        "format": inventory_format,
        "platform": slug,
        "dashboard_title": dashboard_title,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "sales_of": sales_of,
        "sales_of_options": list(_BLINKIT_DRR_SALES_OF),
        "max_date": None,
        "sales_max_date": None,
        "inventory_effective_date": None,
        "elapsed_day": 0,
        "elapsed_days": 0,
        "days_in_month": days_in_month,
        "daily": [],
        "daily_groups": [],
        "rows": [],
        "items": [],
        "total": _blinkit_drr_empty_total(),
        "totals": _blinkit_drr_empty_total(),
        "show_blinkit_drr": True,
        "show_inventory_drr": True,
        "show_value_column": False,
    }
    if max_date is None:
        return Response(empty_response)

    inventory_effective_date = _scalar(
        """
        SELECT MAX(inventory_date)
        FROM all_platform_inventory
        WHERE UPPER(TRIM(format::text)) = %s
          AND inventory_date >= %s
          AND inventory_date <= %s
        """,
        [inventory_format, month_start, month_end],
    )

    daily_sales_of_filter = ""
    daily_params = [sales_format, month_name, year, max_date]
    if sales_of != "ALL":
        daily_sales_of_filter = 'AND UPPER(TRIM("item_head"::text)) = %s'
        daily_params.append(sales_of)

    daily_raw = _dict_rows(
        f"""
        SELECT
            {sale_date_expr} AS sale_date,
            COALESCE(SUM("sales_amt_exc"), 0) AS ops,
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          AND ({sale_date_expr}) <= %s
          AND ({sale_date_expr}) IS NOT NULL
          {daily_sales_of_filter}
        GROUP BY {sale_date_expr}
        ORDER BY {sale_date_expr}
        """,
        daily_params,
    )
    daily_by_date = {row["sale_date"]: row for row in daily_raw}
    daily = []
    for day in range(1, days_in_month + 1):
        current_date = date(year, month, day)
        row = daily_by_date.get(current_date, {}) if current_date <= max_date else {}
        daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "day": day,
            "ops": _num(row.get("ops")),
            "ltr": _num(row.get("ltr")),
        })

    item_rows = _dict_rows(
        f"""
        WITH sales AS (
            SELECT
                UPPER(TRIM(COALESCE("item"::text, ''))) AS item_key,
                MIN(NULLIF(TRIM("item"::text), '')) AS product,
                COALESCE(MIN(NULLIF(UPPER(TRIM("item_head"::text)), '')), 'OTHER') AS item_head,
                COALESCE(SUM("quantity"), 0)::numeric AS qty,
                COALESCE(SUM("ltr_sold"), 0)::numeric AS ltr,
                COALESCE(SUM("sales_amt_exc"), 0)::numeric AS value
            FROM secmaster_mv
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
              AND ({sale_date_expr}) >= %s
              AND ({sale_date_expr}) <= %s
            GROUP BY UPPER(TRIM(COALESCE("item"::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item::text, ''))) AS item_key,
                MIN(NULLIF(TRIM(item::text), '')) AS inventory_item,
                COALESCE(MIN(NULLIF(UPPER(TRIM(item_head::text)), '')), 'OTHER') AS inventory_item_head,
                COALESCE(SUM(soh_unit), 0)::numeric AS cur_day_soh_units,
                COALESCE(SUM(soh_ltr), 0)::numeric AS cur_day_soh_ltr
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(item::text, '')))
        )
        SELECT
            COALESCE(NULLIF(s.item_head, ''), NULLIF(i.inventory_item_head, ''), 'OTHER') AS item_head,
            COALESCE(NULLIF(s.product, ''), NULLIF(i.inventory_item, '')) AS product,
            i.inventory_item,
            COALESCE(s.qty, 0) AS qty,
            COALESCE(s.ltr, 0) AS ltr,
            COALESCE(s.value, 0) AS value,
            COALESCE(i.cur_day_soh_units, 0) AS cur_day_soh_units,
            COALESCE(i.cur_day_soh_ltr, 0) AS cur_day_soh_ltr
        FROM sales s
        FULL OUTER JOIN inventory i
          ON s.item_key = i.item_key
        WHERE COALESCE(s.item_key, i.item_key) <> ''
        ORDER BY
            CASE COALESCE(NULLIF(s.item_head, ''), NULLIF(i.inventory_item_head, ''), 'OTHER')
                WHEN 'PREMIUM' THEN 1
                WHEN 'COMMODITY' THEN 2
                WHEN 'OTHER' THEN 3
                ELSE 4
            END,
            COALESCE(NULLIF(s.product, ''), NULLIF(i.inventory_item, '')) ASC NULLS LAST
        """,
        [sales_format, month_start, max_date, inventory_format, inventory_effective_date],
    )

    items = []
    for row in item_rows:
        row_item_head = (row.get("item_head") or "OTHER").upper()
        # Mirror the daily filter — KPI totals + the per-SKU table must
        # only reflect rows matching the selected item-head bucket.
        if sales_of != "ALL" and row_item_head != sales_of:
            continue
        qty = _num(row.get("qty"))
        ltr = _num(row.get("ltr"))
        value = _num(row.get("value"))
        soh_units = _num(row.get("cur_day_soh_units"))
        soh_ltr = _num(row.get("cur_day_soh_ltr"))
        drr_qty = _safe_div(qty, elapsed_days)
        drr_ltr = _safe_div(ltr, elapsed_days)
        drr_value = _safe_div(value, elapsed_days)
        product = row.get("product") or row.get("inventory_item") or ""
        items.append({
            "item_head": row_item_head,
            "product": product,
            "item": product,
            "inventory_item": row.get("inventory_item") or "",
            "qty": qty,
            "ltr": ltr,
            "liters": ltr,
            "value": value,
            "landing_amt": value,
            "drr_qty": drr_qty,
            "drr_ltr": drr_ltr,
            "drr_liters": drr_ltr,
            "drr_value": drr_value,
            "cur_day_soh_units": soh_units,
            "cur_day_soh_ltr": soh_ltr,
            "doh": _safe_div(soh_units, drr_qty),
        })

    total = _blinkit_drr_total(items, elapsed_days)
    daily_total = {
        "ops": sum(_num(row.get("ops")) for row in daily),
        "ltr": sum(_num(row.get("ltr")) for row in daily),
    }

    return Response({
        "source": {
            "sales": "SecMaster",
            "inventory": "all_platform_inventory",
        },
        "format": inventory_format,
        "platform": slug,
        "dashboard_title": dashboard_title,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "sales_of": sales_of,
        "sales_of_options": list(_BLINKIT_DRR_SALES_OF),
        "max_date": max_date.isoformat(),
        "sales_max_date": max_date.isoformat(),
        "inventory_effective_date": (
            inventory_effective_date.isoformat()
            if hasattr(inventory_effective_date, "isoformat")
            else inventory_effective_date
        ),
        "elapsed_day": elapsed_days,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "daily": daily,
        "daily_groups": _blinkit_drr_daily_groups(daily),
        "daily_total": daily_total,
        "rows": items,
        "items": items,
        "total": total,
        "totals": total,
        "show_blinkit_drr": True,
        "show_inventory_drr": True,
        "show_value_column": False,
        "value_source_note": "VALUE and OPS use SecMaster.sales_amt_exc to match the DRR workbook.",
        "doh_note": "DOH follows the DRR sheet: current SOH units divided by DRR qty.",
    })


def _amazon_drr_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="amazon_sec_daily_master_view",
    )
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]

    item_head = str(
        request.query_params.get("item_head")
        or request.query_params.get("sales_of")
        or "ALL"
    ).strip().upper() or "ALL"
    if item_head not in _AMAZON_DRR_ITEM_HEADS:
        raise ValidationError(
            "`item_head` must be one of ALL, PREMIUM, COMMODITY or OTHER."
        )

    sales_mode = str(
        request.query_params.get("sales_mode")
        or request.query_params.get("mode")
        or "SHIPPED"
    ).strip().upper() or "SHIPPED"
    if sales_mode not in _AMAZON_DRR_SALES_MODES:
        raise ValidationError("`sales_mode` must be ORDERED or SHIPPED.")

    metric_columns = {
        "ORDERED": ("ordered_revenue", "ordered_units", "ordered_litres"),
        "SHIPPED": ("shipped_revenue_2", "shipped_units", "shipped_litres"),
    }
    ops_col, units_col, ltr_col = metric_columns[sales_mode]

    month_start = date(year, month, 1)
    month_end = date(year, month, days_in_month)

    def parse_optional_date(value, field_name: str):
        value = str(value or "").strip()
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise ValidationError(f"`{field_name}` must be a valid YYYY-MM-DD date.")

    from_date = parse_optional_date(request.query_params.get("from_date"), "from_date")
    to_date = parse_optional_date(request.query_params.get("to_date"), "to_date")
    if from_date and to_date and from_date > to_date:
        raise ValidationError("`from_date` cannot be later than `to_date`.")

    item_head_filter = ""
    daily_params = [month_name, year]
    if item_head != "ALL":
        item_head_filter = 'AND UPPER(TRIM("item_head"::text)) = %s'
        daily_params.append(item_head)
    overall_params = list(daily_params)

    date_filter = ""
    if from_date:
        date_filter += ' AND "to_date"::date >= %s'
        daily_params.append(from_date)
    if to_date:
        date_filter += ' AND "to_date"::date <= %s'
        daily_params.append(to_date)

    max_date = _scalar(
        f"""
        SELECT MAX("to_date"::date)
        FROM "amazon_sec_daily_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          {item_head_filter}
          {date_filter}
        """,
        daily_params,
    )
    has_date_range = bool(from_date or to_date)
    daily_start = max(from_date or month_start, month_start)
    if has_date_range:
        daily_end = min(to_date or max_date or month_end, month_end)
    else:
        daily_end = month_end
    if max_date and daily_start <= max_date:
        elapsed_days = (max_date - daily_start).days + 1
    else:
        elapsed_days = 0

    daily_raw = _dict_rows(
        f"""
        SELECT
            "to_date"::date AS sale_date,
            COALESCE(SUM("{ops_col}"), 0) AS ops,
            COALESCE(SUM("{units_col}"), 0) AS units,
            COALESCE(SUM("{ltr_col}"), 0) AS ltr
        FROM "amazon_sec_daily_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          AND "to_date" IS NOT NULL
          {item_head_filter}
          {date_filter}
        GROUP BY "to_date"::date
        ORDER BY "to_date"::date
        """,
        daily_params,
    )
    daily_by_date = {row["sale_date"]: row for row in daily_raw}

    daily = []
    total_ops = 0.0
    total_units = 0.0
    total_ltr = 0.0
    daily_dates = []
    if daily_start <= daily_end:
        daily_dates = [
            daily_start + timedelta(days=offset)
            for offset in range((daily_end - daily_start).days + 1)
        ]
    for current_date in daily_dates:
        row = daily_by_date.get(current_date, {})
        ops = _num(row.get("ops"))
        units = _num(row.get("units"))
        ltr = _num(row.get("ltr"))
        if max_date and current_date <= max_date:
            total_ops += ops
            total_units += units
            total_ltr += ltr
        daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "day": current_date.day,
            "ops": ops,
            "units": units,
            "ltr": ltr,
        })

    overall_max_date = _scalar(
        f"""
        SELECT MAX("to_date"::date)
        FROM "amazon_sec_daily_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          {item_head_filter}
        """,
        overall_params,
    )
    overall_daily_raw = _dict_rows(
        f"""
        SELECT
            "to_date"::date AS sale_date,
            COALESCE(SUM("{ops_col}"), 0) AS ops,
            COALESCE(SUM("{units_col}"), 0) AS units,
            COALESCE(SUM("{ltr_col}"), 0) AS ltr
        FROM "amazon_sec_daily_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          AND "to_date" IS NOT NULL
          {item_head_filter}
        GROUP BY "to_date"::date
        ORDER BY "to_date"::date
        """,
        overall_params,
    )
    overall_daily_by_date = {row["sale_date"]: row for row in overall_daily_raw}
    overall_daily = []
    for day in range(1, days_in_month + 1):
        current_date = date(year, month, day)
        row = overall_daily_by_date.get(current_date, {})
        overall_daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "day": day,
            "ops": _num(row.get("ops")),
            "units": _num(row.get("units")),
            "ltr": _num(row.get("ltr")),
        })

    def build_amazon_drr_row(row: dict) -> dict:
        ops = _num(row.get("ops"))
        units = _num(row.get("units"))
        ltr = _num(row.get("ltr"))
        drr_ops = _safe_div(ops, elapsed_days)
        drr_units = _safe_div(units, elapsed_days)
        drr_ltr = _safe_div(ltr, elapsed_days)
        enriched = dict(row)
        enriched.update({
            "ops": ops,
            "units": units,
            "ltr": ltr,
            "drr_ops": drr_ops,
            "drr_units": drr_units,
            "drr_ltr": drr_ltr,
            "projection_ops": drr_ops * days_in_month,
            "projection_units": drr_units * days_in_month,
            "projection_ltr": drr_ltr * days_in_month,
        })
        return enriched

    sub_category_rows = [
        build_amazon_drr_row(row)
        for row in _dict_rows(
            f"""
            SELECT
                COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
                COALESCE(NULLIF(UPPER(TRIM("category"::text)), ''), 'UNMAPPED') AS category,
                COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'UNMAPPED') AS sub_category,
                COALESCE(
                    NULLIF(
                        STRING_AGG(DISTINCT NULLIF(UPPER(TRIM("brand"::text)), ''), ' / '),
                        ''
                    ),
                    '-'
                ) AS brand,
                COUNT(DISTINCT NULLIF(TRIM("asin"::text), '')) AS sku_count,
                COALESCE(SUM("{ops_col}"), 0) AS ops,
                COALESCE(SUM("{units_col}"), 0) AS units,
                COALESCE(SUM("{ltr_col}"), 0) AS ltr
            FROM "amazon_sec_daily_master_view"
            WHERE UPPER(TRIM("month"::text)) = %s
              AND "year" = %s
              AND "to_date" IS NOT NULL
              {item_head_filter}
              {date_filter}
            GROUP BY
                COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER'),
                COALESCE(NULLIF(UPPER(TRIM("category"::text)), ''), 'UNMAPPED'),
                COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'UNMAPPED')
            ORDER BY ltr DESC, ops DESC, sub_category ASC
            """,
            list(daily_params),
        )
    ]

    sku_rows = [
        build_amazon_drr_row(row)
        for row in _dict_rows(
            f"""
            SELECT
                COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
                COALESCE(NULLIF(UPPER(TRIM("category"::text)), ''), 'UNMAPPED') AS category,
                COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'UNMAPPED') AS sub_category,
                COALESCE(NULLIF(TRIM("brand"::text), ''), '-') AS brand,
                COALESCE(NULLIF(TRIM("per_unit"::text), ''), '-') AS per_ltr,
                COALESCE(NULLIF(TRIM("asin"::text), ''), '-') AS sku_code,
                COALESCE(NULLIF(TRIM("asin"::text), ''), '-') AS asin,
                COALESCE(NULLIF(TRIM("product_title"::text), ''), 'UNMAPPED SKU') AS sku_name,
                COALESCE(NULLIF(TRIM("item"::text), ''), '-') AS item,
                COALESCE(SUM("{ops_col}"), 0) AS ops,
                COALESCE(SUM("{units_col}"), 0) AS units,
                COALESCE(SUM("{ltr_col}"), 0) AS ltr
            FROM "amazon_sec_daily_master_view"
            WHERE UPPER(TRIM("month"::text)) = %s
              AND "year" = %s
              AND "to_date" IS NOT NULL
              {item_head_filter}
              {date_filter}
            GROUP BY
                COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER'),
                COALESCE(NULLIF(UPPER(TRIM("category"::text)), ''), 'UNMAPPED'),
                COALESCE(NULLIF(UPPER(TRIM("sub_category"::text)), ''), 'UNMAPPED'),
                COALESCE(NULLIF(TRIM("brand"::text), ''), '-'),
                COALESCE(NULLIF(TRIM("per_unit"::text), ''), '-'),
                COALESCE(NULLIF(TRIM("asin"::text), ''), '-'),
                COALESCE(NULLIF(TRIM("product_title"::text), ''), 'UNMAPPED SKU'),
                COALESCE(NULLIF(TRIM("item"::text), ''), '-')
            ORDER BY ltr DESC, ops DESC, sku_name ASC
            """,
            list(daily_params),
        )
    ]

    if has_date_range and daily_start <= daily_end:
        max_date_label = (
            f"{daily_start.strftime('%d-%m-%Y')} TO {daily_end.strftime('%d-%m-%Y')}"
        )
    else:
        max_date_label = max_date.strftime("%d %B %Y").upper() if max_date else f"{month_name} {year}"
    overall_max_date_label = (
        overall_max_date.strftime("%d %B %Y").upper()
        if overall_max_date
        else f"{month_name} {year}"
    )
    drr_ops = _safe_div(total_ops, elapsed_days)
    drr_units = _safe_div(total_units, elapsed_days)
    drr_ltr = _safe_div(total_ltr, elapsed_days)
    totals = {
        "ops": total_ops,
        "units": total_units,
        "ltr": total_ltr,
        "avg_value": drr_ops,
        "avg_units": drr_units,
        "avg_ltrs": drr_ltr,
        "drr_ops": drr_ops,
        "drr_units": drr_units,
        "drr_ltr": drr_ltr,
        "projection_ops": drr_ops * days_in_month,
        "projection_units": drr_units * days_in_month,
        "projection_ltr": drr_ltr * days_in_month,
    }

    return Response({
        "source": "amazon_sec_daily_master_view",
        "format": "AMAZON_DRR",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "from_date": from_date.isoformat() if from_date else None,
        "to_date": to_date.isoformat() if to_date else None,
        "date_range_start": daily_start.isoformat() if daily_start <= daily_end else None,
        "date_range_end": daily_end.isoformat() if daily_start <= daily_end else None,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "item_head": item_head,
        "sales_of": item_head,
        "item_head_options": list(_AMAZON_DRR_ITEM_HEADS),
        "sales_mode": sales_mode,
        "sales_mode_options": list(_AMAZON_DRR_SALES_MODES),
        "title": f"JIVO AMAZON SALE ({max_date_label})",
        "overall_title": f"JIVO AMAZON SALE ({overall_max_date_label})",
        "daily": daily,
        "daily_groups": [
            daily[index:index + 9]
            for index in range(0, len(daily), 9)
        ],
        "overall_daily": overall_daily,
        "overall_daily_groups": [
            overall_daily[index:index + 9]
            for index in range(0, len(overall_daily), 9)
        ],
        "rows": sku_rows,
        "items": sku_rows,
        "sku_rows": sku_rows,
        "sub_category_rows": sub_category_rows,
        "totals": totals,
        "summary_note": "Uses amazon_sec_daily_master_view to match DRR rows 1-20 from AMAZON SHEET.xlsx.",
    })


def _flipkart_mp_drr_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="flipkart_secondary_all",
    )
    month_name = _month_name(month)
    days_in_month = monthrange(year, month)[1]
    sales_of = str(request.query_params.get("sales_of") or "ALL").strip().upper() or "ALL"
    if sales_of not in _FLIPKART_MP_DRR_SALES_OF:
        raise ValidationError(
            "`sales_of` must be one of ALL, PREMIUM, COMMODITY or OTHER."
        )

    max_date = _scalar(
        """
        SELECT MAX("Order Date")
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
        """,
        [month_name, year],
    )
    elapsed_days = _sec_elapsed_day(max_date)

    daily_sales_of_filter = ""
    daily_params = [month_name, year]
    if sales_of != "ALL":
        daily_sales_of_filter = 'AND UPPER(TRIM("item_head"::text)) = %s'
        daily_params.append(sales_of)

    daily_raw = _dict_rows(
        f"""
        SELECT
            "Order Date"::date AS sale_date,
            COALESCE(SUM("Final Sale Amount"), 0) AS ops,
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          {daily_sales_of_filter}
        GROUP BY "Order Date"::date
        ORDER BY "Order Date"::date
        """,
        daily_params,
    )
    daily_by_date = {r["sale_date"]: r for r in daily_raw}
    daily = []
    for day in range(1, days_in_month + 1):
        current_date = date(year, month, day)
        row = daily_by_date.get(current_date, {})
        daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "ops": _num(row.get("ops")),
            "ltr": _num(row.get("ltr")),
        })

    item_raw = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(UPPER(TRIM("item"::text)), ''), 'UNMAPPED') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("Final Sale Units"), 0) AS qty,
            COALESCE(SUM("ltr_sold"), 0) AS liters,
            COALESCE(SUM("Final Sale Amount"), 0) AS landing_amt
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
        GROUP BY
            COALESCE(NULLIF(UPPER(TRIM("item"::text)), ''), 'UNMAPPED'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        ORDER BY COALESCE(NULLIF(UPPER(TRIM("item"::text)), ''), 'UNMAPPED')
        """,
        [month_name, year],
    )

    items = []
    for row in item_raw:
        row_item_head = (row.get("item_head") or "OTHER").upper()
        # Mirror the daily filter so KPI totals + the per-SKU table reflect
        # only the selected item-head bucket.
        if sales_of != "ALL" and row_item_head != sales_of:
            continue
        qty = _num(row.get("qty"))
        liters = _num(row.get("liters"))
        landing_amt = _num(row.get("landing_amt"))
        drr_qty = _safe_div(qty, elapsed_days)
        drr_liters = _safe_div(liters, elapsed_days)
        drr_value = _safe_div(landing_amt, elapsed_days)
        items.append({
            "item": row.get("item"),
            "item_head": row_item_head,
            "qty": qty,
            "liters": liters,
            "landing_amt": landing_amt,
            "drr_qty": drr_qty,
            "drr_liters": drr_liters,
            "drr_value": drr_value,
            "estimated_liters": None,
        })

    total_qty = sum(_num(r.get("qty")) for r in items)
    total_liters = sum(_num(r.get("liters")) for r in items)
    total_landing_amt = sum(_num(r.get("landing_amt")) for r in items)
    total_drr_qty = _safe_div(total_qty, elapsed_days)
    total_drr_liters = _safe_div(total_liters, elapsed_days)
    total_drr_value = _safe_div(total_landing_amt, elapsed_days)
    totals = {
        "qty": total_qty,
        "liters": total_liters,
        "landing_amt": total_landing_amt,
        "drr_qty": total_drr_qty,
        "drr_liters": total_drr_liters,
        "drr_value": total_drr_value,
        "estimated_liters": None,
    }

    return Response({
        "source": "flipkart_secondary_all",
        "format": "FLIPKART",
        "defaulted_to_latest": defaulted_to_latest,
        "sales_of": sales_of,
        "sales_of_options": list(_FLIPKART_MP_DRR_SALES_OF),
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "daily": daily,
        "daily_groups": [daily[i:i + 9] for i in range(0, len(daily), 9)],
        "items": items,
        "totals": totals,
        "show_estimated_liters": False,
        "value_label": "Shipped Value",
        "item_label": "Product",
        "qty_label": "Shipped QTY",
        "liters_label": "Shipped LTRS",
        "item_table_subtitle": "Month-to-date shipped totals",
        "summary_note": "Uses flipkart_secondary_all to match the Excel DRR formulas.",
        "normalization_note": "Case-only duplicate product names are normalized so totals reconcile with daily source totals.",
    })


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
@cached_get(timeout=60, prefix="plat.fk_mom")
def flipkart_grocery_month_on_month_sale(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "bigbasket":
        return _bigbasket_month_on_month_analysis_response(request)
    if slug != "flipkart_grocery":
        raise ValidationError(
            "Month On Month Sale is available only for Big Basket and Flipkart Grocery."
        )

    month, year, defaulted_to_latest = _parse_sec_month_year(request.query_params)
    max_date = _scalar(
        """
        SELECT MAX("real_date")
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        """,
        [month, year],
    )

    comparison_months = []
    for index, offset in enumerate([0, -1, -2, -3, -4]):
        compare_month, compare_year = _shift_month(month, year, offset)
        comparison_months.append({
            "key": "current" if index == 0 else f"previous_{index}",
            "month": compare_month,
            "year": compare_year,
            "label": _month_name(compare_month),
        })

    params: list = []
    clauses = []
    for item in comparison_months:
        clauses.append('("month" = %s AND "year" = %s)')
        params.extend([item["month"], item["year"]])

    item_month_rows = _dict_rows(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED') AS item,
            "month",
            "year",
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM "flipkart_grocery_master"
        WHERE {" OR ".join(clauses)}
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED'),
            "month",
            "year"
        """,
        params,
    )
    ltr_by_key = {
        (_norm_sec_key(row.get("item")), int(row.get("month")), int(row.get("year"))): _num(row.get("ltr"))
        for row in item_month_rows
    }

    elapsed_days = max_date.day if max_date else 0
    days_in_month = monthrange(year, month)[1]
    group_map: dict[str, list[dict]] = {}
    for sub_category, item, item_head, target in _FK_GROCERY_MOM_TEMPLATE:
        current_ltr = ltr_by_key.get((_norm_sec_key(item), month, year), 0.0)
        row = {
            "sub_category": sub_category,
            "item": item,
            "item_head": item_head,
            "target": float(target),
            "current_done_ltr": current_ltr,
            "estimated_ltr": _safe_div(current_ltr, elapsed_days) * days_in_month,
            "previous_1_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[1]["month"], comparison_months[1]["year"]),
                0.0,
            ),
            "previous_2_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[2]["month"], comparison_months[2]["year"]),
                0.0,
            ),
            "previous_3_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[3]["month"], comparison_months[3]["year"]),
                0.0,
            ),
            "previous_4_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[4]["month"], comparison_months[4]["year"]),
                0.0,
            ),
        }
        group_map.setdefault(sub_category, []).append(row)

    groups = []
    for sub_category, rows in group_map.items():
        groups.append({
            "sub_category": sub_category,
            "rows": rows,
            "total": _sum_mom_rows(rows),
        })

    group_totals = [group["total"] for group in groups]
    target_summary = [
        {"item_head": item_head, "target": float(target)}
        for item_head, target in _FK_GROCERY_MOM_TARGETS.items()
    ]
    target_summary.append({
        "item_head": "TOTAL",
        "target": float(sum(_FK_GROCERY_MOM_TARGETS.values())),
    })

    return Response({
        "source": "flipkart_grocery_master",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "target_summary": target_summary,
        "comparison_months": comparison_months,
        "groups": groups,
        "grand_total": _sum_mom_rows(group_totals),
    })


# ─── /{slug}/landing-rate  (GET) ───
def _bigbasket_month_on_month_analysis_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_bigbasket",
    )
    month_name = _month_name(month)

    max_date = _scalar(
        """
        SELECT MAX("date")
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        """,
        [month_name, year],
    )

    comparison_months = []
    for index, offset in enumerate([0, -1, -2, -3, -4]):
        compare_month, compare_year = _shift_month(month, year, offset)
        comparison_months.append({
            "key": "current" if index == 0 else f"previous_{index}",
            "month": compare_month,
            "year": compare_year,
            "label": _month_name(compare_month),
        })

    params: list = []
    clauses = []
    for item in comparison_months:
        clauses.append('(UPPER(TRIM("month"::text)) = %s AND "year"::numeric = %s)')
        params.extend([item["label"], item["year"]])

    item_month_rows = _dict_rows(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED') AS item,
            UPPER(TRIM("month"::text)) AS month_name,
            "year"::numeric AS year,
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM secmaster_mv
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
          AND ({" OR ".join(clauses)})
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED'),
            UPPER(TRIM("month"::text)),
            "year"::numeric
        """,
        params,
    )
    ltr_by_key = {
        (
            _norm_sec_key(row.get("item")),
            _norm_sec_key(row.get("month_name")),
            int(row.get("year")),
        ): _num(row.get("ltr"))
        for row in item_month_rows
    }

    elapsed_days = _sec_elapsed_day(max_date)
    projection_days = 30
    group_map: dict[str, list[dict]] = {}
    for sub_category, item, item_head, target in _BIGBASKET_MOM_TEMPLATE:
        current_ltr = ltr_by_key.get(
            (_norm_sec_key(item), month_name, year),
            0.0,
        )
        row = {
            "sub_category": sub_category,
            "item": item,
            "item_head": item_head,
            "target": float(target),
            "current_done_ltr": current_ltr,
            "estimated_ltr": _safe_div(current_ltr, elapsed_days) * projection_days,
            "previous_1_ltr": ltr_by_key.get(
                (
                    _norm_sec_key(item),
                    comparison_months[1]["label"],
                    comparison_months[1]["year"],
                ),
                0.0,
            ),
            "previous_2_ltr": ltr_by_key.get(
                (
                    _norm_sec_key(item),
                    comparison_months[2]["label"],
                    comparison_months[2]["year"],
                ),
                0.0,
            ),
            "previous_3_ltr": ltr_by_key.get(
                (
                    _norm_sec_key(item),
                    comparison_months[3]["label"],
                    comparison_months[3]["year"],
                ),
                0.0,
            ),
            "previous_4_ltr": ltr_by_key.get(
                (
                    _norm_sec_key(item),
                    comparison_months[4]["label"],
                    comparison_months[4]["year"],
                ),
                0.0,
            ),
        }
        group_map.setdefault(sub_category, []).append(row)

    groups = []
    for sub_category, rows in group_map.items():
        groups.append({
            "sub_category": sub_category,
            "rows": rows,
            "total": _sum_mom_rows(rows),
        })

    group_totals = [group["total"] for group in groups]
    target_summary = [
        {"item_head": item_head, "target": float(target)}
        for item_head, target in _BIGBASKET_MOM_TARGETS.items()
    ]
    target_summary.append({
        "item_head": "TOTAL",
        "target": float(sum(_BIGBASKET_MOM_TARGETS.values())),
    })

    return Response({
        "source": "SecMaster",
        "format": "BIG BASKET",
        "dashboard_title": "Big Basket Month On Month Analysis",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "elapsed_days": elapsed_days,
        "days_in_month": monthrange(year, month)[1],
        "projection_days": projection_days,
        "target_summary": target_summary,
        "comparison_months": comparison_months,
        "groups": groups,
        "grand_total": _sum_mom_rows(group_totals),
        "estimation_note": "Estimated LTR uses Excel formula: Done LTR / day(max date) * 30.",
    })


@api_view(["GET"])
@permission_classes([require("platform.landing_rate.view")])
def landing_rate_list(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)

    mode = (request.query_params.get("mode") or "effective").lower()
    month = _parse_month(request.query_params.get("month") or "") or date.today().replace(day=1).isoformat()
    search = (request.query_params.get("search") or "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    # Match stored format aliases, so old `bigbasket` rows and canonical
    # `big basket` rows are shown together.
    format_clause, format_params = _format_match_clause(p)
    base_where = [format_clause]
    base_params: list = format_params
    if search:
        base_where.append('("sku_code"::text ILIKE %s OR "sku_name" ILIKE %s)')
        s = f"%{search}%"
        base_params.extend([s, s])

    try:
        if mode == "history":
            where_sql = " WHERE " + " AND ".join(base_where)
            total = _scalar(
                f'SELECT COUNT(*) FROM "monthly_landing_rate"{where_sql}',
                base_params,
            ) or 0
            rows = _dict_rows(
                f'SELECT * FROM "monthly_landing_rate"{where_sql} '
                f'ORDER BY "month" DESC, "sku_code" ASC LIMIT %s OFFSET %s',
                base_params + [page_size, offset],
            )
        else:
            # Effective view: the rate in effect for the selected month = the
            # newest row per SKU with month <= the selected month. When a SKU has
            # not been re-set this month, its most recent earlier rate carries
            # forward and is flagged `carried_over`, so a brand-new month shows
            # last month's rates instead of an empty sheet. This is display-only —
            # nothing is stored. Editing a carried row writes a fresh row for the
            # selected month (landing_rate_update), leaving earlier months intact.
            where = base_where + [
                '"month"::date < (%s::date + INTERVAL \'1 month\')',
            ]
            where_params = base_params + [month]
            where_sql = " WHERE " + " AND ".join(where)
            # First %s flags rows older than the selected calendar month.
            sub = (
                'SELECT DISTINCT ON ("sku_code") *, '
                '("month"::date < %s::date) AS carried_over '
                f'FROM "monthly_landing_rate"{where_sql} '
                'ORDER BY "sku_code", "month" DESC, "created_at" DESC'
            )
            params = [month] + where_params
            total = _scalar(f"SELECT COUNT(*) FROM ({sub}) t", params) or 0
            rows = _dict_rows(
                f'SELECT * FROM ({sub}) t ORDER BY "sku_code" ASC LIMIT %s OFFSET %s',
                params + [page_size, offset],
            )
    except Exception as e:
        return Response({"data": [], "count": 0, "error": str(e)})

    return Response({
        "data": rows,
        "count": int(total),
        "page": page,
        "page_size": page_size,
        "format": fmt,
        "month": month,
        "mode": mode,
        # True when any row on this page is last month's rate carried forward
        # (this month not set yet) — the UI shows a preview banner.
        "carried_over": any(bool(r.get("carried_over")) for r in rows),
    })


# ─── /{slug}/landing-rate/skus  (GET) ───
# Returns distinct (sku_code, sku_name) pairs already in the table for this
# platform, for autocomplete. Frontend lets the user add new SKUs too.
@api_view(["GET"])
@permission_classes([require("platform.landing_rate.view")])
def landing_rate_skus(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    month = _parse_month(request.query_params.get("month") or "") or date.today().replace(day=1).isoformat()
    format_clause, format_params = _format_match_clause(p)
    try:
        rows = _dict_rows(
            f"""
            SELECT sku_code, sku_name
            FROM (
                SELECT DISTINCT ON (sku_key)
                    sku_code,
                    sku_name
                FROM (
                    SELECT
                        UPPER(TRIM("sku_code"::text)) AS sku_key,
                        TRIM("sku_code"::text) AS sku_code,
                        TRIM("sku_name"::text) AS sku_name,
                        0 AS source_priority,
                        "month"::text AS source_month
                    FROM "monthly_landing_rate"
                    WHERE {format_clause}
                      AND NULLIF(TRIM("sku_code"::text), '') IS NOT NULL

                    UNION ALL

                    SELECT
                        UPPER(TRIM("format_sku_code"::text)) AS sku_key,
                        TRIM("format_sku_code"::text) AS sku_code,
                        TRIM(COALESCE(NULLIF("product_name"::text, ''), "item"::text, '')) AS sku_name,
                        1 AS source_priority,
                        NULL AS source_month
                    FROM "master_sheet"
                    WHERE {format_clause}
                      AND NULLIF(TRIM("format_sku_code"::text), '') IS NOT NULL
                ) candidates
                ORDER BY sku_key, source_priority, source_month DESC NULLS LAST
            ) deduped
            ORDER BY sku_code ASC
            """,
            format_params + format_params,
        )
        rate_rows = _dict_rows(
            f"""
            SELECT DISTINCT UPPER(TRIM("sku_code"::text)) AS sku_key
            FROM "monthly_landing_rate"
            WHERE {format_clause}
              AND "month"::date >= %s::date
              AND "month"::date < (%s::date + INTERVAL '1 month')
            """,
            format_params + [month, month],
        )
        rated_skus = {row["sku_key"] for row in rate_rows}
        for row in rows:
            row["has_rate"] = _norm_sec_key(row.get("sku_code")) in rated_skus
    except Exception:
        rows = []
    return Response({"skus": rows, "format": fmt, "month": month})


@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_sku_add(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    format_clause, format_params = _format_match_clause(p)

    body = request.data or {}
    sku_code = str(body.get("sku_code") or "").strip()
    sku_name = str(body.get("sku_name") or "").strip()

    if not sku_code or not sku_name:
        raise ValidationError("sku_code and sku_name are required.")

    try:
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute(
                f"""
                SELECT "format_sku_code", COALESCE(NULLIF("product_name"::text, ''), "item"::text, '')
                FROM "master_sheet"
                WHERE {format_clause}
                  AND UPPER(TRIM("format_sku_code"::text)) = UPPER(TRIM(%s))
                LIMIT 1
                """,
                format_params + [sku_code],
            )
            existing = cur.fetchone()
            if existing:
                return Response({
                    "ok": True,
                    "created": False,
                    "sku": {
                        "sku_code": existing[0],
                        "sku_name": existing[1] or sku_name,
                        "format": fmt,
                    },
                })

            cur.execute(
                """
                INSERT INTO "master_sheet"
                ("format_sku_code", "product_name", "item", "format")
                VALUES (%s, %s, %s, %s)
                RETURNING "format_sku_code", "product_name", "format"
                """,
                [sku_code, sku_name, sku_name, fmt],
            )
            inserted = cur.fetchone()
    except Exception as e:
        return Response({"ok": False, "error": str(e)}, status=400)

    return Response({
        "ok": True,
        "created": True,
        "sku": {
            "sku_code": inserted[0],
            "sku_name": inserted[1],
            "format": inserted[2] or fmt,
        },
    })


# ─── /{slug}/landing-rate/add  (POST) ───
@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_add(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)

    body = request.data or {}
    sku_code = str(body.get("sku_code") or "").strip()
    sku_name = str(body.get("sku_name") or "").strip()
    month = _parse_month(str(body.get("month") or ""))
    if not sku_code or not sku_name or not month:
        raise ValidationError("sku_code, sku_name and month are required.")

    landing_rate = _decimal_input(body.get("landing_rate"), "landing_rate")
    basic_rate = _landing_basic_rate(body, landing_rate)

    try:
        with connection.cursor() as cur:
            cur.execute(
                'INSERT INTO "monthly_landing_rate" '
                '("sku_code","sku_name","landing_rate","basic_rate","format","month") '
                'VALUES (%s,%s,%s,%s,%s,%s) '
                'RETURNING "created_at"',
                [sku_code, sku_name, landing_rate, basic_rate, fmt, month],
            )
            created_at = cur.fetchone()[0]
    except Exception as e:
        return Response({"ok": False, "error": str(e)}, status=400)

    return Response({
        "ok": True,
        "row": {
            "sku_code": sku_code,
            "sku_name": sku_name,
            "landing_rate": landing_rate,
            "basic_rate": basic_rate,
            "format": fmt,
            "month": month,
            "created_at": created_at.isoformat() if created_at else None,
        },
    })


@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_update(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    format_clause, format_params = _format_match_clause(p)

    body = request.data or {}
    sku_code = str(body.get("sku_code") or "").strip()
    sku_name = str(body.get("sku_name") or "").strip()
    month = _parse_month(str(body.get("month") or ""))
    reason = str(body.get("reason") or "").strip()

    if not sku_code or not month:
        raise ValidationError("sku_code and month are required.")
    if not reason:
        raise ValidationError("reason is required for landing rate updates.")

    landing_rate = _decimal_input(body.get("landing_rate"), "landing_rate")
    basic_rate = _landing_basic_rate(body, landing_rate)

    user = request.user if getattr(request, "user", None) and request.user.is_authenticated else None
    updated_by_id = getattr(user, "id", None)
    updated_by_email = getattr(user, "email", "") or getattr(user, "username", "") if user else ""

    try:
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute(
                f"""
                SELECT ctid::text, "sku_code", "sku_name", "landing_rate", "basic_rate",
                       "format", "month", "created_at"
                FROM "monthly_landing_rate"
                WHERE {format_clause}
                  AND UPPER(TRIM("sku_code"::text)) = UPPER(TRIM(%s))
                  AND "month"::date >= %s::date
                  AND "month"::date < (%s::date + INTERVAL '1 month')
                ORDER BY "created_at" DESC
                LIMIT 1
                FOR UPDATE
                """,
                format_params + [sku_code, month, month],
            )
            row = cur.fetchone()
            if not row:
                # No row for the selected month yet — this happens when the sheet
                # was showing last month's carried-over rates and the user sets
                # one for the new month. Insert a FRESH row for THIS month (with a
                # creation log, old_* = NULL). Earlier months are a different
                # `month` value and are never touched, so prior data is safe.
                next_sku_name = sku_name or sku_code
                cur.execute(
                    """
                    INSERT INTO month_landingrate_logs
                    (sku_code, sku_name, format, month, old_landing_rate, old_basic_rate,
                     new_landing_rate, new_basic_rate, reason, updated_by_id,
                     updated_by_email, source_created_at)
                    VALUES (%s,%s,%s,%s,NULL,NULL,%s,%s,%s,%s,%s,NULL)
                    RETURNING id, updated_at
                    """,
                    [
                        sku_code,
                        next_sku_name,
                        fmt,
                        month,
                        landing_rate,
                        basic_rate,
                        reason,
                        updated_by_id,
                        updated_by_email,
                    ],
                )
                log_id, updated_at = cur.fetchone()
                cur.execute(
                    'INSERT INTO "monthly_landing_rate" '
                    '("sku_code","sku_name","landing_rate","basic_rate","format","month") '
                    'VALUES (%s,%s,%s,%s,%s,%s) '
                    'RETURNING "sku_code","sku_name","landing_rate","basic_rate",'
                    '"format","month","created_at"',
                    [sku_code, next_sku_name, landing_rate, basic_rate, fmt, month],
                )
                created = cur.fetchone()
                return Response({
                    "ok": True,
                    "created": True,
                    "log": {
                        "id": log_id,
                        "updated_at": updated_at.isoformat() if updated_at else None,
                    },
                    "row": {
                        "sku_code": created[0],
                        "sku_name": created[1],
                        "landing_rate": created[2],
                        "basic_rate": created[3],
                        "format": created[4],
                        "month": created[5].isoformat()
                        if hasattr(created[5], "isoformat") else created[5],
                        "created_at": created[6].isoformat() if created[6] else None,
                    },
                })

            (
                row_ctid,
                old_sku_code,
                old_sku_name,
                old_landing_rate,
                old_basic_rate,
                old_format,
                old_month,
                old_created_at,
            ) = row
            next_sku_name = sku_name or old_sku_name

            cur.execute(
                """
                INSERT INTO month_landingrate_logs
                (sku_code, sku_name, format, month, old_landing_rate, old_basic_rate,
                 new_landing_rate, new_basic_rate, reason, updated_by_id,
                 updated_by_email, source_created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id, updated_at
                """,
                [
                    old_sku_code,
                    old_sku_name,
                    old_format,
                    old_month,
                    old_landing_rate,
                    old_basic_rate,
                    landing_rate,
                    basic_rate,
                    reason,
                    updated_by_id,
                    updated_by_email,
                    old_created_at,
                ],
            )
            log_id, updated_at = cur.fetchone()

            cur.execute(
                """
                UPDATE "monthly_landing_rate"
                SET "sku_name" = %s, "landing_rate" = %s, "basic_rate" = %s
                WHERE ctid = %s::tid
                RETURNING "sku_code", "sku_name", "landing_rate", "basic_rate",
                          "format", "month", "created_at"
                """,
                [next_sku_name, landing_rate, basic_rate, row_ctid],
            )
            updated = cur.fetchone()
    except Exception as e:
        return Response({"ok": False, "error": str(e)}, status=400)

    return Response({
        "ok": True,
        "log": {
            "id": log_id,
            "updated_at": updated_at.isoformat() if updated_at else None,
        },
        "row": {
            "sku_code": updated[0],
            "sku_name": updated[1],
            "landing_rate": updated[2],
            "basic_rate": updated[3],
            "format": updated[4] or fmt,
            "month": updated[5],
            "created_at": updated[6].isoformat() if updated[6] else None,
        },
    })


# ─── /{slug}/landing-rate/preview  +  /bulk-upsert  (POST) ─────────────────
# Bulk paste / CSV upload for monthly landing rates. Mirrors the single-row
# add/update behaviour (basic_rate auto-compute, audit log on update) but
# applied across a whole batch in one transaction. SKUs unknown to
# master_sheet for the platform are rejected — the user must add them via
# the existing single-SKU "+ Add entry" flow first.

def _parse_landing_rate_bulk_rows(rows):
    """Validate paste rows; one record per row with classification metadata."""
    parsed = []
    for index, raw in enumerate(rows, start=1):
        record = {
            "index": index,
            "row": raw if isinstance(raw, dict) else {},
            "valid": False,
            "reason": "",
            "sku_code": "",
            "sku_name": "",
            "sku_key": "",
        }
        if not isinstance(raw, dict):
            record["reason"] = "Row must be an object."
            parsed.append(record)
            continue

        sku_code = str(raw.get("sku_code") or "").strip()
        sku_name = str(raw.get("sku_name") or "").strip()
        record["sku_code"] = sku_code
        record["sku_name"] = sku_name
        record["sku_key"] = _norm_sec_key(sku_code)

        if not sku_code:
            record["reason"] = "sku_code is required."
            parsed.append(record)
            continue

        try:
            landing_rate = _decimal_input(raw.get("landing_rate"), "landing_rate")
        except ValidationError as exc:
            record["reason"] = exc.detail if hasattr(exc, "detail") else str(exc)
            parsed.append(record)
            continue

        if landing_rate <= 0:
            record["reason"] = "landing_rate must be greater than 0."
            parsed.append(record)
            continue

        basic_value = raw.get("basic_rate")
        has_basic = basic_value not in (None, "") and str(basic_value).strip() != ""
        if has_basic:
            try:
                basic_rate = _decimal_input(basic_value, "basic_rate")
            except ValidationError as exc:
                record["reason"] = exc.detail if hasattr(exc, "detail") else str(exc)
                parsed.append(record)
                continue
        else:
            basic_rate = landing_rate / _LANDING_BASIC_DIVISOR

        record["valid"] = True
        record["landing_rate"] = landing_rate
        record["basic_rate"] = basic_rate
        parsed.append(record)
    return parsed


def _existing_landing_rates(format_clause, format_params, month, sku_keys):
    """For a given format+month, return latest row per SKU keyed by normalized
    sku_code. Mirrors landing_rate_update's "ORDER BY created_at DESC LIMIT 1"
    logic so a bulk update touches the same row the modal would."""
    keys = sorted({k for k in sku_keys if k})
    if not keys:
        return {}
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (UPPER(TRIM("sku_code"::text)))
                ctid::text                              AS row_ctid,
                "sku_code",
                "sku_name",
                "landing_rate",
                "basic_rate",
                "format",
                "month",
                "created_at",
                UPPER(TRIM("sku_code"::text))           AS sku_key
            FROM "monthly_landing_rate"
            WHERE {format_clause}
              AND UPPER(TRIM("sku_code"::text)) = ANY(%s)
              AND "month"::date >= %s::date
              AND "month"::date < (%s::date + INTERVAL '1 month')
            ORDER BY UPPER(TRIM("sku_code"::text)), "created_at" DESC
            """,
            format_params + [keys, month, month],
        )
        cols = [c[0] for c in cur.description]
        out = {}
        for values in cur.fetchall():
            row = dict(zip(cols, values))
            out[row["sku_key"]] = row
    return out


def _known_master_skus(format_clause, format_params, sku_keys):
    """{sku_key: sku_name} for SKUs that exist in master_sheet for this format.
    Used to reject paste rows whose SKU isn't registered yet."""
    keys = sorted({k for k in sku_keys if k})
    if not keys:
        return {}
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT UPPER(TRIM("format_sku_code"::text)) AS sku_key,
                   COALESCE(NULLIF("product_name"::text, ''), "item"::text, '') AS sku_name
            FROM "master_sheet"
            WHERE {format_clause}
              AND UPPER(TRIM("format_sku_code"::text)) = ANY(%s)
            """,
            format_params + [keys],
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _bulk_classify_row(parsed_row, existing, known):
    """Decide insert / update / skip / invalid for one parsed row. Returns
    (action, reason, sku_name_to_use, existing_row_or_None)."""
    if not parsed_row.get("valid"):
        return "invalid", parsed_row.get("reason", "Invalid row."), "", None
    sku_key = parsed_row["sku_key"]
    if sku_key not in known:
        return "invalid", "SKU not found in master_sheet for this platform.", "", None

    sku_name = parsed_row["sku_name"] or known.get(sku_key, "") or parsed_row["sku_code"]
    existing_row = existing.get(sku_key)
    if not existing_row:
        return "insert", "", sku_name, None

    same = (
        Decimal(str(existing_row["landing_rate"])) == parsed_row["landing_rate"]
        and Decimal(str(existing_row["basic_rate"])) == parsed_row["basic_rate"]
    )
    if same:
        return "skip", "no change", sku_name, existing_row
    return "update", "", sku_name, existing_row


@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_bulk_preview(request, slug: str):
    """Dry-run a bulk paste; no DB writes."""
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    format_clause, format_params = _format_match_clause(p)

    body = request.data or {}
    month = _parse_month(str(body.get("month") or ""))
    raw_rows = body.get("rows") or []

    if not month:
        raise ValidationError("month is required (YYYY-MM or YYYY-MM-DD).")
    if not isinstance(raw_rows, list):
        raise ValidationError("rows must be a list.")

    parsed = _parse_landing_rate_bulk_rows(raw_rows)
    keys = [r["sku_key"] for r in parsed if r["valid"]]
    existing = _existing_landing_rates(format_clause, format_params, month, keys)
    known = _known_master_skus(format_clause, format_params, keys)

    summary = {"insert": 0, "update": 0, "skip": 0, "invalid": 0, "total": len(parsed)}
    preview_rows = []
    for r in parsed:
        action, reason, sku_name, existing_row = _bulk_classify_row(r, existing, known)
        summary[action] += 1
        preview_rows.append({
            "index": r["index"],
            "sku_code": r.get("sku_code", ""),
            "sku_name": sku_name,
            "action": action,
            "reason": reason,
            "landing_rate": str(r.get("landing_rate")) if r.get("valid") else "",
            "basic_rate": str(r.get("basic_rate")) if r.get("valid") else "",
            "existing_landing_rate": str(existing_row["landing_rate"]) if existing_row else "",
            "existing_basic_rate": str(existing_row["basic_rate"]) if existing_row else "",
        })

    return Response({
        "format": fmt,
        "month": month,
        "summary": summary,
        "rows": preview_rows,
    })


@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_bulk_upsert(request, slug: str):
    """Apply a bulk paste in a single transaction. Updates write an audit
    row to month_landingrate_logs the same way the single-row update does."""
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    format_clause, format_params = _format_match_clause(p)

    body = request.data or {}
    month = _parse_month(str(body.get("month") or ""))
    raw_rows = body.get("rows") or []
    audit_reason = (str(body.get("reason") or "").strip() or "bulk upload")

    if not month:
        raise ValidationError("month is required (YYYY-MM or YYYY-MM-DD).")
    if not isinstance(raw_rows, list):
        raise ValidationError("rows must be a list.")

    parsed = _parse_landing_rate_bulk_rows(raw_rows)
    keys = [r["sku_key"] for r in parsed if r["valid"]]
    existing = _existing_landing_rates(format_clause, format_params, month, keys)
    known = _known_master_skus(format_clause, format_params, keys)

    user = request.user if getattr(request, "user", None) and request.user.is_authenticated else None
    updated_by_id = getattr(user, "id", None)
    updated_by_email = getattr(user, "email", "") or getattr(user, "username", "") if user else ""

    summary = {"inserted": 0, "updated": 0, "skipped": 0, "invalid": 0, "total": len(parsed)}
    result_rows = []

    try:
        with transaction.atomic(), connection.cursor() as cur:
            for r in parsed:
                action, reason, sku_name, existing_row = _bulk_classify_row(r, existing, known)

                if action == "invalid":
                    summary["invalid"] += 1
                    result_rows.append({
                        "index": r["index"],
                        "sku_code": r.get("sku_code", ""),
                        "action": "invalid",
                        "reason": reason,
                    })
                    continue
                if action == "skip":
                    summary["skipped"] += 1
                    result_rows.append({
                        "index": r["index"],
                        "sku_code": r["sku_code"],
                        "action": "skip",
                        "reason": "no change",
                    })
                    continue
                if action == "update":
                    cur.execute(
                        """
                        INSERT INTO month_landingrate_logs
                        (sku_code, sku_name, format, month, old_landing_rate, old_basic_rate,
                         new_landing_rate, new_basic_rate, reason, updated_by_id,
                         updated_by_email, source_created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        [
                            existing_row["sku_code"],
                            existing_row["sku_name"],
                            existing_row["format"],
                            existing_row["month"],
                            existing_row["landing_rate"],
                            existing_row["basic_rate"],
                            r["landing_rate"],
                            r["basic_rate"],
                            audit_reason,
                            updated_by_id,
                            updated_by_email,
                            existing_row["created_at"],
                        ],
                    )
                    cur.execute(
                        """
                        UPDATE "monthly_landing_rate"
                        SET "sku_name" = %s, "landing_rate" = %s, "basic_rate" = %s
                        WHERE ctid = %s::tid
                        """,
                        [sku_name, r["landing_rate"], r["basic_rate"], existing_row["row_ctid"]],
                    )
                    summary["updated"] += 1
                    result_rows.append({
                        "index": r["index"],
                        "sku_code": r["sku_code"],
                        "action": "update",
                        "reason": "",
                    })
                    continue

                # insert
                cur.execute(
                    'INSERT INTO "monthly_landing_rate" '
                    '("sku_code","sku_name","landing_rate","basic_rate","format","month") '
                    'VALUES (%s,%s,%s,%s,%s,%s)',
                    [r["sku_code"], sku_name, r["landing_rate"], r["basic_rate"], fmt, month],
                )
                summary["inserted"] += 1
                result_rows.append({
                    "index": r["index"],
                    "sku_code": r["sku_code"],
                    "action": "insert",
                    "reason": "",
                })
    except Exception as exc:  # noqa: BLE001
        return Response({"ok": False, "error": str(exc)}, status=400)

    return Response({
        "ok": True,
        "format": fmt,
        "month": month,
        "summary": summary,
        "rows": result_rows,
    })
