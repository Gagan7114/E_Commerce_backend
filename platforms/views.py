import re
from calendar import monthrange
from datetime import date
from decimal import Decimal, InvalidOperation

from django.db import connection, transaction
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import can_access_platform, require

from .models import PlatformConfig

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LANDING_BASIC_DIVISOR = Decimal("1.05")


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


def _get_platform(slug: str) -> PlatformConfig:
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


# ─── /{slug}/stats ───
@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
def platform_stats(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    inv = _safe_ident(p.inventory_table) if p.inventory_table else None
    sec = _safe_ident(p.secondary_table) if p.secondary_table else None
    master = _safe_ident(p.master_po_table or "master_po")

    filter_col = _safe_col(p.po_filter_column or "platform") or "platform"
    filter_val = p.po_filter_value or p.slug

    inventory_count = 0
    sells_count = 0
    open_pos = 0

    try:
        if inv:
            inventory_count = _scalar(f'SELECT COUNT(*) FROM "{inv}"', []) or 0
    except Exception:
        inventory_count = 0
    try:
        if sec:
            sells_count = _scalar(f'SELECT COUNT(*) FROM "{sec}"', []) or 0
    except Exception:
        sells_count = 0
    try:
        open_pos = _scalar(
            f'SELECT COUNT(*) FROM "{master}" WHERE "{filter_col}" ILIKE %s',
            [f"%{filter_val}%"],
        ) or 0
    except Exception:
        open_pos = 0

    return Response({
        "inventory": int(inventory_count),
        "sells": int(sells_count),
        "openPOs": int(open_pos),
        "activeTrucks": 0,
    })


# ─── /{slug}/pos ───
@api_view(["GET"])
@permission_classes([require("platform.po.view")])
def platform_pos(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    master = _safe_ident(p.master_po_table or "master_po")
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
        "done_value": 0.0,
        "done_ltrs": 0.0,
        "pending_value": 0.0,
        "pending_ltrs": 0.0,
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
        "done_value": _num(row.get("done_value")),
        "done_ltrs": _num(row.get("done_ltrs")),
        "pending_value": _num(row.get("pending_value")),
        "pending_ltrs": _num(row.get("pending_ltrs")),
        "expired_value": _num(row.get("expired_value")),
        "expired_ltrs": _num(row.get("expired_ltrs")),
    }
    result["dp_value"] = result["done_value"] + result["pending_value"]
    result["dp_ltrs"] = result["done_ltrs"] + result["pending_ltrs"]
    if "item" in row:
        result["item"] = row.get("item")
    if include_cancelled:
        result["cancelled_value"] = _num(row.get("cancelled_value"))
        result["cancelled_ltrs"] = _num(row.get("cancelled_ltrs"))
    return result


def _bigbasket_primary_total(rows: list[dict], *, include_cancelled: bool = True) -> dict:
    fields = [
        "done_value",
        "done_ltrs",
        "pending_value",
        "pending_ltrs",
        "dp_value",
        "dp_ltrs",
        "expired_value",
        "expired_ltrs",
    ]
    if include_cancelled:
        fields.extend(["cancelled_value", "cancelled_ltrs"])
    return {field: sum(_num(row.get(field)) for row in rows) for field in fields}


_PRIMARY_DASHBOARD_FORMATS = {
    "bigbasket": "BIG BASKET",
    "blinkit": "BLINKIT",
    "citymall": "CITY MALL",
    "flipkart_grocery": "FLIPKART GROCERY",
    "swiggy": "SWIGGY",
    "zomato": "ZOMATO",
}
_PRIMARY_DASHBOARD_DONE_VALUE_COLUMNS = {
    ("bigbasket", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("bigbasket", "PO MONTH"): "total_delivered_amt_exclusive",
    ("blinkit", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("blinkit", "PO MONTH"): "total_delivered_amt_exclusive",
    ("citymall", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("citymall", "PO MONTH"): "total_delivered_amt_exclusive",
    ("flipkart_grocery", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("flipkart_grocery", "PO MONTH"): "total_delivered_amt_exclusive",
    ("swiggy", "DEL MONTH"): "total_delivered_amt_exclusive",
    ("swiggy", "PO MONTH"): "total_delivered_amt_exclusive",
    ("zomato", "DEL MONTH"): "total_order_amt_exclusive",
    ("zomato", "PO MONTH"): "total_delivered_amt_exclusive",
}


def _bigbasket_primary_period_bounds(month_name: str, year: int) -> tuple[date, date]:
    month_num = _MONTH_NAME_TO_NUM[month_name]
    next_month, next_year = _shift_month(month_num, year, 1)
    return date(year, month_num, 1), date(next_year, next_month, 1)


@api_view(["GET"])
@permission_classes([require("platform.po.view")])
def bigbasket_primary_dashboard(request, slug: str):
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
    period_where = (
        f"UPPER(TRIM(\"format\"::text)) = %s "
        f"AND (({selected_period}) OR {pending_status})"
    )
    filtered_cte = f"""
        WITH filtered AS (
            SELECT
                *,
                ({selected_period}) AS in_selected_period
            FROM "prim_master_po"
            WHERE {period_where}
        )
    """
    filtered_params = [
        period_start,
        period_end,
        platform_format,
        period_start,
        period_end,
    ]

    max_date = _scalar(
        f"""
        SELECT MAX({date_expr})
        FROM "prim_master_po"
        WHERE UPPER(TRIM("format"::text)) = %s
          AND ({date_expr}) >= %s
          AND ({date_expr}) < %s
        """,
        [platform_format, period_start, period_end],
    )

    summary_raw = _dict_rows(
        f"""
        {filtered_cte}
        SELECT
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                THEN "{done_value_col}" ELSE 0 END), 0) AS done_value,
            COALESCE(SUM(CASE WHEN in_selected_period
                AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                THEN "total_delivered_liters" ELSE 0 END), 0) AS done_ltrs,
            COALESCE(SUM(CASE WHEN UPPER(TRIM("po_status"::text)) IN ('APPOINTMENT DONE', 'PENDING')
                THEN "total_order_amt_exclusive" ELSE 0 END), 0) AS pending_value,
            COALESCE(SUM(CASE WHEN UPPER(TRIM("po_status"::text)) IN ('APPOINTMENT DONE', 'PENDING')
                THEN "total_order_liters" ELSE 0 END), 0) AS pending_ltrs,
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
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "{done_value_col}" ELSE 0 END), 0) AS done_value,
                COALESCE(SUM(CASE WHEN in_selected_period
                    AND UPPER(TRIM("po_status"::text)) = 'COMPLETED'
                    THEN "total_delivered_liters" ELSE 0 END), 0) AS done_ltrs,
                COALESCE(SUM(CASE WHEN UPPER(TRIM("po_status"::text)) IN ('APPOINTMENT DONE', 'PENDING')
                    THEN "total_order_amt_exclusive" ELSE 0 END), 0) AS pending_value,
                COALESCE(SUM(CASE WHEN UPPER(TRIM("po_status"::text)) IN ('APPOINTMENT DONE', 'PENDING')
                    THEN "total_order_liters" ELSE 0 END), 0) AS pending_ltrs,
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

    return Response({
        "source": "prim_master_po",
        "format": f"{slug.upper()}_PRIMARY",
        "source_format": platform_format,
        "defaulted_to_latest": defaulted_to_latest,
        "month_type": month_type,
        "month": _MONTH_NAME_TO_NUM.get(month_name),
        "month_name": month_name,
        "year": year,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else None,
        "summary": summary,
        "summary_total": _bigbasket_primary_total(summary),
        "items": items,
        "item_total": _bigbasket_primary_total(items, include_cancelled=False),
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
    COALESCE(SUM(CASE WHEN status_key = 'COMPLETED'
        THEN COALESCE(total_delivered_amt_exclusive, 0) ELSE 0 END), 0) AS done_value,
    COALESCE(SUM(CASE WHEN status_key = 'COMPLETED'
        THEN COALESCE(total_delivered_liters, 0) ELSE 0 END), 0) AS done_ltrs,
    COALESCE(SUM(CASE WHEN status_key = 'COMPLETED'
        THEN COALESCE(delivered_qty, 0) ELSE 0 END), 0) AS done_qty,
    COALESCE(SUM(CASE WHEN status_key IN ('PENDING', 'APPOINTMENT DONE')
        THEN COALESCE(total_order_amt_exclusive, 0) ELSE 0 END), 0) AS pending_value,
    COALESCE(SUM(CASE WHEN status_key IN ('PENDING', 'APPOINTMENT DONE')
        THEN COALESCE(total_order_liters, 0) ELSE 0 END), 0) AS pending_ltrs,
    COALESCE(SUM(CASE WHEN status_key = 'EXPIRED'
        THEN COALESCE(total_order_amt_exclusive, 0) ELSE 0 END), 0) AS expired_value,
    COALESCE(SUM(CASE WHEN status_key = 'EXPIRED'
        THEN COALESCE(total_order_liters, 0) ELSE 0 END), 0) AS expired_ltrs,
    COALESCE(SUM(CASE WHEN status_key = 'CANCELLED'
        THEN COALESCE(total_order_amt_exclusive, 0) ELSE 0 END), 0) AS cancelled_value,
    COALESCE(SUM(CASE WHEN status_key = 'CANCELLED'
        THEN COALESCE(total_order_liters, 0) ELSE 0 END), 0) AS cancelled_ltrs,
    COALESCE(SUM(COALESCE(total_order_amt_exclusive, 0)), 0) AS order_value,
    COALESCE(SUM(COALESCE(total_order_liters, 0)), 0) AS order_ltrs,
    COALESCE(SUM(COALESCE(order_qty, 0)), 0) AS order_qty
"""


_PRIMARY_TREND_METRIC_SQL = """
    COALESCE(SUM(CASE WHEN status_key = 'COMPLETED'
        THEN COALESCE(total_delivered_amt_exclusive, 0) ELSE 0 END), 0) AS done_value,
    COALESCE(SUM(CASE WHEN status_key = 'COMPLETED'
        THEN COALESCE(total_delivered_liters, 0) ELSE 0 END), 0) AS done_ltrs,
    COALESCE(SUM(CASE WHEN status_key = 'COMPLETED'
        THEN COALESCE(delivered_qty, 0) ELSE 0 END), 0) AS done_qty,
    COALESCE(SUM(CASE WHEN status_key IN ('PENDING', 'APPOINTMENT DONE')
        THEN COALESCE(total_order_amt_exclusive, 0) ELSE 0 END), 0) AS pending_value,
    COALESCE(SUM(CASE WHEN status_key IN ('PENDING', 'APPOINTMENT DONE')
        THEN COALESCE(total_order_liters, 0) ELSE 0 END), 0) AS pending_ltrs,
    COALESCE(SUM(CASE WHEN status_key IN ('PENDING', 'APPOINTMENT DONE')
        THEN COALESCE(order_qty, 0) ELSE 0 END), 0) AS pending_qty,
    COALESCE(SUM(COALESCE(total_order_amt_exclusive, 0)), 0) AS order_value,
    COALESCE(SUM(COALESCE(total_order_liters, 0)), 0) AS order_ltrs,
    COALESCE(SUM(COALESCE(order_qty, 0)), 0) AS order_qty
"""


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
def primary_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "zepto":
        raise ValidationError("Primary Dashboard is available only for Zepto.")

    mode, month, year, defaulted_to_latest = _parse_primary_dashboard_params(request.query_params)
    month_name = _month_name(month)
    period_filter = _primary_period_filter(mode)
    period_params = [month_name, year]

    # This mirrors PRIMARY DASHBOARD!D2 in the workbook:
    # MAXIFS(PRIMARY!BM:BM, PRIMARY!AG:AG, month, PRIMARY!AI:AI, year)
    max_date = _scalar(
        f"""
        {_PRIM_MASTER_PO_CTE}
        SELECT MAX(po_dt)
        FROM normalized
        WHERE po_month_key = %s
          AND po_year = %s
        """,
        [month_name, year],
    )

    summary_raw = _dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE}
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

    detail_raw = _dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE}
        SELECT
            sub_category_key,
            per_ltr_key,
            MIN(item_head_key) AS item_head_key,
            MIN(category_key) AS category_key,
            {_PRIMARY_METRIC_SQL}
        FROM normalized
        WHERE {period_filter}
        GROUP BY sub_category_key, per_ltr_key
        """,
        period_params,
    )
    detail_by_key = {
        (_norm_sec_key(row.get("sub_category_key")), _norm_sec_key(row.get("per_ltr_key"))): row
        for row in detail_raw
    }

    details = []
    fixed_detail_keys = set()
    for fmt, item_head, category, sub_category, per_ltr in _ZEPTO_PRIMARY_DETAIL_ROWS:
        detail_key = (_norm_sec_key(sub_category), _norm_sec_key(per_ltr))
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
            "format": "ZEPTO",
            "item_head": row.get("item_head_key") or "OTHER",
            "category": row.get("category_key") or row.get("sub_category_key") or "OTHER",
            "sub_category": row.get("sub_category_key") or "OTHER",
            "per_ltr": row.get("per_ltr_key") or "-",
            "value_per_ltr": None if metrics["done_ltrs"] == 0 else metrics["done_value"] / metrics["done_ltrs"],
            **metrics,
        })

    top_item_raw = _dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE},
        item_agg AS (
            SELECT
                item_key AS item,
                {_PRIMARY_METRIC_SQL}
            FROM normalized
            WHERE {period_filter}
            GROUP BY item_key
        )
        SELECT *
        FROM item_agg
        WHERE COALESCE(done_value, 0) <> 0
           OR COALESCE(done_ltrs, 0) <> 0
           OR COALESCE(done_qty, 0) <> 0
        ORDER BY done_value DESC, done_ltrs DESC, done_qty DESC
        LIMIT 10
        """,
        period_params,
    )
    top_items = [
        {"item": row.get("item") or "OTHER", **_primary_metrics(row)}
        for row in top_item_raw
    ]
    open_vendor_pending = _dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE}
        SELECT
            COALESCE(
                NULLIF(UPPER(TRIM(vendor_new::text)), ''),
                NULLIF(UPPER(TRIM(vendor_name::text)), ''),
                'UNMAPPED'
            ) AS vendor,
            COALESCE(SUM(COALESCE(total_order_amt_exclusive, 0)), 0) AS order_value,
            COALESCE(SUM(COALESCE(total_delivered_amt_exclusive, 0)), 0) AS delivered_value,
            COALESCE(SUM(GREATEST(
                COALESCE(total_order_amt_exclusive, 0)
                - COALESCE(total_delivered_amt_exclusive, 0),
                0
            )), 0) AS pending_value,
            COALESCE(SUM(COALESCE(total_order_liters, 0)), 0) AS order_ltrs,
            COALESCE(SUM(COALESCE(total_delivered_liters, 0)), 0) AS delivered_ltrs,
            COALESCE(SUM(GREATEST(
                COALESCE(total_order_liters, 0)
                - COALESCE(total_delivered_liters, 0),
                0
            )), 0) AS pending_ltrs,
            COALESCE(SUM(COALESCE(order_qty, 0)), 0) AS order_qty,
            COALESCE(SUM(COALESCE(delivered_qty, 0)), 0) AS delivered_qty,
            COALESCE(SUM(GREATEST(
                COALESCE(order_qty, 0)
                - COALESCE(delivered_qty, 0),
                0
            )), 0) AS pending_qty
        FROM normalized
        WHERE UPPER(TRIM(COALESCE(open_close::text, ''))) = 'OPEN'
        GROUP BY 1
        HAVING COALESCE(SUM(GREATEST(
            COALESCE(total_order_amt_exclusive, 0)
            - COALESCE(total_delivered_amt_exclusive, 0),
            0
        )), 0) > 0
        OR COALESCE(SUM(GREATEST(
            COALESCE(total_order_liters, 0)
            - COALESCE(total_delivered_liters, 0),
            0
        )), 0) > 0
        OR COALESCE(SUM(GREATEST(
            COALESCE(order_qty, 0)
            - COALESCE(delivered_qty, 0),
            0
        )), 0) > 0
        ORDER BY pending_value DESC, vendor
        """,
        [],
    )

    detail_total = _primary_total(details)
    summary_total = _primary_total(summary)
    trend_date_col = "delivery_dt" if mode == "DEL MONTH" else "po_dt"
    period_start = date(year, month, 1)
    period_end = date(year, month, monthrange(year, month)[1])

    daily_trend = _primary_trend_rows(_dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE},
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
            COALESCE(a.pending_qty, 0) AS pending_qty
        FROM trend_days d
        LEFT JOIN agg a ON a.period = d.period
        ORDER BY d.period
        """,
        [period_start, period_end] + period_params,
    ))
    monthly_trend = _primary_trend_rows(_dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE},
        bounds AS (
            SELECT
                make_date(%s::integer, 1, 1) AS start_month,
                COALESCE(
                    DATE_TRUNC('month', MAX({trend_date_col}))::date,
                    make_date(%s::integer, 12, 1)
                ) AS end_month
            FROM normalized
            WHERE {trend_date_col} IS NOT NULL
              AND EXTRACT(YEAR FROM {trend_date_col})::integer = %s
        ),
        trend_months AS (
            SELECT generate_series(start_month, end_month, interval '1 month')::date AS period
            FROM bounds
            WHERE start_month IS NOT NULL
        ),
        agg AS (
            SELECT
                DATE_TRUNC('month', {trend_date_col})::date AS period,
                {_PRIMARY_TREND_METRIC_SQL}
            FROM normalized
            WHERE {trend_date_col} IS NOT NULL
              AND EXTRACT(YEAR FROM {trend_date_col})::integer = %s
            GROUP BY DATE_TRUNC('month', {trend_date_col})::date
        )
        SELECT
            m.period,
            TO_CHAR(m.period, 'Mon YYYY') AS label,
            COALESCE(a.done_value, 0) AS done_value,
            COALESCE(a.done_ltrs, 0) AS done_ltrs,
            COALESCE(a.done_qty, 0) AS done_qty,
            COALESCE(a.pending_value, 0) AS pending_value,
            COALESCE(a.pending_ltrs, 0) AS pending_ltrs,
            COALESCE(a.pending_qty, 0) AS pending_qty
        FROM trend_months m
        LEFT JOIN agg a ON a.period = m.period
        ORDER BY m.period
        """,
        [year, year, year, year],
    ))
    yearly_trend = _primary_trend_rows(_dict_rows(
        f"""
        {_PRIM_MASTER_PO_CTE},
        bounds AS (
            SELECT
                MIN(EXTRACT(YEAR FROM {trend_date_col})::integer) AS start_year,
                MAX(EXTRACT(YEAR FROM {trend_date_col})::integer) AS end_year
            FROM normalized
            WHERE {trend_date_col} IS NOT NULL
        ),
        trend_years AS (
            SELECT generate_series(start_year, end_year)::integer AS period
            FROM bounds
            WHERE start_year IS NOT NULL
        ),
        agg AS (
            SELECT
                EXTRACT(YEAR FROM {trend_date_col})::integer AS period,
                {_PRIMARY_TREND_METRIC_SQL}
            FROM normalized
            WHERE {trend_date_col} IS NOT NULL
            GROUP BY EXTRACT(YEAR FROM {trend_date_col})::integer
        )
        SELECT
            y.period,
            y.period::text AS label,
            COALESCE(a.done_value, 0) AS done_value,
            COALESCE(a.done_ltrs, 0) AS done_ltrs,
            COALESCE(a.done_qty, 0) AS done_qty,
            COALESCE(a.pending_value, 0) AS pending_value,
            COALESCE(a.pending_ltrs, 0) AS pending_ltrs,
            COALESCE(a.pending_qty, 0) AS pending_qty
        FROM trend_years y
        LEFT JOIN agg a ON a.period = y.period
        ORDER BY y.period
        """,
        [],
    ))

    return Response({
        "source": "prim_master_po",
        "format": "ZEPTO",
        "dashboard_title": "Zepto Primary Dashboard",
        "mode": mode,
        "month": month,
        "month_name": month_name,
        "year": year,
        "defaulted_to_latest": defaulted_to_latest,
        "max_date": max_date.isoformat() if hasattr(max_date, "isoformat") else max_date,
        "summary": summary,
        "summary_total": summary_total,
        "details": details,
        "detail_total": detail_total,
        "top_items": top_items,
        "open_vendor_pending": open_vendor_pending,
        "trends": {
            "day": daily_trend,
            "month": monthly_trend,
            "year": yearly_trend,
        },
        "detail_rows_fixed": False,
        "extra_detail_rows_included": True,
    })


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


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
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
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "1 LTR"),
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
        date_expr = (
            _secmaster_zepto_date_expr()
            if source_format == "zepto"
            else '"date"'
        )
        latest = _dict_rows(
            f"""
            SELECT "month", "year"
            FROM "SecMaster"
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


def _parse_sec_selected_date(params) -> date | None:
    raw_date = str(params.get("date") or "").strip()
    raw_month = str(params.get("month") or "").strip()
    candidate = raw_date or (
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
        params.get("month_type")
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
        FROM "prim_master_po"
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

_PRIM_MASTER_PO_CTE = f"""
WITH base AS (
    SELECT
        p.*,
        {_PRIM_PO_DATE_EXPR} AS po_dt,
        {_PRIM_PO_EXPIRY_DATE_EXPR} AS expiry_dt,
        {_PRIM_DELIVERY_DATE_EXPR} AS delivery_dt
    FROM public.prim_master_po p
    WHERE REGEXP_REPLACE(LOWER(TRIM(p.format::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
),
normalized AS (
    SELECT
        *,
        COALESCE(NULLIF(UPPER(TRIM(po_status::text)), ''), 'OTHER') AS status_key,
        COALESCE(NULLIF(UPPER(TRIM(item_head::text)), ''), 'OTHER') AS item_head_key,
        COALESCE(NULLIF(UPPER(TRIM(item::text)), ''), NULLIF(UPPER(TRIM(sku_name::text)), ''), 'OTHER') AS item_key,
        COALESCE(NULLIF(UPPER(TRIM(category::text)), ''), 'OTHER') AS category_key,
        COALESCE(NULLIF(UPPER(TRIM(sub_category::text)), ''), 'OTHER') AS sub_category_key,
        UPPER(TRIM(po_month::text)) AS po_month_key,
        UPPER(TRIM(delivery_month::text)) AS delivery_month_key,
        COALESCE("year", EXTRACT(YEAR FROM po_dt)::integer) AS po_year,
        EXTRACT(YEAR FROM expiry_dt)::integer AS expiry_year,
        CASE
            WHEN per_liter IS NULL THEN UPPER(TRIM(unit_of_measure::text))
            WHEN per_liter < 1
                THEN UPPER(TRIM(TO_CHAR(per_liter * 1000, 'FM999999990.###'))) || ' MLS'
            ELSE UPPER(TRIM(TO_CHAR(per_liter, 'FM999999990.###'))) || ' LTR'
        END AS per_ltr_key
    FROM base
)
"""


def _parse_primary_dashboard_params(params) -> tuple[str, int, int, bool]:
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
        latest = _dict_rows(
            f"""
            {_PRIM_MASTER_PO_CTE}
            SELECT
                {order_date} AS period_date,
                COALESCE(expiry_year, EXTRACT(YEAR FROM delivery_dt)::integer, po_year) AS del_year,
                po_year
            FROM normalized
            WHERE {order_date} IS NOT NULL
            ORDER BY {order_date} DESC
            LIMIT 1
            """,
            [],
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
    return "delivery_month_key = %s AND expiry_year = %s"


def _primary_zero_metrics() -> dict:
    return {
        "done_value": 0.0,
        "done_ltrs": 0.0,
        "done_qty": 0.0,
        "pending_value": 0.0,
        "pending_ltrs": 0.0,
        "dp_value": 0.0,
        "dp_ltrs": 0.0,
        "expired_value": 0.0,
        "expired_ltrs": 0.0,
        "cancelled_value": 0.0,
        "cancelled_ltrs": 0.0,
        "order_value": 0.0,
        "order_ltrs": 0.0,
        "order_qty": 0.0,
    }


def _primary_metrics(row: dict | None) -> dict:
    metrics = _primary_zero_metrics()
    if row:
        for key in (
            "done_value",
            "done_ltrs",
            "done_qty",
            "pending_value",
            "pending_ltrs",
            "expired_value",
            "expired_ltrs",
            "cancelled_value",
            "cancelled_ltrs",
            "order_value",
            "order_ltrs",
            "order_qty",
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


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
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

    return Response({
        "source": "flipkart_grocery_master",
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
    months = []
    for month in range(1, 13):
        month_key = _month_name(month)
        day = monthrange(year, month)[1]
        months.append({
            "month": month,
            "key": month_key,
            "label": "FEBURARY" if month == 2 else month_key,
            "day": day,
            "month_day": f"{day}-{month_key}",
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

    max_date = _scalar(
        """
        SELECT MAX("to_date")
        FROM "amazon_sec_range_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
          AND "to_date" IS NOT NULL
        """,
        [month_name, year],
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
        "notes": notes,
        "show_amazon_excel_columns": True,
        "summary": rk_world_summary,
        "summary_total": rk_world_total,
        "details": sku_details,
        "detail_total": sku_total,
        "summary_note": "Uses amazon_sec_range_master_view filtered by year and month_day built from the selected month's max date.",
        "detail_subtitle": "ASIN-level detail from amazon_sec_range_master_view",
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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

    return Response({
        "source": "SecMaster",
        "format": "BIG BASKET",
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

    return Response({
        "source": "flipkart_secondary_all",
        "format": "FLIPKART",
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
def amazon_comparison_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "amazon":
        raise ValidationError("Comparison Dashboard is available only for Amazon.")
    return _amazon_comparison_dashboard_response(request)


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
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

    max_date = _scalar(
        f"""
        SELECT MAX("date")
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value
        FROM "SecMaster"
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
        FROM "SecMaster"
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

    details = []
    for item_head, category, sub_category, per_ltr, last_month in _BLINKIT_SEC_DETAIL_ROWS:
        row = detail_by_key.get((_norm_sec_key(sub_category), _norm_sec_key(per_ltr)), {})
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

    return Response({
        "source": "SecMaster",
        "format": "BLINKIT",
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
        "show_format_column": True,
        "show_last_month": True,
        "dashboard_title": "Blinkit Secondary Dashboard",
        "detail_subtitle": "Excel rows 12-20 from SECONDARY DASHBOARD",
    })


def _swiggy_sec_dashboard_response(request):
    month, year, defaulted_to_latest = _parse_sec_month_year(
        request.query_params,
        latest_source="secmaster_swiggy",
    )
    selected_date = _parse_sec_selected_date(request.query_params)
    date_filter, date_params = _sec_date_filter(selected_date)
    month_name = _month_name(month)
    prev_month, prev_year = _shift_month(month, year, -1)
    prev_month_name = _month_name(prev_month)

    max_date = _scalar(
        f"""
        SELECT MAX("date")
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sales_amt"), 0) AS shipped_value
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
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

    detail_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("sales_amt"), 0) AS shipped_value,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
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

    last_month_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("ltr_sold"), 0) AS last_month
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [prev_month_name, prev_year],
    )
    last_month_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))): r
        for r in last_month_raw
    }

    details = []
    for item_head, category, sub_category, per_ltr in _SWIGGY_SEC_DETAIL_ROWS:
        key = (_norm_sec_key(sub_category), _norm_sec_key(per_ltr))
        row = detail_by_key.get(key, {})
        last_month_row = last_month_by_key.get(key, {})
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
            "last_month": _num(last_month_row.get("last_month")),
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
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
          AND UPPER(TRIM("month"::text)) = %s
          AND "year"::numeric = %s
          {date_filter}
        """,
        [month_name, year, *date_params],
    )

    summary_raw = _dict_rows(
        f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("quantity"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sales_amt_exc"), 0) AS shipped_value
        FROM "SecMaster"
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
        FROM "SecMaster"
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

    return Response({
        "source": "SecMaster",
        "format": "ZEPTO",
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
        "show_format_column": True,
        "dashboard_title": "Zepto SEC Dashboard",
        "detail_subtitle": "Excel rows 14-47; grand total follows rows 14-42",
        "ratio_label": "PER LTR(SHPD)",
        "detail_total_note": "Detail grand total excludes OTHER rows to match Excel F48:I48.",
    })


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
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
        FROM "SecMaster"
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
            FROM "SecMaster"
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
            FROM "SecMaster" sm
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
        FROM "SecMaster" sm
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
            FROM "SecMaster"
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
        FROM "SecMaster" sm
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
        FROM "SecMaster" sm
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
        FROM "SecMaster"
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
@permission_classes([require("platform.secondary.view")])
def flipkart_grocery_drr_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug == "amazon":
        return _amazon_drr_dashboard_response(request)
    if slug == "flipkart":
        return _flipkart_mp_drr_dashboard_response(request)
    if slug != "flipkart_grocery":
        raise ValidationError("DRR Dashboard is available only for Amazon, Flipkart and Flipkart Grocery.")

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

    max_date = _scalar(
        """
        SELECT MAX("to_date"::date)
        FROM "amazon_sec_daily_master_view"
        WHERE UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
        """,
        [month_name, year],
    )
    elapsed_days = _sec_elapsed_day(max_date)

    item_head_filter = ""
    daily_params = [month_name, year]
    if item_head != "ALL":
        item_head_filter = 'AND UPPER(TRIM("item_head"::text)) = %s'
        daily_params.append(item_head)

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
    for day in range(1, days_in_month + 1):
        current_date = date(year, month, day)
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
            "day": day,
            "ops": ops,
            "units": units,
            "ltr": ltr,
        })

    max_date_label = max_date.strftime("%d %B %Y").upper() if max_date else f"{month_name} {year}"
    totals = {
        "ops": total_ops,
        "units": total_units,
        "ltr": total_ltr,
        "avg_value": _safe_div(total_ops, elapsed_days),
        "avg_ltrs": _safe_div(total_ltr, elapsed_days),
    }

    return Response({
        "source": "amazon_sec_daily_master_view",
        "format": "AMAZON_DRR",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "month_name": month_name,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "item_head": item_head,
        "sales_of": item_head,
        "item_head_options": list(_AMAZON_DRR_ITEM_HEADS),
        "sales_mode": sales_mode,
        "sales_mode_options": list(_AMAZON_DRR_SALES_MODES),
        "title": f"JIVO AMAZON SALE ({max_date_label})",
        "daily": daily,
        "daily_groups": [daily[:9], daily[9:18], daily[18:27], daily[27:]],
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
        FROM "SecMaster"
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
        FROM "SecMaster"
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
            # Effective view: show the latest row per SKU inside the selected
            # calendar month only. This keeps May 2026 from showing April 2026
            # or May rows from any other year.
            where = base_where + [
                '"month"::date >= %s::date',
                '"month"::date < (%s::date + INTERVAL \'1 month\')',
            ]
            params = base_params + [month, month]
            where_sql = " WHERE " + " AND ".join(where)
            sub = (
                f'SELECT DISTINCT ON ("sku_code") * FROM "monthly_landing_rate"'
                f'{where_sql} ORDER BY "sku_code", "month" DESC, "created_at" DESC'
            )
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
                return Response(
                    {"ok": False, "error": "No landing rate row found for this SKU and month."},
                    status=404,
                )

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
