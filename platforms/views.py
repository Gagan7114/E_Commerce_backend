import re
from datetime import date

from django.db import connection
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import can_access_platform, require

from .models import PlatformConfig

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


# ─── Monthly Landing Rate ───
# Single shared table `monthly_landing_rate` with columns:
#   sku_code, sku_name, landing_rate, basic_rate, format, month
# `format` partitions rows per-platform (blinkit/zepto/swiggy/bigbasket).
# Only INSERTs are performed — prior rows are kept as history.

_LANDING_PLATFORMS = {"blinkit", "zepto", "swiggy", "bigbasket"}


def _format_for(p: PlatformConfig) -> str:
    # Prefer the platform's po_filter_value (e.g. "big basket") so rows align
    # with the same string already used in master_po. Fall back to slug.
    return (p.po_filter_value or p.slug).strip()


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


# ─── /{slug}/landing-rate  (GET) ───
@api_view(["GET"])
@permission_classes([require("platform.landing_rate.view")])
def landing_rate_list(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError("Monthly landing rate is only available for blinkit, zepto, swiggy, bigbasket.")
    p = _get_platform(slug)
    fmt = _format_for(p)

    mode = (request.query_params.get("mode") or "effective").lower()
    month = _parse_month(request.query_params.get("month") or "") or date.today().replace(day=1).isoformat()
    search = (request.query_params.get("search") or "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    # Case-insensitive format match + trim, so "Blinkit", "blinkit ", and
    # "big basket" vs "Big Basket" all land on the same rows.
    base_where = ['LOWER(TRIM("format"::text)) = LOWER(TRIM(%s))']
    base_params: list = [fmt]
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
            # Effective view: include every row dated *within or before* the
            # requested month, then pick the newest per SKU. Using
            # `month < (next_month_start)` ensures any day inside the target
            # month (e.g. 2026-04-15) still matches 2026-04.
            where = base_where + ['"month"::date < (%s::date + INTERVAL \'1 month\')']
            params = base_params + [month]
            where_sql = " WHERE " + " AND ".join(where)
            sub = (
                f'SELECT DISTINCT ON ("sku_code") * FROM "monthly_landing_rate"'
                f'{where_sql} ORDER BY "sku_code", "month" DESC'
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
        raise ValidationError("Monthly landing rate is only available for blinkit, zepto, swiggy, bigbasket.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    try:
        rows = _dict_rows(
            'SELECT DISTINCT ON ("sku_code") "sku_code", "sku_name" '
            'FROM "monthly_landing_rate" '
            'WHERE LOWER(TRIM("format"::text)) = LOWER(TRIM(%s)) '
            'ORDER BY "sku_code", "month" DESC',
            [fmt],
        )
    except Exception:
        rows = []
    return Response({"skus": rows, "format": fmt})


# ─── /{slug}/landing-rate/add  (POST) ───
@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_add(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError("Monthly landing rate is only available for blinkit, zepto, swiggy, bigbasket.")
    p = _get_platform(slug)
    fmt = _format_for(p)

    body = request.data or {}
    sku_code = str(body.get("sku_code") or "").strip()
    sku_name = str(body.get("sku_name") or "").strip()
    month = _parse_month(str(body.get("month") or ""))
    if not sku_code or not sku_name or not month:
        raise ValidationError("sku_code, sku_name and month are required.")

    try:
        landing_rate = float(body.get("landing_rate"))
        basic_rate = float(body.get("basic_rate"))
    except (TypeError, ValueError):
        raise ValidationError("landing_rate and basic_rate must be numeric.")

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
