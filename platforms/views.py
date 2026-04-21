import re

from django.db import connection
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from accounts.permissions import require

from .models import PlatformConfig

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str) -> str:
    """Validate a SQL identifier before string-interpolating it into a query.

    Protects against a compromised PlatformConfig row injecting SQL via the
    inventory_table / secondary_table / master_po_table fields.
    """
    if not name or not _IDENT.match(name):
        raise ValidationError(f"Invalid table identifier: {name!r}")
    return name


def _dict_rows(sql: str, params: list) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _scalar(sql: str, params: list):
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def _get_platform(slug: str) -> PlatformConfig:
    return get_object_or_404(PlatformConfig, slug=slug, is_active=True)


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
def platform_stats(request, slug: str):
    """4-card stats for a platform: total POs, inventory SKUs, secondary rows, latest-PO date."""
    platform = _get_platform(slug)
    master = _safe_ident(platform.master_po_table or "master_po")
    inv = _safe_ident(platform.inventory_table) if platform.inventory_table else None
    sec = _safe_ident(platform.secondary_table) if platform.secondary_table else None

    po_count = _scalar(f'SELECT COUNT(*) FROM "{master}" WHERE platform = %s', [slug]) or 0
    inv_count = _scalar(f'SELECT COUNT(*) FROM "{inv}"', []) if inv else 0
    sec_count = _scalar(f'SELECT COUNT(*) FROM "{sec}"', []) if sec else 0
    latest_po = _scalar(
        f'SELECT MAX(created_at) FROM "{master}" WHERE platform = %s', [slug]
    )

    return Response({
        "slug": slug,
        "name": platform.name,
        "po_count": po_count,
        "inventory_count": inv_count,
        "secondary_count": sec_count,
        "latest_po_at": latest_po,
    })


def _paginated(request, sql: str, params: list, count_sql: str, count_params: list):
    try:
        page = max(1, int(request.query_params.get("page", 1)))
        page_size = min(200, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 1, 50
    offset = (page - 1) * page_size
    total = _scalar(count_sql, count_params) or 0
    rows = _dict_rows(f"{sql} LIMIT %s OFFSET %s", params + [page_size, offset])
    return Response({
        "count": total,
        "page": page,
        "page_size": page_size,
        "results": rows,
    })


@api_view(["GET"])
@permission_classes([require("platform.po.view")])
def platform_pos(request, slug: str):
    platform = _get_platform(slug)
    master = _safe_ident(platform.master_po_table or "master_po")
    search = request.query_params.get("search", "").strip()
    where = "WHERE platform = %s"
    params: list = [slug]
    if search:
        where += " AND (po_number ILIKE %s)"
        params.append(f"%{search}%")
    count_sql = f'SELECT COUNT(*) FROM "{master}" {where}'
    rows_sql = f'SELECT * FROM "{master}" {where} ORDER BY created_at DESC NULLS LAST'
    return _paginated(request, rows_sql, params, count_sql, params)


@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
def platform_inventory(request, slug: str):
    platform = _get_platform(slug)
    if not platform.inventory_table:
        return Response({"count": 0, "results": []})
    table = _safe_ident(platform.inventory_table)
    sql = f'SELECT * FROM "{table}"'
    count_sql = f'SELECT COUNT(*) FROM "{table}"'
    return _paginated(request, sql, [], count_sql, [])


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
def platform_secondary(request, slug: str):
    platform = _get_platform(slug)
    if not platform.secondary_table:
        return Response({"count": 0, "results": []})
    table = _safe_ident(platform.secondary_table)
    sql = f'SELECT * FROM "{table}"'
    count_sql = f'SELECT COUNT(*) FROM "{table}"'
    return _paginated(request, sql, [], count_sql, [])
