import re
from datetime import date, timedelta

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require
from platforms.models import PlatformConfig

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe(name: str) -> str | None:
    return name if name and _IDENT.match(name) else None


def _scalar(sql: str, params: list | None = None):
    with connection.cursor() as cur:
        cur.execute(sql, params or [])
        row = cur.fetchone()
        return row[0] if row else None


@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def table_counts(request):
    """Count of rows across every configured warehouse table.

    Returns a dict keyed by platform slug with inventory/secondary/po counts,
    skipping tables whose identifier fails validation or tables that don't
    exist (returns 0 on pg_class miss).
    """
    out: dict[str, dict] = {}
    for p in PlatformConfig.objects.filter(is_active=True):
        entry = {"inventory": 0, "secondary": 0, "pos": 0}
        inv = _safe(p.inventory_table)
        sec = _safe(p.secondary_table)
        master = _safe(p.master_po_table or "master_po")
        if inv:
            try:
                entry["inventory"] = _scalar(f'SELECT COUNT(*) FROM "{inv}"') or 0
            except Exception:
                entry["inventory"] = 0
        if sec:
            try:
                entry["secondary"] = _scalar(f'SELECT COUNT(*) FROM "{sec}"') or 0
            except Exception:
                entry["secondary"] = 0
        if master:
            try:
                entry["pos"] = _scalar(
                    f'SELECT COUNT(*) FROM "{master}" WHERE platform = %s', [p.slug]
                ) or 0
            except Exception:
                entry["pos"] = 0
        out[p.slug] = entry
    return Response(out)


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def inventory_chart(request):
    """Per-platform total inventory units for a quick chart."""
    series = []
    for p in PlatformConfig.objects.filter(is_active=True).order_by("slug"):
        inv = _safe(p.inventory_table)
        if not inv:
            series.append({"slug": p.slug, "name": p.name, "total_units": 0})
            continue
        try:
            total = _scalar(f'SELECT COALESCE(SUM(quantity), 0) FROM "{inv}"') or 0
        except Exception:
            total = 0
        series.append({"slug": p.slug, "name": p.name, "total_units": int(total)})
    return Response({"series": series})


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def expiry_alerts(request):
    """SKUs across all platform inventories expiring within N days (default 30)."""
    try:
        days = min(365, max(1, int(request.query_params.get("days", 30))))
    except ValueError:
        days = 30
    cutoff = date.today() + timedelta(days=days)

    alerts = []
    with connection.cursor() as cur:
        for p in PlatformConfig.objects.filter(is_active=True):
            inv = _safe(p.inventory_table)
            if not inv:
                continue
            try:
                cur.execute(
                    f'''
                    SELECT sku, product_name, quantity, expiry_date
                    FROM "{inv}"
                    WHERE expiry_date IS NOT NULL AND expiry_date <= %s
                    ORDER BY expiry_date ASC
                    LIMIT 200
                    ''',
                    [cutoff],
                )
                for row in cur.fetchall():
                    alerts.append({
                        "platform": p.slug,
                        "sku": row[0],
                        "product_name": row[1],
                        "quantity": row[2],
                        "expiry_date": row[3].isoformat() if row[3] else None,
                    })
            except Exception:
                continue
    return Response({"count": len(alerts), "results": alerts})
