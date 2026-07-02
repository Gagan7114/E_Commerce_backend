"""Curated, read-only data tools the chatbot uses to answer questions.

Design rules:
* Never mutate operational data.
* Prefer the real managed models (alerts, shipments, platforms) whose schema we
  know; for the externally-managed PO / inventory tables, introspect columns at
  runtime (their Django models carry placeholder columns only).
* Be defensive: a query problem returns a clear message, never a 500.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from django.db.models import Count, DecimalField, Q, Sum, Value
from django.db.models.functions import Coalesce

from accounts.models import InventoryDohNotification
from platforms.models import PlatformConfig
from shipment.models import AsinDohDaily, Shipment

from . import safe_sql
from .nlu import ParsedQuery

logger = logging.getLogger(__name__)

_DEC = DecimalField(max_digits=20, decimal_places=4)
PREVIEW_ROWS = 15


@dataclass
class DataResult:
    summary: str
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)   # full rows (already capped)
    source: str = ""
    meta: list = field(default_factory=list)
    ok: bool = True
    suggestions: list[str] = field(default_factory=list)
    excel_title: str = "Data"


def _fmt(n) -> str:
    if n is None:
        return "0"
    if isinstance(n, Decimal):
        n = float(n)
    if isinstance(n, float):
        return f"{n:,.2f}".rstrip("0").rstrip(".") if not n.is_integer() else f"{int(n):,}"
    return f"{n:,}" if isinstance(n, int) else str(n)


def get_active_platforms() -> list[dict]:
    try:
        return [
            {"slug": p.slug, "name": p.name, "sales_type": p.sales_type,
             "inventory_table": p.inventory_table, "secondary_table": p.secondary_table,
             "master_po_table": p.master_po_table, "po_filter_column": p.po_filter_column,
             "po_filter_value": p.po_filter_value}
            for p in PlatformConfig.objects.filter(is_active=True)
        ]
    except Exception:
        logger.exception("get_active_platforms failed")
        return []


# --- Tools -------------------------------------------------------------------

def list_platforms(q: ParsedQuery) -> DataResult:
    platforms = PlatformConfig.objects.all().order_by("slug")
    cols = ["slug", "name", "sales_type", "is_active"]
    rows = [[p.slug, p.name, p.sales_type or "", p.is_active] for p in platforms]
    active = [r for r in rows if r[3]]
    names = ", ".join(r[1] for r in active) or "none configured"
    return DataResult(
        summary=f"There are {len(active)} active platform(s): {names}.",
        columns=cols, rows=rows, source="PlatformConfig", excel_title="Platforms",
    )


def alerts(q: ParsedQuery) -> DataResult:
    qs = InventoryDohNotification.objects.all()
    scope = []
    if q.platform_slugs:
        qs = qs.filter(platform_slug__in=q.platform_slugs)
        scope.append("/".join(p["name"] for p in q.platforms))
    if q.severity:
        qs = qs.filter(severity=q.severity)
        scope.append(q.severity)
    active_only = True if q.active_only is None else q.active_only
    if active_only:
        qs = qs.filter(resolved_at__isnull=True)
        scope.append("active")
    else:
        scope.append("incl. resolved")
    if q.date_from and q.date_to:
        qs = qs.filter(inventory_date__range=(q.date_from, q.date_to))

    total = qs.count()
    by_sev = {r["severity"]: r["n"] for r in qs.values("severity").annotate(n=Count("id"))}

    qs = qs.order_by("doh", "-last_seen_at")
    limit = q.top_n or safe_sql.default_max_rows()
    cols = ["format", "sku_code", "item", "doh", "soh_units", "soh_ltr",
            "ltr_sold", "severity", "inventory_date", "resolved_at"]
    rows = [list(r) for r in qs.values_list(*cols)[:limit]]

    scope_txt = ", ".join([s for s in scope if s]) or "all platforms"
    if total == 0:
        summary = f"No inventory DOH alerts found for {scope_txt}."
    else:
        crit = by_sev.get("critical", 0)
        warn = by_sev.get("warning", 0)
        lead = rows[0] if rows else None
        lead_txt = ""
        if lead:
            lead_txt = f" Lowest DOH: {lead[2] or lead[1]} ({lead[0]}) at {_fmt(lead[3])} days."
        summary = (f"Found {_fmt(total)} DOH alert(s) for {scope_txt} "
                   f"— {crit} critical, {warn} warning.{lead_txt}")
    return DataResult(summary=summary, columns=cols, rows=rows,
                      source="InventoryDohNotification",
                      meta=[("scope", scope_txt), ("total_alerts", total)],
                      excel_title="Alerts")


def liters(q: ParsedQuery) -> DataResult:
    """Answer liters questions from the most appropriate available source, and
    always state which source the number came from."""
    is_delivered = q.movement == "delivered" or "deliver" in q.text.lower() or "dispatch" in q.text.lower()

    # 1) Delivered/dispatched liters -> shipment planner (Amazon dispatch).
    if is_delivered:
        statuses = ["delivered"]
        if "dispatch" in q.text.lower():
            statuses = ["dispatched", "in_transit", "delivered"]
        sq = Shipment.objects.filter(status__in=statuses)
        if q.date_from and q.date_to:
            sq = sq.filter(
                Q(dispatch_date_planned__range=(q.date_from, q.date_to))
                | Q(created_at__date__range=(q.date_from, q.date_to))
            )
        agg = sq.aggregate(
            liters=Coalesce(Sum("planned_liters"), Value(0), output_field=_DEC),
            n=Count("id"),
        )
        by_status = list(
            sq.values("status").annotate(
                liters=Coalesce(Sum("planned_liters"), Value(0), output_field=_DEC),
                n=Count("id"),
            ).order_by("-liters")
        )
        cols = ["status", "shipments", "planned_liters"]
        rows = [[r["status"], r["n"], r["liters"]] for r in by_status]
        span = f" from {q.date_label}" if q.date_label else ""
        summary = (
            f"{_fmt(agg['liters'])} liters across {_fmt(agg['n'])} "
            f"{'/'.join(statuses)} shipment(s){span}. "
            "Source: shipment planner (planned_liters on sp_shipments)."
        )
        return DataResult(summary=summary, columns=cols, rows=rows,
                          source="shipment.Shipment", meta=[("statuses", ",".join(statuses))],
                          excel_title="Delivered Liters")

    # 2) Amazon liters sold -> AsinDohDaily (asin-level daily ltr_sold).
    slugs = q.platform_slugs
    if (not slugs or slugs == ["amazon"]) and q.metric == "liters":
        aq = AsinDohDaily.objects.all()
        if q.date_from and q.date_to:
            aq = aq.filter(date__range=(q.date_from, q.date_to))
        agg = aq.aggregate(
            ltr=Coalesce(Sum("ltr_sold"), Value(0), output_field=_DEC),
            units=Coalesce(Sum("units_sold"), Value(0), output_field=_DEC),
        )
        cols = ["metric", "value"]
        rows = [["liters_sold", agg["ltr"]], ["units_sold", agg["units"]]]
        span = f" ({q.date_label})" if q.date_label else ""
        summary = (f"Amazon sold {_fmt(agg['ltr'])} liters{span}. "
                   "Source: sp_asin_doh_daily (Amazon ASIN daily).")
        return DataResult(summary=summary, columns=cols, rows=rows,
                          source="shipment.AsinDohDaily", excel_title="Amazon Liters")

    # 3) Per-platform liters -> inventory DOH snapshots (ltr_sold / soh_ltr).
    dq = InventoryDohNotification.objects.all()
    if slugs:
        dq = dq.filter(platform_slug__in=slugs)
    if q.date_from and q.date_to:
        dq = dq.filter(inventory_date__range=(q.date_from, q.date_to))
    by_fmt = list(
        dq.values("format").annotate(
            ltr_sold=Coalesce(Sum("ltr_sold"), Value(0), output_field=_DEC),
            soh_ltr=Coalesce(Sum("soh_ltr"), Value(0), output_field=_DEC),
            skus=Count("id"),
        ).order_by("-ltr_sold")
    )
    cols = ["format", "ltr_sold", "soh_ltr", "skus"]
    rows = [[r["format"], r["ltr_sold"], r["soh_ltr"], r["skus"]] for r in by_fmt]
    total_ltr = sum((r[1] for r in rows), Decimal(0))
    where = f" for {q.platforms[0]['name']}" if slugs else " across platforms"
    summary = (
        f"{_fmt(total_ltr)} liters sold{where} in the inventory DOH snapshots. "
        "Note: this covers SKUs tracked in DOH alerts, not full secondary sales — "
        "ask for 'secondary sales' or a platform PO report for order-level totals."
    )
    return DataResult(summary=summary, columns=cols, rows=rows,
                      source="InventoryDohNotification (snapshot)", excel_title="Liters")


def shipments(q: ParsedQuery) -> DataResult:
    sq = Shipment.objects.all()
    status_map = {s.value: s.value for s in Shipment.Status}
    picked = [v for v in status_map if v in q.text.lower().replace(" ", "_")]
    if picked:
        sq = sq.filter(status__in=picked)
    if q.date_from and q.date_to:
        sq = sq.filter(
            Q(dispatch_date_planned__range=(q.date_from, q.date_to))
            | Q(created_at__date__range=(q.date_from, q.date_to))
        )
    total = sq.count()
    by_status = list(
        sq.values("status").annotate(
            n=Count("id"),
            liters=Coalesce(Sum("planned_liters"), Value(0), output_field=_DEC),
        ).order_by("-n")
    )
    sq = sq.order_by("-created_at")
    cols = ["id", "status", "destination_fc", "truck_size", "planned_liters",
            "load_percentage", "dispatch_date_planned", "created_at"]
    limit = q.top_n or safe_sql.default_max_rows()
    rows = [list(r) for r in sq.values_list(*cols)[:limit]]
    total_ltr = sum((float(r["liters"] or 0) for r in by_status), 0.0)
    breakdown = ", ".join(f"{r['status']}: {r['n']}" for r in by_status) or "none"
    summary = (f"{_fmt(total)} shipment(s), {_fmt(total_ltr)} planned liters. "
               f"By status — {breakdown}.")
    return DataResult(summary=summary, columns=cols, rows=rows,
                      source="shipment.Shipment", excel_title="Shipments")


def _introspect_select(table: str, q: ParsedQuery, platform_value: str | None = None) -> DataResult:
    """Generic SELECT over an externally-managed table using live column info."""
    if not safe_sql.table_exists(table):
        return DataResult(summary=f"I couldn't find a table named '{table}' in the database.",
                          ok=False, source=table)
    cols_info = safe_sql.table_columns(table)
    if not cols_info:
        return DataResult(summary=f"Table '{table}' has no readable columns.", ok=False, source=table)

    all_cols = [c["name"] for c in cols_info]
    select_cols = all_cols[:25]  # keep the preview/table manageable
    col_sql = ", ".join(f'"{c}"' for c in select_cols)

    where, params = [], []
    plat_col = safe_sql.find_column(cols_info, "platform", "format", "fmt", "channel", "portal")
    if platform_value and plat_col:
        where.append(f'"{plat_col}"::text ILIKE %s')
        params.append(f"%{platform_value}%")
    date_cols = safe_sql.date_like_columns(cols_info)
    order_col = date_cols[0] if date_cols else select_cols[0]
    if q.date_from and q.date_to and date_cols:
        where.append(f'"{date_cols[0]}"::date BETWEEN %s AND %s')
        params.extend([q.date_from, q.date_to])

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    limit = q.top_n or safe_sql.default_max_rows()
    order_sql = f' ORDER BY "{order_col}" DESC' if order_col else ""
    sql = f'SELECT {col_sql} FROM "{table}"{where_sql}{order_sql} LIMIT {int(limit)}'

    try:
        columns, rows, truncated = safe_sql.run_select(sql, params, max_rows=limit)
    except Exception as exc:
        logger.warning("introspect_select failed for %s: %s", table, exc)
        return DataResult(summary=f"I couldn't read '{table}': {exc}", ok=False, source=table)

    count_sql = f'SELECT COUNT(*) FROM "{table}"{where_sql}'
    try:
        _c, crows, _t = safe_sql.run_select(count_sql, params, max_rows=1)
        total = crows[0][0] if crows else len(rows)
    except Exception:
        total = len(rows)
    return DataResult(
        summary=(f"{_fmt(total)} row(s) in '{table}'"
                 + (f" filtered to {platform_value}" if platform_value and plat_col else "")
                 + (f" ({q.date_label})" if q.date_label and date_cols else "")
                 + f". Showing up to {len(rows)}."),
        columns=columns, rows=rows, source=table,
        meta=[("table", table), ("total_rows", total)], excel_title=table,
    )


def purchase_orders(q: ParsedQuery) -> DataResult:
    platforms = {p["slug"]: p for p in get_active_platforms()}
    table = "master_po"
    platform_value = None
    if q.primary_platform:
        slug = q.primary_platform["slug"]
        cfg = platforms.get(slug)
        if cfg and cfg.get("master_po_table"):
            table = cfg["master_po_table"]
        platform_value = (cfg or {}).get("po_filter_value") or q.primary_platform["name"] or slug
    res = _introspect_select(table, q, platform_value)
    if res.ok:
        res.excel_title = "Purchase Orders"
    return res


def inventory(q: ParsedQuery) -> DataResult:
    if not q.primary_platform:
        return DataResult(
            summary="Which platform's inventory? e.g. 'blinkit inventory' or 'zepto stock'.",
            ok=False, suggestions=["blinkit inventory", "zepto stock on hand"],
        )
    slug = q.primary_platform["slug"]
    cfg = {p["slug"]: p for p in get_active_platforms()}.get(slug, {})
    table = cfg.get("inventory_table") or f"{slug}_inventory"
    res = _introspect_select(table, q)
    res.excel_title = f"{q.primary_platform['name']} Inventory"
    return res


def secondary_sales(q: ParsedQuery) -> DataResult:
    if not q.primary_platform:
        return DataResult(
            summary="Which platform's sales? e.g. 'blinkit secondary sales'.",
            ok=False, suggestions=["blinkit secondary sales", "zepto sales this month"],
        )
    slug = q.primary_platform["slug"]
    cfg = {p["slug"]: p for p in get_active_platforms()}.get(slug, {})
    table = cfg.get("secondary_table") or f"{slug}_secondary"
    res = _introspect_select(table, q)
    res.excel_title = f"{q.primary_platform['name']} Sales"
    return res


def master_po_sheet(q: ParsedQuery) -> DataResult:
    try:
        from accounts.google_sheets import read_worksheet
    except Exception as exc:  # pragma: no cover
        return DataResult(summary=f"Google Sheets integration unavailable: {exc}", ok=False)
    try:
        records = read_worksheet("MASTER PO")
    except Exception as exc:
        return DataResult(
            summary=f"I couldn't read the Master PO Google Sheet: {exc}", ok=False,
            source="Google Sheet: MASTER PO",
        )
    if not records:
        return DataResult(summary="The Master PO sheet is empty.", source="Google Sheet: MASTER PO")
    columns = list(records[0].keys())
    limit = q.top_n or safe_sql.default_max_rows()
    rows = [[rec.get(c, "") for c in columns] for rec in records[:limit]]
    return DataResult(
        summary=f"Read {_fmt(len(records))} rows from the Master PO Google Sheet.",
        columns=columns, rows=rows, source="Google Sheet: MASTER PO",
        excel_title="Master PO Sheet",
    )
