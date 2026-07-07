"""Curated, read-only data tools the chatbot uses to answer questions.

Design rules:
* Never mutate operational data.
* Prefer the real managed models (alerts, shipments, platforms) whose schema we
  know; for the externally-managed PO / inventory tables, introspect columns at
  runtime (their Django models carry placeholder columns only).
* Be defensive: a query problem returns a clear message, never a 500.
"""

from __future__ import annotations

import calendar
import logging
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Count, DecimalField, Q, Sum, Value
from django.db.models.functions import Coalesce
from django.utils import timezone

from accounts.models import InventoryDohNotification
from platforms.models import PlatformConfig
from shipment.models import Shipment

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


# --- PO data sources ---------------------------------------------------------
# Liter/ranking answers read purchase-order tables. Most quick-commerce
# platforms live in `master_po` (one row per PO line, keyed by the `format`
# column, whose text is 'BLINKIT', 'BIG BASKET', 'CITY MALL', ...). Amazon is
# loaded separately into reporting."Amazon PO" with its own column names and no
# `format` column — the whole table is Amazon. A source descriptor lets both
# tools query the right table with the right column names.

@dataclass
class _PoSource:
    table: str                 # SQL table expression (already quoted/qualified)
    label: str                 # human/source name
    date_col: str
    order_ltrs: str
    delivered_ltrs: str
    filled_ltrs: str
    missed_ltrs: str
    order_qty: str
    delivered_qty: str
    amount: str                # order amount
    delivered_amount: str      # delivered amount
    format_col: str | None     # platform filter column; None = single-platform table
    dim_cols: dict             # ranking dimension -> real column


_MASTER_PO_SOURCE = _PoSource(
    table="master_po", label="master_po", date_col="po_date",
    order_ltrs="total_order_liters", delivered_ltrs="total_delivered_liters",
    filled_ltrs="filled_ltrs", missed_ltrs="missed_ltrs",
    order_qty="order_qty", delivered_qty="delivered_qty",
    amount="total_order_amt_inclusive", delivered_amount="total_deliver_amt_inclusive",
    format_col="format",
    dim_cols={"state": "state", "city": "city", "location": "location",
              "sku": "sku_code", "brand": "brand", "category": "category",
              "item": "item", "vendor": "vendor_name", "platform": "format"},
)

# Amazon POs live in reporting."Amazon PO": order qty = requested_qty, delivered
# qty = received_qty, amount = total_order_amt_exclusive, date = order_date, and
# the vendor dimension column is `vendor` (not vendor_name). No format column.
_AMAZON_PO_SOURCE = _PoSource(
    table='reporting."Amazon PO"', label='reporting."Amazon PO"', date_col="order_date",
    order_ltrs="total_order_liters", delivered_ltrs="total_delivered_liters",
    filled_ltrs="filled_ltrs", missed_ltrs="missed_ltrs",
    order_qty="requested_qty", delivered_qty="received_qty",
    amount="total_order_amt_exclusive", delivered_amount="total_deliver_amt_exclusive",
    format_col=None,
    dim_cols={"state": "state", "city": "city", "sku": "sku_code",
              "brand": "brand", "category": "category", "item": "item",
              "vendor": "vendor"},
)


def _platform_format_value(slug: str, platforms: dict | None = None) -> str:
    """Text stored in master_po.format for a platform (e.g. 'big basket' for
    slug 'bigbasket', 'city mall' for 'citymall'). Uses the PlatformConfig
    po_filter_value so filtering matches the real data, not the slug. Falls back
    to the slug when unconfigured."""
    if platforms is None:
        platforms = {p["slug"]: p for p in get_active_platforms()}
    cfg = platforms.get(slug, {})
    return (cfg.get("po_filter_value") or "").strip() or slug


def _resolve_po_source(q: ParsedQuery) -> tuple[_PoSource, str, str | None]:
    """Pick the PO table for a query. Returns (source, scope_label, format_value).

    ``format_value`` is the text to match against the source's format column
    (None when there's no platform filter or the source is single-platform)."""
    plat = q.primary_platform
    if plat and plat["slug"] == "amazon":
        return _AMAZON_PO_SOURCE, "Amazon", None
    if plat:
        return _MASTER_PO_SOURCE, plat["name"], _platform_format_value(plat["slug"])
    return _MASTER_PO_SOURCE, "all platforms", None


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
    if "unread" in q.text.lower():
        qs = qs.filter(is_read=False)
        scope.append("unread")
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


def _liters_by_month(q: ParsedQuery, source: _PoSource, scope: str, fmt_val: str | None) -> DataResult:
    """Order/delivered liters broken out one row per calendar month, plus a
    grand-total row. Same platform + optional date-range filters as `liters`.
    Triggered by 'month wise' / 'monthly' / 'all months' style questions."""
    where, params = [], []
    if source.format_col and fmt_val:
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
    if q.date_from and q.date_to:
        where.append(f"{source.date_col} BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    where.append(f"{source.date_col} IS NOT NULL")
    where_sql = " WHERE " + " AND ".join(where)

    sql = f"""
        SELECT to_char({source.date_col}, 'YYYY-MM') AS ym,
               COUNT(*) AS pos,
               COALESCE(SUM({source.order_ltrs}), 0) AS order_ltrs,
               COALESCE(SUM({source.delivered_ltrs}), 0) AS delivered_ltrs,
               COALESCE(SUM({source.missed_ltrs}), 0) AS missed_ltrs
        FROM {source.table}{where_sql}
        GROUP BY ym
        ORDER BY ym
    """
    try:
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=600)
    except Exception as exc:
        logger.warning("monthly liters query failed: %s", exc)
        return DataResult(summary=f"I couldn't total the monthly liters: {exc}",
                          ok=False, source=source.label)
    if not rows:
        return DataResult(summary=f"No PO data found for {scope}.", ok=False, source=source.label)

    cols = ["Month", "PO lines", "Order L", "Delivered L", "Fill %", "Missed L"]
    data_rows = []
    tot_pos = 0
    tot_order = tot_deliv = tot_missed = 0.0
    for ym, pos, order_l, deliv_l, missed_l in rows:
        order_f, deliv_f = float(order_l or 0), float(deliv_l or 0)
        fill = round(deliv_f / order_f * 100.0, 1) if order_f else 0.0
        data_rows.append([ym, pos, order_l, deliv_l, fill, missed_l])
        tot_pos += pos or 0
        tot_order += order_f
        tot_deliv += deliv_f
        tot_missed += float(missed_l or 0)
    grand_fill = round(tot_deliv / tot_order * 100.0, 1) if tot_order else 0.0
    data_rows.append(["TOTAL", tot_pos, tot_order, tot_deliv, grand_fill, tot_missed])

    span = q.date_label or "all months"
    n = len(rows)  # month rows only (data_rows has a trailing TOTAL row)
    body = "\n".join(
        f"{r[0]}: {_fmt(r[2])} L ordered, {_fmt(r[3])} L delivered ({r[4]}%)"
        for r in data_rows[:min(n, 12)]
    )
    more = f"\n…and {n - 12} more month(s) — see the table/Excel." if n > 12 else ""
    summary = (
        f"{scope} — order vs delivered liters by month ({span}), {n} month(s):\n"
        f"{body}{more}\n"
        f"TOTAL: {_fmt(tot_order)} L ordered, {_fmt(tot_deliv)} L delivered "
        f"(fill {grand_fill}%). Source: {source.label}."
    )
    return DataResult(
        summary=summary, columns=cols, rows=data_rows, source=source.label,
        meta=[("scope", scope), ("range", span), ("months", n)],
        excel_title="Liters by Month",
    )


def _liters_by_platform(q: ParsedQuery) -> DataResult:
    """Order/delivered liters broken out one row per platform (master_po), plus a
    grand-total row. Triggered by 'platform wise' / 'by platform' questions."""
    source = _MASTER_PO_SOURCE
    where, params = [], []
    if q.date_from and q.date_to:
        where.append(f"{source.date_col} BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    where.append("format IS NOT NULL AND format::text <> ''")
    where_sql = " WHERE " + " AND ".join(where)
    sql = f"""
        SELECT format, COUNT(*),
               COALESCE(SUM({source.order_ltrs}),0), COALESCE(SUM({source.delivered_ltrs}),0),
               COALESCE(SUM({source.missed_ltrs}),0)
        FROM {source.table}{where_sql}
        GROUP BY format ORDER BY 3 DESC
    """
    try:
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=50)
    except Exception as exc:
        return DataResult(summary=f"I couldn't total platform-wise liters: {exc}", ok=False, source=source.label)
    if not rows:
        return DataResult(summary="No PO data found.", ok=False, source=source.label)

    cols = ["Platform", "PO lines", "Order L", "Delivered L", "Fill %", "Missed L"]
    data_rows, t_pos, t_o, t_d, t_m = [], 0, 0.0, 0.0, 0.0
    for fmt, pos, o, d, m in rows:
        of, df = float(o or 0), float(d or 0)
        fill = round(df / of * 100, 1) if of else 0.0
        data_rows.append([fmt, pos, o, d, fill, m])
        t_pos += pos or 0
        t_o += of
        t_d += df
        t_m += float(m or 0)
    gfill = round(t_d / t_o * 100, 1) if t_o else 0.0
    data_rows.append(["TOTAL", t_pos, t_o, t_d, gfill, t_m])
    span = q.date_label or "all time"
    body = "\n".join(f"{r[0]}: {_fmt(r[2])} L ordered, {_fmt(r[3])} L delivered ({r[4]}%)" for r in data_rows[:-1])
    summary = (f"Order vs delivered liters by platform ({span}):\n{body}\n"
               f"TOTAL: {_fmt(t_o)} L ordered, {_fmt(t_d)} L delivered (fill {gfill}%). Source: master_po.")
    return DataResult(summary=summary, columns=cols, rows=data_rows, source=source.label,
                      meta=[("range", span)], excel_title="Liters by Platform")


def liters(q: ParsedQuery) -> DataResult:
    """Order / delivered liters (and quantities) for a platform. Reads the right
    PO table: master_po for quick-commerce platforms (filtered by the real
    `format` text via po_filter_value, so BigBasket/CityMall match too), and
    reporting."Amazon PO" for Amazon (its own columns; no `format` column).

    A 'month wise' / 'monthly' / 'all months' question returns a per-month
    breakdown instead of a single total."""
    source, scope, fmt_val = _resolve_po_source(q)
    if q.group_by_platform:
        return _liters_by_platform(q)
    if q.group_by_month:
        return _liters_by_month(q, source, scope, fmt_val)

    where, params = [], []
    if source.format_col and fmt_val:
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
    if q.date_from and q.date_to:
        where.append(f"{source.date_col} BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT
            COUNT(*) AS pos,
            COALESCE(SUM({source.order_ltrs}), 0) AS order_ltrs,
            COALESCE(SUM({source.delivered_ltrs}), 0) AS delivered_ltrs,
            COALESCE(SUM({source.filled_ltrs}), 0) AS filled_ltrs,
            COALESCE(SUM({source.missed_ltrs}), 0) AS missed_ltrs,
            COALESCE(SUM({source.order_qty}), 0) AS order_qty,
            COALESCE(SUM({source.delivered_qty}), 0) AS delivered_qty,
            COALESCE(SUM({source.amount}), 0) AS order_amt,
            COALESCE(SUM({source.delivered_amount}), 0) AS delivered_amt
        FROM {source.table}{where_sql}
    """
    try:
        _cols, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    except Exception as exc:
        logger.warning("liters query failed: %s", exc)
        return DataResult(summary=f"I couldn't total the liters: {exc}", ok=False, source=source.label)

    r = rows[0] if rows else [0] * 9
    (pos, order_ltrs, delivered_ltrs, filled_ltrs, missed_ltrs,
     order_qty, delivered_qty, order_amt, delivered_amt) = r
    span = f" in {q.date_label}" if q.date_label else ""
    text = q.text.lower()

    if q.wants_amount:
        if "deliver" in text or q.movement == "delivered":
            headline = f"{scope}: ₹{_fmt(delivered_amt)} delivered value{span}"
        else:
            headline = f"{scope}: ₹{_fmt(order_amt)} order value{span}"
    elif "deliver" in text or q.movement == "delivered":
        headline = f"{scope}: {_fmt(delivered_ltrs)} liters delivered{span}"
    elif "order" in text:
        headline = f"{scope}: {_fmt(order_ltrs)} liters ordered{span}"
    else:
        headline = f"{scope}{span}: {_fmt(order_ltrs)} L ordered, {_fmt(delivered_ltrs)} L delivered"

    fill = (float(delivered_ltrs) / float(order_ltrs) * 100.0) if float(order_ltrs or 0) else 0.0
    summary = (
        f"{headline}. Across {_fmt(pos)} PO line(s): ordered {_fmt(order_ltrs)} L, "
        f"delivered {_fmt(delivered_ltrs)} L (fill {fill:.1f}%), missed {_fmt(missed_ltrs)} L. "
        f"Order value ₹{_fmt(order_amt)}, delivered value ₹{_fmt(delivered_amt)}. "
        f"Source: {source.label}."
    )
    cols = ["metric", "value"]
    data_rows = [
        ["PO lines", pos],
        ["Order liters", order_ltrs],
        ["Delivered liters", delivered_ltrs],
        ["Filled liters", filled_ltrs],
        ["Missed liters", missed_ltrs],
        ["Order qty", order_qty],
        ["Delivered qty", delivered_qty],
        ["Order amount", order_amt],
        ["Delivered amount", delivered_amt],
    ]
    return DataResult(summary=summary, columns=cols, rows=data_rows, source=source.label,
                      meta=[("scope", scope), ("range", q.date_label or "all")],
                      excel_title="Liters")


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


_PO_STATUS_WORDS = {
    "completed": "COMPLETED", "complete": "COMPLETED", "cancelled": "CANCELLED",
    "canceled": "CANCELLED", "expired": "EXPIRED", "appointment done": "APPOINTMENT DONE",
}


def purchase_orders(q: ParsedQuery) -> DataResult:
    text = q.text.lower()
    fmt_val = _platform_format_value(q.primary_platform["slug"]) if q.primary_platform else None
    scope = q.primary_platform["name"] if q.primary_platform else "all platforms"

    # Status count mode ("how many pos completed/cancelled/expired ...")
    status = next((v for k, v in _PO_STATUS_WORDS.items() if k in text), None)
    if status and safe_sql.table_exists("master_po"):
        where, params = ["po_status = %s"], [status]
        if fmt_val:
            where.append("format ILIKE %s")
            params.append(f"%{fmt_val}%")
        if q.date_from and q.date_to:
            where.append("po_date BETWEEN %s AND %s")
            params.extend([q.date_from, q.date_to])
        wsql = " WHERE " + " AND ".join(where)
        try:
            _c, rows, _t = safe_sql.run_select(
                f"SELECT COUNT(DISTINCT po_number), COUNT(*), COALESCE(SUM(total_order_liters),0), "
                f"COALESCE(SUM(total_order_amt_inclusive),0) FROM master_po{wsql}", params, max_rows=1)
            npo, lines_n, ltr, val = rows[0]
            span = f" ({q.date_label})" if q.date_label else ""
            return DataResult(
                summary=(f"{scope}: {_fmt(npo)} {status} PO(s) ({_fmt(lines_n)} line items){span} — "
                         f"{_fmt(ltr)} order liters, ₹{_fmt(val)} order value. Source: master_po."),
                columns=["metric", "value"],
                rows=[["POs", npo], ["Line items", lines_n], ["Order liters", ltr], ["Order value", val]],
                source="master_po", meta=[("status", status)], excel_title=f"{scope} {status} POs")
        except Exception as exc:
            logger.warning("po status count failed: %s", exc)

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


_INVENTORY_TABLE = "all_platform_inventory"


def _threshold_after(text: str, *keywords: str) -> int | None:
    """Number in phrases like 'less than 10' / 'below 5' / 'under 20' / '< 10'."""
    m = re.search(r"(?:less than|below|under|lower than|<)\s*(\d+)", text.lower())
    return int(m.group(1)) if m else None


def inventory(q: ParsedQuery) -> DataResult:
    """Stock-on-hand summary from all_platform_inventory (latest snapshot):
    total SOH units/liters, SKU count and top products. Supports a low-stock
    filter ('soh units less than 10') and city grouping ('stock by city')."""
    table = _INVENTORY_TABLE
    if not safe_sql.table_exists(table):
        return DataResult(summary=f"I couldn't find the '{table}' table.", ok=False, source=table)

    fmt_val = _platform_format_value(q.primary_platform["slug"]) if q.primary_platform else None
    scope = q.primary_platform["name"] if q.primary_platform else "all platforms"
    text = q.text.lower()

    base_where, base_params = [], []
    if fmt_val:
        base_where.append("format ILIKE %s")
        base_params.append(f"%{fmt_val}%")
    base_wsql = (" WHERE " + " AND ".join(base_where)) if base_where else ""

    try:
        _c, drows, _t = safe_sql.run_select(
            f"SELECT MAX(inventory_date) FROM {table}{base_wsql}", base_params, max_rows=1)
        latest = drows[0][0] if drows else None
    except Exception as exc:
        return DataResult(summary=f"I couldn't read inventory: {exc}", ok=False, source=table)
    if latest is None:
        return DataResult(summary=f"No inventory rows found for {scope}.", ok=False, source=table)

    where = list(base_where) + ["inventory_date = %s"]
    params = list(base_params) + [latest]
    wsql = " WHERE " + " AND ".join(where)

    # City / location grouping
    if any(p in text for p in ("by city", "stock by city", "by location", "city wise", "citywise")):
        sql = (f"SELECT location, COALESCE(SUM(soh_unit),0), COALESCE(SUM(soh_ltr),0), COUNT(DISTINCT sku_code) "
               f"FROM {table}{wsql} AND location IS NOT NULL AND location::text <> '' "
               f"GROUP BY location ORDER BY 2 DESC LIMIT {int(q.top_n or 15)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 15)
        top = "; ".join(f"{r[0]} ({_fmt(r[1])} u)" for r in rows[:5])
        return DataResult(summary=f"{scope} stock by city (as of {latest}) — top: {top}.",
                          columns=["City/Location", "SOH units", "SOH ltr", "SKUs"],
                          rows=[list(r) for r in rows], source=table,
                          meta=[("as_of", str(latest))], excel_title=f"{scope} Stock by City")

    # Low-stock threshold
    thr = _threshold_after(text)
    if thr is not None and any(w in text for w in ("soh", "stock", "unit", "inventory")):
        sql = (f"SELECT format, sku_code, item, soh_unit, soh_ltr, location FROM {table}{wsql} "
               f"AND soh_unit < %s ORDER BY soh_unit ASC LIMIT {int(safe_sql.default_max_rows())}")
        _c, rows, _t = safe_sql.run_select(sql, params + [thr], max_rows=safe_sql.default_max_rows())
        return DataResult(summary=f"{scope}: {_fmt(len(rows))} SKU(s) with SOH units below {thr} (as of {latest}).",
                          columns=["format", "sku_code", "item", "soh_unit", "soh_ltr", "location"],
                          rows=[list(r) for r in rows], source=table,
                          meta=[("as_of", str(latest)), ("threshold", thr)],
                          excel_title=f"{scope} Low Stock")

    # Default: totals + top products
    _c, trows, _t = safe_sql.run_select(
        f"SELECT COALESCE(SUM(soh_unit),0), COALESCE(SUM(soh_ltr),0), COUNT(DISTINCT sku_code) FROM {table}{wsql}",
        params, max_rows=1)
    tot_u, tot_l, skus = trows[0]
    _c, prows, _t = safe_sql.run_select(
        f"SELECT item, format, COALESCE(SUM(soh_ltr),0), COALESCE(SUM(soh_unit),0) FROM {table}{wsql} "
        f"AND item IS NOT NULL GROUP BY item, format ORDER BY 3 DESC LIMIT {int(q.top_n or 10)}",
        params, max_rows=q.top_n or 10)
    top = "; ".join(f"{r[0]} ({_fmt(r[2])} L)" for r in prows[:5])
    summary = (f"{scope} inventory (as of {latest}): {_fmt(tot_u)} SOH units, {_fmt(tot_l)} SOH liters "
               f"across {_fmt(skus)} SKU(s). Top products: {top}.")
    return DataResult(summary=summary, columns=["item", "format", "soh_ltr", "soh_unit"],
                      rows=[list(r) for r in prows], source=table,
                      meta=[("as_of", str(latest)), ("soh_units", tot_u), ("soh_ltr", tot_l)],
                      excel_title=f"{scope} Inventory")


# --- Secondary sell-out sources ---------------------------------------------
# Quick-commerce secondary sales live in the public."SecMaster" view (one row
# per SKU/day, ltr_sold / quantity / sales_amt keyed by `format` + `date`).
# Amazon secondary lives in amazon_sec_range_master_view (shipped_litres /
# shipped_units / returns, keyed by to_date; no `format`, no state/city).

@dataclass
class _SecSource:
    table: str
    date_col: str
    ltr: str
    units: str
    value: str
    format_col: str | None
    return_value: str | None
    return_units: str | None
    dim_cols: dict


_SECMASTER = _SecSource(
    table='"SecMaster"', date_col="date", ltr="ltr_sold", units="quantity",
    value="sales_amt", format_col="format", return_value=None, return_units=None,
    dim_cols={"state": "state", "city": "city", "sku": "sku_code", "brand": "brand",
              "category": "category", "item": "item", "platform": "format"},
)

_AMAZON_SEC = _SecSource(
    table="amazon_sec_range_master_view", date_col="to_date", ltr="shipped_litres",
    units="shipped_units", value="calculated_shipped_revenue", format_col=None,
    return_value="return_value", return_units="return_units",
    dim_cols={"sku": "asin", "asin": "asin", "brand": "brand",
              "category": "category", "item": "item"},
)


def _resolve_sec_source(q: ParsedQuery) -> tuple[_SecSource, str, str | None]:
    plat = q.primary_platform
    if plat and plat["slug"] == "amazon":
        return _AMAZON_SEC, "Amazon", None
    if plat:
        return _SECMASTER, plat["name"], _platform_format_value(plat["slug"])
    return _SECMASTER, "all platforms", None


def _sec_metric(source: _SecSource, text: str, q: ParsedQuery) -> tuple[str, str]:
    if any(w in text for w in ("amount", "value", "revenue", "sales value", "worth", "₹")):
        return source.value, "value"
    if q.metric == "units" or any(w in text for w in ("unit", "qty", "quantity")):
        return source.units, "units"
    return source.ltr, "liters"


def _rank_words(text: str) -> bool:
    return bool(re.search(r"\b(top|best|highest|most|leading|largest|rank|ranking)\b", text))


def secondary_sales(q: ParsedQuery) -> DataResult:
    """Secondary sell-out (SecMaster / Amazon secondary view): shipped liters,
    units and value with per-litre; supports returns, a top-N ranking submode,
    and platform + date filters."""
    source, scope, fmt_val = _resolve_sec_source(q)
    text = q.text.lower()

    where, params = [], []
    if source.format_col and fmt_val:
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
    if q.date_from and q.date_to:
        where.append(f"{source.date_col} BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    span = f" ({q.date_label})" if q.date_label else ""

    # Ranking submode: "top 10 skus by ltr sold on blinkit"
    if q.dimension in source.dim_cols and _rank_words(text):
        dim_col = source.dim_cols[q.dimension]
        metric_col, metric_label = _sec_metric(source, text, q)
        w2 = list(where) + [f"{dim_col} IS NOT NULL", f"{dim_col}::text <> ''"]
        wsql2 = " WHERE " + " AND ".join(w2)
        limit = q.top_n or 10
        sql = (f"SELECT {dim_col}, COALESCE(SUM({metric_col}),0) v FROM {source.table}{wsql2} "
               f"GROUP BY {dim_col} ORDER BY v DESC LIMIT {int(limit)}")
        try:
            _c, rows, _t = safe_sql.run_select(sql, params, max_rows=limit)
        except Exception as exc:
            return DataResult(summary=f"I couldn't rank secondary {q.dimension}: {exc}", ok=False, source=source.table)
        lines = [f"{i+1}. {r[0]} — {_fmt(r[1])}" for i, r in enumerate(rows)]
        return DataResult(
            summary=f"Top {len(rows)} {q.dimension}(s) by secondary {metric_label} for {scope}{span}:\n" + "\n".join(lines),
            columns=[q.dimension.title(), f"secondary {metric_label}"], rows=[[r[0], r[1]] for r in rows],
            source=source.table, excel_title=f"Top {q.dimension.title()} (Secondary)")

    # Returns view (Amazon only)
    if "return" in text and source.return_value:
        sql = (f"SELECT COALESCE(SUM({source.return_value}),0), COALESCE(SUM({source.return_units}),0), "
               f"COALESCE(SUM({source.ltr}),0), COALESCE(SUM({source.units}),0) FROM {source.table}{where_sql}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
        rv, ru, sl, su = rows[0]
        return DataResult(
            summary=(f"{scope} secondary returns{span}: ₹{_fmt(rv)} return value, {_fmt(ru)} return units "
                     f"(against {_fmt(sl)} L / {_fmt(su)} units shipped). Source: {source.table}."),
            columns=["metric", "value"],
            rows=[["Return value", rv], ["Return units", ru], ["Shipped liters", sl], ["Shipped units", su]],
            source=source.table, excel_title=f"{scope} Secondary Returns")

    # Default summary
    sql = (f"SELECT COALESCE(SUM({source.ltr}),0), COALESCE(SUM({source.units}),0), "
           f"COALESCE(SUM({source.value}),0), COUNT(*) FROM {source.table}{where_sql}")
    try:
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    except Exception as exc:
        return DataResult(summary=f"I couldn't total secondary sales: {exc}", ok=False, source=source.table)
    ltr, units, value, n = rows[0]
    per_l = (float(value) / float(ltr)) if float(ltr or 0) else 0.0
    summary = (f"{scope} secondary sell-out{span}: {_fmt(ltr)} liters sold, {_fmt(units)} units, "
               f"₹{_fmt(value)} value (₹{per_l:,.1f}/L) across {_fmt(n)} row(s). Source: {source.table}.")
    return DataResult(
        summary=summary, columns=["metric", "value"],
        rows=[["Ltr sold", ltr], ["Units sold", units], ["Value", value], ["Per liter", round(per_l, 2)]],
        source=source.table, meta=[("scope", scope), ("range", q.date_label or "all")],
        excel_title=f"{scope} Secondary")


def drr(q: ParsedQuery) -> DataResult:
    """Daily run rate from secondary sell-out: drr_ltr / drr_qty / drr_value =
    period totals ÷ number of days. 'day wise' gives a per-day OPS/LTR grid."""
    source, scope, fmt_val = _resolve_sec_source(q)
    text = q.text.lower()
    today = timezone.localdate()
    if q.date_from and q.date_to:
        dfrom, dto = q.date_from, q.date_to
    else:
        dfrom, dto = today.replace(day=1), today
    ndays = max((dto - dfrom).days + 1, 1)
    span = q.date_label or f"{dfrom} to {dto}"

    where, params = [], []
    if source.format_col and fmt_val:
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
    where.append(f"{source.date_col} BETWEEN %s AND %s")
    params.extend([dfrom, dto])
    where_sql = " WHERE " + " AND ".join(where)

    if any(p in text for p in ("day wise", "daywise", "day-wise", "per day", "daily", "each day")):
        sql = (f"SELECT {source.date_col} d, COALESCE(SUM({source.units}),0) ops, COALESCE(SUM({source.ltr}),0) ltr "
               f"FROM {source.table}{where_sql} GROUP BY d ORDER BY d")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=400)
        body = "\n".join(f"{r[0]}: {_fmt(r[1])} ops, {_fmt(r[2])} L" for r in rows[:12])
        more = f"\n…and {len(rows)-12} more day(s) — see table." if len(rows) > 12 else ""
        return DataResult(
            summary=f"{scope} day-wise OPS & liters ({span}):\n{body}{more}\nSource: {source.table}.",
            columns=["Day", "OPS", "LTR"], rows=[[r[0], r[1], r[2]] for r in rows],
            source=source.table, excel_title=f"{scope} Daily OPS-LTR")

    sql = (f"SELECT COALESCE(SUM({source.ltr}),0), COALESCE(SUM({source.units}),0), "
           f"COALESCE(SUM({source.value}),0) FROM {source.table}{where_sql}")
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    ltr, units, value = rows[0]
    drr_ltr = float(ltr) / ndays
    drr_qty = float(units) / ndays
    drr_val = float(value) / ndays
    summary = (f"{scope} DRR ({span}, {ndays} days): {_fmt(drr_ltr)} L/day, {_fmt(drr_qty)} units/day, "
               f"₹{_fmt(drr_val)}/day. Period totals: {_fmt(ltr)} L, {_fmt(units)} units. Source: {source.table}.")
    return DataResult(
        summary=summary, columns=["metric", "value"],
        rows=[["DRR ltr/day", round(drr_ltr, 2)], ["DRR qty/day", round(drr_qty, 2)],
              ["DRR value/day", round(drr_val, 2)], ["Total ltr", ltr], ["Total units", units], ["Days", ndays]],
        source=source.table, meta=[("scope", scope), ("days", ndays)], excel_title=f"{scope} DRR")


def _item_head_from_text(text: str) -> str | None:
    if "premium" in text:
        return "PREMIUM"
    if "commodity" in text:
        return "COMMODITY"
    return None


def targets(q: ParsedQuery) -> DataResult:
    """Monthly targets vs done liters and achieved %. Secondary (month_targets)
    by default, primary_month_targets for 'primary target' (adds DRR / require
    DRR / pending), call_center_targets for 'call center'."""
    text = q.text.lower()
    is_primary = "primary" in text
    is_cc = "call center" in text or "call-center" in text or "callcenter" in text
    table = "call_center_targets" if is_cc else ("primary_month_targets" if is_primary else "month_targets")

    if q.date_from:
        yr, mo = q.date_from.year, q.date_from.month
    else:
        _c, r, _t = safe_sql.run_select(
            f"SELECT year, month FROM {table} ORDER BY year DESC, month DESC LIMIT 1", [], max_rows=1)
        if not r:
            return DataResult(summary="No target data available.", ok=False, source=table)
        yr, mo = r[0]

    where, params = ["year=%s", "month=%s"], [yr, mo]
    scope = "all platforms"
    if not is_cc and q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
        scope = q.primary_platform["name"]
    ih = _item_head_from_text(text)
    if ih:
        where.append("UPPER(item_head)=%s")
        params.append(ih)
    wsql = " WHERE " + " AND ".join(where)
    when = f"{yr}-{mo:02d}"
    head_txt = f" {ih.lower()}" if ih else ""

    # Per-platform "who is behind" mode
    per_platform = (not q.primary_platform and not is_cc
                    and any(w in text for w in ("behind", "which platform", "each platform",
                                                "all platform", "by platform", "overall")))
    if per_platform and "overall" not in text:
        sql = (f"SELECT format, COALESCE(SUM(targets),0) t, COALESCE(SUM(done_ltrs),0) d "
               f"FROM {table}{wsql} GROUP BY format ORDER BY 2 DESC")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=50)
        data, lines = [], []
        for fmt, t, d in rows:
            pct = round(float(d) / float(t) * 100, 1) if float(t or 0) else 0.0
            data.append([fmt, t, d, pct])
            lines.append(f"{fmt}: {pct}% ({_fmt(d)}/{_fmt(t)} L)")
        behind = [r for r in data if r[3] < 100]
        summary = (f"{when}{head_txt} target achievement by platform ({len(behind)} behind 100%):\n"
                   + "\n".join(lines[:12]) + f"\nSource: {table}.")
        return DataResult(summary=summary, columns=["Platform", "Target", "Done ltrs", "Achieved %"],
                          rows=data, source=table, excel_title="Targets by Platform")

    if is_cc:
        sql = f"SELECT COALESCE(SUM(targets),0), COALESCE(SUM(done_ltrs),0) FROM {table}{wsql}"
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
        t, d = rows[0]
        pct = round(float(d) / float(t) * 100, 1) if float(t or 0) else 0.0
        return DataResult(summary=f"Call-center targets ({when}{head_txt}): {_fmt(d)} / {_fmt(t)} L done ({pct}% achieved). Source: {table}.",
                          columns=["metric", "value"], rows=[["Target", t], ["Done ltrs", d], ["Achieved %", pct]],
                          source=table, excel_title="Call Center Targets")

    if is_primary:
        sql = (f"SELECT COALESCE(SUM(targets),0), COALESCE(SUM(done_ltrs),0), COALESCE(SUM(est_ltr),0), "
               f"COALESCE(AVG(drr),0), COALESCE(AVG(require_drr),0), COALESCE(SUM(pending_ltr),0) FROM {table}{wsql}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
        t, d, est, drr_v, req_drr, pend = rows[0]
        pct = round(float(d) / float(t) * 100, 1) if float(t or 0) else 0.0
        summary = (f"{scope} primary target ({when}{head_txt}): {_fmt(d)} / {_fmt(t)} L done ({pct}% achieved), "
                   f"est {_fmt(est)} L. DRR {_fmt(drr_v)} vs required {_fmt(req_drr)} L/day, pending {_fmt(pend)} L. Source: {table}.")
        rows_out = [["Target", t], ["Done ltrs", d], ["Achieved %", pct], ["Est ltr", est],
                    ["DRR", drr_v], ["Require DRR", req_drr], ["Pending ltr", pend]]
        return DataResult(summary=summary, columns=["metric", "value"], rows=rows_out, source=table,
                          excel_title=f"{scope} Primary Target")

    sql = (f"SELECT COALESCE(SUM(targets),0), COALESCE(SUM(done_ltrs),0), COALESCE(SUM(done_value),0), "
           f"COALESCE(SUM(est_ltr),0), COALESCE(AVG(growth_pct),0) FROM {table}{wsql}")
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    t, d, dv, est, growth = rows[0]
    pct = round(float(d) / float(t) * 100, 1) if float(t or 0) else 0.0
    summary = (f"{scope} secondary target ({when}{head_txt}): {_fmt(d)} / {_fmt(t)} L done ({pct}% achieved), "
               f"est {_fmt(est)} L, growth {_fmt(growth)}% vs last month. Source: {table}.")
    rows_out = [["Target", t], ["Done ltrs", d], ["Achieved %", pct], ["Done value", dv],
                ["Est ltr", est], ["Growth %", growth]]
    return DataResult(summary=summary, columns=["metric", "value"], rows=rows_out, source=table,
                      excel_title=f"{scope} Target")


def landing_rate(q: ParsedQuery) -> DataResult:
    """Monthly landing / basic rate per SKU (monthly_landing_rate). Filter by
    platform, month, a SKU code, or list SKUs with no rate set."""
    table = "monthly_landing_rate"
    text = q.text.lower()
    where, params = [], []
    scope = "all platforms"
    if q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
        scope = q.primary_platform["name"]
    if q.date_from:
        where.append("month = %s")
        params.append(f"{q.date_from.year}-{q.date_from.month:02d}-01")
    else:
        _c, r, _t = safe_sql.run_select(f"SELECT month FROM {table} ORDER BY month DESC LIMIT 1", [], max_rows=1)
        if r:
            where.append("month = %s")
            params.append(r[0][0])

    no_rate = any(p in text for p in ("no landing rate", "without landing rate", "missing landing rate",
                                      "no rate", "not set", "unset"))
    code = re.search(r"\b(\d{6,})\b", text)
    if code:
        where.append("sku_code = %s")
        params.append(code.group(1))
    wsql = (" WHERE " + " AND ".join(where)) if where else ""

    if no_rate:
        extra = " AND (landing_rate IS NULL OR landing_rate = 0)"
        sql = f"SELECT sku_code, sku_name, landing_rate, basic_rate, format FROM {table}{wsql}{extra} ORDER BY sku_name LIMIT 500"
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=500)
        return DataResult(summary=f"{scope}: {_fmt(len(rows))} SKU(s) with no landing rate set.",
                          columns=["sku_code", "sku_name", "landing_rate", "basic_rate", "format"],
                          rows=[list(r) for r in rows], source=table, excel_title=f"{scope} Missing Landing Rate")

    sql = f"SELECT sku_code, sku_name, landing_rate, basic_rate, format FROM {table}{wsql} ORDER BY sku_name LIMIT 500"
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=500)
    if not rows:
        return DataResult(summary=f"No landing-rate rows found for {scope}.", ok=False, source=table)
    if len(rows) <= 3:
        lines = "; ".join(f"{r[1] or r[0]}: landing ₹{_fmt(r[2])}, basic ₹{_fmt(r[3])}" for r in rows)
        summary = f"{scope} landing rate — {lines}. Source: {table}."
    else:
        summary = f"{scope}: {_fmt(len(rows))} SKU landing rates. Source: {table}."
    return DataResult(summary=summary, columns=["sku_code", "sku_name", "landing_rate", "basic_rate", "format"],
                      rows=[list(r) for r in rows], source=table, excel_title=f"{scope} Landing Rate")


def pendency(q: ParsedQuery) -> DataResult:
    """Open-PO pendency from master_po (po_status PENDING / APPOINTMENT DONE):
    pending liters/units/value, optionally grouped by city / warehouse / vendor."""
    table = "master_po"
    text = q.text.lower()
    where = ["po_status IN ('PENDING', 'APPOINTMENT DONE')"]
    params: list = []
    scope = "all platforms"
    if q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
        scope = q.primary_platform["name"]
    if q.date_from and q.date_to:
        where.append("po_date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    older = re.search(r"older than\s*(\d+)\s*day|(\d+)\s*days? old", text)
    if older or "older than 30" in text:
        days = int(next(g for g in (older.groups() if older else ["30"]) if g)) if older else 30
        where.append("po_date < %s")
        params.append(timezone.localdate() - timedelta(days=days))
    wsql = " WHERE " + " AND ".join(where)

    pl = "COALESCE(SUM(total_order_liters - COALESCE(total_delivered_liters,0)),0)"
    pu = "COALESCE(SUM(order_qty - COALESCE(delivered_qty,0)),0)"
    pv = "COALESCE(SUM(total_order_amt_exclusive),0)"

    group = None
    if "by city" in text or ("city" in text and "warehouse" not in text):
        group = ("city", "City")
    elif "warehouse" in text or "location" in text:
        group = ("location", "Warehouse")
    elif "vendor" in text or "distributor" in text:
        group = ("vendor_new", "Vendor")

    if group:
        col, label = group
        order_expr = "pv" if "value" in text else "pl"
        sql = (f"SELECT {col}, {pl} pl, {pu} pu, {pv} pv, COUNT(*) FROM {table}{wsql} "
               f"AND {col} IS NOT NULL AND {col}::text <> '' GROUP BY {col} ORDER BY {order_expr} DESC LIMIT {int(q.top_n or 15)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 15)
        top = "; ".join(f"{r[0]} ({_fmt(r[1])} L)" for r in rows[:5])
        return DataResult(summary=f"{scope} pendency by {label.lower()} — top: {top}. Source: master_po.",
                          columns=[label, "Pending ltr", "Pending units", "Open value", "Open POs"],
                          rows=[list(r) for r in rows], source=table, excel_title=f"{scope} Pendency by {label}")

    sql = f"SELECT {pl}, {pu}, {pv}, COUNT(*) FROM {table}{wsql}"
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    pltr, punit, pval, n = rows[0]
    summary = (f"{scope} pendency: {_fmt(pltr)} pending liters, {_fmt(punit)} pending units, "
               f"₹{_fmt(pval)} open order value across {_fmt(n)} open PO line(s). Source: master_po.")
    return DataResult(summary=summary, columns=["metric", "value"],
                      rows=[["Pending liters", pltr], ["Pending units", punit], ["Open value", pval], ["Open POs", n]],
                      source=table, meta=[("scope", scope)], excel_title=f"{scope} Pendency")


# --- Marketing: ads / brand fund / coupons ----------------------------------
_ADS_SPEND = {
    "blinkit": ("blinkit_ads_master", "ad_spent"),
    "zepto": ("zepto_ads_master", "ad_spent"),
    "swiggy": ("swiggy_ads_master", "ad_spent"),
    "bigbasket": ("bigbasket_ads_master", "ad_spent"),
    "amazon": ("amazon_ads_master", "total_cost"),
    "flipkart": ("flipkart_ads_master", "ad_spend"),
}
_BRANDFUND = {
    "blinkit": "blinkit_brandfund_master",
    "swiggy": "swiggy_brandfund_master",
    "zepto": "zepto_brandfund_master",
}


def _ads_cols(cols: list[dict]) -> dict:
    names = [c["name"] for c in cols]

    def pick(*cands):
        for c in cands:
            if c in names:
                return c
        return None

    return {
        "spend": pick("ad_spent", "ad_spend", "total_cost", "spend"),
        "sales": pick("direct_gmv", "gmv", "sales", "total_revenue", "revenue"),
        "impressions": pick("impressions", "views"),
        "clicks": pick("clicks"),
        "qty": pick("direct_qty_sold", "units_sold", "total_converted_units", "purchases"),
        "ltr": pick("ads_ltr_sold"),
        "roas": pick("roas", "roi"),
        "ntb_sales": pick("sales_ntb"),
        "ntb_orders": pick("purchases_ntb"),
        "dpv": pick("detail_page_views"),
        "ctr": pick("ctr", "click_through_rate"),
        "item": pick("item", "campaign_name", "portfolio_name", "sku_name"),
        "indirect": pick("indirect_gmv"),
    }


def ads(q: ParsedQuery) -> DataResult:
    """Ad performance per platform: spend, sales/GMV, ROAS, ACOS, impressions
    (Amazon adds clicks/CTR/CPC/NTB/DPV). Cross-platform 'which platform spent
    most' and item/campaign/portfolio rankings supported."""
    text = q.text.lower()
    slugs = [p["slug"] for p in q.platforms if p["slug"] in _ADS_SPEND]
    cross = (len(slugs) >= 2) or (not slugs and any(
        w in text for w in ("which platform", "highest ad", "by platform", "each platform", "compare")))

    if cross:
        targets_ = slugs or list(_ADS_SPEND)
        out = []
        for slug in targets_:
            tbl, spend = _ADS_SPEND[slug]
            w, p = "", []
            if q.date_from and q.date_to:
                w, p = " WHERE date BETWEEN %s AND %s", [q.date_from, q.date_to]
            try:
                _c, r, _t = safe_sql.run_select(f"SELECT COALESCE(SUM({spend}),0) FROM {tbl}{w}", p, max_rows=1)
                out.append((slug.title(), float(r[0][0]) if r else 0.0))
            except Exception:
                out.append((slug.title(), 0.0))
        out.sort(key=lambda x: -x[1])
        span = f" ({q.date_label})" if q.date_label else ""
        lines = [f"{i+1}. {n} — ₹{_fmt(v)}" for i, (n, v) in enumerate(out)]
        return DataResult(summary=f"Ad spend by platform{span}:\n" + "\n".join(lines),
                          columns=["Platform", "Ad spend"], rows=[[n, v] for n, v in out],
                          source="ads_master", excel_title="Ad Spend by Platform")

    if not q.primary_platform or q.primary_platform["slug"] not in _ADS_SPEND:
        return DataResult(summary="Which platform's ads? e.g. 'blinkit ad spent in june' or 'amazon acos'.",
                          ok=False, suggestions=["blinkit ad spent june", "amazon roas and acos june"])
    slug = q.primary_platform["slug"]
    scope = q.primary_platform["name"]
    table = _ADS_SPEND[slug][0]
    C = _ads_cols(safe_sql.table_columns(table))

    where, params = [], []
    if q.date_from and q.date_to:
        where.append("date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    span = f" ({q.date_label})" if q.date_label else ""

    spend_e = f"COALESCE(SUM({C['spend']}),0)" if C["spend"] else "0"
    sales_e = f"COALESCE(SUM({C['sales']}),0)" if C["sales"] else "0"

    # Ranking submode
    rank = (_rank_words(text) or "item wise" in text or "campaign wise" in text
            or "portfolio wise" in text or "campaign" in text)
    if rank:
        dim_col = C["item"]
        if slug == "amazon":
            if "portfolio" in text:
                dim_col = "portfolio_name"
            elif "campaign" in text:
                dim_col = "campaign_name"
            elif "asin" in text:
                dim_col = "advertised_product_id"
        if "impression" in text and C["impressions"]:
            order_e, mlabel = f"COALESCE(SUM({C['impressions']}),0)", "impressions"
        elif ("sales" in text or "gmv" in text or "revenue" in text) and C["sales"]:
            order_e, mlabel = sales_e, "sales"
        elif "acos" in text:
            order_e, mlabel = f"({spend_e}/NULLIF({sales_e},0)*100)", "acos %"
        elif "roas" in text:
            order_e, mlabel = f"({sales_e}/NULLIF({spend_e},0))", "roas"
        else:
            order_e, mlabel = spend_e, "ad spend"
        sql = (f"SELECT {dim_col}, {spend_e} sp, {sales_e} sa, {order_e} m FROM {table}{wsql} "
               f"{'AND' if wsql else 'WHERE'} {dim_col} IS NOT NULL AND {dim_col}::text <> '' "
               f"GROUP BY {dim_col} ORDER BY m DESC LIMIT {int(q.top_n or 10)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 10)
        lines = [f"{i+1}. {r[0]} — {mlabel} {_fmt(r[3])} (spend ₹{_fmt(r[1])})" for i, r in enumerate(rows)]
        return DataResult(summary=f"{scope} ads top by {mlabel}{span}:\n" + "\n".join(lines),
                          columns=[dim_col, "spend", "sales", mlabel], rows=[list(r) for r in rows],
                          source=table, excel_title=f"{scope} Ads Ranking")

    # Summary
    extra_sel, extra_labels = [], []
    for keyname, lbl in [("impressions", "impressions"), ("clicks", "clicks"),
                         ("qty", "units"), ("ltr", "ads ltr"), ("ntb_sales", "ntb sales"),
                         ("ntb_orders", "ntb orders"), ("dpv", "detail page views")]:
        if C.get(keyname):
            extra_sel.append(f"COALESCE(SUM({C[keyname]}),0)")
            extra_labels.append((keyname, lbl))
    sel = [spend_e, sales_e] + extra_sel
    sql = f"SELECT {', '.join(sel)} FROM {table}{wsql}"
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    vals = list(rows[0])
    spend, sales = float(vals[0] or 0), float(vals[1] or 0)
    roas = sales / spend if spend else 0.0
    acos = spend / sales * 100 if sales else 0.0
    parts = [f"₹{_fmt(spend)} spend", f"₹{_fmt(sales)} sales", f"ROAS {roas:.2f}", f"ACOS {acos:.1f}%"]
    data_rows = [["Ad spend", spend], ["Sales/GMV", sales], ["ROAS", round(roas, 2)], ["ACOS %", round(acos, 1)]]
    for (keyname, lbl), v in zip(extra_labels, vals[2:]):
        parts.append(f"{_fmt(v)} {lbl}")
        data_rows.append([lbl, v])
    summary = f"{scope} ads{span}: " + ", ".join(parts) + f". Source: {table}."
    return DataResult(summary=summary, columns=["metric", "value"], rows=data_rows,
                      source=table, excel_title=f"{scope} Ads")


def brand_fund(q: ParsedQuery) -> DataResult:
    """Brand-fund spend (blinkit/swiggy/zepto): total, by item / sub-category, or
    a day-wise trend."""
    text = q.text.lower()
    if not q.primary_platform or q.primary_platform["slug"] not in _BRANDFUND:
        return DataResult(summary="Brand fund is tracked for Blinkit, Swiggy and Zepto. Try 'blinkit brand fund in june'.",
                          ok=False, suggestions=["blinkit brand fund june", "top items by brand fund on zepto"])
    slug = q.primary_platform["slug"]
    scope = q.primary_platform["name"]
    table = _BRANDFUND[slug]
    where, params = [], []
    if q.date_from and q.date_to:
        where.append("date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    span = f" ({q.date_label})" if q.date_label else ""

    if any(p in text for p in ("day wise", "daywise", "trend", "daily")):
        sql = f"SELECT date, COALESCE(SUM(brand_fund_spent),0) FROM {table}{wsql} GROUP BY date ORDER BY date"
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=400)
        body = "; ".join(f"{r[0]}: ₹{_fmt(r[1])}" for r in rows[:10])
        return DataResult(summary=f"{scope} brand fund day-wise{span}: {body}. Source: {table}.",
                          columns=["date", "brand_fund_spent"], rows=[list(r) for r in rows],
                          source=table, excel_title=f"{scope} Brand Fund Trend")

    if any(p in text for p in ("sub category", "sub-category", "subcategory", "item wise", "top")):
        dim = "sub_category" if "sub" in text else "item"
        sql = (f"SELECT {dim}, COALESCE(SUM(brand_fund_spent),0) v FROM {table}{wsql} "
               f"{'AND' if wsql else 'WHERE'} {dim} IS NOT NULL AND {dim}::text <> '' "
               f"GROUP BY {dim} ORDER BY v DESC LIMIT {int(q.top_n or 10)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 10)
        lines = [f"{i+1}. {r[0]} — ₹{_fmt(r[1])}" for i, r in enumerate(rows)]
        return DataResult(summary=f"{scope} brand fund by {dim}{span}:\n" + "\n".join(lines),
                          columns=[dim, "brand_fund_spent"], rows=[list(r) for r in rows],
                          source=table, excel_title=f"{scope} Brand Fund")

    sql = f"SELECT COALESCE(SUM(brand_fund_spent),0), COUNT(*) FROM {table}{wsql}"
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    total, n = rows[0]
    return DataResult(summary=f"{scope} brand fund{span}: ₹{_fmt(total)} spent across {_fmt(n)} row(s). Source: {table}.",
                      columns=["metric", "value"], rows=[["Brand fund spent", total], ["Rows", n]],
                      source=table, excel_title=f"{scope} Brand Fund")


def coupon(q: ParsedQuery) -> DataResult:
    """Amazon coupon performance: clips, redemptions, budget spent/remaining,
    premium-vs-commodity split, or the highest budget-used coupons."""
    table = "amazon_coupon_master"
    text = q.text.lower()
    where, params = [], []
    if q.date_from and q.date_to:
        where.append("date BETWEEN %s AND %s")
        span = f" ({q.date_label})"
    else:
        _c, r, _t = safe_sql.run_select(f"SELECT MAX(date) FROM {table}", [], max_rows=1)
        latest = r[0][0] if r else None
        where.append("date = %s")
        params.append(latest)
        span = f" (as of {latest})"
    if q.date_from and q.date_to:
        params.extend([q.date_from, q.date_to])
    wsql = " WHERE " + " AND ".join(where)

    if ("premium" in text or "commodity" in text or "split" in text) and "coupon" in text:
        sql = (f"SELECT UPPER(COALESCE(item_head,'OTHER')), COALESCE(SUM(budget_spent),0), COUNT(*) "
               f"FROM {table}{wsql} GROUP BY UPPER(COALESCE(item_head,'OTHER')) ORDER BY 2 DESC")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=10)
        lines = "; ".join(f"{r[0]}: ₹{_fmt(r[1])} ({r[2]} coupons)" for r in rows)
        return DataResult(summary=f"Amazon coupon budget by item head{span}: {lines}. Source: {table}.",
                          columns=["item_head", "budget_spent", "coupons"], rows=[list(r) for r in rows],
                          source=table, excel_title="Coupon Split")

    if "highest" in text or "budget used" in text or "top" in text:
        sql = (f"SELECT coupon_name, item_head, budget_spent, budget_remaining, budget_used, total_budget "
               f"FROM {table}{wsql} ORDER BY budget_used DESC NULLS LAST LIMIT {int(q.top_n or 10)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 10)
        lines = [f"{i+1}. {r[0]} — {_fmt(r[4])}% used (₹{_fmt(r[2])} spent)" for i, r in enumerate(rows)]
        return DataResult(summary=f"Amazon coupons by budget used{span}:\n" + "\n".join(lines),
                          columns=["coupon_name", "item_head", "budget_spent", "budget_remaining", "budget_used", "total_budget"],
                          rows=[list(r) for r in rows], source=table, excel_title="Coupons by Budget Used")

    sql = (f"SELECT COALESCE(SUM(clips),0), COALESCE(SUM(redemptions),0), COALESCE(SUM(budget_spent),0), "
           f"COALESCE(SUM(budget_remaining),0), COALESCE(SUM(total_budget),0), COUNT(*) FROM {table}{wsql}")
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    clips, red, spent, remain, total, n = rows[0]
    summary = (f"Amazon coupons{span}: {_fmt(clips)} clips, {_fmt(red)} redemptions, ₹{_fmt(spent)} budget spent, "
               f"₹{_fmt(remain)} remaining of ₹{_fmt(total)} across {_fmt(n)} coupon(s). Source: {table}.")
    return DataResult(summary=summary, columns=["metric", "value"],
                      rows=[["Clips", clips], ["Redemptions", red], ["Budget spent", spent],
                            ["Budget remaining", remain], ["Total budget", total], ["Coupons", n]],
                      source=table, excel_title="Amazon Coupons")


def expiry(q: ParsedQuery) -> DataResult:
    """POs expiring soon (days_to_expiry 1..N). Works on master_po (open POs) or
    reporting.\"Amazon PO\" for Amazon; N defaults to 5 (7 if 'week')."""
    source, scope, fmt_val = _resolve_po_source(q)
    text = q.text.lower()
    m = re.search(r"(?:next|within|in)\s+(\d+)\s*day", text)
    days = int(m.group(1)) if m else (7 if "week" in text else 5)

    where = ["days_to_expiry BETWEEN 1 AND %s"]
    params: list = [days]
    if source.format_col and fmt_val:
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
    where.append("po_status IN ('PENDING', 'APPOINTMENT DONE')" if source.format_col
                 else "po_status = 'PENDING'")
    wsql = " WHERE " + " AND ".join(where)

    try:
        _c, rows, _t = safe_sql.run_select(
            f"SELECT COUNT(*), COALESCE(SUM({source.order_ltrs}),0), COALESCE(SUM({source.amount}),0) "
            f"FROM {source.table}{wsql}", params, max_rows=1)
    except Exception as exc:
        return DataResult(summary=f"I couldn't read expiry data: {exc}", ok=False, source=source.label)
    n, ltr, val = rows[0]
    _c, listrows, _t = safe_sql.run_select(
        f"SELECT po_number, days_to_expiry, {source.order_ltrs}, {source.amount} "
        f"FROM {source.table}{wsql} ORDER BY days_to_expiry ASC LIMIT 100", params, max_rows=100)
    summary = (f"{scope}: {_fmt(n)} PO(s) expiring within {days} day(s) — {_fmt(ltr)} order liters, "
               f"₹{_fmt(val)} order value. Source: {source.label}.")
    return DataResult(summary=summary, columns=["po_number", "days_to_expiry", "order_ltrs", "order_amt"],
                      rows=[list(r) for r in listrows], source=source.label,
                      meta=[("scope", scope), ("days", days)], excel_title=f"{scope} Expiring POs")


_AMZ_PO = 'reporting."Amazon PO"'


def amazon_po(q: ParsedQuery) -> DataResult:
    """Amazon PO questions on reporting.\"Amazon PO\": pending / MOV counts, fill
    rate by FC, PO list for an FC, requested-vs-received by sub-category, and new
    POs on a date."""
    text = q.text.lower()
    where, params = [], []
    if q.date_from and q.date_to:
        where.append("order_date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    fc = re.search(r"\bfc\s+([a-z0-9]{3,5})\b", text)
    if fc:
        where.append("UPPER(fulfillment_center) = %s")
        params.append(fc.group(1).upper())
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    span = f" ({q.date_label})" if q.date_label else ""

    # fill rate by FC
    if "fill rate" in text and ("fc" in text or "fulfillment" in text or "center" in text):
        sql = (f"SELECT fulfillment_center, COALESCE(SUM(requested_qty),0) req, COALESCE(SUM(received_qty),0) rec, "
               f"ROUND(COALESCE(SUM(received_qty),0)/NULLIF(SUM(requested_qty),0)*100,1) fr "
               f"FROM {_AMZ_PO}{wsql} {'AND' if wsql else 'WHERE'} fulfillment_center IS NOT NULL "
               f"GROUP BY fulfillment_center ORDER BY fr DESC LIMIT {int(q.top_n or 20)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 20)
        lines = [f"{r[0]}: {_fmt(r[3])}% ({_fmt(r[2])}/{_fmt(r[1])})" for r in rows[:10]]
        return DataResult(summary=f"Amazon fill rate by FC{span}:\n" + "\n".join(lines),
                          columns=["FC", "requested", "received", "fill %"], rows=[list(r) for r in rows],
                          source=_AMZ_PO, excel_title="Amazon Fill Rate by FC")

    # requested vs received by sub_category
    if "requested" in text and "received" in text:
        gb = "sub_category" if "sub" in text else ("category" if "category" in text else "item_head")
        sql = (f"SELECT {gb}, COALESCE(SUM(requested_qty),0), COALESCE(SUM(received_qty),0) FROM {_AMZ_PO}{wsql} "
               f"{'AND' if wsql else 'WHERE'} {gb} IS NOT NULL GROUP BY {gb} ORDER BY 2 DESC LIMIT {int(q.top_n or 20)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 20)
        lines = [f"{r[0]}: req {_fmt(r[1])} / rec {_fmt(r[2])}" for r in rows[:10]]
        return DataResult(summary=f"Amazon requested vs received by {gb}{span}:\n" + "\n".join(lines),
                          columns=[gb, "requested_qty", "received_qty"], rows=[list(r) for r in rows],
                          source=_AMZ_PO, excel_title="Amazon Req vs Rec")

    # new POs on a date
    if "new po" in text:
        sql = (f"SELECT COUNT(DISTINCT po_number), COALESCE(SUM(requested_qty),0), "
               f"COALESCE(SUM(total_order_liters),0), COALESCE(SUM(total_order_amt_exclusive),0) FROM {_AMZ_PO}{wsql}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
        npo, qty, ltr, val = rows[0]
        return DataResult(summary=(f"Amazon new POs{span}: {_fmt(npo)} PO(s), {_fmt(qty)} order units, "
                                   f"{_fmt(ltr)} order liters, ₹{_fmt(val)} order value. Source: {_AMZ_PO}."),
                          columns=["metric", "value"],
                          rows=[["POs", npo], ["Order units", qty], ["Order liters", ltr], ["Order value", val]],
                          source=_AMZ_PO, excel_title="Amazon New POs")

    # status counts (mov / pending / completed ...)
    status = None
    for s in ("mov", "pending", "completed", "cancelled", "expired"):
        if s in text:
            status = s.upper()
            break
    st_where = list(where)
    st_params = list(params)
    if status:
        st_where.append("po_status = %s")
        st_params.append(status)
    stw = (" WHERE " + " AND ".join(st_where)) if st_where else ""
    sql = (f"SELECT COUNT(DISTINCT po_number), COUNT(*), COALESCE(SUM(total_order_liters),0), "
           f"COALESCE(SUM(total_order_amt_exclusive),0) FROM {_AMZ_PO}{stw}")
    _c, rows, _t = safe_sql.run_select(sql, st_params, max_rows=1)
    npo, lines_n, ltr, val = rows[0]
    label = f"{status} " if status else ""
    fc_txt = f" at FC {fc.group(1).upper()}" if fc else ""
    summary = (f"Amazon {label}POs{fc_txt}{span}: {_fmt(npo)} PO(s) ({_fmt(lines_n)} line items), "
               f"{_fmt(ltr)} order liters, ₹{_fmt(val)} order value. Source: {_AMZ_PO}.")
    return DataResult(summary=summary, columns=["metric", "value"],
                      rows=[["POs", npo], ["Line items", lines_n], ["Order liters", ltr], ["Order value", val]],
                      source=_AMZ_PO, excel_title="Amazon POs")


def appointments(q: ParsedQuery) -> DataResult:
    """Amazon appointments (reporting.appointment) + Vendor-Central carton/unit
    commits (appointment_commit)."""
    text = q.text.lower()
    apid = re.search(r"\b(\d{10,})\b", text)
    if apid and any(w in text for w in ("carton", "unit", "commit", "vc", "vendor central")):
        sql = ("SELECT appointment_id, destination_fc, carton_count, unit_count, updated_by, updated_at "
               "FROM appointment_commit WHERE appointment_id::text = %s")
        _c, rows, _t = safe_sql.run_select(sql, [apid.group(1)], max_rows=5)
        if not rows:
            return DataResult(summary=f"No Vendor-Central carton/unit commit found for appointment {apid.group(1)}.",
                              ok=False, source="appointment_commit")
        r = rows[0]
        return DataResult(summary=(f"Appointment {r[0]} (FC {r[1]}): {_fmt(r[2])} cartons, {_fmt(r[3])} units "
                                   f"(updated by {r[4]} at {r[5]}). Source: appointment_commit."),
                          columns=["appointment_id", "destination_fc", "carton_count", "unit_count", "updated_by", "updated_at"],
                          rows=[list(r) for r in rows], source="appointment_commit", excel_title="Appointment Commit")

    table = "reporting.appointment"
    where, params = [], []
    if "today" in text:
        where.append("appointment_time::date = %s")
        params.append(timezone.localdate())
    elif q.date_from and q.date_to:
        where.append("appointment_time::date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    span = " today" if "today" in text else (f" ({q.date_label})" if q.date_label else "")

    if "destination fc" in text or "most appointment" in text or "which fc" in text or "by fc" in text:
        sql = (f"SELECT destination_fc, COUNT(*) FROM {table}{wsql} "
               f"{'AND' if wsql else 'WHERE'} destination_fc IS NOT NULL GROUP BY destination_fc ORDER BY 2 DESC LIMIT {int(q.top_n or 15)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 15)
        lines = [f"{r[0]}: {_fmt(r[1])}" for r in rows[:10]]
        return DataResult(summary=f"Appointments by destination FC{span}:\n" + "\n".join(lines),
                          columns=["destination_fc", "count"], rows=[list(r) for r in rows],
                          source=table, excel_title="Appointments by FC")

    sql = f"SELECT COALESCE(status,'(none)'), COUNT(*) FROM {table}{wsql} GROUP BY status ORDER BY 2 DESC"
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=20)
    by = {r[0]: r[1] for r in rows}
    total = sum(by.values())
    breakdown = ", ".join(f"{k}: {_fmt(v)}" for k, v in by.items()) or "none"
    return DataResult(summary=f"{_fmt(total)} appointment(s){span} — {breakdown}. Source: {table}.",
                      columns=["status", "count"], rows=[list(r) for r in rows],
                      source=table, excel_title="Appointments")


def amazon_mp(q: ParsedQuery) -> DataResult:
    """Amazon Marketplace (amazon_mp_master_view): delivered liters / quantity /
    invoice value, top ship-to states, or unmapped ASINs."""
    table = "amazon_mp_master_view"
    text = q.text.lower()
    where, params = [], []
    if q.date_from and q.date_to:
        # shipment_date is inconsistently formatted text; filter on the reliable
        # shipment_year (int) + shipment_month (month name) instead.
        months, y, mo = [], q.date_from.year, q.date_from.month
        while (y, mo) <= (q.date_to.year, q.date_to.month):
            months.append((y, calendar.month_name[mo].upper()))
            mo, y = (1, y + 1) if mo == 12 else (mo + 1, y)
        ors = " OR ".join(["(shipment_year = %s AND UPPER(shipment_month) = %s)"] * len(months))
        where.append(f"({ors})")
        for yy, nm in months:
            params.extend([yy, nm])
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    span = f" ({q.date_label})" if q.date_label else ""

    if "unmapped" in text:
        sql = (f"SELECT DISTINCT asin, item_description FROM {table}{wsql} "
               f"{'AND' if wsql else 'WHERE'} (item_head IS NULL OR item_head::text = '') LIMIT 500")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=500)
        return DataResult(summary=f"Amazon MP unmapped ASINs{span}: {_fmt(len(rows))} found. Source: {table}.",
                          columns=["asin", "item_description"], rows=[list(r) for r in rows],
                          source=table, excel_title="Amazon MP Unmapped ASINs")

    if "state" in text:
        sql = (f"SELECT ship_to_state, COALESCE(SUM(delivered_ltr),0), COALESCE(SUM(invoice_amount),0) FROM {table}{wsql} "
               f"{'AND' if wsql else 'WHERE'} ship_to_state IS NOT NULL GROUP BY ship_to_state ORDER BY 2 DESC LIMIT {int(q.top_n or 10)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 10)
        lines = [f"{i+1}. {r[0]} — {_fmt(r[1])} L" for i, r in enumerate(rows)]
        return DataResult(summary=f"Amazon MP top states by delivered liters{span}:\n" + "\n".join(lines),
                          columns=["ship_to_state", "delivered_ltr", "invoice_amount"], rows=[list(r) for r in rows],
                          source=table, excel_title="Amazon MP by State")

    sql = (f"SELECT COALESCE(SUM(delivered_ltr),0), COALESCE(SUM(quantity),0), COALESCE(SUM(invoice_amount),0), COUNT(*) "
           f"FROM {table}{wsql}")
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    ltr, qty, val, n = rows[0]
    return DataResult(summary=(f"Amazon MP{span}: {_fmt(ltr)} delivered liters, {_fmt(qty)} units, "
                               f"₹{_fmt(val)} invoice value across {_fmt(n)} row(s). Source: {table}."),
                      columns=["metric", "value"],
                      rows=[["Delivered ltr", ltr], ["Quantity", qty], ["Invoice amount", val], ["Rows", n]],
                      source=table, excel_title="Amazon MP")


def lead_time(q: ParsedQuery) -> DataResult:
    """Average PO lead-time days from master_po, overall or by vendor."""
    table = "master_po"
    text = q.text.lower()
    where = ["lead_time IS NOT NULL"]
    params: list = []
    scope = "all platforms"
    if q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
        scope = q.primary_platform["name"]
    if q.date_from and q.date_to:
        where.append("po_date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    wsql = " WHERE " + " AND ".join(where)

    if "vendor" in text or "distributor" in text:
        sql = (f"SELECT vendor_new, ROUND(AVG(lead_time)::numeric,1), COUNT(*) FROM {table}{wsql} "
               f"AND vendor_new IS NOT NULL GROUP BY vendor_new ORDER BY 2 DESC LIMIT {int(q.top_n or 15)}")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 15)
        lines = [f"{r[0]}: {_fmt(r[1])} days" for r in rows[:10]]
        return DataResult(summary=f"{scope} average lead time by vendor:\n" + "\n".join(lines),
                          columns=["vendor", "avg_lead_time_days", "po_lines"], rows=[list(r) for r in rows],
                          source=table, excel_title=f"{scope} Lead Time by Vendor")

    sql = f"SELECT ROUND(AVG(lead_time)::numeric,2), COUNT(*) FROM {table}{wsql}"
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    avg, n = rows[0]
    return DataResult(summary=f"{scope} average lead time: {_fmt(avg)} days across {_fmt(n)} PO line(s). Source: master_po.",
                      columns=["metric", "value"], rows=[["Avg lead time (days)", avg], ["PO lines", n]],
                      source=table, excel_title=f"{scope} Lead Time")


_STATE_NAMES = [
    "maharashtra", "gujarat", "goa", "rajasthan", "delhi", "punjab", "haryana",
    "uttar pradesh", "uttarakhand", "himachal pradesh", "jammu and kashmir", "chandigarh",
    "karnataka", "tamil nadu", "kerala", "andhra pradesh", "telangana", "puducherry",
    "west bengal", "bihar", "odisha", "orissa", "jharkhand", "madhya pradesh",
    "chhattisgarh", "chattisgarh", "assam", "tripura", "meghalaya", "manipur",
    "nagaland", "mizoram", "sikkim", "arunachal pradesh",
]
_STATE_REGION = {
    "MAHARASHTRA": "West", "GUJARAT": "West", "GOA": "West", "RAJASTHAN": "West",
    "DELHI": "North", "PUNJAB": "North", "HARYANA": "North", "UTTAR PRADESH": "North",
    "UTTARAKHAND": "North", "HIMACHAL PRADESH": "North", "JAMMU AND KASHMIR": "North", "CHANDIGARH": "North",
    "KARNATAKA": "South", "TAMIL NADU": "South", "KERALA": "South", "ANDHRA PRADESH": "South",
    "TELANGANA": "South", "PUDUCHERRY": "South",
    "WEST BENGAL": "East", "BIHAR": "East", "ODISHA": "East", "ORISSA": "East", "JHARKHAND": "East",
    "MADHYA PRADESH": "Central", "CHHATTISGARH": "Central", "CHATTISGARH": "Central",
    "ASSAM": "Northeast", "TRIPURA": "Northeast", "MEGHALAYA": "Northeast", "MANIPUR": "Northeast",
    "NAGALAND": "Northeast", "MIZORAM": "Northeast", "SIKKIM": "Northeast", "ARUNACHAL PRADESH": "Northeast",
}


def state_sales(q: ParsedQuery) -> DataResult:
    """State-wise secondary sell-out (SecMaster): a named state's value/liters,
    top cities within a state, region roll-up, brand (JIVO vs SANO) split, or top
    states by liters."""
    table = '"SecMaster"'
    text = q.text.lower()
    metric = "sales_amt" if any(w in text for w in ("value", "amount", "revenue", "worth", "₹")) else "ltr_sold"
    mlabel = "sales value" if metric == "sales_amt" else "liters"
    unit = "₹" if metric == "sales_amt" else ""

    where, params = [], []
    if q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
    if q.date_from and q.date_to:
        where.append("date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    named_state = next((s for s in _STATE_NAMES if re.search(r"\b" + re.escape(s) + r"\b", text)), None)
    scope = q.primary_platform["name"] if q.primary_platform else "all platforms"
    span = f" ({q.date_label})" if q.date_label else ""

    # Brand split (JIVO vs SANO)
    if ("jivo" in text and "sano" in text) or ("brand" in text and "split" in text):
        wsql = (" WHERE " + " AND ".join(where)) if where else ""
        sql = (f"SELECT UPPER(brand), COALESCE(SUM({metric}),0) FROM {table}{wsql} "
               f"{'AND' if where else 'WHERE'} brand IS NOT NULL GROUP BY UPPER(brand) ORDER BY 2 DESC")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=10)
        lines = "; ".join(f"{r[0]}: {unit}{_fmt(r[1])}" for r in rows)
        return DataResult(summary=f"{scope} state sales by brand{span}: {lines}. Source: SecMaster.",
                          columns=["brand", mlabel], rows=[list(r) for r in rows], source=table,
                          excel_title="Brand Sales Split")

    # Region roll-up
    if "region" in text:
        wsql = (" WHERE " + " AND ".join(where)) if where else ""
        sql = (f"SELECT UPPER(state), COALESCE(SUM({metric}),0) FROM {table}{wsql} "
               f"{'AND' if where else 'WHERE'} state IS NOT NULL GROUP BY UPPER(state)")
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=200)
        agg: dict[str, float] = {}
        for st, v in rows:
            agg[_STATE_REGION.get(st, "Other")] = agg.get(_STATE_REGION.get(st, "Other"), 0.0) + float(v or 0)
        ordered = sorted(agg.items(), key=lambda kv: -kv[1])
        lines = "; ".join(f"{k}: {unit}{_fmt(v)}" for k, v in ordered)
        return DataResult(summary=f"{scope} sales by region{span}: {lines}. Source: SecMaster.",
                          columns=["region", mlabel], rows=[[k, round(v, 1)] for k, v in ordered],
                          source=table, excel_title="Region Sales")

    # Cities within a named state
    if named_state and ("city" in text or "cities" in text):
        w = where + ["UPPER(state) = %s", "city IS NOT NULL", "city::text <> ''"]
        p = params + [named_state.upper()]
        sql = (f"SELECT city, COALESCE(SUM({metric}),0) FROM {table} WHERE " + " AND ".join(w)
               + f" GROUP BY city ORDER BY 2 DESC LIMIT {int(q.top_n or 10)}")
        _c, rows, _t = safe_sql.run_select(sql, p, max_rows=q.top_n or 10)
        lines = [f"{i+1}. {r[0]} — {unit}{_fmt(r[1])}" for i, r in enumerate(rows)]
        return DataResult(summary=f"Top cities in {named_state.title()} by {mlabel}{span}:\n" + "\n".join(lines),
                          columns=["city", mlabel], rows=[list(r) for r in rows], source=table,
                          excel_title=f"{named_state.title()} Cities")

    # A single named state total
    if named_state:
        w = where + ["UPPER(state) = %s"]
        p = params + [named_state.upper()]
        sql = f"SELECT COALESCE(SUM(sales_amt),0), COALESCE(SUM(ltr_sold),0), COUNT(*) FROM {table} WHERE " + " AND ".join(w)
        _c, rows, _t = safe_sql.run_select(sql, p, max_rows=1)
        val, ltr, n = rows[0]
        return DataResult(summary=(f"{named_state.title()} {scope} secondary sales{span}: ₹{_fmt(val)} value, "
                                   f"{_fmt(ltr)} liters across {_fmt(n)} row(s). Source: SecMaster."),
                          columns=["metric", "value"], rows=[["Sales value", val], ["Liters", ltr], ["Rows", n]],
                          source=table, excel_title=f"{named_state.title()} Sales")

    # Top states by metric
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = (f"SELECT state, COALESCE(SUM({metric}),0) FROM {table}{wsql} "
           f"{'AND' if where else 'WHERE'} state IS NOT NULL AND state::text <> '' "
           f"GROUP BY state ORDER BY 2 DESC LIMIT {int(q.top_n or 10)}")
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=q.top_n or 10)
    lines = [f"{i+1}. {r[0]} — {unit}{_fmt(r[1])}" for i, r in enumerate(rows)]
    return DataResult(summary=f"{scope} top states by secondary {mlabel}{span}:\n" + "\n".join(lines),
                      columns=["state", mlabel], rows=[list(r) for r in rows], source=table,
                      excel_title="Top States (Secondary)")


def realise(q: ParsedQuery) -> DataResult:
    """Realise per litre and distributor commission from master_po."""
    table = "master_po"
    where, params = [], []
    scope = "all platforms"
    if q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
        scope = q.primary_platform["name"]
    if q.date_from and q.date_to:
        where.append("po_date BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    span = f" ({q.date_label})" if q.date_label else ""

    sql = (f"SELECT COALESCE(SUM(total_deliver_amt_inclusive),0), COALESCE(SUM(total_delivered_liters),0), "
           f"COALESCE(SUM(total_distributor_commission),0) FROM {table}{wsql}")
    _c, rows, _t = safe_sql.run_select(sql, params, max_rows=1)
    value, ltr, comm = rows[0]
    gross_per_l = float(value) / float(ltr) if float(ltr or 0) else 0.0
    net_per_l = (float(value) - float(comm)) / float(ltr) if float(ltr or 0) else 0.0
    summary = (f"{scope} realise{span}: ₹{gross_per_l:,.2f}/L gross, ₹{net_per_l:,.2f}/L net of distributor commission. "
               f"Delivered value ₹{_fmt(value)}, commission ₹{_fmt(comm)}, {_fmt(ltr)} L. Source: master_po.")
    return DataResult(summary=summary, columns=["metric", "value"],
                      rows=[["Delivered value", value], ["Delivered liters", ltr], ["Distributor commission", comm],
                            ["Gross realise/L", round(gross_per_l, 2)], ["Net realise/L", round(net_per_l, 2)]],
                      source=table, meta=[("scope", scope)], excel_title=f"{scope} Realise")


def sap_info(q: ParsedQuery) -> DataResult:
    """SAP/HANA data (JM primary sales, SAP warehouse inventory, distributor
    balances, FIFO distributor inventory) is not wired into the chatbot yet."""
    return DataResult(
        summary=("That data lives in the SAP HANA system (JM primary sales analysis, SAP warehouse stock "
                 "value / below-min / zero-stock, distributor balances & invoices, distributor FIFO inventory). "
                 "The chatbot reads the operational Postgres database, which doesn't include SAP HANA yet — "
                 "please use the JM Primary / SAP Inventory / Distributors dashboards for those. "
                 "I can still answer platform PO, secondary, inventory, ads, targets, pendency and state-sales questions."),
        ok=False, source="SAP HANA (not connected)",
        suggestions=["state wise sales for june", "total distributor commission for june", "blinkit inventory"],
    )


def datetime_now(q: ParsedQuery) -> DataResult:
    """Current date / day / time (server local time)."""
    now = timezone.localtime()
    return DataResult(
        summary=f"Today is {now:%A, %d %B %Y}, and the current time is {now:%I:%M %p}.",
        columns=["metric", "value"],
        rows=[["Date", now.strftime("%Y-%m-%d")], ["Day", now.strftime("%A")],
              ["Time", now.strftime("%I:%M %p")]],
        source="clock", excel_title="Date & Time")


def app_control(q: ParsedQuery) -> DataResult:
    """Graceful reply for app-control requests the bot can't perform."""
    return DataResult(
        summary=("I can read and analyse your Jivo data, but I can't control the app itself "
                 "(log out, refresh, or navigate pages) — please use the app's own menu for that. "
                 "Meanwhile I can pull POs, liters, alerts, ads, targets, pendency and more — just ask."),
        ok=True, source="app")


def max_date(q: ParsedQuery) -> DataResult:
    """Latest data date for a platform — primary (master_po po_date / delivery_date)
    or secondary (SecMaster date)."""
    text = q.text.lower()
    if any(w in text for w in ("secondary", "sec ", "sold", "shipped", "sell out")):
        table, col, label = '"SecMaster"', "date", "secondary sale"
    else:
        col = "delivery_date" if ("del" in text or "deliver" in text) else "po_date"
        table, label = "master_po", f"primary {col.replace('_', ' ')}"
    where, params = [], []
    scope = "all platforms"
    if q.primary_platform:
        where.append("format ILIKE %s")
        params.append(f"%{_platform_format_value(q.primary_platform['slug'])}%")
        scope = q.primary_platform["name"]
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    try:
        _c, rows, _t = safe_sql.run_select(f"SELECT MAX({col}) FROM {table}{wsql}", params, max_rows=1)
    except Exception as exc:
        return DataResult(summary=f"I couldn't read the latest date: {exc}", ok=False, source=table)
    md = rows[0][0] if rows else None
    if md is None:
        return DataResult(summary=f"No {label} data found for {scope}.", ok=False, source=table)
    return DataResult(summary=f"Latest {label} for {scope}: {md}. Source: {table}.",
                      columns=["metric", "value"], rows=[[f"latest {label}", md]],
                      source=table, excel_title="Latest Date")


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


def _metric_key(q: ParsedQuery) -> tuple[str, str]:
    """Pick the metric to rank by, from the question text (source-independent).
    Returns a logical key + human label. Defaults to order liters."""
    t = q.text.lower()
    if "fill rate" in t or "fillrate" in t or "fill %" in t or "fill percentage" in t:
        return "fill_rate", "fill %"
    if any(w in t for w in ("amount", "amt", "value", "revenue", "sales", "worth", "₹", " rs")):
        return "amount", "order amount"
    if "missed" in t or "missing" in t:
        return "missed_ltrs", "missed liters"
    if "filled" in t:
        return "filled_ltrs", "filled liters"
    if q.movement == "delivered" or "deliver" in t:
        return "delivered_ltrs", "delivered liters"
    if q.metric == "units" or any(w in t for w in ("qty", "quantity", "unit")):
        return "order_qty", "order qty"
    if ("order" in t or "po" in t) and any(w in t for w in ("count", "number", "how many", "no of")):
        return "count", "PO lines"
    return "order_ltrs", "order liters"


def _metric_sql(source: _PoSource, key: str) -> str:
    """Build the SQL aggregate for a metric key against a specific source."""
    if key == "count":
        return "COUNT(*)"
    if key == "fill_rate":
        # Ratio metric: delivered / ordered, as a percentage.
        return (f"ROUND(COALESCE(SUM({source.delivered_ltrs}), 0) / "
                f"NULLIF(SUM({source.order_ltrs}), 0) * 100, 1)")
    col = {
        "amount": source.amount,
        "delivered_ltrs": source.delivered_ltrs,
        "order_qty": source.order_qty,
        "order_ltrs": source.order_ltrs,
        "missed_ltrs": source.missed_ltrs,
        "filled_ltrs": source.filled_ltrs,
    }[key]
    return f"COALESCE(SUM({col}), 0)"


def ranking(q: ParsedQuery) -> DataResult:
    """Top-N ranking by a dimension (state / city / brand / sku / category /
    vendor / ...) over the right PO table — master_po for quick-commerce
    platforms, reporting."Amazon PO" for Amazon — for any platform + date."""
    source, scope_name, fmt_val = _resolve_po_source(q)

    dim = q.dimension or "state"
    dim_col = source.dim_cols.get(dim)
    if not dim_col:
        avail = ", ".join(sorted(source.dim_cols))
        return DataResult(
            summary=f"I can't rank {scope_name} by {dim}. Try one of: {avail}.",
            ok=False, source=source.label,
        )

    key, metric_label = _metric_key(q)
    metric_sql = _metric_sql(source, key)

    where, params = [], []
    scope = ""
    # Platform filter only when the source carries a format column and we're not
    # ranking the platform dimension itself. Amazon's table is already scoped.
    if source.format_col and fmt_val and dim != "platform":
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
        scope = f" for {scope_name}"
    elif source.format_col is None and q.primary_platform:
        scope = f" for {scope_name}"
    if q.date_from and q.date_to:
        where.append(f"{source.date_col} BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    where.append(f"{dim_col} IS NOT NULL")
    where.append(f"{dim_col}::text <> ''")
    where_sql = " WHERE " + " AND ".join(where)

    # For the fill-rate ratio, ignore trivial-volume groups (a single tiny PO at
    # 100% would otherwise top the list); require a meaningful ordered volume.
    having_sql = ""
    if key == "fill_rate":
        having_sql = f" HAVING SUM({source.order_ltrs}) >= 500"

    limit = q.top_n or 10
    sql = f"""
        SELECT {dim_col} AS label, {metric_sql} AS value
        FROM {source.table}{where_sql}
        GROUP BY {dim_col}{having_sql}
        ORDER BY value DESC
        LIMIT {int(limit)}
    """
    try:
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=limit)
    except Exception as exc:
        logger.warning("ranking query failed: %s", exc)
        return DataResult(summary=f"I couldn't rank by {dim}: {exc}", ok=False, source=source.label)

    if not rows:
        return DataResult(summary=f"No {dim} data found{scope}.", ok=False, source=source.label)

    label = dim.title()
    span = f" ({q.date_label})" if q.date_label else ""
    lines = [f"{i + 1}. {r[0]} — {_fmt(r[1])}" for i, r in enumerate(rows)]
    data_rows = [[r[0], r[1]] for r in rows]

    # Append a TOTAL row for additive metrics (a summed fill-% would be meaningless).
    total_txt = ""
    if key != "fill_rate":
        try:
            total = sum(float(r[1] or 0) for r in rows)
            total = int(total) if float(total).is_integer() else round(total, 2)
            data_rows.append([f"TOTAL ({len(rows)} {label.lower()}s)", total])
            total_txt = f"\nTotal ({len(rows)} {label.lower()}s): {_fmt(total)}"
        except Exception:
            pass

    summary = (
        f"Top {len(rows)} {label.lower()}(s) by {metric_label}{scope}{span}:\n"
        + "\n".join(lines)
        + total_txt
        + f"\nSource: {source.label}."
    )
    return DataResult(
        summary=summary, columns=[label, metric_label], rows=data_rows,
        source=source.label, meta=[("dimension", dim), ("metric", metric_label)],
        excel_title=f"Top {label}",
    )


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def _prev_month_of(d: date) -> tuple[date, date]:
    py, pm = (d.year - 1, 12) if d.month == 1 else (d.year, d.month - 1)
    return _month_bounds(py, pm)


def movers(q: ParsedQuery) -> DataResult:
    """Top risers / fallers for a dimension (default SKU) between the current
    month and the previous month (or an explicit month vs the one before it)."""
    source, scope_name, fmt_val = _resolve_po_source(q)
    dim = q.dimension if q.dimension in source.dim_cols else "sku"
    dim_col = source.dim_cols.get(dim, source.dim_cols["sku"] if "sku" in source.dim_cols else "sku_code")

    key, metric_label = _metric_key(q)
    if key in ("count", "fill_rate"):
        key, metric_label = "delivered_ltrs", "delivered liters"
    metric_sql = _metric_sql(source, key)

    today = timezone.localdate()
    if q.date_from and q.date_to:
        cur_from, cur_to = q.date_from, q.date_to
    else:
        cur_from, cur_to = today.replace(day=1), today
    prev_from, prev_to = _prev_month_of(cur_from)

    fmt_clause = ""
    fmt_param: list = []
    if source.format_col and fmt_val:
        fmt_clause = f" AND {source.format_col} ILIKE %s"
        fmt_param = [f"%{fmt_val}%"]

    seg = (f"{dim_col} IS NOT NULL AND {dim_col}::text <> '' "
           f"AND {source.date_col} BETWEEN %s AND %s{fmt_clause}")
    sql = f"""
        WITH cur AS (
            SELECT {dim_col} AS label, {metric_sql} AS v
            FROM {source.table} WHERE {seg} GROUP BY {dim_col}
        ),
        prev AS (
            SELECT {dim_col} AS label, {metric_sql} AS v
            FROM {source.table} WHERE {seg} GROUP BY {dim_col}
        )
        SELECT COALESCE(cur.label, prev.label) AS label,
               COALESCE(cur.v, 0) AS cur_v, COALESCE(prev.v, 0) AS prev_v,
               COALESCE(cur.v, 0) - COALESCE(prev.v, 0) AS delta
        FROM cur FULL OUTER JOIN prev ON cur.label = prev.label
        ORDER BY delta DESC
    """
    params = [cur_from, cur_to, *fmt_param, prev_from, prev_to, *fmt_param]
    try:
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=5000)
    except Exception as exc:
        logger.warning("movers query failed: %s", exc)
        return DataResult(summary=f"I couldn't compute movers: {exc}", ok=False, source=source.label)
    if not rows:
        return DataResult(summary=f"No {dim} movement data found{(' for ' + scope_name) if q.primary_platform else ''}.",
                          ok=False, source=source.label)

    n = q.top_n or 5
    risers = [r for r in rows if float(r[3] or 0) > 0][:n]
    fallers = [r for r in reversed(rows) if float(r[3] or 0) < 0][:n]
    scope = f" for {scope_name}" if q.primary_platform else ""
    cur_lbl, prev_lbl = cur_from.strftime("%b %Y"), prev_from.strftime("%b %Y")

    def line(r, sign):
        return f"{r[0]}: {_fmt(r[1])} vs {_fmt(r[2])} ({sign}{_fmt(abs(float(r[3])))})"
    up = "\n".join(line(r, "+") for r in risers) or "  (none)"
    down = "\n".join(line(r, "-") for r in fallers) or "  (none)"
    summary = (
        f"Top {dim} movers by {metric_label}{scope} — {cur_lbl} vs {prev_lbl}:\n"
        f"Risers:\n{up}\nFallers:\n{down}\nSource: {source.label}."
    )
    cols = [dim.title(), f"{cur_lbl}", f"{prev_lbl}", "Change"]
    data_rows = [[r[0], r[1], r[2], r[3]] for r in (risers + fallers)]
    return DataResult(summary=summary, columns=cols, rows=data_rows, source=source.label,
                      meta=[("dimension", dim), ("metric", metric_label)],
                      excel_title=f"{dim.title()} Movers")


def premium_commodity_split(q: ParsedQuery) -> DataResult:
    """Premium vs Commodity vs Other split of liters (or amount) grouped by a
    dimension — platform by default (e.g. 'premium vs commodity by platform')."""
    source, scope_name, fmt_val = _resolve_po_source(q)
    if q.dimension in source.dim_cols and q.dimension != "platform":
        dim = q.dimension
    elif q.primary_platform and source.format_col:
        dim = "category"          # split within the named platform
    else:
        dim = "platform" if source.format_col else "category"
    dim_col = source.dim_cols.get(dim, "category")

    key, metric_label = _metric_key(q)
    if key in ("count", "fill_rate", "missed_ltrs", "filled_ltrs"):
        key, metric_label = ("delivered_ltrs", "delivered liters") if (q.movement == "delivered" or "deliver" in q.text.lower()) else ("order_ltrs", "order liters")
    metric_sql = _metric_sql(source, key)

    where, params = [], []
    if source.format_col and fmt_val and dim != "platform":
        where.append(f"{source.format_col} ILIKE %s")
        params.append(f"%{fmt_val}%")
    if q.date_from and q.date_to:
        where.append(f"{source.date_col} BETWEEN %s AND %s")
        params.extend([q.date_from, q.date_to])
    where.append(f"{dim_col} IS NOT NULL AND {dim_col}::text <> ''")
    where_sql = " WHERE " + " AND ".join(where)

    sql = f"""
        SELECT {dim_col} AS label, UPPER(COALESCE(item_head, 'OTHER')) AS head, {metric_sql} AS v
        FROM {source.table}{where_sql}
        GROUP BY {dim_col}, UPPER(COALESCE(item_head, 'OTHER'))
    """
    try:
        _c, rows, _t = safe_sql.run_select(sql, params, max_rows=5000)
    except Exception as exc:
        logger.warning("split query failed: %s", exc)
        return DataResult(summary=f"I couldn't compute the split: {exc}", ok=False, source=source.label)
    if not rows:
        return DataResult(summary="No item-head split data found.", ok=False, source=source.label)

    agg: dict[str, dict[str, float]] = {}
    for label, head, v in rows:
        d = agg.setdefault(label, {"PREMIUM": 0.0, "COMMODITY": 0.0, "OTHER": 0.0})
        bucket = head if head in d else "OTHER"
        d[bucket] += float(v or 0)
    ordered = sorted(agg.items(), key=lambda kv: sum(kv[1].values()), reverse=True)
    limit = q.top_n or 15
    ordered = ordered[:limit]

    cols = [dim.title(), "Premium", "Commodity", "Other", "Total"]
    data_rows = []
    for label, d in ordered:
        total = d["PREMIUM"] + d["COMMODITY"] + d["OTHER"]
        data_rows.append([label, round(d["PREMIUM"], 1), round(d["COMMODITY"], 1), round(d["OTHER"], 1), round(total, 1)])
    span = f" ({q.date_label})" if q.date_label else ""
    top_lines = "\n".join(
        f"{r[0]}: premium {_fmt(r[1])} L, commodity {_fmt(r[2])} L" for r in data_rows[:8]
    )
    summary = (
        f"Premium vs commodity {metric_label} by {dim}{span}:\n{top_lines}\n"
        f"Source: {source.label}."
    )
    return DataResult(summary=summary, columns=cols, rows=data_rows, source=source.label,
                      meta=[("dimension", dim), ("metric", metric_label)],
                      excel_title=f"Premium vs Commodity by {dim.title()}")
