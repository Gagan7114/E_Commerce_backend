"""Blinkit Sale & Target — Marketing section, Blinkit only.

Reproduces two offline tracking sheets in one payload:

  1. DAILY SALE — "Blinkit Sale (6th Aug)" / "Blinkit Sale (7th Aug)": one row
     per product for a chosen date and a comparison date (SKU, item head,
     litres sold, sale value excluding tax), plus PREMIUM / COMMODITY
     sub-totals and a grand total.

  2. LITRE-WISE TARGET — "Blinkit Premium / Commodity Litre Vise Target": one
     row per product per item head carrying the manually entered monthly litre
     target, month-to-date done litres, a full-month projection, the previous
     six month-closes, growth of the projection over the previous month and the
     % of target achieved. Section totals + a grand total mirror the sheet.

Source
------
`secmaster_mv` filtered to format BLINKIT — the same materialized view the
Secondary dashboards and Sec Targets read, so litres and sale values agree with
them by construction. `ltr_sold` is the litre column; `sales_amt_exc` is the
"Sale Exclusive" (tax-excluded) value.

The ONLY stored data is the per-product target, one row per
(product, month, year) in `blinkit_product_targets` (migration 0071). This
module never reads or writes `month_targets` / `primary_month_targets`, so the
existing Sec Targets and Prim Targets pages are unaffected.

Formulas (verified against the sheet)
-------------------------------------
    projection    = done_ltr / elapsed_days * days_in_month
    growth_pct    = projection / previous_month_close - 1
    achieved_pct  = done_ltr / target_ltr
`elapsed_days` is the day-of-month of the selected "as on" date, so a sheet
dated the 7th annualises seven days over the full month — exactly what the
offline workbook does.

Contract
--------
  GET  /api/platform/<slug>/blinkit-sale-target
         ?date=YYYY-MM-DD          (default: latest date with Blinkit data)
         &compare_date=YYYY-MM-DD  (default: the day before `date`)
         &close_months=YYYY-MM,…   (default: the six months before `date`'s
                                    month; any closed month may be picked, in
                                    any combination, and growth is measured
                                    from the newest one picked. The literal
                                    `none` means no close columns at all.)
  POST /api/platform/<slug>/blinkit-sale-target/set-target
         body {item, item_head, month, year, target_ltrs, category?}

Permissions mirror the other Marketing dashboards and the targets endpoints:
GET needs `platform.stats.view`, the target write needs `target_sheet.edit`.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.db import connection
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import can_access_platform, require
from config.perf_cache import cached_get


SLUG = "blinkit"

# Rendering order of the two target sheets — PREMIUM first, as in the workbook.
ITEM_HEADS = ("PREMIUM", "COMMODITY")

# How many closed months to show to the left of "Growth From <prev>".
CLOSE_MONTHS = 6

# How many closed months the picker offers. Twelve consecutive months carry
# twelve distinct month names, so the columns never need a year to tell them
# apart — which is what keeps the headers reading like the workbook's.
CLOSE_MONTH_CHOICES = 12

# Products with no sales in this many months back from the selected month drop
# off the target sheet (a delisted SKU shouldn't linger forever). A product with
# a saved target for the selected month is always shown, however old.
CATALOG_LOOKBACK_MONTHS = 13

# Matches every spelling of the format in secmaster_mv ('BLINKIT', 'Blinkit', …).
_BLINKIT_FORMAT_SQL = (
    "REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'"
)

_EDIT_PERMISSION = require("target_sheet.edit")


# ─── small helpers ───────────────────────────────────────────────────────────


def _dict_rows(sql: str, params: list) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _ensure_blinkit(user, slug: str) -> None:
    if slug != SLUG:
        raise ValidationError("Sale & Target is available only for Blinkit.")
    if not can_access_platform(user, slug):
        raise PermissionDenied("Your account is not authorized for the 'blinkit' platform.")


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _ordinal(day: int) -> str:
    if 11 <= day % 100 <= 13:
        return f"{day}th"
    return f"{day}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th') }"


def _day_label(d: date) -> str:
    """'7th Aug' — the sheet's own way of titling a daily table."""
    return f"{_ordinal(d.day)} {d.strftime('%b')}"


def _month_key(year: int, month: int) -> str:
    return f"{year}-{month:02d}"


def _month_shift(year: int, month: int, back: int) -> tuple[int, int]:
    """The (year, month) `back` months before (year, month)."""
    index = year * 12 + (month - 1) - back
    return index // 12, index % 12 + 1


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _split_month_key(key: str) -> tuple[int, int]:
    """'2026-07' -> (2026, 7)."""
    year, month = key.split("-")
    return int(year), int(month)


def _close_month_meta(key: str, with_year: bool = False) -> dict:
    """One close column's labels — plain "Jul Close" / "Jul", as the workbook
    writes them.

    `with_year` is the safety valve, not the norm. The picker offers twelve
    consecutive months, whose names are all distinct, so the year is redundant
    and stays off. It only switches on if a gap in the data ever pushed two
    same-named months into one selection, where three columns would otherwise
    all read "Jul Close".
    """
    year, month = _split_month_key(key)
    first = date(year, month, 1)
    short = first.strftime("%b-%y") if with_year else first.strftime("%b")
    return {
        "key": key,
        "label": f"{short} Close",
        "option_label": short,
        "short": short,
        "month": month,
        "year": year,
    }


def _parse_close_months(raw: str, month: int, year: int) -> list[str]:
    """Parse `close_months=2026-07,2026-06` into validated 'YYYY-MM' keys.

    Months at or after the selected one are dropped rather than rejected: a
    "close" is by definition a month that has finished, and silently ignoring a
    stale pick keeps the sheet usable when the user moves the As-on date back.
    """
    keys: list[str] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            k_year, k_month = _split_month_key(part)
            date(k_year, k_month, 1)
        except (ValueError, TypeError):
            raise ValidationError(
                f"`close_months` entries must look like YYYY-MM (got '{part}')."
            )
        if (k_year, k_month) >= (year, month):
            continue
        keys.append(_month_key(k_year, k_month))
    return keys


def _parse_date(raw, field: str):
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return date.fromisoformat(str(raw).strip())
    except ValueError:
        raise ValidationError(f"`{field}` must be a YYYY-MM-DD date.")


def _ratio(numerator, denominator):
    """A fraction (0..1 style) or None when the denominator carries no signal —
    the frontend renders None as '-' instead of a misleading 0%."""
    denominator = _num(denominator)
    if not denominator:
        return None
    return _num(numerator) / denominator


def _max_data_date():
    rows = _dict_rows(
        f'SELECT MAX("date") AS max_date FROM secmaster_mv WHERE {_BLINKIT_FORMAT_SQL}',
        [],
    )
    return rows[0]["max_date"] if rows else None


# ─── daily sale ──────────────────────────────────────────────────────────────


def _daily_rows(days: list[date]) -> dict[date, list[dict]]:
    """Per-product litres + tax-excluded sale for each requested day."""
    if not days:
        return {}
    placeholders = ", ".join(["%s"] * len(days))
    rows = _dict_rows(
        f"""
        SELECT "date" AS day,
               UPPER(TRIM("item")) AS item_key,
               MAX(TRIM("item")) AS item,
               MAX(UPPER(TRIM("item_head"))) AS item_head,
               MAX(UPPER(TRIM("category"))) AS category,
               COALESCE(SUM("ltr_sold"), 0) AS ltrs,
               COALESCE(SUM("sales_amt_exc"), 0) AS sales_exc,
               COALESCE(SUM("quantity"), 0) AS qty
          FROM secmaster_mv
         WHERE {_BLINKIT_FORMAT_SQL}
           AND "date" IN ({placeholders})
           AND "item" IS NOT NULL AND TRIM("item") <> ''
         GROUP BY 1, 2
        """,
        list(days),
    )
    by_day: dict[date, list[dict]] = {d: [] for d in days}
    for row in rows:
        by_day.setdefault(row["day"], []).append({
            "item": row["item"],
            "item_head": row["item_head"] or "",
            "category": row["category"] or "",
            "ltrs": _num(row["ltrs"]),
            "sales_exc": _num(row["sales_exc"]),
            "qty": _num(row["qty"]),
        })
    return by_day


def _daily_block(day, rows: list[dict]) -> dict:
    """One "Blinkit Sale (<day>)" table: products ordered PREMIUM-then-COMMODITY,
    each head's sub-total, and the grand total."""
    def sort_key(row):
        head = row["item_head"]
        return (
            ITEM_HEADS.index(head) if head in ITEM_HEADS else len(ITEM_HEADS),
            row["category"],
            row["item"],
        )

    ordered = sorted(rows, key=sort_key)

    by_head = []
    for head in ITEM_HEADS:
        slice_ = [r for r in ordered if r["item_head"] == head]
        if not slice_:
            continue
        by_head.append({
            "item_head": head,
            "ltrs": sum(r["ltrs"] for r in slice_),
            "sales_exc": sum(r["sales_exc"] for r in slice_),
        })

    return {
        "date": day.isoformat() if day else None,
        "label": _day_label(day) if day else None,
        "rows": ordered,
        "by_head": by_head,
        "total": {
            "ltrs": sum(r["ltrs"] for r in ordered),
            "sales_exc": sum(r["sales_exc"] for r in ordered),
        },
    }


# ─── litre-wise target ───────────────────────────────────────────────────────


def _catalog(window_start: date, window_end: date) -> dict[str, dict]:
    """Every Blinkit product seen in the lookback window, keyed by UPPER(item).

    item_head / category come from the product's most recent row so a
    re-classified SKU shows its current head, not a stale one.
    """
    rows = _dict_rows(
        f"""
        SELECT DISTINCT ON (UPPER(TRIM("item")))
               UPPER(TRIM("item")) AS item_key,
               TRIM("item") AS item,
               UPPER(TRIM("item_head")) AS item_head,
               UPPER(TRIM("category")) AS category
          FROM secmaster_mv
         WHERE {_BLINKIT_FORMAT_SQL}
           AND "date" >= %s AND "date" < %s
           AND "item" IS NOT NULL AND TRIM("item") <> ''
         ORDER BY UPPER(TRIM("item")), "date" DESC
        """,
        [window_start, window_end],
    )
    return {
        r["item_key"]: {
            "item": r["item"],
            "item_head": r["item_head"] or "",
            "category": r["category"] or "",
        }
        for r in rows
    }


def _available_close_months(before: date) -> list[str]:
    """The most recent CLOSE_MONTH_CHOICES months with Blinkit sales, strictly
    before the selected month, newest first — the Close-months pick-list."""
    rows = _dict_rows(
        f"""
        SELECT DISTINCT TO_CHAR("date", 'YYYY-MM') AS ym
          FROM secmaster_mv
         WHERE {_BLINKIT_FORMAT_SQL}
           AND "date" < %s
         ORDER BY 1 DESC
         LIMIT %s
        """,
        [before, CLOSE_MONTH_CHOICES],
    )
    return [r["ym"] for r in rows]


def _monthly_closes(
    window_start: date, window_end: date, months: list[str]
) -> dict[str, dict[str, float]]:
    """{item_key: {'YYYY-MM': litres}} for exactly the requested months.

    The date range is the span of those months, so the scan stays bounded, and
    the `= ANY` keeps a non-contiguous pick (say Jul and Feb) from dragging in
    everything between them.
    """
    if not months:
        return {}
    rows = _dict_rows(
        f"""
        SELECT UPPER(TRIM("item")) AS item_key,
               TO_CHAR("date", 'YYYY-MM') AS ym,
               COALESCE(SUM("ltr_sold"), 0) AS ltrs
          FROM secmaster_mv
         WHERE {_BLINKIT_FORMAT_SQL}
           AND "date" >= %s AND "date" < %s
           AND TO_CHAR("date", 'YYYY-MM') = ANY(%s)
           AND "item" IS NOT NULL AND TRIM("item") <> ''
         GROUP BY 1, 2
        """,
        [window_start, window_end, months],
    )
    closes: dict[str, dict[str, float]] = {}
    for row in rows:
        closes.setdefault(row["item_key"], {})[row["ym"]] = _num(row["ltrs"])
    return closes


def _month_to_date(month_start: date, as_on: date) -> dict[str, dict]:
    """Done litres + tax-excluded sale from the 1st through the `as on` date."""
    rows = _dict_rows(
        f"""
        SELECT UPPER(TRIM("item")) AS item_key,
               COALESCE(SUM("ltr_sold"), 0) AS ltrs,
               COALESCE(SUM("sales_amt_exc"), 0) AS sales_exc
          FROM secmaster_mv
         WHERE {_BLINKIT_FORMAT_SQL}
           AND "date" >= %s AND "date" <= %s
           AND "item" IS NOT NULL AND TRIM("item") <> ''
         GROUP BY 1
        """,
        [month_start, as_on],
    )
    return {
        r["item_key"]: {"ltrs": _num(r["ltrs"]), "sales_exc": _num(r["sales_exc"])}
        for r in rows
    }


def _saved_targets(month: int, year: int) -> dict[str, dict]:
    rows = _dict_rows(
        """
        SELECT UPPER(TRIM(item)) AS item_key, TRIM(item) AS item,
               UPPER(TRIM(item_head)) AS item_head, UPPER(TRIM(COALESCE(category, ''))) AS category,
               target_ltrs
          FROM blinkit_product_targets
         WHERE month = %s AND year = %s
        """,
        [month, year],
    )
    return {r["item_key"]: r for r in rows}


def _target_row(item_key, meta, target, mtd, closes, close_keys, prev_key,
                elapsed_days, days_in_month) -> dict:
    done = _num(mtd.get("ltrs"))
    projection = done / elapsed_days * days_in_month if elapsed_days else 0.0
    prev_close = _num(closes.get(prev_key)) if prev_key else 0.0
    return {
        "item_key": item_key,
        "item": meta["item"],
        "item_head": meta["item_head"],
        "category": meta["category"],
        "target_ltr": None if target is None else _num(target),
        "done_ltr": done,
        "done_value": _num(mtd.get("sales_exc")),
        "projection_ltr": projection,
        # {'YYYY-MM': litres} — the frontend renders one column per close month.
        "closes": {key: _num(closes.get(key)) for key in close_keys},
        # A month with no prior sale has no meaningful growth base -> null, not
        # an infinite/100% jump.
        "growth_pct": (projection / prev_close - 1) if prev_close else None,
        "achieved_pct": _ratio(done, target),
    }


def _aggregate(rows: list[dict], close_keys, prev_key, elapsed_days, days_in_month,
               label: str) -> dict:
    """Section / grand total. Every figure is the SUM of its column and the two
    ratios are recomputed from those sums — never an average of per-row ratios."""
    done = sum(r["done_ltr"] for r in rows)
    target = sum(r["target_ltr"] or 0 for r in rows)
    has_target = any(r["target_ltr"] is not None for r in rows)
    projection = sum(r["projection_ltr"] for r in rows)
    closes = {key: sum(r["closes"].get(key, 0.0) for r in rows) for key in close_keys}
    prev_close = closes.get(prev_key, 0.0) if prev_key else 0.0
    return {
        "label": label,
        "target_ltr": target if has_target else None,
        "done_ltr": done,
        "done_value": sum(r["done_value"] for r in rows),
        "projection_ltr": projection,
        "closes": closes,
        "growth_pct": (projection / prev_close - 1) if prev_close else None,
        "achieved_pct": _ratio(done, target) if has_target else None,
    }


# ─── endpoints ───────────────────────────────────────────────────────────────


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.blinkit_sale_target")
def blinkit_sale_target(request, slug: str):
    """GET /api/platform/<slug>/blinkit-sale-target

    Daily sale (selected date vs a comparison date) + the litre-wise target
    sheet for the selected date's month. See the module docstring.
    """
    _ensure_blinkit(request.user, slug)

    max_date = _max_data_date()
    as_on = _parse_date(request.query_params.get("date"), "date") or max_date
    if as_on is None:
        # No Blinkit rows at all — return an empty but well-formed payload so
        # the page renders its own "no data" state instead of erroring.
        today = timezone.localdate()
        return Response({
            "slug": slug,
            "as_on": None,
            "compare_date": None,
            "max_date": None,
            "month": today.month,
            "year": today.year,
            "month_label": today.strftime("%b-%y"),
            "days_in_month": monthrange(today.year, today.month)[1],
            "elapsed_days": 0,
            "editable": True,
            "daily": {"current": _daily_block(None, []), "compare": _daily_block(None, [])},
            "targets": {
                "close_months": [],
                "available_close_months": [],
                "prev_month_label": None,
                "sections": [],
                "grand_total": None,
            },
        })

    compare = _parse_date(request.query_params.get("compare_date"), "compare_date")
    if compare is None:
        compare = as_on - timedelta(days=1)

    month, year = as_on.month, as_on.year
    days_in_month = monthrange(year, month)[1]
    # The sheet annualises the days elapsed SO FAR, so a mid-month "as on" date
    # projects from that many days; a date at month end projects from the whole
    # month and the projection collapses to the actual close.
    elapsed_days = min(as_on.day, days_in_month)
    month_start = _month_start(year, month)

    next_year, next_month = _month_shift(year, month, -1)
    window_end = _month_start(next_year, next_month)

    # Close columns. `close_months=YYYY-MM,YYYY-MM,…` picks them explicitly;
    # with no param the sheet keeps its old shape — the CLOSE_MONTHS months
    # immediately before the selected one ("Jul Close, Jun Close, … Feb Close").
    available = _available_close_months(month_start)
    raw = str(request.query_params.get("close_months") or "").strip()
    if raw.lower() == "none":
        # Explicitly "no close columns". Needed because an empty query param is
        # dropped in transit, which would be indistinguishable from sending no
        # param at all and would silently restore the six defaults.
        close_keys = []
    elif raw:
        close_keys = _parse_close_months(raw, month, year)
    else:
        close_keys = [
            _month_key(*_month_shift(year, month, back))
            for back in range(1, CLOSE_MONTHS + 1)
        ]
    # Newest first, so the columns read Jul, Jun, May … whatever order they were
    # ticked in, and the growth basis below is always the most recent pick.
    close_keys = sorted(set(close_keys), reverse=True)
    # Headers stay bare ("Jul Close") unless two picks share a month name, which
    # the twelve-month pick-list makes impossible in practice — see
    # `_close_month_meta`.
    names = [k[5:] for k in close_keys]
    ambiguous = len(set(names)) != len(names)
    close_months = [_close_month_meta(key, ambiguous) for key in close_keys]
    # Growth compares against the NEWEST selected close month, not blindly
    # against month-1: every figure on the sheet then has its basis visible in a
    # column. With the default selection that newest month IS month-1, so the
    # untouched sheet reads exactly as before.
    prev_key = close_keys[0] if close_keys else None

    # One scan spanning the oldest selected close month to the selected month.
    closes_start = (
        _month_start(*_split_month_key(close_keys[-1])) if close_keys else month_start
    )

    catalog_year, catalog_month = _month_shift(year, month, CATALOG_LOOKBACK_MONTHS)
    # A close month older than the catalog lookback still needs its products
    # listed, or its column would be blank for them.
    catalog_start = min(_month_start(catalog_year, catalog_month), closes_start)
    catalog = _catalog(catalog_start, window_end)
    closes_by_item = _monthly_closes(closes_start, window_end, close_keys)
    mtd_by_item = _month_to_date(month_start, as_on)
    targets_by_item = _saved_targets(month, year)

    # A product with a saved target is always listed, even if it has not sold in
    # the lookback window — otherwise its target would silently vanish.
    for item_key, saved in targets_by_item.items():
        if item_key not in catalog:
            catalog[item_key] = {
                "item": saved["item"],
                "item_head": saved["item_head"] or "",
                "category": saved["category"] or "",
            }

    rows = [
        _target_row(
            item_key,
            meta,
            (targets_by_item.get(item_key) or {}).get("target_ltrs"),
            mtd_by_item.get(item_key, {}),
            closes_by_item.get(item_key, {}),
            close_keys,
            prev_key,
            elapsed_days,
            days_in_month,
        )
        for item_key, meta in catalog.items()
    ]

    sections = []
    for head in ITEM_HEADS:
        head_rows = sorted(
            (r for r in rows if r["item_head"] == head),
            key=lambda r: (r["category"], r["item"]),
        )
        if not head_rows:
            continue
        sections.append({
            "item_head": head,
            "title": f"Blinkit {head.title()} Litre Vise Target",
            "rows": head_rows,
            "total": _aggregate(
                head_rows, close_keys, prev_key, elapsed_days, days_in_month, "Total",
            ),
        })

    listed = [r for section in sections for r in section["rows"]]
    grand_total = _aggregate(
        listed, close_keys, prev_key, elapsed_days, days_in_month, "Grand Total",
    ) if listed else None

    daily_by_day = _daily_rows(sorted({as_on, compare}))

    today = timezone.localdate()
    return Response({
        "slug": slug,
        "as_on": as_on.isoformat(),
        "compare_date": compare.isoformat(),
        "max_date": max_date.isoformat() if max_date else None,
        "month": month,
        "year": year,
        "month_label": date(year, month, 1).strftime("%b-%y"),
        "days_in_month": days_in_month,
        "elapsed_days": elapsed_days,
        # Past months are read-only once a target has been saved (see
        # `blinkit_sale_target_set_target`); the UI greys the inputs out.
        "editable": (year, month) >= (today.year, today.month),
        "daily": {
            "current": _daily_block(as_on, daily_by_day.get(as_on, [])),
            "compare": _daily_block(compare, daily_by_day.get(compare, [])),
        },
        "targets": {
            "close_months": close_months,
            # Every closed month with Blinkit data — the Close-months picker's
            # options, newest first.
            "available_close_months": [_close_month_meta(k) for k in available],
            "prev_month_label": close_months[0]["short"] if close_months else None,
            "sections": sections,
            "grand_total": grand_total,
        },
    })


@api_view(["POST"])
@permission_classes([_EDIT_PERMISSION])
def blinkit_sale_target_set_target(request, slug: str):
    """POST /api/platform/<slug>/blinkit-sale-target/set-target

    Upsert ONE product's litre target for one month.
    Body: {item, item_head, month, year, target_ltrs, category?}

    Month safety, mirroring the other target sheets: the write is keyed on
    (UPPER(item), month, year) — the exact month the sheet is showing — so no
    other month can be touched. The current month and any future month stay
    editable; a month that has already closed accepts a FIRST target (so the
    workbook's history can be typed in) but an existing one can no longer be
    overwritten.
    """
    _ensure_blinkit(request.user, slug)

    body = request.data or {}
    item = str(body.get("item") or "").strip()
    if not item:
        raise ValidationError("`item` (the product) is required.")

    item_head = str(body.get("item_head") or "").strip().upper()
    if item_head not in ITEM_HEADS:
        raise ValidationError(f"`item_head` must be one of {ITEM_HEADS}.")

    category = str(body.get("category") or "").strip().upper() or None

    try:
        month = int(body.get("month"))
        year = int(body.get("year"))
    except (TypeError, ValueError):
        raise ValidationError("`month` (1-12) and `year` (YYYY) are required integers.")
    if not 1 <= month <= 12:
        raise ValidationError("`month` must be 1-12.")
    if not 2000 <= year <= 2100:
        raise ValidationError("`year` looks out of range.")

    raw = body.get("target_ltrs")
    if raw is None or str(raw).strip() == "":
        raise ValidationError("`target_ltrs` is required — clearing a saved target is not supported.")
    try:
        target = Decimal(str(raw).replace(",", "").strip())
    except (InvalidOperation, ValueError, TypeError):
        raise ValidationError("`target_ltrs` must be a number.")
    if target < 0:
        raise ValidationError("`target_ltrs` must be >= 0.")

    today = timezone.localdate()
    month_closed = (year, month) < (today.year, today.month)
    if month_closed:
        existing = _dict_rows(
            """
            SELECT target_ltrs FROM blinkit_product_targets
             WHERE UPPER(TRIM(item)) = UPPER(TRIM(%s)) AND month = %s AND year = %s
            """,
            [item, month, year],
        )
        if existing and existing[0]["target_ltrs"] is not None:
            return Response({
                "ok": False,
                "error": (
                    f"{item} already has a target for {month:02d}-{year}, and that "
                    "month is closed. Targets can only be corrected during or "
                    "before the reporting month."
                ),
            })

    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO blinkit_product_targets
                (item, item_head, category, month, year, target_ltrs, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT ((UPPER(TRIM(item))), month, year)
            DO UPDATE SET target_ltrs = EXCLUDED.target_ltrs,
                          item_head   = EXCLUDED.item_head,
                          category    = COALESCE(EXCLUDED.category, blinkit_product_targets.category),
                          updated_at  = NOW()
            RETURNING target_ltrs
            """,
            [item, item_head, category, month, year, target],
        )
        saved = cur.fetchone()[0]

    return Response({
        "ok": True,
        "item": item,
        "item_head": item_head,
        "month": month,
        "year": year,
        "target_ltrs": _num(saved),
    })
