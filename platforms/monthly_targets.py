"""Monthly Targets — replicates the `ALL PLATFORM SECONDARY SALES` sheet.

Row lifecycle (see MONTHLY_TARGETS_SPEC.md §2.1 and §3.4):
  * One row per (format, item_head, month, year), created via INSERT only.
  * POST fails with 409 if a row for that tuple already exists — past months
    are never overwritten.
  * A separate refresh endpoint recomputes only the derived columns on the
    current-month row. `targets` and `last_month` are locked after INSERT.

Source routing:
  * SecMaster platforms: blinkit, swiggy, zepto, bigbasket, flipkart (B2C)
  * master_po  platforms: zomato, citymall  (filter status = 'COMPLETED')
  * Out of scope:         amazon, jiomart, flipkart_grocery
"""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime
from decimal import Decimal

from django.db import connection, transaction
from django.db.utils import IntegrityError
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import (
    can_access_platform,
    require,
    user_platform_slugs,
)

from .models import PlatformConfig


# ─── Scope ───

# Platforms whose data lives in the SecMaster view.
SECMASTER_SLUGS = {"blinkit", "swiggy", "zepto", "bigbasket", "flipkart"}

# Platforms sourced from master_po (with status = 'COMPLETED').
MASTER_PO_SLUGS = {"zomato", "citymall"}

# Slugs explicitly out of scope — spec §8.1.
SKIPPED_SLUGS = {"amazon", "jiomart", "flipkart_grocery"}

# All in-scope slugs, in the order the combined dashboard renders.
IN_SCOPE_SLUGS = ("blinkit", "swiggy", "zepto", "bigbasket", "zomato", "citymall", "flipkart")

ITEM_HEADS = ("PREMIUM", "COMMODITY")


def _source_for(slug: str) -> str:
    if slug in SECMASTER_SLUGS:
        return "secmaster"
    if slug in MASTER_PO_SLUGS:
        return "master_po"
    raise ValidationError(
        f"Platform '{slug}' is not supported for Monthly Targets. "
        f"In-scope platforms: {', '.join(sorted(SECMASTER_SLUGS | MASTER_PO_SLUGS))}."
    )


def _get_platform(slug: str) -> PlatformConfig:
    return get_object_or_404(PlatformConfig, slug=slug, is_active=True)


def _ensure_scope(user, slug: str) -> None:
    if not can_access_platform(user, slug):
        raise PermissionDenied(f"Your account is not authorized for the '{slug}' platform.")


def _format_for(p: PlatformConfig) -> str:
    """Canonical `format` string we store in month_targets — matches what
    SecMaster / master_po already use (e.g. 'blinkit', 'big basket')."""
    return (p.po_filter_value or p.slug).strip()


# ─── Month/year parsing ───

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _parse_month_year(body_or_params) -> tuple[int, int]:
    """Accept `month`+`year` (ints) or a combined `month=YYYY-MM` string."""
    raw_month = body_or_params.get("month")
    raw_year = body_or_params.get("year")

    if raw_month and isinstance(raw_month, str) and _MONTH_RE.match(raw_month.strip()):
        y, m = raw_month.strip().split("-")
        return int(m), int(y)
    if raw_month and isinstance(raw_month, str) and _DATE_RE.match(raw_month.strip()):
        y, m, _ = raw_month.strip()[:10].split("-")
        return int(m), int(y)

    try:
        month = int(raw_month)
        year = int(raw_year)
    except (TypeError, ValueError):
        raise ValidationError("`month` (1–12) and `year` (YYYY) are required.")

    if not 1 <= month <= 12:
        raise ValidationError("`month` must be 1–12.")
    if year < 2000 or year > 2100:
        raise ValidationError("`year` looks out of range.")
    return month, year


def _prev_month(month: int, year: int) -> tuple[int, int]:
    return (12, year - 1) if month == 1 else (month - 1, year)


def _is_current_month(month: int, year: int, today: date | None = None) -> bool:
    t = today or date.today()
    return month == t.month and year == t.year


# ─── Source-table reads ───

_MONTH_NAMES = [
    "", "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
    "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
]


def _read_secmaster(fmt: str, item_head: str, month: int, year: int) -> dict:
    """Read (done_ltrs, done_value, latest_date) from SecMaster for one
    (format, item_head, month, year) slice.

    SecMaster schema in practice (see live DB):
      - `month` is TEXT holding the uppercase month name ('APRIL', …).
      - `year` is NUMERIC.
      - `format` is TEXT in uppercase ('BLINKIT', 'BIG BASKET', …).
      - `item_head` is TEXT ('PREMIUM' / 'COMMODITY' / 'OTHER').
      - `ltr_sold` is DOUBLE PRECISION — note the SINGULAR column name.

    Value-column fallback: the sheet's "DONE VALUE" maps to
    `sales_amt_exc` (sales excluding tax). For some platform/SKU-group
    slices the upstream ingestion leaves that column zero even though
    real sales exist — Zepto COMMODITY and the whole Flipkart feed are
    known examples. When `sales_amt_exc` sums to zero we fall back to
    `sales_amt` (tax-inclusive sales) and finally to `amount` (GMV) so
    the dashboard still reflects the business reality. The fallback is
    computed inside a single scalar subquery so it's evaluated once.
    """
    month_name = _MONTH_NAMES[month]
    sql = """
        SELECT
            COALESCE(SUM("ltr_sold"), 0)                                    AS done_ltrs,
            COALESCE(
                NULLIF(SUM("sales_amt_exc"), 0),
                NULLIF(SUM("sales_amt"), 0),
                SUM("amount"),
                0
            )                                                                AS done_value,
            MAX("date")                                                      AS latest_date
        FROM "SecMaster"
        WHERE LOWER(TRIM("format"::text))    = LOWER(TRIM(%s))
          AND UPPER(TRIM("item_head"::text)) = UPPER(TRIM(%s))
          AND UPPER(TRIM("month"::text))     = %s
          AND "year"::numeric                = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [fmt, item_head, month_name, year])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "done_value": Decimal(row[1] or 0),
        "latest_date": row[2],
    }


def _read_master_po(fmt: str, item_head: str, month: int, year: int) -> dict:
    """Read (done_ltrs, done_value, latest_date) from master_po for
    Zomato / CityMall. Filters: status = 'COMPLETED', delivery_month = month,
    year = year, format = fmt, item_head = item_head.

    Note: `delivery_month` is TEXT in the live DB holding uppercase month
    names ('APRIL', …) — same convention as SecMaster. `year` is INTEGER.
    """
    month_name = _MONTH_NAMES[month]
    sql = """
        SELECT
            COALESCE(SUM("total_delivered_liters"), 0)        AS done_ltrs,
            COALESCE(SUM("total_delivered_amt_exclusive"), 0) AS done_value,
            MAX("delivery_date")                              AS latest_date
        FROM "master_po"
        WHERE LOWER(TRIM("format"::text))          = LOWER(TRIM(%s))
          AND UPPER(TRIM("item_head"::text))       = UPPER(TRIM(%s))
          AND UPPER(TRIM("status"::text))          = 'COMPLETED'
          AND UPPER(TRIM("delivery_month"::text))  = %s
          AND "year"                               = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [fmt, item_head, month_name, year])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "done_value": Decimal(row[1] or 0),
        "latest_date": row[2],
    }


def _read_source(slug: str, fmt: str, item_head: str, month: int, year: int) -> dict:
    if _source_for(slug) == "secmaster":
        return _read_secmaster(fmt, item_head, month, year)
    return _read_master_po(fmt, item_head, month, year)


def _read_prev_est_ltr(fmt: str, item_head: str, month: int, year: int) -> Decimal:
    """Look up the prior month's stored `est_ltr` for the same (format,
    item_head). Returns 0 if no prior row exists."""
    pm, py = _prev_month(month, year)
    sql = """
        SELECT COALESCE("est_ltr", 0) FROM month_targets
         WHERE LOWER(TRIM("format"))    = LOWER(TRIM(%s))
           AND UPPER(TRIM(item_head))   = UPPER(TRIM(%s))
           AND month = %s
           AND year  = %s
         LIMIT 1
    """
    with connection.cursor() as cur:
        cur.execute(sql, [fmt, item_head, pm, py])
        row = cur.fetchone()
    return Decimal(row[0]) if row and row[0] is not None else Decimal(0)


# ─── Derived-column math ───

def _days_in_month(month: int, year: int) -> int:
    return calendar.monthrange(year, month)[1]


def _day_of_month(d: date | None, month: int, year: int) -> int:
    """Day-of-month used in the Est.LTR projection. If `d` is missing (no
    source rows yet), fall back to today's day when inside the current
    month, else the full month length (so past-with-no-data estimates
    equal `done_ltrs`)."""
    if d:
        return d.day
    if _is_current_month(month, year):
        return date.today().day
    return _days_in_month(month, year)


def _compute_derived(
    targets: Decimal,
    done_ltrs: Decimal,
    done_value: Decimal,
    latest_date: date | None,
    last_month: Decimal,
    month: int,
    year: int,
) -> dict:
    """Pure function: turn (targets, source aggregates, last_month) into the
    full derived column set. No DB reads.

    Percentages stored as fractions (0.5 = 50%) to match the sheet's NUMERIC
    representation. Division-by-zero cases return None (→ SQL NULL) so the
    UI can render `—`.
    """
    dom = _day_of_month(latest_date, month, year)
    month_days = _days_in_month(month, year)

    targets = Decimal(targets or 0)
    done_ltrs = Decimal(done_ltrs or 0)
    done_value = Decimal(done_value or 0)
    last_month = Decimal(last_month or 0)

    achieved_pct: Decimal | None = None
    if targets > 0:
        achieved_pct = done_ltrs / targets

    if dom > 0:
        est_ltr = (done_ltrs / Decimal(dom)) * Decimal(month_days)
        est_value = (done_value / Decimal(dom)) * Decimal(month_days)
    else:
        est_ltr = Decimal(0)
        est_value = Decimal(0)

    est_ltr_pct: Decimal | None = None
    if targets > 0:
        est_ltr_pct = est_ltr / targets

    growth = est_ltr - last_month

    growth_pct: Decimal | None = None
    if last_month > 0:
        growth_pct = growth / last_month

    return {
        "done_ltrs": done_ltrs,
        "done_value": done_value,
        "date": latest_date,
        "achieved_pct": achieved_pct,
        "est_ltr": est_ltr,
        "est_value": est_value,
        "est_ltr_pct": est_ltr_pct,
        "last_month": last_month,
        "growth": growth,
        "growth_pct": growth_pct,
    }


# ─── Row serialization ───

_ROW_COLS = [
    "id", "format", "type", "item_head", "month", "year", "date",
    "targets", "done_ltrs", "done_value", "achieved_pct",
    "est_ltr", "est_value", "est_ltr_pct",
    "last_month", "growth", "growth_pct",
    "created_at", "updated_at",
]


def _row_to_dict(row: tuple) -> dict:
    d = dict(zip(_ROW_COLS, row))
    # JSON-serializable coercions.
    for k, v in list(d.items()):
        if isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, (date, datetime)):
            d[k] = v.isoformat()
    return d


def _select_row(where_sql: str, params: list) -> dict | None:
    sql = f"""
        SELECT {", ".join(_ROW_COLS)}
          FROM month_targets
         {where_sql}
         LIMIT 1
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


def _select_rows(where_sql: str, params: list, order_sql: str = "") -> list[dict]:
    sql = f"""
        SELECT {", ".join(_ROW_COLS)}
          FROM month_targets
         {where_sql}
         {order_sql}
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        return [_row_to_dict(r) for r in cur.fetchall()]


# ─── Endpoints: per-platform ───

@api_view(["GET"])
@permission_classes([require("platform.month_targets.view")])
def month_targets_list(request, slug: str):
    """GET /api/platform/<slug>/month-targets?month=M&year=Y

    Returns every month_targets row for the platform, optionally filtered
    by month/year. Both PREMIUM and COMMODITY rows are included.
    """
    _ensure_scope(request.user, slug)
    if slug in SKIPPED_SLUGS:
        raise ValidationError(f"Platform '{slug}' is out of scope for Monthly Targets.")
    _source_for(slug)  # raises if unsupported
    p = _get_platform(slug)
    fmt = _format_for(p)

    where = ['LOWER(TRIM("format")) = LOWER(TRIM(%s))']
    params: list = [fmt]

    month_q = request.query_params.get("month")
    year_q = request.query_params.get("year")
    if month_q and year_q:
        m, y = _parse_month_year({"month": month_q, "year": year_q})
        where.append("month = %s AND year = %s")
        params.extend([m, y])

    rows = _select_rows(
        "WHERE " + " AND ".join(where),
        params,
        'ORDER BY "year" DESC, "month" DESC, UPPER(TRIM(item_head)) ASC',
    )
    return Response({
        "data": rows,
        "format": fmt,
        "type": p.sales_type,
        "source": _source_for(slug),
    })


@api_view(["POST"])
@permission_classes([require("platform.month_targets.edit")])
def month_targets_create(request, slug: str):
    """POST /api/platform/<slug>/month-targets

    INSERT-only. Body: {targets, item_head, month, year}. Returns 409 if a
    row already exists for (format, item_head, month, year).
    """
    _ensure_scope(request.user, slug)
    if slug in SKIPPED_SLUGS:
        raise ValidationError(f"Platform '{slug}' is out of scope for Monthly Targets.")
    src = _source_for(slug)
    p = _get_platform(slug)
    fmt = _format_for(p)

    body = request.data or {}
    item_head = str(body.get("item_head") or "").strip().upper()
    if item_head not in ITEM_HEADS:
        raise ValidationError(f"`item_head` must be one of {ITEM_HEADS}.")

    try:
        targets = Decimal(str(body.get("targets", "0")))
    except Exception:
        raise ValidationError("`targets` must be a number.")
    if targets < 0:
        raise ValidationError("`targets` must be >= 0.")

    month, year = _parse_month_year(body)

    # Phase A — INSERT. Read source + prior month → compute → insert.
    source = _read_source(slug, fmt, item_head, month, year)
    last_month = _read_prev_est_ltr(fmt, item_head, month, year)
    derived = _compute_derived(
        targets=targets,
        done_ltrs=source["done_ltrs"],
        done_value=source["done_value"],
        latest_date=source["latest_date"],
        last_month=last_month,
        month=month,
        year=year,
    )

    insert_sql = """
        INSERT INTO month_targets (
            "format", "type", item_head, month, year, "date",
            targets, done_ltrs, done_value, achieved_pct,
            est_ltr, est_value, est_ltr_pct,
            last_month, growth, growth_pct,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            NOW(), NOW()
        )
        RETURNING id
    """
    params = [
        fmt, p.sales_type or "B2B", item_head, month, year, derived["date"],
        targets, derived["done_ltrs"], derived["done_value"], derived["achieved_pct"],
        derived["est_ltr"], derived["est_value"], derived["est_ltr_pct"],
        derived["last_month"], derived["growth"], derived["growth_pct"],
    ]

    try:
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute(insert_sql, params)
            new_id = cur.fetchone()[0]
    except IntegrityError:
        existing = _select_row(
            """WHERE LOWER(TRIM("format")) = LOWER(TRIM(%s))
                 AND UPPER(TRIM(item_head)) = %s
                 AND month = %s AND year = %s""",
            [fmt, item_head, month, year],
        )
        return Response(
            {
                "ok": False,
                "error": (
                    f"A target for {fmt}/{item_head}/{month:02d}-{year} "
                    "has already been set. Targets are set once per month and "
                    "cannot be overwritten."
                ),
                "existing": existing,
            },
            status=409,
        )

    row = _select_row("WHERE id = %s", [new_id])
    return Response({"ok": True, "row": row}, status=201)


@api_view(["POST"])
@permission_classes([require("platform.month_targets.edit")])
def month_targets_refresh(request, slug: str, row_id: int):
    """POST /api/platform/<slug>/month-targets/<id>/refresh

    Phase B. Recomputes the derived columns on an existing current-month
    row. `targets` and `last_month` are not touched. Rejects with 400 if
    the row is for a closed calendar month.
    """
    _ensure_scope(request.user, slug)
    if slug in SKIPPED_SLUGS:
        raise ValidationError(f"Platform '{slug}' is out of scope for Monthly Targets.")
    _source_for(slug)
    p = _get_platform(slug)
    fmt = _format_for(p)

    existing = _select_row(
        """WHERE id = %s
             AND LOWER(TRIM("format")) = LOWER(TRIM(%s))""",
        [row_id, fmt],
    )
    if not existing:
        raise ValidationError(f"No month_targets row {row_id} for platform '{slug}'.")

    month = int(existing["month"])
    year = int(existing["year"])
    item_head = str(existing["item_head"] or "").strip().upper()

    if not _is_current_month(month, year):
        raise ValidationError(
            f"Row is for {month:02d}-{year}, which is closed. "
            "Refresh only applies to rows in the current calendar month."
        )

    source = _read_source(slug, fmt, item_head, month, year)
    last_month = Decimal(existing["last_month"] or 0)  # locked, re-use stored
    targets = Decimal(existing["targets"] or 0)        # locked, re-use stored

    derived = _compute_derived(
        targets=targets,
        done_ltrs=source["done_ltrs"],
        done_value=source["done_value"],
        latest_date=source["latest_date"],
        last_month=last_month,
        month=month,
        year=year,
    )

    update_sql = """
        UPDATE month_targets
           SET "date"       = %s,
               done_ltrs    = %s,
               done_value   = %s,
               achieved_pct = %s,
               est_ltr      = %s,
               est_value    = %s,
               est_ltr_pct  = %s,
               growth       = %s,
               growth_pct   = %s,
               updated_at   = NOW()
         WHERE id = %s
    """
    with connection.cursor() as cur:
        cur.execute(update_sql, [
            derived["date"],
            derived["done_ltrs"], derived["done_value"], derived["achieved_pct"],
            derived["est_ltr"], derived["est_value"], derived["est_ltr_pct"],
            derived["growth"], derived["growth_pct"],
            row_id,
        ])

    row = _select_row("WHERE id = %s", [row_id])
    return Response({"ok": True, "row": row})


# ─── Target correction (UPDATE + audit log) ───

def _insert_log(row: dict, *, change_type: str, reason: str | None, user) -> None:
    """Snapshot the pre-edit row into `month_target_logs`. Called before
    any UPDATE or DELETE so the audit table always reflects what the row
    LOOKED LIKE before the change."""
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO month_target_logs (
                month_target_id, "format", "type", item_head, month, year, "date",
                targets, done_ltrs, done_value, achieved_pct,
                est_ltr, est_value, est_ltr_pct,
                last_month, growth, growth_pct,
                change_type, reason,
                changed_by_id, changed_by_email, changed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, NOW()
            )
            """,
            [
                row.get("id"), row.get("format"), row.get("type"), row.get("item_head"),
                row.get("month"), row.get("year"), row.get("date"),
                row.get("targets"), row.get("done_ltrs"), row.get("done_value"), row.get("achieved_pct"),
                row.get("est_ltr"), row.get("est_value"), row.get("est_ltr_pct"),
                row.get("last_month"), row.get("growth"), row.get("growth_pct"),
                change_type, reason,
                getattr(user, "id", None),
                getattr(user, "email", None) or getattr(user, "username", None),
            ],
        )


@api_view(["POST"])
@permission_classes([require("platform.month_targets.edit")])
def month_targets_update(request, slug: str, row_id: int):
    """POST /api/platform/<slug>/month-targets/<id>/update

    Correct a wrong `targets` value on an existing current-month row.

    Flow:
      1. Load the row. Reject with 400 if it's a closed month — past
         months stay frozen.
      2. Snapshot the PRE-EDIT row into `month_target_logs` with
         `change_type='UPDATE'` (plus optional `reason` from the body
         and the editing user's id/email for audit).
      3. Recompute all derived columns with the NEW `targets` (done_ltrs
         and source-data aggregates stay the same; the ratios and
         projections change).
      4. UPDATE `month_targets` with the new targets + recomputed
         derived columns. `last_month` is NOT touched — it was locked at
         INSERT time.

    Body: {"targets": <number>, "reason": "<optional>"}
    """
    _ensure_scope(request.user, slug)
    if slug in SKIPPED_SLUGS:
        raise ValidationError(f"Platform '{slug}' is out of scope for Monthly Targets.")
    _source_for(slug)
    p = _get_platform(slug)
    fmt = _format_for(p)

    existing = _select_row(
        """WHERE id = %s
             AND LOWER(TRIM("format")) = LOWER(TRIM(%s))""",
        [row_id, fmt],
    )
    if not existing:
        raise ValidationError(f"No month_targets row {row_id} for platform '{slug}'.")

    month = int(existing["month"])
    year = int(existing["year"])
    item_head = str(existing["item_head"] or "").strip().upper()

    if not _is_current_month(month, year):
        raise ValidationError(
            f"Row is for {month:02d}-{year}, which is closed. "
            "Targets can only be corrected during the reporting month; "
            "historical rows are frozen for audit integrity."
        )

    body = request.data or {}
    try:
        new_targets = Decimal(str(body.get("targets")))
    except Exception:
        raise ValidationError("`targets` must be a number.")
    if new_targets < 0:
        raise ValidationError("`targets` must be >= 0.")

    reason = str(body.get("reason") or "").strip() or None

    old_targets = Decimal(existing["targets"] or 0)
    if new_targets == old_targets:
        return Response({
            "ok": False,
            "error": f"New target ({new_targets}) equals the existing value; nothing to update.",
        }, status=400)

    # Re-read the source data so the derived columns reflect the most
    # recent sales alongside the corrected target.
    source = _read_source(slug, fmt, item_head, month, year)
    last_month = Decimal(existing["last_month"] or 0)  # still locked

    derived = _compute_derived(
        targets=new_targets,
        done_ltrs=source["done_ltrs"],
        done_value=source["done_value"],
        latest_date=source["latest_date"],
        last_month=last_month,
        month=month,
        year=year,
    )

    with transaction.atomic():
        # Step 2: audit log of the OLD (wrong) state.
        _insert_log(existing, change_type="UPDATE", reason=reason, user=request.user)

        # Step 4: overwrite the main row with the corrected value + fresh
        # derived columns.
        with connection.cursor() as cur:
            cur.execute(
                """
                UPDATE month_targets
                   SET targets      = %s,
                       "date"       = %s,
                       done_ltrs    = %s,
                       done_value   = %s,
                       achieved_pct = %s,
                       est_ltr      = %s,
                       est_value    = %s,
                       est_ltr_pct  = %s,
                       growth       = %s,
                       growth_pct   = %s,
                       updated_at   = NOW()
                 WHERE id = %s
                """,
                [
                    new_targets,
                    derived["date"],
                    derived["done_ltrs"], derived["done_value"], derived["achieved_pct"],
                    derived["est_ltr"], derived["est_value"], derived["est_ltr_pct"],
                    derived["growth"], derived["growth_pct"],
                    row_id,
                ],
            )

    row = _select_row("WHERE id = %s", [row_id])
    return Response({
        "ok": True,
        "row": row,
        "previous_targets": float(old_targets),
    })


# ─── Endpoints: combined dashboard ───

@api_view(["GET"])
@permission_classes([require("platform.month_targets.view")])
def month_targets_dashboard(request):
    """GET /api/platform/month-targets/dashboard?month=M&year=Y

    Cross-platform roll-up for a single month. Returns rows grouped into
    PREMIUM and COMMODITY blocks, each with a Grand Total computed from
    the sums (percentages re-derived, not averaged).
    """
    month, year = _parse_month_year(request.query_params)

    # Scope to platforms the user can see, intersected with in-scope slugs.
    allowed_slugs = set(user_platform_slugs(request.user)) & set(IN_SCOPE_SLUGS)
    if not allowed_slugs:
        return Response({"premium": {"rows": [], "total": _empty_total()},
                         "commodity": {"rows": [], "total": _empty_total()},
                         "month": month, "year": year})

    # Preserve the sheet's display order.
    ordered = [s for s in IN_SCOPE_SLUGS if s in allowed_slugs]
    platforms = {p.slug: p for p in PlatformConfig.objects.filter(slug__in=ordered)}

    # One query per SKU-group. Using LOWER/TRIM on format so the row joins
    # regardless of stored casing.
    formats = [_format_for(platforms[s]) for s in ordered if s in platforms]
    if not formats:
        return Response({"premium": {"rows": [], "total": _empty_total()},
                         "commodity": {"rows": [], "total": _empty_total()},
                         "month": month, "year": year})

    result = {}
    for item_head in ITEM_HEADS:
        params = [month, year, item_head] + formats
        placeholder = ",".join(["LOWER(TRIM(%s))"] * len(formats))
        sql = f"""
            SELECT {", ".join(_ROW_COLS)}
              FROM month_targets
             WHERE month = %s AND year = %s
               AND UPPER(TRIM(item_head)) = UPPER(TRIM(%s))
               AND LOWER(TRIM("format")) IN ({placeholder})
        """
        with connection.cursor() as cur:
            cur.execute(sql, params)
            raw = [_row_to_dict(r) for r in cur.fetchall()]

        # Re-order rows to match the sheet's platform order.
        by_format = {r["format"].strip().lower(): r for r in raw}
        rows = []
        for slug in ordered:
            p = platforms.get(slug)
            if not p:
                continue
            fmt = _format_for(p).lower()
            row = by_format.get(fmt)
            if row:
                row["slug"] = slug
                row["platform_name"] = p.name
                rows.append(row)
            else:
                # Placeholder row so the UI can still show the platform with
                # a "No target set" hint.
                rows.append({
                    "id": None, "slug": slug, "platform_name": p.name,
                    "format": _format_for(p), "type": p.sales_type or "B2B",
                    "item_head": item_head, "month": month, "year": year,
                    "date": None, "targets": None,
                    "done_ltrs": None, "done_value": None, "achieved_pct": None,
                    "est_ltr": None, "est_value": None, "est_ltr_pct": None,
                    "last_month": None, "growth": None, "growth_pct": None,
                })

        result[item_head.lower()] = {
            "rows": rows,
            "total": _grand_total(rows),
        }

    result["month"] = month
    result["year"] = year
    return Response(result)


def _empty_total() -> dict:
    return {
        "targets": 0, "done_ltrs": 0, "done_value": 0,
        "est_ltr": 0, "est_value": 0, "last_month": 0, "growth": 0,
        "achieved_pct": None, "est_ltr_pct": None, "growth_pct": None,
    }


def _grand_total(rows: list[dict]) -> dict:
    """Per spec §5.3: sums for absolutes; percentages re-derived from totals."""
    s_tgt = sum(_num(r.get("targets")) for r in rows)
    s_done_l = sum(_num(r.get("done_ltrs")) for r in rows)
    s_done_v = sum(_num(r.get("done_value")) for r in rows)
    s_est_l = sum(_num(r.get("est_ltr")) for r in rows)
    s_est_v = sum(_num(r.get("est_value")) for r in rows)
    s_lm = sum(_num(r.get("last_month")) for r in rows)
    s_growth = s_est_l - s_lm

    return {
        "targets": s_tgt,
        "done_ltrs": s_done_l,
        "done_value": s_done_v,
        "est_ltr": s_est_l,
        "est_value": s_est_v,
        "last_month": s_lm,
        "growth": s_growth,
        "achieved_pct": (s_done_l / s_tgt) if s_tgt else None,
        "est_ltr_pct": (s_est_l / s_tgt) if s_tgt else None,
        "growth_pct": (s_growth / s_lm) if s_lm else None,
    }


def _num(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
