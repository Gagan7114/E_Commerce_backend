"""Primary Monthly Targets sourced from ``master_po``.

This module intentionally uses its own table/API so the existing secondary
monthly target sheet and dates remain untouched.
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

from accounts.permissions import can_access_platform, require, user_platform_slugs

from .models import PlatformConfig


IN_SCOPE_SLUGS = {
    "amazon",
    "blinkit",
    "swiggy",
    "zepto",
    "bigbasket",
    "zomato",
    "citymall",
    "flipkart",
    "flipkart_grocery",
    "amazon_mp",
}
SKIPPED_SLUGS = {"jiomart"}

DEFAULT_ITEM_HEADS = ("PREMIUM", "COMMODITY")
FLIPKART_GROCERY_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")
DASHBOARD_ITEM_HEADS = ("PREMIUM", "COMMODITY")
PRIMARY_DASHBOARD_SLUGS = (
    "blinkit",
    "swiggy",
    "zepto",
    "bigbasket",
    "flipkart_grocery",
    "zomato",
    "citymall",
)

SPECIAL_DASHBOARD_ROWS = (
    {
        "key": "amazon_secondary",
        "format": "AMAZON SECONDARY",
        "platform_name": "Amazon",
        "type": "sec",
        "access_slug": "amazon",
        "logo_slug": "amazon",
        "source": "amazon_sec_range_master_view",
        "target_source": "month_targets",
        "target_format": "AMAZON",
    },
    {
        "key": "amazon_mp",
        "format": "AMAZON MP",
        "platform_name": "Amazon MP",
        "type": "sec",
        "access_slug": "amazon",
        "logo_slug": "amazon",
        "source": "amazon_mp_master",
    },
    {
        "key": "flipkart_secondary",
        "format": "FLIPKART SECONDARY",
        "platform_name": "Flipkart MP",
        "type": "sec",
        "access_slug": "flipkart",
        "logo_slug": "flipkart",
        "source": "flipkart_secondary_all",
        "target_source": "month_targets",
        "target_format": "FLIPKART",
    },
)

# Per-platform slugs whose Primary-sheet target is the SAME single target set on
# the Secondary sheet (stored in month_targets) — one target, shown on both
# sheets. Maps slug -> (month_targets format, primary done source_format). The
# all-platforms dashboard already honours target_source for these; this lets the
# per-platform list endpoint surface the same shared target instead of an empty
# primary_month_targets row.
_PRIMARY_TARGET_FROM_SECONDARY = {
    "amazon": ("AMAZON", "AMAZON SECONDARY"),
}

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_MONTH_NAMES = [
    "",
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
]


def _item_heads_for(slug: str) -> tuple[str, ...]:
    if slug == "flipkart_grocery":
        return FLIPKART_GROCERY_ITEM_HEADS
    return DEFAULT_ITEM_HEADS


def _get_platform(slug: str) -> PlatformConfig:
    return get_object_or_404(PlatformConfig, slug=slug, is_active=True)


def _ensure_scope(user, slug: str) -> None:
    access_slug = "amazon" if slug == "amazon_mp" else slug
    if not can_access_platform(user, access_slug):
        raise PermissionDenied(f"Your account is not authorized for the '{slug}' platform.")
    if slug in SKIPPED_SLUGS or slug not in IN_SCOPE_SLUGS:
        raise ValidationError(f"Platform '{slug}' is out of scope for Primary Monthly Targets.")


def _format_for(p: PlatformConfig) -> str:
    return (p.po_filter_value or p.slug).strip().upper()


def _platform_target_meta(slug: str) -> dict:
    if slug == "amazon_mp":
        return {
            "format": "AMAZON MP",
            "type": "sec",
            "source": "amazon_mp_master",
        }
    p = _get_platform(slug)
    return {
        "format": _format_for(p),
        "type": "prim",
        "source": "master_po",
    }


def _format_key(value: str | None) -> str:
    return str(value or "").strip().upper()


def _parse_month_year(body_or_params) -> tuple[int, int]:
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
        raise ValidationError("`month` (1-12) and `year` (YYYY) are required.")

    if not 1 <= month <= 12:
        raise ValidationError("`month` must be 1-12.")
    if year < 2000 or year > 2100:
        raise ValidationError("`year` looks out of range.")
    return month, year


def _parse_month_year_or_current(body_or_params) -> tuple[int, int]:
    raw_month = body_or_params.get("month") if body_or_params else None
    raw_year = body_or_params.get("year") if body_or_params else None
    if raw_month or raw_year:
        return _parse_month_year(body_or_params)
    today = date.today()
    return today.month, today.year


def _is_current_month(month: int, year: int, today: date | None = None) -> bool:
    t = today or date.today()
    return month == t.month and year == t.year


def _days_in_month(month: int, year: int) -> int:
    return calendar.monthrange(year, month)[1]


def _day_of_month(d: date | None, month: int, year: int) -> int:
    if d:
        return d.day
    if _is_current_month(month, year):
        return date.today().day
    return _days_in_month(month, year)


def _read_master_po(fmt: str, item_head: str, month: int, year: int) -> dict:
    """Read delivered litres and latest delivery date from master_po."""
    month_name = _MONTH_NAMES[month]
    sql = """
        SELECT
            COALESCE(SUM(COALESCE("total_delivered_liters", 0)), 0) AS done_ltrs,
            -- Latest delivery date, ignoring future-dated POs (scheduled but not
            -- yet "today"); matches the Primary Dashboard's max date.
            MAX("delivery_date") FILTER (WHERE "delivery_date" <= CURRENT_DATE) AS latest_date
        FROM "master_po"
        WHERE LOWER(TRIM("format"::text))         = LOWER(TRIM(%s))
          AND UPPER(TRIM("item_head"::text))      = UPPER(TRIM(%s))
          AND UPPER(TRIM("delivery_month"::text)) = %s
          AND "delivered_year"                    = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [fmt, item_head, month_name, year])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "latest_date": row[1],
    }


def _read_amazon_primary_max_date(month: int, year: int) -> date | None:
    """Latest Amazon PRIMARY (Amazon PO) order date for the month, capped at today
    — mirrors the Amazon Primary Dashboard's max date. The Amazon prim-target row
    surfaces THIS date even though its Done Ltrs come from Amazon secondary."""
    sql = """
        SELECT MAX("order_date"::date)
        FROM reporting."Amazon PO"
        WHERE po_month::integer = %s
          AND "year"::integer    = %s
          AND "order_date"::date <= CURRENT_DATE
    """
    with connection.cursor() as cur:
        cur.execute(sql, [month, year])
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def _read_amazon_secondary(item_head: str, month: int, year: int) -> dict:
    """Read Amazon Secondary from the latest range snapshot in the month."""
    month_name = _MONTH_NAMES[month]
    month_day_suffix = f"%-{month_name}"
    sql = """
        WITH filtered AS MATERIALIZED (
            SELECT
                "shipped_litres",
                NULLIF(split_part("month_day"::text, '-', 1), '')::int AS day_num
            FROM "amazon_sec_range_master_view"
            WHERE UPPER(TRIM("item_head"::text)) = UPPER(TRIM(%s))
              AND UPPER(TRIM("month_day"::text)) LIKE %s
              AND "year" = %s
        )
        SELECT
            COALESCE(SUM("shipped_litres"), 0) AS done_ltrs,
            MAX(day_num)                       AS max_day
        FROM filtered
        WHERE day_num = (SELECT MAX(day_num) FROM filtered)
    """
    with connection.cursor() as cur:
        cur.execute(sql, [item_head, month_day_suffix, year])
        row = cur.fetchone()

    max_day = row[1] if row else None
    try:
        latest_date = date(year, month, int(max_day)) if max_day else None
    except (TypeError, ValueError):
        latest_date = None
    return {
        "done_ltrs": Decimal(row[0] or 0) if row else Decimal(0),
        # Date follows the Amazon PRIMARY dashboard (product decision); falls back
        # to the secondary snapshot date when primary has no data for the month.
        "latest_date": _read_amazon_primary_max_date(month, year) or latest_date,
    }


def _read_amazon_mp(item_head: str, month: int, year: int) -> dict:
    month_name = _MONTH_NAMES[month]
    sql = """
        SELECT
            COALESCE(SUM("delivered_ltr"), 0) AS done_ltrs,
            MAX(
                CASE
                    WHEN "shipment_date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}'
                        THEN to_timestamp("shipment_date", 'DD/MM/YY HH24:MI')::date
                    WHEN "shipment_date" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                        THEN "shipment_date"::date
                    WHEN "shipment_date" ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}'
                        THEN to_date("shipment_date", 'DD-MM-YYYY')
                    ELSE NULL
                END
            ) AS latest_date
        FROM "amazon_mp_master"
        WHERE UPPER(TRIM("item_head"::text)) = UPPER(TRIM(%s))
          AND UPPER(TRIM("shipment_month"::text)) = %s
          AND "shipment_year" = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [item_head, month_name, year])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "latest_date": row[1],
    }


def _read_flipkart_secondary(item_head: str, month: int, year: int) -> dict:
    month_name = _MONTH_NAMES[month]
    sql = """
        SELECT
            COALESCE(SUM("ltr_sold"), 0) AS done_ltrs,
            MAX("Order Date")            AS latest_date
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("item_head"::text)) = UPPER(TRIM(%s))
          AND UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [item_head, month_name, year])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "latest_date": row[1],
    }


def _read_primary_target_source(fmt: str, item_head: str, month: int, year: int) -> dict:
    key = _format_key(fmt)
    if key == "AMAZON SECONDARY":
        return _read_amazon_secondary(item_head, month, year)
    if key == "AMAZON MP":
        return _read_amazon_mp(item_head, month, year)
    if key == "FLIPKART SECONDARY":
        return _read_flipkart_secondary(item_head, month, year)
    return _read_master_po(fmt, item_head, month, year)


def _read_master_po_many(formats: list[str], item_heads: tuple[str, ...], month: int, year: int) -> dict[tuple[str, str], dict]:
    if not formats:
        return {}
    month_name = _MONTH_NAMES[month]
    format_placeholder = ",".join(["LOWER(TRIM(%s))"] * len(formats))
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        SELECT
            UPPER(TRIM("format"::text))    AS fmt,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM(COALESCE("total_delivered_liters", 0)), 0) AS done_ltrs,
            MAX("delivery_date") FILTER (WHERE "delivery_date" <= CURRENT_DATE) AS latest_date
        FROM "master_po"
        WHERE LOWER(TRIM("format"::text)) IN ({format_placeholder})
          AND UPPER(TRIM("item_head"::text)) IN ({item_placeholder})
          AND UPPER(TRIM("delivery_month"::text)) = %s
          AND "delivered_year" = %s
        GROUP BY UPPER(TRIM("format"::text)), UPPER(TRIM("item_head"::text))
    """
    with connection.cursor() as cur:
        cur.execute(sql, formats + list(item_heads) + [month_name, year])
        rows = cur.fetchall()
    return {
        (_format_key(fmt), _format_key(item_head)): {
            "done_ltrs": Decimal(done_ltrs or 0),
            "latest_date": latest_date,
        }
        for fmt, item_head, done_ltrs, latest_date in rows
    }


def _read_amazon_secondary_many(item_heads: tuple[str, ...], month: int, year: int) -> dict[tuple[str, str], dict]:
    month_day_suffix = f"%-{_MONTH_NAMES[month]}"
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        WITH filtered AS MATERIALIZED (
            SELECT
                UPPER(TRIM("item_head"::text)) AS item_head,
                "shipped_litres",
                NULLIF(split_part("month_day"::text, '-', 1), '')::int AS day_num
            FROM "amazon_sec_range_master_view"
            WHERE UPPER(TRIM("item_head"::text)) IN ({item_placeholder})
              AND UPPER(TRIM("month_day"::text)) LIKE %s
              AND "year" = %s
        ),
        latest AS (
            SELECT item_head, MAX(day_num) AS max_day
              FROM filtered
             GROUP BY item_head
        )
        SELECT
            f.item_head,
            COALESCE(SUM(f."shipped_litres"), 0) AS done_ltrs,
            l.max_day
        FROM filtered f
        JOIN latest l
          ON l.item_head = f.item_head
         AND l.max_day = f.day_num
        GROUP BY f.item_head, l.max_day
    """
    with connection.cursor() as cur:
        cur.execute(sql, list(item_heads) + [month_day_suffix, year])
        rows = cur.fetchall()

    # Date follows the Amazon PRIMARY dashboard (product decision); computed once
    # for the month and applied to every item head (Done Ltrs stay secondary).
    primary_max = _read_amazon_primary_max_date(month, year)
    result: dict[tuple[str, str], dict] = {}
    for item_head, done_ltrs, max_day in rows:
        try:
            latest_date = date(year, month, int(max_day)) if max_day else None
        except (TypeError, ValueError):
            latest_date = None
        result[("AMAZON SECONDARY", _format_key(item_head))] = {
            "done_ltrs": Decimal(done_ltrs or 0),
            "latest_date": primary_max or latest_date,
        }
    return result


def _read_amazon_mp_many(item_heads: tuple[str, ...], month: int, year: int) -> dict[tuple[str, str], dict]:
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("delivered_ltr"), 0) AS done_ltrs,
            MAX(
                CASE
                    WHEN "shipment_date" ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{2}}'
                        THEN to_timestamp("shipment_date", 'DD/MM/YY HH24:MI')::date
                    WHEN "shipment_date" ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                        THEN "shipment_date"::date
                    WHEN "shipment_date" ~ '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}'
                        THEN to_date("shipment_date", 'DD-MM-YYYY')
                    ELSE NULL
                END
            ) AS latest_date
        FROM "amazon_mp_master"
        WHERE UPPER(TRIM("item_head"::text)) IN ({item_placeholder})
          AND UPPER(TRIM("shipment_month"::text)) = %s
          AND "shipment_year" = %s
        GROUP BY UPPER(TRIM("item_head"::text))
    """
    with connection.cursor() as cur:
        cur.execute(sql, list(item_heads) + [_MONTH_NAMES[month], year])
        rows = cur.fetchall()
    return {
        ("AMAZON MP", _format_key(item_head)): {
            "done_ltrs": Decimal(done_ltrs or 0),
            "latest_date": latest_date,
        }
        for item_head, done_ltrs, latest_date in rows
    }


def _read_flipkart_secondary_many(item_heads: tuple[str, ...], month: int, year: int) -> dict[tuple[str, str], dict]:
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ltr_sold"), 0) AS done_ltrs,
            MAX("Order Date") AS latest_date
        FROM "flipkart_secondary_all"
        WHERE UPPER(TRIM("item_head"::text)) IN ({item_placeholder})
          AND UPPER(TRIM("month"::text)) = %s
          AND "year" = %s
        GROUP BY UPPER(TRIM("item_head"::text))
    """
    with connection.cursor() as cur:
        cur.execute(sql, list(item_heads) + [_MONTH_NAMES[month], year])
        rows = cur.fetchall()
    return {
        ("FLIPKART SECONDARY", _format_key(item_head)): {
            "done_ltrs": Decimal(done_ltrs or 0),
            "latest_date": latest_date,
        }
        for item_head, done_ltrs, latest_date in rows
    }


def _compute_derived(
    targets: Decimal,
    done_ltrs: Decimal,
    latest_date: date | None,
    month: int,
    year: int,
) -> dict:
    dom = _day_of_month(latest_date, month, year)
    month_days = _days_in_month(month, year)

    targets = Decimal(targets or 0)
    done_ltrs = Decimal(done_ltrs or 0)

    achieved_pct = done_ltrs / targets if targets > 0 else None
    est_ltr = (done_ltrs / Decimal(dom)) * Decimal(month_days) if dom > 0 else Decimal(0)
    est_ltr_pct = est_ltr / targets if targets > 0 else None
    drr = done_ltrs / Decimal(dom) if dom > 0 else Decimal(0)
    pending_ltr = targets - done_ltrs
    if pending_ltr < 0:
        pending_ltr = Decimal(0)

    remaining_days = month_days - dom
    if _is_current_month(month, year):
        remaining_days = max(remaining_days, 1)
    require_drr = (
        pending_ltr / Decimal(remaining_days)
        if remaining_days > 0
        else Decimal(0)
    )
    dp_ltrs = done_ltrs + pending_ltr

    return {
        "done_ltrs": done_ltrs,
        "date": latest_date,
        "achieved_pct": achieved_pct,
        "est_ltr": est_ltr,
        "est_ltr_pct": est_ltr_pct,
        "drr": drr,
        "require_drr": require_drr,
        "pending_ltr": pending_ltr,
        "dp_ltrs": dp_ltrs,
    }


_ROW_COLS = [
    "id",
    "format",
    "type",
    "item_head",
    "month",
    "year",
    "date",
    "targets",
    "done_ltrs",
    "achieved_pct",
    "est_ltr",
    "est_ltr_pct",
    "drr",
    "require_drr",
    "pending_ltr",
    "dp_ltrs",
    "created_at",
    "updated_at",
]


def _row_to_dict(row: tuple) -> dict:
    d = dict(zip(_ROW_COLS, row))
    for k, v in list(d.items()):
        if isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, (date, datetime)):
            d[k] = v.isoformat()
    return d


def _select_row(where_sql: str, params: list) -> dict | None:
    sql = f"""
        SELECT {", ".join(_ROW_COLS)}
          FROM primary_month_targets
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
          FROM primary_month_targets
         {where_sql}
         {order_sql}
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        return [_row_to_dict(r) for r in cur.fetchall()]


_REFRESH_UPDATE_SQL = """
    UPDATE primary_month_targets
       SET "date"       = %s,
           done_ltrs    = %s,
           achieved_pct = %s,
           est_ltr      = %s,
           est_ltr_pct  = %s,
           drr          = %s,
           require_drr  = %s,
           pending_ltr  = %s,
           dp_ltrs      = %s,
           updated_at   = NOW()
     WHERE id = %s
"""


def _refresh_existing_row(row: dict, fmt: str | None = None) -> dict:
    row_id = int(row["id"])
    month = int(row["month"])
    year = int(row["year"])
    item_head = str(row["item_head"] or "").strip().upper()
    source_format = fmt or str(row.get("format") or "").strip()

    source = _read_primary_target_source(source_format, item_head, month, year)
    derived = _compute_derived(
        targets=Decimal(str(row["targets"] or 0)),
        done_ltrs=source["done_ltrs"],
        latest_date=source["latest_date"],
        month=month,
        year=year,
    )

    with connection.cursor() as cur:
        cur.execute(
            _REFRESH_UPDATE_SQL,
            [
                derived["date"],
                derived["done_ltrs"],
                derived["achieved_pct"],
                derived["est_ltr"],
                derived["est_ltr_pct"],
                derived["drr"],
                derived["require_drr"],
                derived["pending_ltr"],
                derived["dp_ltrs"],
                row_id,
            ],
        )

    return _select_row("WHERE id = %s", [row_id])


def _refresh_format_rows(fmt: str, month: int, year: int) -> list[dict]:
    rows = _select_rows(
        """WHERE LOWER(TRIM("format")) = LOWER(TRIM(%s))
             AND month = %s AND year = %s""",
        [fmt, month, year],
        'ORDER BY UPPER(TRIM(item_head)) ASC',
    )
    return [_refresh_existing_row(row, fmt) for row in rows]


@api_view(["GET"])
@permission_classes([require("platform.month_targets.view")])
def primary_month_targets_list(request, slug: str):
    _ensure_scope(request.user, slug)
    meta = _platform_target_meta(slug)
    fmt = meta["format"]

    # Platforms whose target is the single shared value set on the Secondary
    # sheet (e.g. Amazon) read it from month_targets, so the Primary per-platform
    # page shows the same number instead of an empty primary_month_targets row.
    sec_map = _PRIMARY_TARGET_FROM_SECONDARY.get(slug)
    if sec_map:
        sec_fmt, source_format = sec_map
        m, y = _parse_month_year_or_current({
            "month": request.query_params.get("month"),
            "year": request.query_params.get("year"),
        })
        rows = _list_secondary_sourced_targets(slug, sec_fmt, source_format, m, y)
        return Response({
            "data": rows,
            "format": fmt,
            "type": meta["type"],
            "source": meta["source"],
        })

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
        "type": meta["type"],
        "source": meta["source"],
    })


@api_view(["POST"])
@permission_classes([require("target_sheet.edit")])
def primary_month_targets_create(request, slug: str):
    _ensure_scope(request.user, slug)
    meta = _platform_target_meta(slug)
    fmt = meta["format"]

    body = request.data or {}
    item_head = str(body.get("item_head") or "").strip().upper()
    allowed_item_heads = _item_heads_for(slug)
    if item_head not in allowed_item_heads:
        raise ValidationError(f"`item_head` must be one of {allowed_item_heads}.")

    try:
        targets = Decimal(str(body.get("targets", "0")))
    except Exception:
        raise ValidationError("`targets` must be a number.")
    if targets < 0:
        raise ValidationError("`targets` must be >= 0.")

    month, year = _parse_month_year(body)
    source = _read_primary_target_source(fmt, item_head, month, year)
    derived = _compute_derived(
        targets=targets,
        done_ltrs=source["done_ltrs"],
        latest_date=source["latest_date"],
        month=month,
        year=year,
    )

    insert_sql = """
        INSERT INTO primary_month_targets (
            "format", "type", item_head, month, year, "date",
            targets, done_ltrs, achieved_pct,
            est_ltr, est_ltr_pct, drr, require_drr, pending_ltr, dp_ltrs,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            NOW(), NOW()
        )
        RETURNING id
    """
    params = [
        fmt,
        meta["type"],
        item_head,
        month,
        year,
        derived["date"],
        targets,
        derived["done_ltrs"],
        derived["achieved_pct"],
        derived["est_ltr"],
        derived["est_ltr_pct"],
        derived["drr"],
        derived["require_drr"],
        derived["pending_ltr"],
        derived["dp_ltrs"],
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
                    f"A primary target for {fmt}/{item_head}/{month:02d}-{year} "
                    "has already been set."
                ),
                "existing": existing,
            },
            status=409,
        )

    row = _select_row("WHERE id = %s", [new_id])
    return Response({"ok": True, "row": row}, status=201)


@api_view(["POST"])
@permission_classes([require("target_sheet.edit")])
def primary_month_targets_refresh_platform(request, slug: str):
    _ensure_scope(request.user, slug)
    meta = _platform_target_meta(slug)
    fmt = meta["format"]

    body = request.data if request.data else request.query_params
    month, year = _parse_month_year_or_current(body)

    with transaction.atomic():
        rows = _refresh_format_rows(fmt, month, year)

    return Response({
        "ok": True,
        "month": month,
        "year": year,
        "updated": len(rows),
        "rows": rows,
        "format": fmt,
        "type": meta["type"],
        "source": meta["source"],
    })


def _insert_log(
    row: dict,
    *,
    reason: str | None,
    user,
    new_targets: Decimal,
) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO primary_month_target_logs (
                primary_month_target_id, "format", "type", item_head,
                month, year, "date", targets, new_targets,
                done_ltrs, achieved_pct, est_ltr, est_ltr_pct,
                drr, require_drr, pending_ltr, dp_ltrs,
                change_type, reason, changed_by_id, changed_by_email, changed_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                'UPDATE', %s, %s, %s, NOW()
            )
            """,
            [
                row.get("id"),
                row.get("format"),
                row.get("type"),
                row.get("item_head"),
                row.get("month"),
                row.get("year"),
                row.get("date"),
                row.get("targets"),
                new_targets,
                row.get("done_ltrs"),
                row.get("achieved_pct"),
                row.get("est_ltr"),
                row.get("est_ltr_pct"),
                row.get("drr"),
                row.get("require_drr"),
                row.get("pending_ltr"),
                row.get("dp_ltrs"),
                reason,
                getattr(user, "id", None),
                getattr(user, "email", None) or getattr(user, "username", None),
            ],
        )


@api_view(["POST"])
@permission_classes([require("target_sheet.edit")])
def primary_month_targets_update(request, slug: str, row_id: int):
    _ensure_scope(request.user, slug)
    meta = _platform_target_meta(slug)
    fmt = meta["format"]

    existing = _select_row(
        """WHERE id = %s
             AND LOWER(TRIM("format")) = LOWER(TRIM(%s))""",
        [row_id, fmt],
    )
    if not existing:
        raise ValidationError(f"No primary_month_targets row {row_id} for platform '{slug}'.")

    month = int(existing["month"])
    year = int(existing["year"])
    item_head = str(existing["item_head"] or "").strip().upper()

    if not _is_current_month(month, year):
        raise ValidationError(
            f"Row is for {month:02d}-{year}, which is closed. "
            "Targets can only be corrected during the reporting month."
        )

    body = request.data or {}
    try:
        new_targets = Decimal(str(body.get("targets")))
    except Exception:
        raise ValidationError("`targets` must be a number.")
    if new_targets < 0:
        raise ValidationError("`targets` must be >= 0.")

    old_targets = Decimal(existing["targets"] or 0)
    if new_targets == old_targets:
        return Response(
            {
                "ok": False,
                "error": f"New target ({new_targets}) equals the existing value; nothing to update.",
            },
            status=400,
        )

    source = _read_primary_target_source(fmt, item_head, month, year)
    derived = _compute_derived(
        targets=new_targets,
        done_ltrs=source["done_ltrs"],
        latest_date=source["latest_date"],
        month=month,
        year=year,
    )
    reason = str(body.get("reason") or "").strip() or None

    with transaction.atomic():
        _insert_log(existing, reason=reason, user=request.user, new_targets=new_targets)
        with connection.cursor() as cur:
            cur.execute(
                """
                UPDATE primary_month_targets
                   SET targets      = %s,
                       "date"       = %s,
                       done_ltrs    = %s,
                       achieved_pct = %s,
                       est_ltr      = %s,
                       est_ltr_pct  = %s,
                       drr          = %s,
                       require_drr  = %s,
                       pending_ltr  = %s,
                       dp_ltrs      = %s,
                       updated_at   = NOW()
                 WHERE id = %s
                """,
                [
                    new_targets,
                    derived["date"],
                    derived["done_ltrs"],
                    derived["achieved_pct"],
                    derived["est_ltr"],
                    derived["est_ltr_pct"],
                    derived["drr"],
                    derived["require_drr"],
                    derived["pending_ltr"],
                    derived["dp_ltrs"],
                    row_id,
                ],
            )

    row = _select_row("WHERE id = %s", [row_id])
    return Response({"ok": True, "row": row, "previous_targets": float(old_targets)})


def _dashboard_row_defs(user) -> list[dict]:
    allowed_slugs = set(user_platform_slugs(user))
    ordered_slugs = [slug for slug in PRIMARY_DASHBOARD_SLUGS if slug in allowed_slugs]
    platforms = {
        p.slug: p
        for p in PlatformConfig.objects.filter(slug__in=ordered_slugs, is_active=True)
    }

    rows: list[dict] = []
    for slug in ordered_slugs:
        p = platforms.get(slug)
        if not p:
            continue
        fmt = _format_for(p)
        rows.append({
            "key": slug,
            "slug": slug,
            "logo_slug": slug,
            "platform_name": p.name,
            "format": fmt,
            "source_format": fmt,
            "type": "prim",
            "source": "master_po",
        })

    for item in SPECIAL_DASHBOARD_ROWS:
        if item["access_slug"] not in allowed_slugs:
            continue
        rows.append({
            **item,
            "slug": item["key"],
            "source_format": item["format"],
        })

    return rows


def _select_dashboard_rows(
    formats: list[str],
    item_head: str,
    month: int,
    year: int,
) -> dict[str, dict]:
    if not formats:
        return {}
    placeholder = ",".join(["LOWER(TRIM(%s))"] * len(formats))
    sql = f"""
        SELECT {", ".join(_ROW_COLS)}
          FROM primary_month_targets
         WHERE month = %s
           AND year = %s
           AND UPPER(TRIM(item_head)) = UPPER(TRIM(%s))
           AND LOWER(TRIM("format")) IN ({placeholder})
    """
    with connection.cursor() as cur:
        cur.execute(sql, [month, year, item_head] + formats)
        rows = [_row_to_dict(r) for r in cur.fetchall()]
    return {_format_key(r.get("format")): r for r in rows}


def _select_secondary_target_dashboard_rows(
    formats: list[str],
    item_head: str,
    month: int,
    year: int,
) -> dict[str, dict]:
    if not formats:
        return {}
    placeholder = ",".join(["LOWER(TRIM(%s))"] * len(formats))
    sql = f"""
        SELECT
            id,
            "format",
            type,
            item_head,
            month,
            year,
            "date",
            targets,
            done_ltrs,
            achieved_pct,
            est_ltr,
            est_ltr_pct,
            created_at,
            updated_at
          FROM month_targets
         WHERE month = %s
           AND year = %s
           AND UPPER(TRIM(item_head)) = UPPER(TRIM(%s))
           AND LOWER(TRIM("format")) IN ({placeholder})
    """
    with connection.cursor() as cur:
        cur.execute(sql, [month, year, item_head] + formats)
        rows = cur.fetchall()

    out: dict[str, dict] = {}
    for row in rows:
        data = dict(zip(
            [
                "id",
                "format",
                "type",
                "item_head",
                "month",
                "year",
                "date",
                "targets",
                "done_ltrs",
                "achieved_pct",
                "est_ltr",
                "est_ltr_pct",
                "created_at",
                "updated_at",
            ],
            row,
        ))
        out[_format_key(data.get("format"))] = _json_ready(data)
    return out


def _json_ready(row: dict) -> dict:
    out = dict(row)
    for k, v in list(out.items()):
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
    return out


def _dashboard_sources(row_defs: list[dict], item_heads: tuple[str, ...], month: int, year: int) -> dict[tuple[str, str], dict]:
    source_map: dict[tuple[str, str], dict] = {}
    master_formats = [
        d["source_format"]
        for d in row_defs
        if d.get("source") == "master_po"
    ]
    source_map.update(_read_master_po_many(master_formats, item_heads, month, year))

    special_sources = {d.get("source") for d in row_defs}
    if "amazon_sec_range_master_view" in special_sources:
        source_map.update(_read_amazon_secondary_many(item_heads, month, year))
    if "amazon_mp_master" in special_sources:
        source_map.update(_read_amazon_mp_many(item_heads, month, year))
    if "flipkart_secondary_all" in special_sources:
        source_map.update(_read_flipkart_secondary_many(item_heads, month, year))
    return source_map


def _dashboard_row_from_source(
    defn: dict,
    stored: dict | None,
    item_head: str,
    month: int,
    year: int,
    source: dict | None = None,
) -> dict:
    source = source or _read_primary_target_source(defn["source_format"], item_head, month, year)
    targets = Decimal(str(stored["targets"])) if stored and stored.get("targets") is not None else Decimal(0)
    derived = _compute_derived(
        targets=targets,
        done_ltrs=source["done_ltrs"],
        latest_date=source["latest_date"],
        month=month,
        year=year,
    )

    if stored:
        row = dict(stored)
        row.update(derived)
    else:
        row = {
            "id": None,
            "format": defn["format"],
            "type": defn["type"],
            "item_head": item_head,
            "month": month,
            "year": year,
            "date": derived["date"],
            "targets": None,
            "done_ltrs": derived["done_ltrs"],
            "achieved_pct": None,
            "est_ltr": derived["est_ltr"],
            "est_ltr_pct": None,
            "drr": derived["drr"],
            "require_drr": None,
            "pending_ltr": None,
            "dp_ltrs": None,
            "created_at": None,
            "updated_at": None,
        }

    row.update({
        "slug": defn["slug"],
        "logo_slug": defn.get("logo_slug") or defn["slug"],
        "platform_name": defn["platform_name"],
        "format": defn["format"],
        "type": defn["type"],
        "source": defn["source"],
    })
    return _json_ready(row)


def _dashboard_row_from_secondary_target(defn: dict, stored: dict) -> dict:
    # The TARGET is the single shared value from the saved Secondary
    # (month_targets) row, but DONE and all derived figures are recomputed from
    # the LIVE source — so the Primary sheet matches the Secondary sheet and
    # auto-updates (e.g. after re-tagging item_head in master_sheet) instead of
    # echoing the frozen month_targets snapshot.
    item_head = stored.get("item_head")
    month = stored.get("month")
    year = stored.get("year")
    source = _read_primary_target_source(defn["source_format"], item_head, month, year)
    derived = _compute_derived(
        targets=Decimal(str(stored.get("targets") or 0)),
        done_ltrs=source["done_ltrs"],
        latest_date=source["latest_date"],
        month=month,
        year=year,
    )
    row = {
        "id": stored.get("id"),
        "format": defn["format"],
        "type": defn["type"],
        "item_head": item_head,
        "month": month,
        "year": year,
        "targets": stored.get("targets"),
        "created_at": stored.get("created_at"),
        "updated_at": stored.get("updated_at"),
        "slug": defn["slug"],
        "logo_slug": defn.get("logo_slug") or defn["slug"],
        "platform_name": defn["platform_name"],
        "source": defn["source"],
    }
    row.update(derived)
    return _json_ready(row)


def _list_secondary_sourced_targets(
    slug: str,
    sec_fmt: str,
    source_format: str,
    month: int,
    year: int,
) -> list[dict]:
    """Per-platform Primary list rows for a platform whose target is the single
    shared value set on the Secondary sheet (month_targets). Reads that target
    and recomputes Primary done/derived (DRR, pending, dp …) from the live
    primary source, so the same number shows on both sheets."""
    rows: list[dict] = []
    for item_head in _item_heads_for(slug):
        stored = _select_secondary_target_dashboard_rows(
            [sec_fmt], item_head, month, year
        ).get(_format_key(sec_fmt))
        if not stored or stored.get("targets") is None:
            continue
        source = _read_primary_target_source(source_format, item_head, month, year)
        derived = _compute_derived(
            targets=Decimal(str(stored["targets"])),
            done_ltrs=source["done_ltrs"],
            latest_date=source["latest_date"],
            month=month,
            year=year,
        )
        row = {
            "id": None,
            "format": sec_fmt,
            "type": "sec",
            "item_head": item_head,
            "month": month,
            "year": year,
            "targets": Decimal(str(stored["targets"])),
        }
        row.update(derived)
        rows.append(_json_ready(row))
    return rows


def _primary_empty_total() -> dict:
    return {
        "targets": 0,
        "done_ltrs": 0,
        "achieved_pct": None,
        "est_ltr": 0,
        "est_ltr_pct": None,
        "drr": 0,
        "require_drr": 0,
        "pending_ltr": 0,
        "dp_ltrs": 0,
    }


def _primary_grand_total(rows: list[dict]) -> dict:
    s_tgt = sum(_num(r.get("targets")) for r in rows)
    s_done = sum(_num(r.get("done_ltrs")) for r in rows)
    s_est = sum(_num(r.get("est_ltr")) for r in rows)
    s_drr = sum(_num(r.get("drr")) for r in rows)
    s_req_drr = sum(_num(r.get("require_drr")) for r in rows)
    s_pending = sum(_num(r.get("pending_ltr")) for r in rows)
    s_dp = sum(_num(r.get("dp_ltrs")) for r in rows)
    return {
        "targets": s_tgt,
        "done_ltrs": s_done,
        "achieved_pct": (s_done / s_tgt) if s_tgt else None,
        "est_ltr": s_est,
        "est_ltr_pct": (s_est / s_tgt) if s_tgt else None,
        "drr": s_drr,
        "require_drr": s_req_drr,
        "pending_ltr": s_pending,
        "dp_ltrs": s_dp,
    }


def _num(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


@api_view(["GET"])
@permission_classes([require("platform.month_targets.view")])
def primary_month_targets_dashboard(request):
    month, year = _parse_month_year(request.query_params)
    row_defs = _dashboard_row_defs(request.user)
    if not row_defs:
        return Response({
            "premium": {"rows": [], "total": _primary_empty_total()},
            "commodity": {"rows": [], "total": _primary_empty_total()},
            "month": month,
            "year": year,
        })

    result: dict[str, dict] = {}
    formats = [d["format"] for d in row_defs]
    source_map = _dashboard_sources(row_defs, DASHBOARD_ITEM_HEADS, month, year)
    for item_head in DASHBOARD_ITEM_HEADS:
        stored_by_format = _select_dashboard_rows(formats, item_head, month, year)
        secondary_target_formats = [
            defn["target_format"]
            for defn in row_defs
            if defn.get("target_source") == "month_targets"
        ]
        secondary_by_format = _select_secondary_target_dashboard_rows(
            secondary_target_formats,
            item_head,
            month,
            year,
        )
        rows = []
        for defn in row_defs:
            if (
                item_head.upper() == "COMMODITY"
                and str(defn.get("key") or defn.get("slug") or "").lower() == "zomato"
            ):
                continue

            if defn.get("target_source") == "month_targets":
                secondary_row = secondary_by_format.get(_format_key(defn.get("target_format")))
                if secondary_row:
                    rows.append(_dashboard_row_from_secondary_target(defn, secondary_row))
                    continue

            rows.append(
                _dashboard_row_from_source(
                    defn,
                    stored_by_format.get(_format_key(defn["format"])),
                    item_head,
                    month,
                    year,
                    source_map.get(
                        (_format_key(defn["source_format"]), _format_key(item_head)),
                        {"done_ltrs": Decimal(0), "latest_date": None},
                    ),
                )
            )

        result[item_head.lower()] = {
            "rows": rows,
            "total": _primary_grand_total(rows),
        }

    result["month"] = month
    result["year"] = year
    return Response(result)


@api_view(["POST"])
@permission_classes([require("target_sheet.edit")])
def primary_month_targets_refresh_all(request):
    body = request.data if request.data else request.query_params
    month, year = _parse_month_year_or_current(body)

    row_defs = _dashboard_row_defs(request.user)
    formats = [d["format"] for d in row_defs]
    if not formats:
        return Response({
            "ok": True,
            "month": month,
            "year": year,
            "updated": 0,
            "rows": [],
        })

    placeholder = ",".join(["LOWER(TRIM(%s))"] * len(formats))
    sql = f"""
        SELECT {", ".join(_ROW_COLS)}
          FROM primary_month_targets
         WHERE month = %s
           AND year = %s
           AND LOWER(TRIM("format")) IN ({placeholder})
         ORDER BY UPPER(TRIM("format")), UPPER(TRIM(item_head))
    """
    with connection.cursor() as cur:
        cur.execute(sql, [month, year] + formats)
        rows = [_row_to_dict(r) for r in cur.fetchall()]

    secondary_refreshed: list[dict] = []
    with transaction.atomic():
        refreshed = [_refresh_existing_row(row) for row in rows]
        secondary_target_defs = [
            d for d in row_defs if d.get("target_source") == "month_targets"
        ]
        if secondary_target_defs:
            from . import monthly_targets as secondary_targets

            platform_slugs = {
                d["access_slug"] for d in secondary_target_defs if d.get("access_slug")
            }
            platforms = {
                p.slug: p
                for p in PlatformConfig.objects.filter(slug__in=platform_slugs, is_active=True)
            }
            for defn in secondary_target_defs:
                slug = defn.get("access_slug")
                platform = platforms.get(slug)
                if not slug or not platform:
                    continue
                secondary_refreshed.extend(
                    secondary_targets._refresh_platform_rows(slug, platform, month, year)
                )

    return Response({
        "ok": True,
        "month": month,
        "year": year,
        "updated": len(refreshed) + len(secondary_refreshed),
        "rows": refreshed,
        "secondary_rows": secondary_refreshed,
    })
