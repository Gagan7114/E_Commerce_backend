"""Monthly Targets — replicates the `ALL PLATFORM SECONDARY SALES` sheet.

Row lifecycle (see MONTHLY_TARGETS_SPEC.md §2.1 and §3.4):
  * One row per (format, item_head, month, year), created via INSERT only.
  * POST fails with 409 if a row for that tuple already exists — past months
    are never overwritten.
  * A separate refresh endpoint recomputes only the derived columns on the
    current-month row. `targets` and `last_month` are locked after INSERT.

Source routing:
  * SecMaster platforms: blinkit, swiggy, zepto, bigbasket, flipkart (B2C)
  * master_po platforms: zomato, citymall (filter status = 'COMPLETED')
  * Flipkart Grocery:     flipkart_grocery_master + monthly_landing_rate
  * Out of scope:         amazon, jiomart
"""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

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
FLIPKART_GROCERY_SLUGS = {"flipkart_grocery"}

# Platforms whose Monthly Targets row is populated by user input only — no
# automatic source query is run because the platform's sales/PO data lives
# outside the secmaster / master_po / flipkart_grocery_master sources
# this module supports. Setting a target row still goes through the normal
# POST UI; done_ltrs / done_value start at 0 and the user updates them via
# the edit flow.
AMAZON_SLUGS = {"amazon"}
# Kept for backwards compatibility / future manual platforms (none today).
MANUAL_SLUGS: set[str] = set()

# Slugs explicitly out of scope — spec §8.1.
SKIPPED_SLUGS = {"jiomart"}

# All in-scope slugs, in the order the combined dashboard renders. Amazon
# is now a write-path platform too — targets can be created / updated /
# refreshed, with `done_ltrs` / `done_value` pulled from
# `amazon_sec_range_master_view` (same source the Amazon Secondary Monthly
# Dashboard uses) via `_read_amazon`.
IN_SCOPE_SLUGS = (
    "amazon",
    "blinkit",
    "swiggy",
    "zepto",
    "bigbasket",
    "zomato",
    "citymall",
    "flipkart",
    "flipkart_grocery",
)

# Display-only slug order for the Monthly Targets dashboard endpoint —
# preserves the original "Amazon at top" rendering even now that Amazon is in
# IN_SCOPE_SLUGS. De-duplication via tuple(dict.fromkeys(...)) keeps the
# expression idempotent if the order ever changes.
DASHBOARD_DISPLAY_SLUGS = tuple(dict.fromkeys(("amazon",) + IN_SCOPE_SLUGS))

# Amazon MP is a secondary-sales channel whose data lives in `amazon_mp_master`
# and which has NO PlatformConfig row of its own — exactly like the Primary
# dashboard, which surfaces it via SPECIAL_DASHBOARD_ROWS. We render it as a
# synthetic dashboard row gated by Amazon access. Deliberately kept OUT of
# IN_SCOPE_SLUGS so the set/refresh write paths stay untouched; it is display +
# live-source only.
AMAZON_MP_FORMAT = "AMAZON MP"
_AMAZON_MP_PLATFORM = SimpleNamespace(
    slug="amazon_mp",
    name="Amazon MP",
    po_filter_value=AMAZON_MP_FORMAT,
    sales_type="sec",
)

DASHBOARD_ITEM_HEADS = ("PREMIUM", "COMMODITY")
DEFAULT_ITEM_HEADS = ("PREMIUM", "COMMODITY")
FLIPKART_GROCERY_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")


def _item_heads_for(slug: str) -> tuple[str, ...]:
    if slug in FLIPKART_GROCERY_SLUGS:
        return FLIPKART_GROCERY_ITEM_HEADS
    return DEFAULT_ITEM_HEADS


def _source_for(slug: str) -> str:
    if slug in SECMASTER_SLUGS:
        return "secmaster"
    if slug in MASTER_PO_SLUGS:
        return "master_po"
    if slug in FLIPKART_GROCERY_SLUGS:
        return "flipkart_grocery"
    if slug in AMAZON_SLUGS:
        return "amazon"
    if slug in MANUAL_SLUGS:
        return "manual"
    raise ValidationError(
        f"Platform '{slug}' is not supported for Monthly Targets. "
        f"In-scope platforms: "
        f"{', '.join(sorted(SECMASTER_SLUGS | MASTER_PO_SLUGS | FLIPKART_GROCERY_SLUGS | AMAZON_SLUGS | MANUAL_SLUGS))}."
    )


def _get_platform(slug: str) -> PlatformConfig:
    return get_object_or_404(PlatformConfig, slug=slug, is_active=True)


def _ensure_scope(user, slug: str) -> None:
    if not can_access_platform(user, slug):
        raise PermissionDenied(f"Your account is not authorized for the '{slug}' platform.")


def _format_for(p: PlatformConfig) -> str:
    """Canonical uppercase `format` string stored in month_targets."""
    return (p.po_filter_value or p.slug).strip().upper()


def _format_key(value) -> str:
    return str(value or "").strip().upper()


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


def _parse_month_year_or_current(body_or_params) -> tuple[int, int]:
    """Accept month/year when supplied, otherwise default to the current month."""
    raw_month = body_or_params.get("month") if body_or_params else None
    raw_year = body_or_params.get("year") if body_or_params else None
    if raw_month or raw_year:
        return _parse_month_year(body_or_params)

    today = date.today()
    return today.month, today.year


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


def _month_start_iso(month: int, year: int) -> str:
    return f"{year}-{month:02d}-01"


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
    delivered_year = year, format = fmt, item_head = item_head.

    Note: `delivery_month` is TEXT in the live DB holding uppercase month
    names ('APRIL', …) — same convention as SecMaster. `delivered_year` is
    INTEGER.
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
          AND "delivered_year"                     = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [fmt, item_head, month_name, year])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "done_value": Decimal(row[1] or 0),
        "latest_date": row[2],
    }


def _read_flipkart_grocery(fmt: str, item_head: str, month: int, year: int) -> dict:
    """Read Flipkart Grocery target source data.

    `done_ltrs` comes from flipkart_grocery_master.ltr_sold.
    `done_value` follows the business rule:
        SALE AMT(EXCLUSIVE) == done_value

    For refreshes, prefer the current monthly_landing_rate.basic_rate for
    the SKU/month, then the latest available Flipkart Grocery basic rate, then
    the stored flipkart_grocery_master.basic_rate, and finally the already
    stored sale_amt_exclusive.
    """
    rate_month = _month_start_iso(month, year)
    sql = """
        WITH exact_rates AS (
            SELECT DISTINCT ON (sku_code)
                   sku_code,
                   basic_rate
              FROM monthly_landing_rate
             WHERE UPPER(TRIM(format)) = 'FLIPKART GROCERY'
               AND month = %s
             ORDER BY sku_code, created_at DESC
        ),
        fallback_rates AS (
            SELECT DISTINCT ON (sku_code)
                   sku_code,
                   basic_rate
              FROM monthly_landing_rate
             WHERE UPPER(TRIM(format)) = 'FLIPKART GROCERY'
             ORDER BY sku_code, month DESC, created_at DESC
        )
        SELECT
            COALESCE(SUM(fgm.ltr_sold), 0) AS done_ltrs,
            COALESCE(
                SUM(
                    COALESCE(
                        fgm.qty * COALESCE(
                            exact_rates.basic_rate,
                            fallback_rates.basic_rate,
                            NULLIF(fgm.basic_rate, 0)
                        ),
                        fgm.sale_amt_exclusive,
                        0
                    )
                ),
                0
            ) AS done_value,
            MAX(fgm.real_date) AS latest_date
        FROM flipkart_grocery_master fgm
        LEFT JOIN exact_rates
               ON exact_rates.sku_code = fgm.sku_id
        LEFT JOIN fallback_rates
               ON fallback_rates.sku_code = fgm.sku_id
        WHERE fgm.month = %s
          AND fgm.year = %s
          AND UPPER(TRIM(fgm.item_head::text)) = UPPER(TRIM(%s))
    """
    with connection.cursor() as cur:
        cur.execute(sql, [rate_month, month, year, item_head])
        row = cur.fetchone()
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "done_value": Decimal(row[1] or 0),
        "latest_date": row[2],
    }


def _read_amazon(item_head: str, month: int, year: int) -> dict:
    """Read (done_ltrs, done_value, latest_date) for Amazon from
    `amazon_sec_range_master_view`.

        done_ltrs   = SUM(shipped_litres)              ┐ at the latest single
        done_value  = SUM(calculated_shipped_revenue)  ┘ month_day in the period
        latest_date = date(year, month, day-part of that month_day)

    Filter columns: `item_head`, `month_day`, `year`.

    Amazon range uploads are cumulative — each upload covers
    `from_date = 1st of month .. to_date = report date`. Summing across
    every month_day in the month therefore double-counts. We pick the
    LATEST month_day available (max day-of-month for that month_day's
    suffix) and sum only that snapshot — exactly one row per ASIN, no
    overlap.
    """
    month_name = _MONTH_NAMES[month].upper()  # 'MAY'
    month_day_suffix = f"%-{month_name}"      # '%-MAY'

    sql = """
        WITH latest AS (
            SELECT MAX(
                NULLIF(split_part("month_day"::text, '-', 1), '')::int
            ) AS max_day
            FROM "amazon_sec_range_master_view"
            WHERE UPPER(TRIM("item_head"::text)) = UPPER(TRIM(%s))
              AND UPPER(TRIM("month_day"::text)) LIKE %s
              AND "year" = %s
        )
        SELECT
            COALESCE(SUM(v."shipped_litres"), 0)              AS done_ltrs,
            COALESCE(SUM(v."calculated_shipped_revenue"), 0)  AS done_value,
            l.max_day                                          AS max_day
        FROM "amazon_sec_range_master_view" v
        CROSS JOIN latest l
        WHERE UPPER(TRIM(v."item_head"::text)) = UPPER(TRIM(%s))
          AND UPPER(TRIM(v."month_day"::text)) LIKE %s
          AND v."year" = %s
          AND NULLIF(split_part(v."month_day"::text, '-', 1), '')::int = l.max_day
        GROUP BY l.max_day
    """
    params = [
        item_head, month_day_suffix, year,  # for the `latest` CTE
        item_head, month_day_suffix, year,  # for the outer SELECT
    ]
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if not row:
        return {"done_ltrs": Decimal(0), "done_value": Decimal(0), "latest_date": None}

    max_day = row[2]
    try:
        latest_date = date(year, month, int(max_day)) if max_day else None
    except (TypeError, ValueError):
        latest_date = None
    return {
        "done_ltrs": Decimal(row[0] or 0),
        "done_value": Decimal(row[1] or 0),
        "latest_date": latest_date,
    }


def _read_source(slug: str, fmt: str, item_head: str, month: int, year: int) -> dict:
    source = _source_for(slug)
    if source == "secmaster":
        return _read_secmaster(fmt, item_head, month, year)
    if source == "flipkart_grocery":
        return _read_flipkart_grocery(fmt, item_head, month, year)
    if source == "amazon":
        return _read_amazon(item_head, month, year)
    if source == "manual":
        # Manual platforms: no auto-aggregation. The user sets the
        # target; done values start at 0 and are updated via the edit flow.
        return {
            "done_ltrs": Decimal(0),
            "done_value": Decimal(0),
            "latest_date": None,
        }
    return _read_master_po(fmt, item_head, month, year)


def _read_secmaster_dashboard_many(
    formats: list[str],
    item_heads: tuple[str, ...],
    month: int,
    year: int,
) -> dict[tuple[str, str], dict]:
    """Fast litre aggregates for the home/target dashboard hot path.

    `SecMaster` is a raw UNION view with expensive master/rate joins, and the
    existing `SecMaster_Mat` can lag behind current uploads. The home KPI cards
    only need litres, so read the month-filtered raw platform tables directly
    and join only the master-sheet fields needed for `item_head` and litres.
    """
    if not formats:
        return {}
    requested = {_format_key(fmt) for fmt in formats}
    if not requested:
        return {}

    start = date(year, month, 1)
    end = date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))

    parts: list[str] = []
    params: list = []

    def add_part(format_name: str, sql: str, extra_params: list) -> None:
        if format_name not in requested:
            return
        parts.append(sql)
        params.extend(extra_params)
        params.extend(item_heads)

    add_part(
        "BLINKIT",
        f"""
        SELECT 'BLINKIT' AS fmt,
               UPPER(TRIM(m.item_head::text)) AS item_head,
               COALESCE(SUM(
                   CASE WHEN m.is_litre = 'Y'
                        THEN COALESCE(b.qty_sold, 0)::numeric * COALESCE(m.per_unit_value, 0)::numeric
                        ELSE 0 END
               ), 0) AS done_ltrs,
               MAX(b.date) AS latest_date
          FROM "blinkitSec" b
          LEFT JOIN LATERAL (
                SELECT ms.item_head, ms.per_unit_value, ms.is_litre
                  FROM master_sheet ms
                 WHERE UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(b.item_id::text))
                   AND regexp_replace(lower(TRIM(ms.format::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
                 ORDER BY ms.product_name, ms.item, ms.per_unit
                 LIMIT 1
          ) m ON true
         WHERE b.date >= %s AND b.date < %s
           AND UPPER(TRIM(m.item_head::text)) IN ({item_placeholder})
         GROUP BY UPPER(TRIM(m.item_head::text))
        """,
        [start, end],
    )
    add_part(
        "SWIGGY",
        f"""
        SELECT 'SWIGGY' AS fmt,
               UPPER(TRIM(m.item_head::text)) AS item_head,
               COALESCE(SUM(
                   CASE WHEN m.is_litre = 'Y'
                        THEN COALESCE(s."UNITS_SOLD", 0)::numeric * COALESCE(m.per_unit_value, 0)::numeric
                        ELSE 0 END
               ), 0) AS done_ltrs,
               MAX(s."ORDERED_DATE") AS latest_date
          FROM "swiggySec" s
          LEFT JOIN LATERAL (
                SELECT ms.item_head, ms.per_unit_value, ms.is_litre
                  FROM master_sheet ms
                 WHERE UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(s."ITEM_CODE"::text))
                   AND regexp_replace(lower(TRIM(ms.format::text)), '[^a-z0-9]+', '', 'g') = 'swiggy'
                 ORDER BY ms.product_name, ms.item, ms.per_unit
                 LIMIT 1
          ) m ON true
         WHERE s."ORDERED_DATE" >= %s AND s."ORDERED_DATE" < %s
           AND UPPER(TRIM(m.item_head::text)) IN ({item_placeholder})
         GROUP BY UPPER(TRIM(m.item_head::text))
        """,
        [start, end],
    )
    add_part(
        "ZEPTO",
        f"""
        SELECT 'ZEPTO' AS fmt,
               UPPER(TRIM(m.item_head::text)) AS item_head,
               COALESCE(SUM(
                   CASE WHEN m.is_litre = 'Y'
                        THEN COALESCE(z."Sales (Qty) - Units", 0)::numeric * COALESCE(m.per_unit_value, 0)::numeric
                        ELSE 0 END
               ), 0) AS done_ltrs,
               MAX(z."Date") AS latest_date
          FROM "zeptoSec" z
          LEFT JOIN LATERAL (
                SELECT ms.item_head, ms.per_unit_value, ms.is_litre
                  FROM master_sheet ms
                 WHERE UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(z."SKU Number"::text))
                   AND regexp_replace(lower(TRIM(ms.format::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
                 ORDER BY ms.product_name, ms.item, ms.per_unit
                 LIMIT 1
          ) m ON true
         WHERE z."Date" >= %s AND z."Date" < %s
           AND UPPER(TRIM(m.item_head::text)) IN ({item_placeholder})
         GROUP BY UPPER(TRIM(m.item_head::text))
        """,
        [start, end],
    )
    add_part(
        "BIG BASKET",
        f"""
        SELECT 'BIG BASKET' AS fmt,
               UPPER(TRIM(m.item_head::text)) AS item_head,
               COALESCE(SUM(
                   CASE WHEN m.is_litre = 'Y'
                        THEN COALESCE(bb.total_quantity, 0)::numeric * COALESCE(m.per_unit_value, 0)::numeric
                        ELSE 0 END
               ), 0) AS done_ltrs,
               MAX(bb.date_range) AS latest_date
          FROM "bigbasketSec" bb
          LEFT JOIN LATERAL (
                SELECT ms.item_head, ms.per_unit_value, ms.is_litre
                  FROM master_sheet ms
                 WHERE UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(bb.source_sku_id::text))
                   AND regexp_replace(lower(TRIM(ms.format::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
                 ORDER BY ms.product_name, ms.item, ms.per_unit
                 LIMIT 1
          ) m ON true
         WHERE bb.date_range >= %s AND bb.date_range < %s
           AND UPPER(TRIM(m.item_head::text)) IN ({item_placeholder})
         GROUP BY UPPER(TRIM(m.item_head::text))
        """,
        [start, end],
    )
    add_part(
        "FLIPKART",
        f"""
        SELECT 'FLIPKART' AS fmt,
               UPPER(TRIM(m.item_head::text)) AS item_head,
               COALESCE(SUM(
                   CASE WHEN m.is_litre = 'Y'
                        THEN COALESCE(fk."Final Sale Units", 0)::numeric * COALESCE(m.per_unit_value, 0)::numeric
                        ELSE 0 END
               ), 0) AS done_ltrs,
               MAX(fk."Order Date") AS latest_date
          FROM "flipkartSec" fk
          LEFT JOIN LATERAL (
                SELECT ms.item_head, ms.per_unit_value, ms.is_litre
                  FROM master_sheet ms
                 WHERE ms.format = 'FLIPKART'
                   AND UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(fk."Product Id"::text))
                 ORDER BY ms.product_name, ms.item, ms.per_unit
                 LIMIT 1
          ) m ON true
         WHERE fk."Order Date" >= %s AND fk."Order Date" < %s
           AND UPPER(TRIM(m.item_head::text)) IN ({item_placeholder})
         GROUP BY UPPER(TRIM(m.item_head::text))
        """,
        [start, end],
    )

    if not parts:
        return {}

    sql = " UNION ALL ".join(parts)
    with connection.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return {
        (_format_key(fmt), _format_key(item_head)): {
            "done_ltrs": Decimal(done_ltrs or 0),
            "done_value": None,
            "latest_date": latest_date,
        }
        for fmt, item_head, done_ltrs, latest_date in rows
    }


def _read_master_po_dashboard_many(
    formats: list[str],
    item_heads: tuple[str, ...],
    month: int,
    year: int,
) -> dict[tuple[str, str], dict]:
    if not formats:
        return {}
    month_name = _MONTH_NAMES[month]
    format_placeholder = ",".join(["LOWER(TRIM(%s))"] * len(formats))
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        SELECT
            UPPER(TRIM("format"::text))    AS fmt,
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("total_delivered_liters"), 0)        AS done_ltrs,
            COALESCE(SUM("total_delivered_amt_exclusive"), 0) AS done_value,
            MAX("delivery_date")                              AS latest_date
        FROM "master_po"
        WHERE LOWER(TRIM("format"::text)) IN ({format_placeholder})
          AND UPPER(TRIM("item_head"::text)) IN ({item_placeholder})
          AND UPPER(TRIM("status"::text)) = 'COMPLETED'
          AND UPPER(TRIM("delivery_month"::text)) = %s
          AND "delivered_year" = %s
        GROUP BY UPPER(TRIM("format"::text)), UPPER(TRIM("item_head"::text))
    """
    with connection.cursor() as cur:
        cur.execute(sql, list(formats) + list(item_heads) + [month_name, year])
        rows = cur.fetchall()
    return {
        (_format_key(fmt), _format_key(item_head)): {
            "done_ltrs": Decimal(done_ltrs or 0),
            "done_value": Decimal(done_value or 0),
            "latest_date": latest_date,
        }
        for fmt, item_head, done_ltrs, done_value, latest_date in rows
    }


def _read_flipkart_grocery_dashboard_many(
    item_heads: tuple[str, ...],
    month: int,
    year: int,
) -> dict[tuple[str, str], dict]:
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("ltr_sold"), 0)   AS done_ltrs,
            MAX("real_date")               AS latest_date
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
          AND UPPER(TRIM("item_head"::text)) IN ({item_placeholder})
        GROUP BY UPPER(TRIM("item_head"::text))
    """
    with connection.cursor() as cur:
        cur.execute(sql, [month, year] + list(item_heads))
        rows = cur.fetchall()
    return {
        ("FLIPKART GROCERY", _format_key(item_head)): {
            "done_ltrs": Decimal(done_ltrs or 0),
            "done_value": None,
            "latest_date": latest_date,
        }
        for item_head, done_ltrs, latest_date in rows
    }


def _read_amazon_dashboard_many(
    item_heads: tuple[str, ...],
    month: int,
    year: int,
) -> dict[tuple[str, str], dict]:
    month_day_suffix = f"%-{_MONTH_NAMES[month]}"
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = f"""
        WITH filtered AS MATERIALIZED (
            SELECT
                UPPER(TRIM("item_head"::text)) AS item_head,
                "shipped_litres",
                "calculated_shipped_revenue",
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
            COALESCE(SUM(f."calculated_shipped_revenue"), 0) AS done_value,
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

    out: dict[tuple[str, str], dict] = {}
    for item_head, done_ltrs, done_value, max_day in rows:
        try:
            latest_date = date(year, month, int(max_day)) if max_day else None
        except (TypeError, ValueError):
            latest_date = None
        out[("AMAZON", _format_key(item_head))] = {
            "done_ltrs": Decimal(done_ltrs or 0),
            "done_value": Decimal(done_value or 0),
            "latest_date": latest_date,
        }
    return out


def _read_amazon_mp_dashboard_many(
    item_heads: tuple[str, ...],
    month: int,
    year: int,
) -> dict[tuple[str, str], dict]:
    """Live done_ltrs for Amazon MP, read from `amazon_mp_master` — the same
    source the Primary dashboard uses. Amazon MP carries no revenue column, so
    `done_value` is reported as 0. Keyed to the AMAZON MP synthetic format."""
    month_name = _MONTH_NAMES[month]
    item_placeholder = ",".join(["UPPER(TRIM(%s))"] * len(item_heads))
    sql = """
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
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
        WHERE UPPER(TRIM("item_head"::text)) IN (__ITEMS__)
          AND UPPER(TRIM("shipment_month"::text)) = %s
          AND "shipment_year" = %s
        GROUP BY UPPER(TRIM("item_head"::text))
    """.replace("__ITEMS__", item_placeholder)
    with connection.cursor() as cur:
        cur.execute(sql, list(item_heads) + [month_name, year])
        rows = cur.fetchall()

    fmt_key = _format_key(AMAZON_MP_FORMAT)
    out: dict[tuple[str, str], dict] = {}
    for item_head, done_ltrs, latest_date in rows:
        out[(fmt_key, _format_key(item_head))] = {
            "done_ltrs": Decimal(done_ltrs or 0),
            "done_value": Decimal(0),
            "latest_date": latest_date,
        }
    return out


def _read_amazon_mp_primary_target(item_head: str, month: int, year: int) -> dict | None:
    """Amazon MP keeps a SINGLE target shared by the Primary and Secondary
    sheets — it is stored only in `primary_month_targets` (set via Prim
    Targets). The Secondary `month_targets` table has no Amazon MP row, so the
    Sec sheet showed "No target". Return a stored-row-shaped dict carrying that
    shared target so the Secondary dashboard surfaces the same number the user
    set on Prim Targets. Returns None when no target has been set yet."""
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT targets FROM primary_month_targets
             WHERE UPPER(TRIM("format")) = UPPER(TRIM(%s))
               AND UPPER(TRIM(item_head)) = UPPER(TRIM(%s))
               AND month = %s AND year = %s
             ORDER BY updated_at DESC NULLS LAST
             LIMIT 1
            """,
            [AMAZON_MP_FORMAT, item_head, month, year],
        )
        r = cur.fetchone()
    if not r or r[0] is None:
        return None
    return {
        "format": AMAZON_MP_FORMAT,
        "type": "sec",
        "item_head": item_head,
        "month": month,
        "year": year,
        "targets": Decimal(str(r[0])),
        "done_value": Decimal(0),
        "last_month": Decimal(0),
    }


def _dashboard_source_map(
    ordered_slugs: list[str] | tuple[str, ...],
    platforms: dict[str, PlatformConfig],
    item_heads: tuple[str, ...],
    month: int,
    year: int,
) -> dict[tuple[str, str], dict]:
    source_map: dict[tuple[str, str], dict] = {}

    secmaster_formats = [
        _format_for(platforms[slug])
        for slug in ordered_slugs
        if slug in SECMASTER_SLUGS and slug in platforms
    ]
    source_map.update(
        _read_secmaster_dashboard_many(secmaster_formats, item_heads, month, year)
    )

    master_formats = [
        _format_for(platforms[slug])
        for slug in ordered_slugs
        if slug in MASTER_PO_SLUGS and slug in platforms
    ]
    source_map.update(
        _read_master_po_dashboard_many(master_formats, item_heads, month, year)
    )

    if any(slug in FLIPKART_GROCERY_SLUGS for slug in ordered_slugs):
        source_map.update(
            _read_flipkart_grocery_dashboard_many(item_heads, month, year)
        )

    if any(slug in AMAZON_SLUGS for slug in ordered_slugs):
        source_map.update(_read_amazon_dashboard_many(item_heads, month, year))

    if "amazon_mp" in ordered_slugs:
        source_map.update(_read_amazon_mp_dashboard_many(item_heads, month, year))

    return source_map


def _read_platform_latest_date(slug: str, fmt: str, month: int, year: int) -> date | None:
    """Return the same platform-wide max date shown on the secondary dashboard.

    Target rows keep their item-head-specific done_ltrs / done_value, but the
    DATE column is compared by users against the platform dashboard's Max Date.
    """
    month_name = _MONTH_NAMES[month]
    if slug == "flipkart":
        sql = """
            SELECT MAX("Order Date")
            FROM "flipkart_secondary_all"
            WHERE UPPER(TRIM("month"::text)) = %s
              AND "year" = %s
        """
        params = [month_name, year]
    elif slug == "flipkart_grocery":
        sql = """
            SELECT MAX("real_date")
            FROM "flipkart_grocery_master"
            WHERE "month" = %s
              AND "year" = %s
        """
        params = [month, year]
    else:
        return None

    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row and row[0] else None


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


def _json_ready(row: dict) -> dict:
    # JSON-serializable coercions.
    for k, v in list(row.items()):
        if isinstance(v, Decimal):
            row[k] = float(v)
        elif isinstance(v, (date, datetime)):
            row[k] = v.isoformat()
    return row


def _row_to_dict(row: tuple) -> dict:
    return _json_ready(dict(zip(_ROW_COLS, row)))


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

_REFRESH_UPDATE_SQL = """
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


def _refresh_existing_row(slug: str, row: dict, fmt: str | None = None) -> dict:
    """Refresh derived columns for one already-loaded month_targets row."""
    row_id = int(row["id"])
    month = int(row["month"])
    year = int(row["year"])
    item_head = str(row["item_head"] or "").strip().upper()
    source_format = fmt or str(row.get("format") or "").strip()

    source = _read_source(slug, source_format, item_head, month, year)
    platform_latest_date = _read_platform_latest_date(slug, source_format, month, year)
    if platform_latest_date:
        source["latest_date"] = platform_latest_date
    derived = _compute_derived(
        targets=Decimal(str(row["targets"] or 0)),
        done_ltrs=source["done_ltrs"],
        done_value=source["done_value"],
        latest_date=source["latest_date"],
        last_month=Decimal(str(row["last_month"] or 0)),
        month=month,
        year=year,
    )

    with connection.cursor() as cur:
        cur.execute(
            _REFRESH_UPDATE_SQL,
            [
                derived["date"],
                derived["done_ltrs"], derived["done_value"], derived["achieved_pct"],
                derived["est_ltr"], derived["est_value"], derived["est_ltr_pct"],
                derived["growth"], derived["growth_pct"],
                row_id,
            ],
        )

    return _select_row("WHERE id = %s", [row_id])


def _refresh_platform_rows(slug: str, platform: PlatformConfig, month: int, year: int) -> list[dict]:
    """Refresh all monthly-target rows for one platform/month."""
    fmt = _format_for(platform)
    rows = _select_rows(
        """WHERE LOWER(TRIM("format")) = LOWER(TRIM(%s))
             AND month = %s AND year = %s""",
        [fmt, month, year],
        'ORDER BY UPPER(TRIM(item_head)) ASC',
    )
    return [_refresh_existing_row(slug, row, fmt) for row in rows]


def _refresh_dashboard_rows(
    ordered_slugs: list[str] | tuple[str, ...],
    platforms: dict[str, PlatformConfig],
    month: int,
    year: int,
) -> list[dict]:
    """Refresh all visible target rows before a dashboard response is built."""
    refreshed_rows: list[dict] = []
    with transaction.atomic():
        for slug in ordered_slugs:
            _source_for(slug)
            platform = platforms.get(slug)
            if not platform:
                continue
            refreshed_rows.extend(_refresh_platform_rows(slug, platform, month, year))
    return refreshed_rows


def _source_backed_dashboard_row(
    slug: str,
    platform: PlatformConfig,
    item_head: str,
    month: int,
    year: int,
    source: dict | None = None,
    stored: dict | None = None,
) -> dict:
    """Dashboard row with live display litres but no hot-path DB update."""
    fmt = _format_for(platform)
    source = source or {
        "done_ltrs": Decimal(0),
        "done_value": None,
        "latest_date": None,
    }
    stored_done_value = Decimal(str((stored or {}).get("done_value") or 0))
    done_value = source.get("done_value")
    if done_value is None:
        done_value = stored_done_value

    derived = _compute_derived(
        targets=Decimal(str((stored or {}).get("targets") or 0)),
        done_ltrs=source["done_ltrs"],
        done_value=done_value,
        latest_date=source["latest_date"],
        last_month=Decimal(str((stored or {}).get("last_month") or 0)),
        month=month,
        year=year,
    )

    if stored:
        row = dict(stored)
        row.update(derived)
        row["slug"] = slug
        row["platform_name"] = platform.name
        return _json_ready(row)

    return _json_ready({
        "id": None,
        "slug": slug,
        "platform_name": platform.name,
        "format": fmt,
        "type": platform.sales_type or "B2B",
        "item_head": item_head,
        "month": month,
        "year": year,
        "date": derived["date"],
        "targets": None,
        "done_ltrs": derived["done_ltrs"],
        "done_value": derived["done_value"],
        "achieved_pct": None,
        "est_ltr": derived["est_ltr"],
        "est_value": derived["est_value"],
        "est_ltr_pct": None,
        "last_month": None,
        "growth": None,
        "growth_pct": None,
    })


@api_view(["POST"])
@permission_classes([require("platform.month_targets.edit")])
def month_targets_refresh_all(request):
    """POST /api/platform/month-targets/refresh

    Refreshes derived columns for every authorized platform row in the
    selected month. Targets and last_month stay locked.
    """
    body = request.data if request.data else request.query_params
    month, year = _parse_month_year_or_current(body)

    allowed_slugs = set(user_platform_slugs(request.user)) & set(IN_SCOPE_SLUGS)
    if not allowed_slugs:
        return Response({
            "ok": True,
            "month": month,
            "year": year,
            "updated": 0,
            "platforms": {},
            "rows": [],
        })

    ordered_slugs = [slug for slug in IN_SCOPE_SLUGS if slug in allowed_slugs]
    platforms = {
        p.slug: p for p in PlatformConfig.objects.filter(slug__in=ordered_slugs)
    }

    refreshed_rows = _refresh_dashboard_rows(ordered_slugs, platforms, month, year)
    platform_counts: dict[str, int] = {}
    for row in refreshed_rows:
        row_format = str(row.get("format") or "").strip().lower()
        for slug in ordered_slugs:
            platform = platforms.get(slug)
            if platform and row_format == _format_for(platform).strip().lower():
                platform_counts[slug] = platform_counts.get(slug, 0) + 1
                break

    return Response({
        "ok": True,
        "month": month,
        "year": year,
        "updated": len(refreshed_rows),
        "platforms": platform_counts,
        "rows": refreshed_rows,
    })


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
def month_targets_refresh_platform(request, slug: str):
    """POST /api/platform/<slug>/month-targets/refresh

    Refreshes derived columns for all rows on one platform in the selected
    month. Targets and last_month stay locked.
    """
    _ensure_scope(request.user, slug)
    if slug in SKIPPED_SLUGS:
        raise ValidationError(f"Platform '{slug}' is out of scope for Monthly Targets.")
    _source_for(slug)
    p = _get_platform(slug)

    body = request.data if request.data else request.query_params
    month, year = _parse_month_year_or_current(body)

    with transaction.atomic():
        rows = _refresh_platform_rows(slug, p, month, year)

    return Response({
        "ok": True,
        "month": month,
        "year": year,
        "updated": len(rows),
        "rows": rows,
        "format": _format_for(p),
        "type": p.sales_type,
        "source": _source_for(slug),
    })


@api_view(["POST"])
@permission_classes([require("platform.month_targets.edit")])
def month_targets_refresh(request, slug: str, row_id: int):
    """POST /api/platform/<slug>/month-targets/<id>/refresh

    Phase B. Recomputes the derived columns on an existing row. `targets`
    and `last_month` are not touched.
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

    row = _refresh_existing_row(slug, existing, fmt)
    return Response({"ok": True, "row": row})


# ─── Target correction (UPDATE + audit log) ───

def _insert_log(
    row: dict,
    *,
    change_type: str,
    reason: str | None,
    user,
    new_targets: Decimal | None = None,
) -> None:
    """Snapshot the pre-edit row into `month_target_logs`. Called before
    any UPDATE or DELETE so the audit table always reflects what the row
    LOOKED LIKE before the change.

    `targets` here is the PREVIOUS value (from the pre-edit row).
    `new_targets` is the value being written by the current edit, so a
    single log row captures both old and new side by side.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO month_target_logs (
                month_target_id, "format", "type", item_head, month, year, "date",
                targets, new_targets, done_ltrs, done_value, achieved_pct,
                est_ltr, est_value, est_ltr_pct,
                last_month, growth, growth_pct,
                change_type, reason,
                changed_by_id, changed_by_email, changed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, NOW()
            )
            """,
            [
                row.get("id"), row.get("format"), row.get("type"), row.get("item_head"),
                row.get("month"), row.get("year"), row.get("date"),
                row.get("targets"), new_targets, row.get("done_ltrs"), row.get("done_value"), row.get("achieved_pct"),
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
        _insert_log(
            existing,
            change_type="UPDATE",
            reason=reason,
            user=request.user,
            new_targets=new_targets,
        )

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

    # Scope to platforms the user can see, intersected with the dashboard
    # display order. DASHBOARD_DISPLAY_SLUGS adds Amazon to the front of
    # IN_SCOPE_SLUGS for display only — write paths still use IN_SCOPE_SLUGS.
    allowed_slugs = set(user_platform_slugs(request.user)) & set(DASHBOARD_DISPLAY_SLUGS)
    if not allowed_slugs:
        return Response({"premium": {"rows": [], "total": _empty_total()},
                         "commodity": {"rows": [], "total": _empty_total()},
                         "month": month, "year": year})

    # Preserve the sheet's display order.
    ordered = [s for s in DASHBOARD_DISPLAY_SLUGS if s in allowed_slugs]
    platforms = {p.slug: p for p in PlatformConfig.objects.filter(slug__in=ordered)}

    # Amazon MP rides on Amazon access — it has no PlatformConfig of its own,
    # so inject the synthetic row right after Amazon (mirrors the Primary sheet).
    if "amazon" in allowed_slugs and "amazon" in ordered:
        platforms["amazon_mp"] = _AMAZON_MP_PLATFORM
        ordered.insert(ordered.index("amazon") + 1, "amazon_mp")

    # One query per SKU-group. Using LOWER/TRIM on format so the row joins
    # regardless of stored casing.
    formats = [_format_for(platforms[s]) for s in ordered if s in platforms]
    if not formats:
        return Response({"premium": {"rows": [], "total": _empty_total()},
                         "commodity": {"rows": [], "total": _empty_total()},
                         "month": month, "year": year})

    # Home dashboard loads must stay fast. Use the indexed/materialized source
    # reads for displayed litres, while explicit refresh endpoints remain the
    # place that writes recalculated values back into month_targets.
    source_map = _dashboard_source_map(
        ordered,
        platforms,
        DASHBOARD_ITEM_HEADS,
        month,
        year,
    )

    result = {}
    for item_head in DASHBOARD_ITEM_HEADS:
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
            if item_head.upper() == "COMMODITY" and slug == "zomato":
                continue
            p = platforms.get(slug)
            if not p:
                continue
            fmt = _format_for(p)
            fmt_key = _format_key(fmt)
            row = by_format.get(fmt.lower())
            source = source_map.get((fmt_key, _format_key(item_head)))
            # Amazon MP shares one target across the Primary and Secondary
            # sheets, stored only in primary_month_targets. If there is no
            # Secondary row, fall back to that shared Primary target so the MP
            # row shows the same number the user set on Prim Targets.
            if not row and slug == "amazon_mp":
                row = _read_amazon_mp_primary_target(item_head, month, year)
            if row:
                rows.append(
                    _source_backed_dashboard_row(
                        slug,
                        p,
                        item_head,
                        month,
                        year,
                        source,
                        row,
                    )
                )
            else:
                rows.append(
                    _source_backed_dashboard_row(
                        slug,
                        p,
                        item_head,
                        month,
                        year,
                        source,
                    )
                )

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
