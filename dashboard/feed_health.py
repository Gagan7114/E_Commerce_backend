"""Read-only feed-freshness + data-quality checks.

Addresses the "silent failure" class from the 2026-07-06 audit:
  * #8  JioMart secondary/inventory feeds silently died (Apr 15).
  * #10 Ads feeds silently froze per-platform (BigBasket, Flipkart, ...).
  * #18 master_sheet SKUs with a null per-litre value roll up as 0 litres.

None of these are code bugs — the feeds stopped upstream and the master data
has gaps — so code can't "restore" them. What it CAN do is surface them loudly
so a human/cron catches the death instead of trusting a stale dashboard.

DB SAFETY: every query here is a single MAX()/COUNT() over one table (plus one
GROUP BY over secmaster_mv). No full-table transfer, no writes, no DDL. Cheap
enough to run on a schedule. The endpoint that serves this is cached.
"""
from datetime import date

from django.db import connection
from django.utils import timezone


# (label, table, date_col, max_age_days, category). Thresholds reflect each
# feed's real cadence: daily feeds go stale fast; the monthly Flipkart ad batch
# and the periodic Amazon price sheet are given longer windows.
_TABLE_FEEDS = [
    ("Blinkit inventory",   "blinkit_inventory",   "inventory_date", 2,  "inventory"),
    ("Zepto inventory",     "zepto_inventory",     "inventory_date", 2,  "inventory"),
    ("Swiggy inventory",    "swiggy_inventory",     "inventory_date", 2,  "inventory"),
    ("BigBasket inventory", "bigbasket_inventory",  "inventory_date", 2,  "inventory"),
    ("JioMart inventory",   "jiomart_inventory",    "inventory_date", 3,  "inventory"),
    ("Amazon inventory",    "amazon_inventory",     "inventory_date", 3,  "inventory"),
    # Flipkart has no inventory feed at all (audit #11). Listing it here makes
    # the ABSENCE visible ("no_data") instead of silently missing — so "no feed"
    # is distinguishable from "zero stock". Turns green once a feed is ingested.
    ("Flipkart inventory",  "flipkart_inventory",   "inventory_date", 3,  "inventory"),
    ("Amazon secondary",    "amazon_sec_daily",     "report_date",    3,  "secondary"),
    ("Blinkit ads",         "blinkit_ads",          "date",           3,  "ads"),
    ("Amazon ads",          "amazon_ads",           "date",           3,  "ads"),
    ("Swiggy ads",          "swiggy_ads",           "date",           3,  "ads"),
    ("Zepto ads",           "zepto_ads",            "date",           3,  "ads"),
    ("BigBasket ads",       "bigbasket_ads",        "date",           5,  "ads"),
    ("Flipkart ads",        "flipkart_ads",         "date",           10, "ads"),  # monthly batch
    ("Amazon price",        "amazon_price_data",    "upload_date",    14, "price"),
]

# secmaster_mv covers the QC secondary feeds; resolved in ONE grouped query.
# key = normalised format -> (label, max_age_days).
_SECMASTER_FEEDS = {
    "blinkit":   ("Blinkit secondary",   2),
    "zepto":     ("Zepto secondary",     2),
    "swiggy":    ("Swiggy secondary",    2),
    "bigbasket": ("BigBasket secondary", 2),
    "flipkart":  ("Flipkart secondary",  3),
    "jiomart":   ("JioMart secondary",   3),
}


def _ident(name: str) -> str:
    """Quote one of OUR OWN constant table names (never request input)."""
    return '"' + name.replace('"', '') + '"'


def _max_date(table: str, col: str):
    with connection.cursor() as cur:
        cur.execute(f'SELECT MAX({_ident(col)}) FROM {_ident(table)}')
        row = cur.fetchone()
        return row[0] if row else None


def _status_row(label, category, max_date, max_age, today):
    if max_date is None:
        return {
            "feed": label, "category": category, "max_date": None,
            "age_days": None, "threshold_days": max_age, "status": "no_data",
        }
    age = (today - max_date).days
    return {
        "feed": label, "category": category,
        "max_date": max_date.isoformat(), "age_days": age,
        "threshold_days": max_age,
        "status": "stale" if age > max_age else "fresh",
    }


def feed_freshness(today=None):
    """One status row per feed: {feed, category, max_date, age_days,
    threshold_days, status in fresh|stale|no_data}. Stale feeds sort first."""
    today = today or timezone.localdate()
    rows = []
    for label, table, col, max_age, cat in _TABLE_FEEDS:
        try:
            md = _max_date(table, col)
        except Exception:
            md = None
        rows.append(_status_row(label, cat, md, max_age, today))

    # All secmaster-backed secondary feeds in a single grouped scan.
    found = {}
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') AS fmt, "
                'MAX("date") FROM secmaster_mv GROUP BY 1'
            )
            found = {r[0]: r[1] for r in cur.fetchall()}
    except Exception:
        found = {}
    for fmt, (label, max_age) in _SECMASTER_FEEDS.items():
        rows.append(_status_row(label, "secondary", found.get(fmt), max_age, today))

    rows.sort(key=lambda r: (r["status"] == "fresh", r["feed"]))
    return rows


def null_per_litre_skus(limit: int = 50):
    """master_sheet SKUs with no per-litre value → they roll up as 0 litres
    (audit #18). Read-only report so ops can fill the gaps; code never guesses
    the value."""
    total = nulls = 0
    sample = []
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE per_unit_value IS NULL) FROM master_sheet")
            total, nulls = cur.fetchone()
            cur.execute(
                "SELECT format, format_sku_code, item FROM master_sheet "
                "WHERE per_unit_value IS NULL ORDER BY format, format_sku_code LIMIT %s",
                [limit],
            )
            sample = [{"format": r[0], "sku_code": r[1], "item": r[2]} for r in cur.fetchall()]
    except Exception:
        pass
    total = int(total or 0)
    nulls = int(nulls or 0)
    return {
        "total_skus": total,
        "null_per_litre": nulls,
        "pct": round(nulls * 100.0 / total, 1) if total else 0.0,
        "sample": sample,
    }


def data_health(today=None):
    today = today or timezone.localdate()
    feeds = feed_freshness(today)
    return {
        "generated_for": today.isoformat(),
        "stale_count": sum(1 for f in feeds if f["status"] == "stale"),
        "no_data_count": sum(1 for f in feeds if f["status"] == "no_data"),
        "feeds": feeds,
        "per_litre_gaps": null_per_litre_skus(),
    }
