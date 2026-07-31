"""Backfill monthly_landing_rate for 2025 + early-2026 months that never had a rate.

Context: `monthly_landing_rate` only starts in 2026-01 (BigBasket/Blinkit), 2026-03
(Swiggy) or 2026-04 (Zepto/Flipkart Grocery), but secondary sales data goes back to
2024. The carry-forward laterals added in platforms/0049 + uploads/0079 only look
BACKWARD (latest rate with month <= the sale's month), which is why a month with no
rate yet shows the previous month's value — July-2026 correctly reads June-2026.
That mechanism cannot help months that are EARLIER than a SKU's first-ever rate:
those find nothing and fall to 0, so 2025 and early-2026 secondary sales showed no
landing rate at all (2025: 325,059 rows, 100% unrated).

User decision (2026-07-31): fill those months with the MAY-2026 rate, scope = 2025
and 2026, written as REAL rows in monthly_landing_rate (not a view-level fallback)
so they are visible and editable in the Landing Rate screen.

WHICH CELLS GET FILLED
----------------------
For each (format, sku_code) that has a May-2026 rate, every month from 2025-01 up to
the month BEFORE that key's earliest existing rate.

Months at or after a key's earliest rate are deliberately NOT touched — the existing
carry-forward already covers them, and overwriting would be wrong in both directions:
  * Blinkit has no 2026-03 rate but does have 2026-02; carry-forward gives it Feb's
    rate. Inserting May there would silently re-price March.
  * July-2026 has no rate and must keep reading June-2026 (the behaviour the user
    confirmed is correct). Inserting a July row would break exactly that.
This is also what makes the migration self-limiting: after it runs, each key's
earliest rate becomes 2025-01, so a re-run selects nothing. ON CONFLICT DO NOTHING is
a second guard — a real, hand-entered rate can never be overwritten.

Keys are matched on UPPER(TRIM(...)) for BOTH format and sku_code. One Zepto SKU is
stored uppercase in May and lowercase in June (CF896675-...); joining on raw text
would read its earliest rate as 2026-05 for one spelling and 2026-06 for the other
and insert a duplicate June row that the consumers' UPPER(TRIM()) laterals would then
pick over the genuine one (ties break on created_at DESC, and ours is newer).

ROWS: 1,408 — BigBasket 384, Zepto 541, Swiggy 345, Blinkit 108, Flipkart Grocery 30.

NOT COVERED (no May-2026 rate exists for these SKUs, so there is nothing to copy):
FLIPKART 267 SKUs / 23,268 rows and JIO MART 20 SKUs / 9,434 rows — neither platform
has EVER had a landing rate in any month, so they stay at 0. (FLIPKART here is the
marketplace, a different platform from FLIPKART GROCERY.) Plus Swiggy 8 SKUs / 621
rows and Zepto 2 SKUs / 276 rows that are absent from the May sheet.

AUDIT / REVERSAL: every inserted row is stamped created_at = 2026-07-31T00:00:00Z, a
constant that no genuine upload produces. That marks them as carried-forward rather
than hand-entered, and lets backwards() delete exactly this batch and nothing else.
"""
from django.db import migrations, transaction

# Constant batch stamp — audit marker AND the exact reversal key. Deliberately not
# now(): the reverse must be able to find these rows on any environment/date.
BATCH_STAMP = "2026-07-31 00:00:00+00"

MONTH_FROM = "2025-01-01"  # scope floor: 2025 + 2026 (2024 left untouched per user)
MONTH_TO = "2026-06-01"    # last month with a real rate; never fill beyond it

BACKFILL = f"""
WITH may AS (
    -- One row per NORMALISED key; if a SKU is stored under two spellings, keep the
    -- most recently loaded May row.
    SELECT DISTINCT ON (UPPER(TRIM(format)), UPPER(TRIM(sku_code)))
           UPPER(TRIM(format))   AS fmt_key,
           UPPER(TRIM(sku_code)) AS sku_key,
           sku_code, sku_name, landing_rate, basic_rate, format
      FROM monthly_landing_rate
     WHERE TRIM(month) = '2026-05-01'
       AND (COALESCE(basic_rate, 0) > 0 OR COALESCE(landing_rate, 0) > 0)
     ORDER BY UPPER(TRIM(format)), UPPER(TRIM(sku_code)), created_at DESC
), earliest AS (
    SELECT UPPER(TRIM(format))   AS fmt_key,
           UPPER(TRIM(sku_code)) AS sku_key,
           MIN(TRIM(month))      AS first_rate_month
      FROM monthly_landing_rate
     GROUP BY 1, 2
), months AS (
    SELECT to_char(gs, 'YYYY-MM-01') AS mon
      FROM generate_series(DATE '{MONTH_FROM}', DATE '{MONTH_TO}', INTERVAL '1 month') gs
)
INSERT INTO monthly_landing_rate
    (sku_code, sku_name, landing_rate, basic_rate, format, month, created_at)
SELECT m.sku_code, m.sku_name, m.landing_rate, m.basic_rate, m.format,
       mo.mon, TIMESTAMPTZ '{BATCH_STAMP}'
  FROM may m
  JOIN earliest e
    ON e.fmt_key = m.fmt_key AND e.sku_key = m.sku_key
 CROSS JOIN months mo
 WHERE mo.mon < e.first_rate_month
ON CONFLICT (sku_code, format, month) DO NOTHING;
"""

UNDO = f"DELETE FROM monthly_landing_rate WHERE created_at = TIMESTAMPTZ '{BATCH_STAMP}';"

# Everything downstream that bakes a landing rate into stored rows. secmaster_mv
# first: secmaster_ads_summary_mv aggregates the same union and is read alongside it,
# so refreshing it after keeps the pair consistent.
_MATVIEWS = (
    "public.secmaster_mv",
    "public.secmaster_ads_summary_mv",
    "public.blinkit_ads_master_mv",
    "public.swiggy_ads_master_mv",
    "public.zepto_ads_master_mv",
    "public.bigbasket_ads_master_mv",
    "public.swiggyads_daily_master_mv",
    "public.zeptoads_daily_master_mv",
    "public.bigbasketads_daily_master_mv",
)


def _refresh(apps, schema_editor):
    """Refresh each matview in its OWN transaction.

    A plain REFRESH takes an exclusive lock that blocks readers until COMMIT. Nine of
    them in one transaction would hold every lock for the whole run; separate
    transactions release each as it finishes. Best-effort per view — a matview that
    is absent (its migration not applied here) or fails to refresh must not roll back
    the backfill, which is the part that matters and is idempotent anyway.
    """
    conn = schema_editor.connection
    for mv in _MATVIEWS:
        try:
            with transaction.atomic():
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass(%s)", [mv])
                    if cur.fetchone()[0] is None:
                        continue
                    cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
        except Exception:
            pass


def _apply(apps, schema_editor):
    with transaction.atomic():
        with schema_editor.connection.cursor() as cur:
            cur.execute(BACKFILL)
    _refresh(apps, schema_editor)


def _unapply(apps, schema_editor):
    with transaction.atomic():
        with schema_editor.connection.cursor() as cur:
            cur.execute(UNDO)
    _refresh(apps, schema_editor)


class Migration(migrations.Migration):
    # atomic=False so the INSERT commits on its own and each matview refresh holds
    # its lock only for its own transaction (see _refresh).
    atomic = False

    dependencies = [
        ("platforms", "0067_zomato_manual_status_no_auto_expiry"),
    ]

    operations = [
        migrations.RunPython(_apply, _unapply),
    ]
