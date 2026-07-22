"""Pre-aggregated secondary matview for the cross-platform Ads Summary.

The Ads Summary union folds in secondary sell-out from `secmaster_mv` (quantity +
sales value) at that matview's full city×date×sku grain — ~853k rows, of which a
single month is still tens of thousands. In the 13-branch union the planner scans
the whole matview for the secmaster branch (~720ms of the query), because the ads
grain needs none of secmaster's city/sku detail — only SUM(quantity) /
SUM(sales_amt_exc) / SUM(amount) per (format, item_head, category, sub_category,
item), per date (date is kept so the dashboard's optional date-range filter still
works).

`secmaster_ads_summary_mv` pre-collapses exactly that: same columns the union's
secmaster branch reads, pre-summed to (year, month, date, format, + the 4 SKU
dimensions). July 2025 goes from 7,473 rows to 1,348. Because SUM is associative,
the endpoint's outer per-dimension GROUP BY over the pre-summed rows yields
identical totals to summing the raw rows — the result is unchanged.

Refresh: chained after secmaster_mv in
platforms.master_po_refresh.refresh_secmaster_mv, so a secondary upload refreshes
both. Reversible; no row data is read, written, or changed by this migration
beyond building the derived matview.
"""

from django.db import migrations


FORWARD = r"""
DROP MATERIALIZED VIEW IF EXISTS public.secmaster_ads_summary_mv CASCADE;
CREATE MATERIALIZED VIEW public.secmaster_ads_summary_mv AS
SELECT "year",
       "month",
       "date",
       "format",
       item_head,
       category,
       sub_category,
       item,
       -- Pre-compute the SAME per-row expressions the Ads Summary union applied
       -- to secmaster_mv, then sum them. Crucially the value expression's CASE
       -- mixes amount (real) and sales_amt_exc (numeric); Postgres resolves that
       -- CASE to a float type, so each row is coerced through real EXACTLY as in
       -- the live union. Baking that per-row cast into the matview (instead of
       -- re-applying the CASE over pre-summed clean-numeric columns) makes the
       -- endpoint's totals BIT-IDENTICAL to the old FROM secmaster_mv path —
       -- numeric SUM is associative, so pre-summing per (date, format, dims) then
       -- re-summing per dimension yields the exact same value.
       SUM(COALESCE(quantity, 0)::numeric) AS sec_qty,
       SUM((CASE WHEN UPPER(TRIM("format"::text)) = 'FLIPKART'
                 THEN COALESCE(amount, 0)
                 ELSE COALESCE(sales_amt_exc, 0) END)::numeric) AS sec_value
  FROM public.secmaster_mv
 GROUP BY "year", "month", "date", "format",
          item_head, category, sub_category, item;
CREATE INDEX IF NOT EXISTS idx_secmaster_ads_summary_mv_year_month
    ON public.secmaster_ads_summary_mv ("year", "month");
"""

REVERSE = r"""
DROP MATERIALIZED VIEW IF EXISTS public.secmaster_ads_summary_mv CASCADE;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0064_ads_summary_hot_indexes"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE),
    ]
