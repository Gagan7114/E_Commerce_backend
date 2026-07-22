"""Two exact-match indexes for the cross-platform Ads Summary cold query.

EXPLAIN ANALYZE of `marketing_ads_summary` (month/year filter) showed the cost
concentrated in two seq scans:

  * secmaster_mv seq scan ~705ms — the union filters the 853k-row secondary
    matview by bare `year = %s AND month = %s` (the union projects secmaster_mv's
    plain columns, and `_ads_build_where` emits bare-column predicates). The only
    secmaster_mv indexes are EXPRESSION indexes (UPPER(TRIM(month)) …), which a
    bare `month = 'JULY'` predicate cannot use -> full scan. A plain btree on
    (year, month) turns it into an index scan.
  * QC ads_sale LATERALs ~570ms — each blinkit/zepto/bigbasket/swiggy row does a
    carry-forward lookup into monthly_landing_rate keyed on
    REGEXP_REPLACE(LOWER(format), '[^a-z0-9]+', '', 'g') = <slug>
    AND UPPER(TRIM(sku_code)) = <sku>. Migration 0059's index leads with
    LOWER(TRIM(format)) (note the TRIM) so it does NOT match this predicate's
    LOWER(format) expression -> the lateral re-scans the whole table per key.
    An index on the EXACT expressions the query uses makes each lookup a probe.

Both are read-only, additive, and reversible; no data is read, written, or
changed. Values in secmaster_mv.month are already stored uppercase (the endpoint
matches on 'JULY'), so the plain index is directly usable.
"""

from django.db import migrations


FORWARD = r"""
CREATE INDEX IF NOT EXISTS idx_secmaster_mv_year_month_plain
    ON public.secmaster_mv ("year", "month");

CREATE INDEX IF NOT EXISTS idx_mlr_fmtexpr_sku
    ON public.monthly_landing_rate (
        REGEXP_REPLACE(LOWER("format"::text), '[^a-z0-9]+', ''::text, 'g'),
        UPPER(TRIM(BOTH FROM "sku_code"::text))
    );
"""

REVERSE = r"""
DROP INDEX IF EXISTS public.idx_secmaster_mv_year_month_plain;
DROP INDEX IF EXISTS public.idx_mlr_fmtexpr_sku;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0063_materialize_amazon_and_range_ads_views"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE),
    ]
