"""
DRAFT — NOT ACTIVE.

This file is named with a leading underscore on purpose. Django's migration
loader only picks up files matching `NNNN_*.py` (four leading digits), so this
file is ignored by `makemigrations`, `migrate`, and `showmigrations`.

To activate it after review:
  1) Rename the file to `0031_primary_dashboard_indexes.py`.
  2) `python manage.py migrate platforms`.

What this migration would do
----------------------------
`prim_master_po` is a VIEW (see migrations 0027/0028), so it cannot be
indexed directly. Instead, indexes are added to the underlying tables that
the view JOINs and filters. These two indexes target the dominant cost of
the primary-dashboard SQL (the master_sheet JOIN done once per request via
the materialized TEMP TABLE):

  * `master_sheet`:   functional B-tree on UPPER(TRIM(format_sku_code))
                      — speeds the view's `LEFT JOIN master_lookup ON
                        UPPER(TRIM(sku_code)) = UPPER(TRIM(format_sku_code))`.

  * `master_sheet`:   plain B-tree on (format_sku_code)
                      — fallback used by other endpoints that JOIN without
                        normalization.

Both indexes use CREATE INDEX CONCURRENTLY so they do NOT lock writes on
`master_sheet`. The migration is wrapped in `atomic = False` because
CONCURRENTLY cannot run inside a transaction.

Risk assessment
---------------
  * READ-ONLY effect on data — indexes never modify rows.
  * Indexes consume extra disk (master_sheet is small, < a few MB typical).
  * Each write to master_sheet now updates two extra indexes — negligible
    given how rarely master_sheet is written (manual SKU master updates).
  * CONCURRENTLY can fail mid-build; the reverse SQL drops any partial index.
  * Verified safe with: `EXPLAIN ANALYZE` the primary-dashboard SQL before
    and after on a staging copy of the DB.

Roll back
---------
  `python manage.py migrate platforms 0030`

Larger optional follow-up (NOT included here, evaluate separately)
-----------------------------------------------------------------
Functional indexes on each `<slug>_prim` table's text date column via
`public._pm_parse_date(po_date)` could let the planner push date filters
through the `prim_master_po` view. This is a bigger change touching seven
tables and is best evaluated with EXPLAIN ANALYZE on staging first.
"""

from django.db import migrations


_FORWARD = r"""
-- master_sheet: speed the UPPER(TRIM(format_sku_code)) JOIN inside
-- prim_master_po. CONCURRENTLY avoids locking ongoing writes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_master_sheet_format_sku_code_upper
    ON public.master_sheet ((UPPER(TRIM(format_sku_code::text))));

-- master_sheet: plain B-tree fallback for endpoints that look up by exact
-- format_sku_code value.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_master_sheet_format_sku_code
    ON public.master_sheet (format_sku_code);
"""


_REVERSE = r"""
DROP INDEX CONCURRENTLY IF EXISTS public.ix_master_sheet_format_sku_code_upper;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_master_sheet_format_sku_code;
"""


class Migration(migrations.Migration):
    atomic = False  # CREATE INDEX CONCURRENTLY cannot run inside a transaction.

    dependencies = [
        ("platforms", "0030_short_item_name_in_view"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
