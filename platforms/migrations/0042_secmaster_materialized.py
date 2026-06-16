"""Materialize the SecMaster secondary view for fast dashboard reads.

`SecMaster` is a plain SQL view that UNIONs every platform's secondary table and
LEFT JOIN LATERALs into master_sheet + monthly_landing_rate per row, so a single
read recomputes ~726k rows in ~6-7s. The DRR / Secondary / Summary dashboards each
run several such reads -> 5-10s loads.

This creates `secmaster_mv`, a materialized copy (SELECT * so columns are
identical), with:
  - a UNIQUE index on a synthetic mv_row_id -> enables REFRESH ... CONCURRENTLY,
    which rebuilds without locking out dashboard reads;
  - a composite index on the normalized format/month/year used by the WHERE
    filters.
The dashboards are repointed from "SecMaster" to secmaster_mv (see
platforms/views.py), and it is refreshed after secondary uploads
(platforms/master_po_refresh.refresh_secmaster_mv_async) + on a schedule.

Idempotent: only creates the matview if it doesn't already exist.
"""

from django.db import migrations


CREATE_SQL = r"""
DO $$
BEGIN
  IF to_regclass('public.secmaster_mv') IS NULL THEN
    CREATE MATERIALIZED VIEW public.secmaster_mv AS
      SELECT *, row_number() OVER () AS mv_row_id
      FROM "SecMaster";

    -- Unique index is required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
    CREATE UNIQUE INDEX uq_secmaster_mv_rowid
      ON public.secmaster_mv (mv_row_id);

    -- Matches the dashboards' WHERE filters (normalized format + month + year).
    CREATE INDEX idx_secmaster_mv_fmt_month_year
      ON public.secmaster_mv (
        REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g'),
        UPPER(TRIM("month"::text)),
        "year"
      );
  END IF;
END $$;
"""

DROP_SQL = "DROP MATERIALIZED VIEW IF EXISTS public.secmaster_mv;"


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0041_amazon_mp_master_materialized"),
    ]

    operations = [
        migrations.RunSQL(CREATE_SQL, reverse_sql=DROP_SQL),
    ]
