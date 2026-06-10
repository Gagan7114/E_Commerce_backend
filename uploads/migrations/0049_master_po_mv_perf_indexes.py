from django.db import migrations

# Performance indexes on the master_po_mv materialized view.
# The dashboards filter master_po by a normalized-format regex (Primary Dashboard,
# primary_overview_total) and by open_close/format/po_year/po_month (pendency /
# "Latest POs"). Neither matched the two existing indexes
# (idx_mpmv_delivmonth_year_head, idx_mpmv_format), so every such read did a full
# sequential scan of all ~41k rows. These functional indexes make those filters
# index lookups. Guarded with IF NOT EXISTS so it is safe if they already exist.


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0048_ads_master_mapping_fallback"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS idx_mpmv_format_norm
                ON public.master_po_mv
                ((REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g')));

            CREATE INDEX IF NOT EXISTS idx_mpmv_pendency
                ON public.master_po_mv
                (UPPER(TRIM(open_close::text)), UPPER(TRIM(format::text)),
                 po_year, UPPER(TRIM(po_month::text)));
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.idx_mpmv_format_norm;
            DROP INDEX IF EXISTS public.idx_mpmv_pendency;
            """,
        ),
    ]
