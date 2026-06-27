from django.db import migrations

# D1 (perf): partial index on master_po_mv for the home "expiry alerts" widget.
#
# dashboard.views.platform_expiry_alerts filters:
#     days_to_expiry IS NOT NULL AND days_to_expiry >= 1 AND days_to_expiry <= 5
#     AND UPPER(TRIM(po_status::text)) IN ('PENDING','APPOINTMENT DONE')
# No index covered days_to_expiry / po_status, so it sequentially scanned all
# ~41k master_po_mv rows for a very selective result (POs expiring within 5 days).
# This partial index is scoped to exactly that 1..5 band and keyed on the
# normalized po_status so the IN-list is index-resolved too.
#
# Pure access-path change: the endpoint SQL is unchanged, so results are
# identical by construction (an index cannot alter rows or aggregates).
#
# po_status (migration 0027 line 365) and days_to_expiry (0027 line 453) are
# real derived columns of the master_po view, materialized into master_po_mv
# (migration 0040). Built CONCURRENTLY (atomic=False) so it never write-locks
# the matview; IF NOT EXISTS makes it safe to re-run.


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("uploads", "0058_amazon_po_report_perf_indexes"),
        # Guarantees master_po_mv exists before we index it.
        ("platforms", "0040_master_po_materialized"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mpmv_days_to_expiry_1_5
                ON public.master_po_mv (UPPER(TRIM(po_status::text)))
                WHERE days_to_expiry >= 1 AND days_to_expiry <= 5;
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS public.idx_mpmv_days_to_expiry_1_5;
            """,
        ),
    ]
