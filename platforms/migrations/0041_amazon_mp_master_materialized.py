"""Materialize amazon_mp_master.

`amazon_mp_master` was a plain view that, on EVERY read, re-parsed each
amazon_mp row's `shipment_date` with several regexp_match() calls to derive
shipment_month/year (plus a DISTINCT-ON master_sheet dedup). On the Home page
that cost ~1.5s inside the targets dashboards ("Loading targets…").

Its source tables (amazon_mp, master_sheet) only change on upload, so we
materialize it and refresh on upload — exactly like master_po_mv (migration
0040). The original view logic is preserved verbatim as `amazon_mp_master_view`
(renamed, not rewritten) and the matview simply selects from it, so REFRESH
recomputes through the same definition. Reads become a cheap table scan.

Refresh is wired via platforms.master_po_refresh.refresh_amazon_mp_master, called
from the upload hook (uploads/views.py) alongside refresh_master_po_mv.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0040_master_po_materialized"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER VIEW IF EXISTS public.amazon_mp_master RENAME TO amazon_mp_master_view;
            CREATE MATERIALIZED VIEW public.amazon_mp_master AS
                SELECT * FROM public.amazon_mp_master_view
                WITH DATA;
            CREATE INDEX IF NOT EXISTS idx_amp_master_month_year_head
                ON public.amazon_mp_master (
                    UPPER(TRIM(shipment_month::text)),
                    shipment_year,
                    UPPER(TRIM(item_head::text))
                );
            """,
            reverse_sql="""
            DROP MATERIALIZED VIEW IF EXISTS public.amazon_mp_master;
            ALTER VIEW IF EXISTS public.amazon_mp_master_view RENAME TO amazon_mp_master;
            """,
        ),
    ]
