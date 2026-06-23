from django.db import migrations


class Migration(migrations.Migration):
    """Flipkart "Consolidated FSN Report" raw upload table.

    Stores the 14 columns the user uploads. The 5 enrichment columns
    (ITEM / SKU CODE / CATEGORY / SUB CATEGORY / ITEM HEAD) are NOT stored here
    — they are joined live from master_sheet in the consolidated_fsn_report_master
    view (next migration) on sku_id = master_sheet.product_name. Each upload is a
    full snapshot (the uploader wipes + reloads via the batch `replace_all` flag),
    so no per-row date / dedup key is needed.
    """

    dependencies = [
        ("uploads", "0054_flipkart_state_sales"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.consolidated_fsn_report (
                id                   BIGSERIAL PRIMARY KEY,
                campaign_id          TEXT,
                campaign_name        TEXT,
                adgroup_id           TEXT,
                adgroup_name         TEXT,
                sku_id               TEXT,
                product_name         TEXT,
                views                NUMERIC,
                clicks               NUMERIC,
                direct_units_sold    NUMERIC,
                indirect_units_sold  NUMERIC,
                total_revenue        NUMERIC,
                conversion_rate      NUMERIC,
                roi                  NUMERIC,
                ad_spend             NUMERIC,
                format               TEXT NOT NULL DEFAULT 'FLIPKART',
                uploaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS consolidated_fsn_report_sku_idx
                ON public.consolidated_fsn_report (sku_id);
            CREATE INDEX IF NOT EXISTS consolidated_fsn_report_campaign_idx
                ON public.consolidated_fsn_report (campaign_id);
            """,
            reverse_sql="DROP TABLE IF EXISTS public.consolidated_fsn_report;",
        ),
    ]
