from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view: consolidated_fsn_report + 5 live master_sheet columns.

    Enrichment join: consolidated_fsn_report.sku_id = master_sheet.product_name.
    A product_name can exist for several formats, so a LATERAL pick takes the
    FLIPKART row first and otherwise falls back to any match — deterministic, and
    always reflects the current master_sheet (no re-upload needed).
    """

    dependencies = [
        ("uploads", "0055_consolidated_fsn_report"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE OR REPLACE VIEW public.consolidated_fsn_report_master AS
            SELECT
                c.campaign_id,
                c.campaign_name,
                c.adgroup_id,
                c.adgroup_name,
                c.sku_id,
                c.product_name,
                c.views,
                c.clicks,
                c.direct_units_sold,
                c.indirect_units_sold,
                c.total_revenue,
                c.conversion_rate,
                c.roi,
                c.ad_spend,
                ms.item            AS item,
                ms.format_sku_code AS sku_code,
                ms.category        AS category,
                ms.sub_category    AS sub_category,
                ms.item_head       AS item_head,
                c.format,
                c.uploaded_at
            FROM public.consolidated_fsn_report c
            LEFT JOIN LATERAL (
                SELECT m.item, m.format_sku_code, m.category, m.sub_category, m.item_head
                FROM public.master_sheet m
                WHERE UPPER(TRIM(m.product_name::text)) = UPPER(TRIM(c.sku_id))
                ORDER BY (UPPER(TRIM(m.format::text)) = 'FLIPKART') DESC,
                         m.format_sku_code
                LIMIT 1
            ) ms ON TRUE;
            """,
            reverse_sql="DROP VIEW IF EXISTS public.consolidated_fsn_report_master;",
        ),
    ]
