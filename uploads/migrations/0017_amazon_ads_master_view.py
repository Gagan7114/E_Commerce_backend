from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view amazon_ads_master.

    All columns of amazon_ads + 3 derived date columns (year, month, month_day)
    + 3 master_sheet columns (category, sub_category, item_head) joined on
    amazon_ads.advertised_product_id = master_sheet.format_sku_code for the
    Amazon format only.
    """

    dependencies = [
        ("uploads", "0016_amazon_ads_date"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.amazon_ads_master AS
            SELECT
                a.*,
                EXTRACT(YEAR FROM a.date)::integer        AS year,
                UPPER(TO_CHAR(a.date, 'FMMonth'))          AS month,
                LPAD(EXTRACT(DAY FROM a.date)::text, 2, '0')
                    || '-' || UPPER(TO_CHAR(a.date, 'FMMonth')) AS month_day,
                m.category      AS category,
                m.sub_category  AS sub_category,
                m.item_head     AS item_head
            FROM public.amazon_ads a
            LEFT JOIN public.master_sheet m
                ON UPPER(TRIM(a.advertised_product_id))
                 = UPPER(TRIM(m.format_sku_code::text))
               AND UPPER(TRIM(m.format::text)) = 'AMAZON';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.amazon_ads_master;
            """,
        ),
    ]
