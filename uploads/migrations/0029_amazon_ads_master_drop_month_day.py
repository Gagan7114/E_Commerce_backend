from django.db import migrations


class Migration(migrations.Migration):
    """Drop `month_day` column from amazon_ads_master view.

    Daily uploads mean the column adds no signal beyond (year, month, date) —
    redundant for the dashboard's filtering and reporting purposes.

    The view is recreated identically to migration 0017 minus the `month_day`
    derived column. All other columns (every `amazon_ads.*` column, `year`,
    `month`, `category`, `sub_category`, `item_head`) are preserved.
    """

    dependencies = [
        ("uploads", "0028_blinkit_ads_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            # CREATE OR REPLACE VIEW cannot DROP columns from a view's column
            # list — it only appends. So we DROP + CREATE.
            sql="""
            DROP VIEW IF EXISTS public.amazon_ads_master;

            CREATE VIEW public.amazon_ads_master AS
            SELECT
                a.*,
                EXTRACT(YEAR FROM a.date)::integer        AS year,
                UPPER(TO_CHAR(a.date, 'FMMonth'))         AS month,
                m.category      AS category,
                m.sub_category  AS sub_category,
                m.item_head     AS item_head
            FROM public.amazon_ads a
            LEFT JOIN public.master_sheet m
                ON UPPER(TRIM(a.advertised_product_id))
                 = UPPER(TRIM(m.format_sku_code::text));
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.amazon_ads_master;

            CREATE VIEW public.amazon_ads_master AS
            SELECT
                a.*,
                EXTRACT(YEAR FROM a.date)::integer        AS year,
                UPPER(TO_CHAR(a.date, 'FMMonth'))         AS month,
                LPAD(EXTRACT(DAY FROM a.date)::text, 2, '0')
                    || '-' || UPPER(TO_CHAR(a.date, 'FMMonth')) AS month_day,
                m.category      AS category,
                m.sub_category  AS sub_category,
                m.item_head     AS item_head
            FROM public.amazon_ads a
            LEFT JOIN public.master_sheet m
                ON UPPER(TRIM(a.advertised_product_id))
                 = UPPER(TRIM(m.format_sku_code::text));
            """,
        ),
    ]
