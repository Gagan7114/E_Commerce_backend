from django.db import migrations


class Migration(migrations.Migration):
    """Dedupe master_sheet by format + format_sku_code.

    The Amazon Ads master view joins on Amazon ASINs, so master_sheet must not
    contain more than one row for the same normalized platform/SKU pair.
    """

    dependencies = [
        ("uploads", "0017_amazon_ads_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            WITH ranked AS (
                SELECT
                    ctid,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            COALESCE(UPPER(TRIM(format::text)), ''),
                            UPPER(TRIM(format_sku_code::text))
                        ORDER BY ctid::text
                    ) AS rn
                FROM public.master_sheet
                WHERE COALESCE(TRIM(format_sku_code::text), '') <> ''
            )
            DELETE FROM public.master_sheet m
            USING ranked r
            WHERE m.ctid = r.ctid
              AND r.rn > 1;

            CREATE UNIQUE INDEX IF NOT EXISTS master_sheet_format_sku_unique_idx
            ON public.master_sheet (
                COALESCE(UPPER(TRIM(format::text)), ''),
                UPPER(TRIM(format_sku_code::text))
            )
            WHERE COALESCE(TRIM(format_sku_code::text), '') <> '';

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
            DROP INDEX IF EXISTS public.master_sheet_format_sku_unique_idx;
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
                 = UPPER(TRIM(m.format_sku_code::text));
            """,
        ),
    ]
