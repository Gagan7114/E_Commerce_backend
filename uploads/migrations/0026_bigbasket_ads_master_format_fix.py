from django.db import migrations


class Migration(migrations.Migration):
    """Fix `bigbasket_ads_master` to space-insensitively match the BigBasket
    format string in `master_sheet`.

    The platform's canonical format value in `master_sheet` is 'BIG BASKET'
    (with a space, matching the platforms.js config `poFilterValue: 'big basket'`),
    but `bigbasket_ads.format` is stored as 'BIGBASKET' (no space, per the
    uploader spec). The original view (migration 0025) filtered
    `master_sheet.format = 'BIGBASKET'` literally, so it matched 0/10 SKUs
    and returned NULL metadata.

    This migration replaces the view with a defensive filter that strips
    whitespace before comparing — matches both 'BIG BASKET' and 'BIGBASKET'.
    """

    dependencies = [
        ("uploads", "0025_bigbasket_ads_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.bigbasket_ads_master AS
            SELECT
                b.date                                          AS date,
                b.product_id                                    AS sku_id,
                b.product_name                                  AS sku_name,
                b.orders_sku                                    AS direct_qty_sold,
                b.other_sku_orders                              AS indirect_qty_sold,
                b.ad_impressions                                AS impressions,
                b.ad_spend                                      AS ad_spent,
                b.ad_revenue                                    AS gmv,

                ms.format                                       AS format,
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,
                ms.per_unit                                     AS per_unit,
                ms.per_unit_value                               AS per_ltr,

                (COALESCE(ms.per_unit_value, 0)
                   * COALESCE(b.orders_sku, 0))                 AS ads_ltr_sold,
                UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM b.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

            FROM public.bigbasket_ads b

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(b.product_id))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BIGBASKET';
            """,
            reverse_sql="""
            CREATE OR REPLACE VIEW public.bigbasket_ads_master AS
            SELECT
                b.date                                          AS date,
                b.product_id                                    AS sku_id,
                b.product_name                                  AS sku_name,
                b.orders_sku                                    AS direct_qty_sold,
                b.other_sku_orders                              AS indirect_qty_sold,
                b.ad_impressions                                AS impressions,
                b.ad_spend                                      AS ad_spent,
                b.ad_revenue                                    AS gmv,

                ms.format                                       AS format,
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,
                ms.per_unit                                     AS per_unit,
                ms.per_unit_value                               AS per_ltr,

                (COALESCE(ms.per_unit_value, 0)
                   * COALESCE(b.orders_sku, 0))                 AS ads_ltr_sold,
                UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM b.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

            FROM public.bigbasket_ads b

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(b.product_id))
                  AND UPPER(TRIM(ms.format::text)) = 'BIGBASKET';
            """,
        ),
    ]
