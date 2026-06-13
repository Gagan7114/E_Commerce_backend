from django.db import migrations


class Migration(migrations.Migration):
    """Empty structural copies of the platform ads tables + their master views.

    swiggy_ads/zepto_ads/bigbasket_ads  ->  *ads_daily
    *_ads_master                        ->  *ads_daily_master

    Same format/columns/indexes, new names, no data copied. The copy tables get
    their own id sequences. The copy views read the copy tables but reuse the
    shared ads_master_bs + master_sheet mapping tables unchanged.

    See ADS_DAILY_COPY_TABLES_PLAN.md.
    """

    dependencies = [
        ("uploads", "0049_master_po_mv_perf_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            -- ── Copy tables (structure only, no data) ──────────────────────
            CREATE TABLE IF NOT EXISTS public.swiggyads_daily
                (LIKE public.swiggy_ads INCLUDING ALL);
            CREATE SEQUENCE IF NOT EXISTS public.swiggyads_daily_id_seq
                OWNED BY public.swiggyads_daily.id;
            ALTER TABLE public.swiggyads_daily
                ALTER COLUMN id SET DEFAULT nextval('public.swiggyads_daily_id_seq');

            CREATE TABLE IF NOT EXISTS public.zeptoads_daily
                (LIKE public.zepto_ads INCLUDING ALL);
            CREATE SEQUENCE IF NOT EXISTS public.zeptoads_daily_id_seq
                OWNED BY public.zeptoads_daily.id;
            ALTER TABLE public.zeptoads_daily
                ALTER COLUMN id SET DEFAULT nextval('public.zeptoads_daily_id_seq');

            CREATE TABLE IF NOT EXISTS public.bigbasketads_daily
                (LIKE public.bigbasket_ads INCLUDING ALL);
            CREATE SEQUENCE IF NOT EXISTS public.bigbasketads_daily_id_seq
                OWNED BY public.bigbasketads_daily.id;
            ALTER TABLE public.bigbasketads_daily
                ALTER COLUMN id SET DEFAULT nextval('public.bigbasketads_daily_id_seq');

            -- ── Copy master views (same logic, swapped source table) ───────
            CREATE OR REPLACE VIEW public.swiggyads_daily_master AS
            SELECT
                s.date,
                s.campaign_id,
                s.campaign_name,
                s.total_conversions   AS direct_qty_sold,
                s.total_impressions   AS impressions,
                s.total_budget_burnt  AS ad_spent,
                s.total_gmv           AS direct_gmv,
                s.format,
                amb.sku_id            AS format_sku_code,
                ms.sku_sap_name       AS sap_sku_name,
                ms.category,
                ms.sub_category,
                ms.item,
                ms.item_head,
                ms.per_unit,
                ms.per_unit_value     AS per_ltr,
                (COALESCE(ms.per_unit_value, 0) * COALESCE(s.total_conversions, 0)) AS ads_ltr_sold,
                UPPER(TO_CHAR(s.date, 'FMMonth'))  AS month,
                EXTRACT(YEAR FROM s.date)::integer AS year,
                (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0') || '-'
                   || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day
            FROM public.swiggyads_daily s
            LEFT JOIN LATERAL (
                SELECT amb_1.sku_id
                FROM public.ads_master_bs amb_1
                JOIN public.swiggyads_daily s2 ON s2.campaign_id = amb_1.campaign_id
                WHERE UPPER(TRIM(amb_1.format)) = 'SWIGGY'
                  AND (amb_1.campaign_id = s.campaign_id
                       OR UPPER(TRIM(s2.campaign_name)) = UPPER(TRIM(s.campaign_name)))
                ORDER BY (amb_1.campaign_id = s.campaign_id) DESC,
                         (UPPER(TRIM(amb_1.month)) = UPPER(TO_CHAR(s.date, 'FMMonth'))) DESC,
                         amb_1.updated_at DESC NULLS LAST,
                         amb_1.created_at DESC NULLS LAST
                LIMIT 1
            ) amb ON true
            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(amb.sku_id))
                  AND UPPER(TRIM(ms.format::text)) = 'SWIGGY';

            CREATE OR REPLACE VIEW public.zeptoads_daily_master AS
            SELECT
                z.date,
                z.product_id    AS sku_id,
                z.product_name  AS sku_name,
                z.same_skus     AS direct_qty_sold,
                z.other_skus    AS indirect_qty_sold,
                z.impressions,
                z.spend         AS ad_spent,
                z.revenue       AS gmv,
                ms.format,
                ms.category,
                ms.sub_category,
                ms.item,
                ms.item_head,
                ms.per_unit,
                ms.per_unit_value AS per_ltr,
                (COALESCE(ms.per_unit_value, 0) * COALESCE(z.same_skus, 0)) AS ads_ltr_sold,
                UPPER(TO_CHAR(z.date, 'FMMonth'))  AS month,
                EXTRACT(YEAR FROM z.date)::integer AS year,
                (LPAD(EXTRACT(DAY FROM z.date)::text, 2, '0') || '-'
                   || UPPER(TO_CHAR(z.date, 'FMMonth'))) AS month_day
            FROM public.zeptoads_daily z
            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(z.product_id))
                  AND UPPER(TRIM(ms.format::text)) = 'ZEPTO';

            CREATE OR REPLACE VIEW public.bigbasketads_daily_master AS
            SELECT
                b.date,
                b.product_id       AS sku_id,
                b.product_name     AS sku_name,
                b.orders_sku       AS direct_qty_sold,
                b.other_sku_orders AS indirect_qty_sold,
                b.ad_impressions   AS impressions,
                b.ad_spend         AS ad_spent,
                b.ad_revenue       AS gmv,
                ms.format,
                ms.category,
                ms.sub_category,
                ms.item,
                ms.item_head,
                ms.per_unit,
                ms.per_unit_value  AS per_ltr,
                (COALESCE(ms.per_unit_value, 0) * COALESCE(b.orders_sku, 0)) AS ads_ltr_sold,
                UPPER(TO_CHAR(b.date, 'FMMonth'))  AS month,
                EXTRACT(YEAR FROM b.date)::integer AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0') || '-'
                   || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day
            FROM public.bigbasketads_daily b
            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(b.product_id))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BIGBASKET';
            """,
            reverse_sql=r"""
            DROP VIEW IF EXISTS public.swiggyads_daily_master;
            DROP VIEW IF EXISTS public.zeptoads_daily_master;
            DROP VIEW IF EXISTS public.bigbasketads_daily_master;
            DROP TABLE IF EXISTS public.swiggyads_daily;
            DROP TABLE IF EXISTS public.zeptoads_daily;
            DROP TABLE IF EXISTS public.bigbasketads_daily;
            """,
        ),
    ]
