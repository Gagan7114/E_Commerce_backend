from django.db import migrations

# Materialize the per-day ads master views (Swiggy/Zepto/BigBasket Daily Ads
# dashboards). Like blinkit_ads_master_mv / swiggy_ads_master_mv (migration
# 0060-era), the plain views recompute an expensive per-row LATERAL join
# (monthly_landing_rate + master_sheet + campaign->SKU bridge) on EVERY dashboard
# read, and _ads_dashboard_payload reads the source 6-7x per request -> 5-10s.
# Materializing turns every read into a cheap table scan; the matviews are
# refreshed after each daily-ads upload (refresh_ads_master_mvs).
FORWARD = r"""
DROP MATERIALIZED VIEW IF EXISTS public.swiggyads_daily_master_mv CASCADE;
CREATE MATERIALIZED VIEW public.swiggyads_daily_master_mv AS
SELECT s.date,
    s.campaign_id,
    s.campaign_name,
    s.total_conversions AS direct_qty_sold,
    s.total_impressions AS impressions,
    s.total_budget_burnt AS ad_spent,
    s.total_gmv AS direct_gmv,
    s.format,
    amb.sku_id AS format_sku_code,
    ms.sku_sap_name AS sap_sku_name,
    ms.category,
    ms.sub_category,
    ms.item,
    ms.item_head,
    ms.per_unit,
    ms.per_unit_value AS per_ltr,
    COALESCE(ms.per_unit_value, 0::real) * COALESCE(s.total_conversions, 0::numeric)::double precision AS ads_ltr_sold,
    upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM s.date)::integer AS year,
    (lpad(EXTRACT(day FROM s.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text)) AS month_day,
    (( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(amb.sku_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'SWIGGY'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, s.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1)) * COALESCE(s.total_conversions, 0::numeric) AS total_sale_basic_rate,
    ( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(amb.sku_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'SWIGGY'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, s.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1) AS basic_rate
   FROM swiggyads_daily s
     LEFT JOIN LATERAL ( SELECT amb_1.sku_id
           FROM ads_master_bs amb_1
             JOIN swiggyads_daily s2 ON s2.campaign_id = amb_1.campaign_id
          WHERE upper(TRIM(BOTH FROM amb_1.format)) = 'SWIGGY'::text AND (amb_1.campaign_id = s.campaign_id OR upper(TRIM(BOTH FROM s2.campaign_name)) = upper(TRIM(BOTH FROM s.campaign_name)))
          ORDER BY (amb_1.campaign_id = s.campaign_id) DESC, (upper(TRIM(BOTH FROM amb_1.month)) = upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text))) DESC, amb_1.updated_at DESC NULLS LAST, amb_1.created_at DESC NULLS LAST
         LIMIT 1) amb ON true
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM amb.sku_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'SWIGGY'::text;
CREATE OR REPLACE VIEW public.swiggyads_daily_master AS SELECT * FROM public.swiggyads_daily_master_mv;
DROP MATERIALIZED VIEW IF EXISTS public.zeptoads_daily_master_mv CASCADE;
CREATE MATERIALIZED VIEW public.zeptoads_daily_master_mv AS
SELECT z.date,
    z.product_id AS sku_id,
    z.product_name AS sku_name,
    z.same_skus AS direct_qty_sold,
    z.other_skus AS indirect_qty_sold,
    z.impressions,
    z.spend AS ad_spent,
    z.revenue AS gmv,
    ms.format,
    ms.category,
    ms.sub_category,
    ms.item,
    ms.item_head,
    ms.per_unit,
    ms.per_unit_value AS per_ltr,
    COALESCE(ms.per_unit_value, 0::real) * COALESCE(z.same_skus, 0::numeric)::double precision AS ads_ltr_sold,
    upper(to_char(z.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM z.date)::integer AS year,
    (lpad(EXTRACT(day FROM z.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(z.date::timestamp with time zone, 'FMMonth'::text)) AS month_day,
    (( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(z.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'ZEPTO'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, z.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1)) * COALESCE(z.same_skus, 0::numeric) AS total_sale_basic_rate,
    ( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(z.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'ZEPTO'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, z.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1) AS basic_rate
   FROM zeptoads_daily z
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z.product_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'ZEPTO'::text;
CREATE OR REPLACE VIEW public.zeptoads_daily_master AS SELECT * FROM public.zeptoads_daily_master_mv;
DROP MATERIALIZED VIEW IF EXISTS public.bigbasketads_daily_master_mv CASCADE;
CREATE MATERIALIZED VIEW public.bigbasketads_daily_master_mv AS
SELECT b.date,
    b.product_id AS sku_id,
    b.product_name AS sku_name,
    b.orders_sku AS direct_qty_sold,
    b.other_sku_orders AS indirect_qty_sold,
    b.ad_impressions AS impressions,
    b.ad_spend AS ad_spent,
    b.ad_revenue AS gmv,
    ms.format,
    ms.category,
    ms.sub_category,
    ms.item,
    ms.item_head,
    ms.per_unit,
    ms.per_unit_value AS per_ltr,
    COALESCE(ms.per_unit_value, 0::real) * COALESCE(b.orders_sku, 0::numeric)::double precision AS ads_ltr_sold,
    upper(to_char(b.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM b.date)::integer AS year,
    (lpad(EXTRACT(day FROM b.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(b.date::timestamp with time zone, 'FMMonth'::text)) AS month_day,
    (( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(b.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'BIGBASKET'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, b.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1)) * COALESCE(b.orders_sku, 0::numeric) AS total_sale_basic_rate,
    ( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(b.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'BIGBASKET'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, b.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1) AS basic_rate
   FROM bigbasketads_daily b
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM b.product_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'BIGBASKET'::text;
CREATE OR REPLACE VIEW public.bigbasketads_daily_master AS SELECT * FROM public.bigbasketads_daily_master_mv;
"""

REVERSE = r"""
CREATE OR REPLACE VIEW public.swiggyads_daily_master AS
SELECT s.date,
    s.campaign_id,
    s.campaign_name,
    s.total_conversions AS direct_qty_sold,
    s.total_impressions AS impressions,
    s.total_budget_burnt AS ad_spent,
    s.total_gmv AS direct_gmv,
    s.format,
    amb.sku_id AS format_sku_code,
    ms.sku_sap_name AS sap_sku_name,
    ms.category,
    ms.sub_category,
    ms.item,
    ms.item_head,
    ms.per_unit,
    ms.per_unit_value AS per_ltr,
    COALESCE(ms.per_unit_value, 0::real) * COALESCE(s.total_conversions, 0::numeric)::double precision AS ads_ltr_sold,
    upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM s.date)::integer AS year,
    (lpad(EXTRACT(day FROM s.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text)) AS month_day,
    (( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(amb.sku_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'SWIGGY'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, s.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1)) * COALESCE(s.total_conversions, 0::numeric) AS total_sale_basic_rate,
    ( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(amb.sku_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'SWIGGY'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, s.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1) AS basic_rate
   FROM swiggyads_daily s
     LEFT JOIN LATERAL ( SELECT amb_1.sku_id
           FROM ads_master_bs amb_1
             JOIN swiggyads_daily s2 ON s2.campaign_id = amb_1.campaign_id
          WHERE upper(TRIM(BOTH FROM amb_1.format)) = 'SWIGGY'::text AND (amb_1.campaign_id = s.campaign_id OR upper(TRIM(BOTH FROM s2.campaign_name)) = upper(TRIM(BOTH FROM s.campaign_name)))
          ORDER BY (amb_1.campaign_id = s.campaign_id) DESC, (upper(TRIM(BOTH FROM amb_1.month)) = upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text))) DESC, amb_1.updated_at DESC NULLS LAST, amb_1.created_at DESC NULLS LAST
         LIMIT 1) amb ON true
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM amb.sku_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'SWIGGY'::text;
DROP MATERIALIZED VIEW IF EXISTS public.swiggyads_daily_master_mv CASCADE;
CREATE OR REPLACE VIEW public.zeptoads_daily_master AS
SELECT z.date,
    z.product_id AS sku_id,
    z.product_name AS sku_name,
    z.same_skus AS direct_qty_sold,
    z.other_skus AS indirect_qty_sold,
    z.impressions,
    z.spend AS ad_spent,
    z.revenue AS gmv,
    ms.format,
    ms.category,
    ms.sub_category,
    ms.item,
    ms.item_head,
    ms.per_unit,
    ms.per_unit_value AS per_ltr,
    COALESCE(ms.per_unit_value, 0::real) * COALESCE(z.same_skus, 0::numeric)::double precision AS ads_ltr_sold,
    upper(to_char(z.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM z.date)::integer AS year,
    (lpad(EXTRACT(day FROM z.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(z.date::timestamp with time zone, 'FMMonth'::text)) AS month_day,
    (( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(z.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'ZEPTO'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, z.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1)) * COALESCE(z.same_skus, 0::numeric) AS total_sale_basic_rate,
    ( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(z.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'ZEPTO'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, z.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1) AS basic_rate
   FROM zeptoads_daily z
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z.product_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'ZEPTO'::text;
DROP MATERIALIZED VIEW IF EXISTS public.zeptoads_daily_master_mv CASCADE;
CREATE OR REPLACE VIEW public.bigbasketads_daily_master AS
SELECT b.date,
    b.product_id AS sku_id,
    b.product_name AS sku_name,
    b.orders_sku AS direct_qty_sold,
    b.other_sku_orders AS indirect_qty_sold,
    b.ad_impressions AS impressions,
    b.ad_spend AS ad_spent,
    b.ad_revenue AS gmv,
    ms.format,
    ms.category,
    ms.sub_category,
    ms.item,
    ms.item_head,
    ms.per_unit,
    ms.per_unit_value AS per_ltr,
    COALESCE(ms.per_unit_value, 0::real) * COALESCE(b.orders_sku, 0::numeric)::double precision AS ads_ltr_sold,
    upper(to_char(b.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM b.date)::integer AS year,
    (lpad(EXTRACT(day FROM b.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(b.date::timestamp with time zone, 'FMMonth'::text)) AS month_day,
    (( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(b.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'BIGBASKET'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, b.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1)) * COALESCE(b.orders_sku, 0::numeric) AS total_sale_basic_rate,
    ( SELECT mlr.basic_rate
           FROM monthly_landing_rate mlr
          WHERE upper(btrim(mlr.sku_code::text)) = upper(btrim(b.product_id)) AND replace(upper(btrim(mlr.format::text)), ' '::text, ''::text) = 'BIGBASKET'::text AND btrim(mlr.month::text) = to_char(date_trunc('month'::text, b.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY mlr.created_at DESC NULLS LAST
         LIMIT 1) AS basic_rate
   FROM bigbasketads_daily b
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM b.product_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'BIGBASKET'::text;
DROP MATERIALIZED VIEW IF EXISTS public.bigbasketads_daily_master_mv CASCADE;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0054_meta_data_unique_key"),
    ]
    operations = [migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE)]
