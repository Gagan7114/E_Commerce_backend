# Adds a `basic_rate` column to the per-platform ads master views (Blinkit /
# Swiggy / Zepto / BigBasket / Flipkart — NOT Amazon), alongside the
# total_sale_basic_rate added in 0071. `basic_rate` is the per-SKU monthly basic
# rate looked up from monthly_landing_rate (sku_code + normalised format +
# reporting month); total_sale_basic_rate is that same rate × direct_qty_sold.
#
# CREATE OR REPLACE only appends the trailing column, so basic_rate lands after
# total_sale_basic_rate. Flipkart has no SKU code, so its basic_rate is NULL.
from django.db import migrations


def _basic_rate(sku_expr, date_expr, platform):
    return f"""(
        SELECT mlr.basic_rate
        FROM monthly_landing_rate mlr
        WHERE upper(btrim(mlr.sku_code)) = upper(btrim({sku_expr}))
          AND replace(upper(btrim(mlr.format)), ' ', '') = '{platform}'
          AND btrim(mlr.month) = to_char(date_trunc('month', {date_expr}::timestamp), 'YYYY-MM-DD')
        ORDER BY mlr.created_at DESC NULLS LAST
        LIMIT 1
    )"""


FORWARD = f"""
CREATE OR REPLACE VIEW blinkit_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, indirect_qty_sold,
        impressions, ad_spent, direct_gmv, indirect_gmv, format, format_sku_code,
        sap_sku_name, category, sub_category, item, item_head, per_unit, per_ltr,
        ads_ltr_sold, month, year, month_day,
        {_basic_rate('format_sku_code', 'date', 'BLINKIT')} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate,
        {_basic_rate('format_sku_code', 'date', 'BLINKIT')} AS basic_rate
   FROM blinkit_ads_master_mv;

CREATE OR REPLACE VIEW swiggy_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, impressions, ad_spent,
        direct_gmv, format, format_sku_code, sap_sku_name, category, sub_category,
        item,
        COALESCE(NULLIF(TRIM(BOTH FROM item_head), ''::text), 'PREMIUM'::text) AS item_head,
        per_unit, per_ltr, ads_ltr_sold, month, year, month_day,
        {_basic_rate('format_sku_code', 'date', 'SWIGGY')} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate,
        {_basic_rate('format_sku_code', 'date', 'SWIGGY')} AS basic_rate
   FROM swiggy_ads_master_mv;

CREATE OR REPLACE VIEW zepto_ads_master AS
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
        {_basic_rate('z.product_id', 'z.date', 'ZEPTO')} * COALESCE(z.same_skus, 0::numeric)
            AS total_sale_basic_rate,
        {_basic_rate('z.product_id', 'z.date', 'ZEPTO')} AS basic_rate
   FROM zepto_ads z
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z.product_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'ZEPTO'::text;

CREATE OR REPLACE VIEW bigbasket_ads_master AS
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
        {_basic_rate('b.product_id', 'b.date', 'BIGBASKET')} * COALESCE(b.orders_sku, 0::numeric)
            AS total_sale_basic_rate,
        {_basic_rate('b.product_id', 'b.date', 'BIGBASKET')} AS basic_rate
   FROM bigbasket_ads b
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM b.product_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'BIGBASKET'::text;

CREATE OR REPLACE VIEW flipkart_ads_master AS
 SELECT id, date, campaign_id, campaign_name, campaign_status, campaign_type,
        budgeting_type, campaign_budget, ad_spend, views, clicks,
        total_converted_units, total_revenue, roi, click_through_rate,
        conversion_rate, format, uploaded_at,
        EXTRACT(year FROM date)::integer AS year,
        upper(to_char(date::timestamp with time zone, 'FMMonth'::text)) AS month,
        NULL::numeric AS total_sale_basic_rate,
        NULL::numeric AS basic_rate
   FROM flipkart_ads f;
"""


# ── Reverse: recreate the 0071 view definitions (total_sale_basic_rate only). ─
REVERSE = f"""
DROP VIEW IF EXISTS blinkit_ads_master;
CREATE VIEW blinkit_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, indirect_qty_sold,
        impressions, ad_spent, direct_gmv, indirect_gmv, format, format_sku_code,
        sap_sku_name, category, sub_category, item, item_head, per_unit, per_ltr,
        ads_ltr_sold, month, year, month_day,
        {_basic_rate('format_sku_code', 'date', 'BLINKIT')} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate
   FROM blinkit_ads_master_mv;

DROP VIEW IF EXISTS swiggy_ads_master;
CREATE VIEW swiggy_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, impressions, ad_spent,
        direct_gmv, format, format_sku_code, sap_sku_name, category, sub_category,
        item,
        COALESCE(NULLIF(TRIM(BOTH FROM item_head), ''::text), 'PREMIUM'::text) AS item_head,
        per_unit, per_ltr, ads_ltr_sold, month, year, month_day,
        {_basic_rate('format_sku_code', 'date', 'SWIGGY')} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate
   FROM swiggy_ads_master_mv;

DROP VIEW IF EXISTS zepto_ads_master;
CREATE VIEW zepto_ads_master AS
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
        {_basic_rate('z.product_id', 'z.date', 'ZEPTO')} * COALESCE(z.same_skus, 0::numeric)
            AS total_sale_basic_rate
   FROM zepto_ads z
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z.product_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'ZEPTO'::text;

DROP VIEW IF EXISTS bigbasket_ads_master;
CREATE VIEW bigbasket_ads_master AS
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
        {_basic_rate('b.product_id', 'b.date', 'BIGBASKET')} * COALESCE(b.orders_sku, 0::numeric)
            AS total_sale_basic_rate
   FROM bigbasket_ads b
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM b.product_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'BIGBASKET'::text;

DROP VIEW IF EXISTS flipkart_ads_master;
CREATE VIEW flipkart_ads_master AS
 SELECT id, date, campaign_id, campaign_name, campaign_status, campaign_type,
        budgeting_type, campaign_budget, ad_spend, views, clicks,
        total_converted_units, total_revenue, roi, click_through_rate,
        conversion_rate, format, uploaded_at,
        EXTRACT(year FROM date)::integer AS year,
        upper(to_char(date::timestamp with time zone, 'FMMonth'::text)) AS month,
        NULL::numeric AS total_sale_basic_rate
   FROM flipkart_ads f;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0071_ads_master_total_sale_basic_rate"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE),
    ]
