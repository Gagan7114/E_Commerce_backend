# Adds total_sale_basic_rate + basic_rate to the per-day ads master views
# (Swiggy / Zepto / BigBasket daily), mirroring what 0071/0072 did for the
# month-level *_ads_master views. The date-wise ads dashboards read from these
# daily views, so they need the same columns. Blinkit has no daily master view
# (its dashboard reads blinkit_ads_master directly).
#
# basic_rate is looked up from monthly_landing_rate (sku_code + normalised
# format + reporting month); total_sale_basic_rate = basic_rate * direct_qty_sold.
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


_SWIGGY_FROM = """
   FROM swiggyads_daily s
     LEFT JOIN LATERAL ( SELECT amb_1.sku_id
           FROM ads_master_bs amb_1
             JOIN swiggyads_daily s2 ON s2.campaign_id = amb_1.campaign_id
          WHERE upper(TRIM(BOTH FROM amb_1.format)) = 'SWIGGY'::text AND (amb_1.campaign_id = s.campaign_id OR upper(TRIM(BOTH FROM s2.campaign_name)) = upper(TRIM(BOTH FROM s.campaign_name)))
          ORDER BY (amb_1.campaign_id = s.campaign_id) DESC, (upper(TRIM(BOTH FROM amb_1.month)) = upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text))) DESC, amb_1.updated_at DESC NULLS LAST, amb_1.created_at DESC NULLS LAST
         LIMIT 1) amb ON true
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM amb.sku_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'SWIGGY'::text
"""

_SWIGGY_COLS = """
    s.date,
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
    (lpad(EXTRACT(day FROM s.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(s.date::timestamp with time zone, 'FMMonth'::text)) AS month_day
"""

_ZEPTO_FROM = """
   FROM zeptoads_daily z
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z.product_id)) AND upper(TRIM(BOTH FROM ms.format)) = 'ZEPTO'::text
"""

_ZEPTO_COLS = """
    z.date,
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
    (lpad(EXTRACT(day FROM z.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(z.date::timestamp with time zone, 'FMMonth'::text)) AS month_day
"""

_BB_FROM = """
   FROM bigbasketads_daily b
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM b.product_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'BIGBASKET'::text
"""

_BB_COLS = """
    b.date,
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
    (lpad(EXTRACT(day FROM b.date)::text, 2, '0'::text) || '-'::text) || upper(to_char(b.date::timestamp with time zone, 'FMMonth'::text)) AS month_day
"""

_SWIGGY_EXTRA = (
    f"{_basic_rate('amb.sku_id', 's.date', 'SWIGGY')} * COALESCE(s.total_conversions, 0::numeric) AS total_sale_basic_rate,\n"
    f"    {_basic_rate('amb.sku_id', 's.date', 'SWIGGY')} AS basic_rate"
)
_ZEPTO_EXTRA = (
    f"{_basic_rate('z.product_id', 'z.date', 'ZEPTO')} * COALESCE(z.same_skus, 0::numeric) AS total_sale_basic_rate,\n"
    f"    {_basic_rate('z.product_id', 'z.date', 'ZEPTO')} AS basic_rate"
)
_BB_EXTRA = (
    f"{_basic_rate('b.product_id', 'b.date', 'BIGBASKET')} * COALESCE(b.orders_sku, 0::numeric) AS total_sale_basic_rate,\n"
    f"    {_basic_rate('b.product_id', 'b.date', 'BIGBASKET')} AS basic_rate"
)

FORWARD = f"""
CREATE OR REPLACE VIEW swiggyads_daily_master AS
 SELECT {_SWIGGY_COLS.strip()},
    {_SWIGGY_EXTRA}
{_SWIGGY_FROM.strip()};

CREATE OR REPLACE VIEW zeptoads_daily_master AS
 SELECT {_ZEPTO_COLS.strip()},
    {_ZEPTO_EXTRA}
{_ZEPTO_FROM.strip()};

CREATE OR REPLACE VIEW bigbasketads_daily_master AS
 SELECT {_BB_COLS.strip()},
    {_BB_EXTRA}
{_BB_FROM.strip()};
"""

REVERSE = f"""
DROP VIEW IF EXISTS swiggyads_daily_master;
CREATE VIEW swiggyads_daily_master AS
 SELECT {_SWIGGY_COLS.strip()}
{_SWIGGY_FROM.strip()};

DROP VIEW IF EXISTS zeptoads_daily_master;
CREATE VIEW zeptoads_daily_master AS
 SELECT {_ZEPTO_COLS.strip()}
{_ZEPTO_FROM.strip()};

DROP VIEW IF EXISTS bigbasketads_daily_master;
CREATE VIEW bigbasketads_daily_master AS
 SELECT {_BB_COLS.strip()}
{_BB_FROM.strip()};
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0072_ads_master_basic_rate"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE),
    ]
