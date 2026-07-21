# Carry the previous month's landing rate FORWARD inside the per-platform ads
# master views (Blinkit / Swiggy / Zepto / BigBasket — NOT Amazon/Flipkart), so
# their basic_rate / total_sale_basic_rate columns stop showing 0 for a month
# whose monthly_landing_rate sheet hasn't been uploaded yet.
#
# 0071/0072 (month views) and 0073 (daily views) looked the rate up with an
# EXACT-month match, so the first day of a new month zeroed out Ads Sale until
# that month's rates were entered. The Secondary/SecMaster dashboards already
# solved this in platforms migration 0049 by picking the latest rate whose month
# is <= the reporting month. This migration applies the SAME carry-forward rule
# to the ads views (and the cross-platform Ads Summary union does likewise in
# platforms/views.py).
#
# The view bodies are copied verbatim from 0072 (month) and 0073 (daily); only
# the basic-rate scalar subquery changes (exact-month -> carry-forward). Columns
# are unchanged, so CREATE OR REPLACE works in both directions with no DROP /
# dependency cascade. The reverse path restores the exact-month lookup (the
# pre-migration state). Nothing is written to monthly_landing_rate; these are
# query-time views, so the fix takes effect the moment they are replaced (no
# matview refresh needed).
from django.db import migrations


# ── Carry-forward: latest rate with month <= the row's reporting month. ──────
def _cf(sku_expr, date_expr, platform):
    return f"""(
        SELECT mlr.basic_rate
        FROM monthly_landing_rate mlr
        WHERE upper(btrim(mlr.sku_code)) = upper(btrim({sku_expr}))
          AND replace(upper(btrim(mlr.format)), ' ', '') = '{platform}'
          AND mlr.month::date <= date_trunc('month', {date_expr}::timestamp)::date
        ORDER BY mlr.month::date DESC, mlr.created_at DESC NULLS LAST
        LIMIT 1
    )"""


# ── Exact-month (the 0072/0073 lookup) — used only for the reverse path. ─────
def _exact(sku_expr, date_expr, platform):
    return f"""(
        SELECT mlr.basic_rate
        FROM monthly_landing_rate mlr
        WHERE upper(btrim(mlr.sku_code)) = upper(btrim({sku_expr}))
          AND replace(upper(btrim(mlr.format)), ' ', '') = '{platform}'
          AND btrim(mlr.month) = to_char(date_trunc('month', {date_expr}::timestamp), 'YYYY-MM-DD')
        ORDER BY mlr.created_at DESC NULLS LAST
        LIMIT 1
    )"""


# ── Month-level views (verbatim from 0072, parameterised by the rate builder). ─
def _month_views(br):
    return f"""
CREATE OR REPLACE VIEW blinkit_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, indirect_qty_sold,
        impressions, ad_spent, direct_gmv, indirect_gmv, format, format_sku_code,
        sap_sku_name, category, sub_category, item, item_head, per_unit, per_ltr,
        ads_ltr_sold, month, year, month_day,
        {br('format_sku_code', 'date', 'BLINKIT')} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate,
        {br('format_sku_code', 'date', 'BLINKIT')} AS basic_rate
   FROM blinkit_ads_master_mv;

CREATE OR REPLACE VIEW swiggy_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, impressions, ad_spent,
        direct_gmv, format, format_sku_code, sap_sku_name, category, sub_category,
        item,
        COALESCE(NULLIF(TRIM(BOTH FROM item_head), ''::text), 'PREMIUM'::text) AS item_head,
        per_unit, per_ltr, ads_ltr_sold, month, year, month_day,
        {br('format_sku_code', 'date', 'SWIGGY')} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate,
        {br('format_sku_code', 'date', 'SWIGGY')} AS basic_rate
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
        {br('z.product_id', 'z.date', 'ZEPTO')} * COALESCE(z.same_skus, 0::numeric)
            AS total_sale_basic_rate,
        {br('z.product_id', 'z.date', 'ZEPTO')} AS basic_rate
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
        {br('b.product_id', 'b.date', 'BIGBASKET')} * COALESCE(b.orders_sku, 0::numeric)
            AS total_sale_basic_rate,
        {br('b.product_id', 'b.date', 'BIGBASKET')} AS basic_rate
   FROM bigbasket_ads b
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM b.product_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'BIGBASKET'::text;
"""


# ── Daily-view column / from clauses (verbatim from 0073). ───────────────────
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


# ── Daily-level views (verbatim from 0073, parameterised by the rate builder). ─
def _daily_views(br):
    swiggy_extra = (
        f"{br('amb.sku_id', 's.date', 'SWIGGY')} * COALESCE(s.total_conversions, 0::numeric) AS total_sale_basic_rate,\n"
        f"    {br('amb.sku_id', 's.date', 'SWIGGY')} AS basic_rate"
    )
    zepto_extra = (
        f"{br('z.product_id', 'z.date', 'ZEPTO')} * COALESCE(z.same_skus, 0::numeric) AS total_sale_basic_rate,\n"
        f"    {br('z.product_id', 'z.date', 'ZEPTO')} AS basic_rate"
    )
    bb_extra = (
        f"{br('b.product_id', 'b.date', 'BIGBASKET')} * COALESCE(b.orders_sku, 0::numeric) AS total_sale_basic_rate,\n"
        f"    {br('b.product_id', 'b.date', 'BIGBASKET')} AS basic_rate"
    )
    return f"""
CREATE OR REPLACE VIEW swiggyads_daily_master AS
 SELECT {_SWIGGY_COLS.strip()},
    {swiggy_extra}
{_SWIGGY_FROM.strip()};

CREATE OR REPLACE VIEW zeptoads_daily_master AS
 SELECT {_ZEPTO_COLS.strip()},
    {zepto_extra}
{_ZEPTO_FROM.strip()};

CREATE OR REPLACE VIEW bigbasketads_daily_master AS
 SELECT {_BB_COLS.strip()},
    {bb_extra}
{_BB_FROM.strip()};
"""


FORWARD = _month_views(_cf) + _daily_views(_cf)
REVERSE = _month_views(_exact) + _daily_views(_exact)


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0078_bigbasket_sec_range"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE),
    ]
