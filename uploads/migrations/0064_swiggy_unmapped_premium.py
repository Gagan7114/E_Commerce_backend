from django.db import migrations

# Swiggy ads have only PREMIUM mapped (no Commodity), so any ad whose SKU is
# unmapped should fall into PREMIUM rather than "(Unmapped)". Done at the thin
# pass-through view level (swiggy_ads_master reads swiggy_ads_master_mv), so no
# matview rebuild is needed and every consumer — Ads Summary, the Swiggy Ads
# dashboard, Realise — sees the same classification. Swiggy-only.
SET_PREMIUM = """
CREATE OR REPLACE VIEW public.swiggy_ads_master AS
SELECT
    date, campaign_id, campaign_name, direct_qty_sold, impressions, ad_spent,
    direct_gmv, format, format_sku_code, sap_sku_name, category, sub_category, item,
    COALESCE(NULLIF(TRIM(item_head::text), ''), 'PREMIUM') AS item_head,
    per_unit, per_ltr, ads_ltr_sold, month, year, month_day
FROM public.swiggy_ads_master_mv;
"""

RESTORE = """
CREATE OR REPLACE VIEW public.swiggy_ads_master AS
SELECT
    date, campaign_id, campaign_name, direct_qty_sold, impressions, ad_spent,
    direct_gmv, format, format_sku_code, sap_sku_name, category, sub_category, item,
    item_head,
    per_unit, per_ltr, ads_ltr_sold, month, year, month_day
FROM public.swiggy_ads_master_mv;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0063_amazon_portfolio_head_more"),
    ]

    operations = [
        migrations.RunSQL(sql=SET_PREMIUM, reverse_sql=RESTORE),
    ]
