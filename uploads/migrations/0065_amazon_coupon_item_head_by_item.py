from django.db import migrations

# amazon_coupon_master previously resolved item_head through a fragile two-hop
# chain: coupon_name -> ads_master_bs.campaign_id -> ads_master_bs.sku_id ->
# master_sheet.format_sku_code. That matched almost nothing, so the Coupon
# dashboard's ITEM HEAD column was mostly empty and the donut lumped ~93% into
# "Other". Amazon coupon names ARE item names ("CANOLA 5L"), and master_sheet
# has an `item` column, so match coupon_name = master_sheet.item directly
# (AMAZON row preferred). The old chain is kept as a fallback (only used when the
# direct match misses), so nothing that mapped before can regress.
NEW_VIEW = """
CREATE OR REPLACE VIEW public.amazon_coupon_master AS
SELECT
    c.date,
    c.coupon_name,
    c.start_date,
    c.end_date,
    c.clips,
    c.redemptions,
    c.total_discount,
    c.budget_spent,
    c.budget_remaining,
    c.budget_used,
    c.total_budget,
    COALESCE(ms.format_sku_code, ms2.format_sku_code) AS asin,
    COALESCE(ms.item_head, ms2.item_head)             AS item_head,
    COALESCE(ms.brand, ms2.brand)                     AS brand,
    COALESCE(ms.category, ms2.category)               AS category,
    COALESCE(ms.sub_category, ms2.sub_category)       AS sub_category,
    upper(to_char(c.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM c.date)::integer AS year
FROM public.amazon_coupon c
LEFT JOIN LATERAL (
    SELECT m.format_sku_code, m.item_head, m.brand, m.category, m.sub_category
    FROM public.master_sheet m
    WHERE upper(TRIM(m.item::text)) = upper(TRIM(c.coupon_name))
    ORDER BY (replace(upper(TRIM(m.format::text)), ' ', '') = 'AMAZON') DESC NULLS LAST,
             m.format_sku_code
    LIMIT 1
) ms ON TRUE
LEFT JOIN public.ads_master_bs amb
    ON ms.item_head IS NULL
   AND upper(TRIM(amb.campaign_id)) = upper(TRIM(c.coupon_name))
   AND replace(upper(TRIM(amb.format)), ' ', '') = 'AMAZON'
LEFT JOIN public.master_sheet ms2
    ON ms.item_head IS NULL
   AND upper(TRIM(ms2.format_sku_code::text)) = upper(TRIM(amb.sku_id))
   AND replace(upper(TRIM(ms2.format)), ' ', '') = 'AMAZON';
"""

OLD_VIEW = """
CREATE OR REPLACE VIEW public.amazon_coupon_master AS
SELECT c.date,
    c.coupon_name,
    c.start_date,
    c.end_date,
    c.clips,
    c.redemptions,
    c.total_discount,
    c.budget_spent,
    c.budget_remaining,
    c.budget_used,
    c.total_budget,
    ms.format_sku_code AS asin,
    ms.item_head,
    ms.brand,
    ms.category,
    ms.sub_category,
    upper(to_char(c.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    EXTRACT(year FROM c.date)::integer AS year
   FROM amazon_coupon c
     LEFT JOIN ads_master_bs amb ON upper(TRIM(BOTH FROM amb.campaign_id)) = upper(TRIM(BOTH FROM c.coupon_name)) AND replace(upper(TRIM(BOTH FROM amb.format)), ' '::text, ''::text) = 'AMAZON'::text
     LEFT JOIN master_sheet ms ON upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM amb.sku_id)) AND replace(upper(TRIM(BOTH FROM ms.format)), ' '::text, ''::text) = 'AMAZON'::text;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0064_swiggy_unmapped_premium"),
    ]

    operations = [
        migrations.RunSQL(sql=NEW_VIEW, reverse_sql=OLD_VIEW),
    ]
