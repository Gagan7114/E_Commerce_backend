from django.db import migrations

# The swiggy/blinkit ads master views map a campaign to its SKU/item via
# ads_master_bs, joined STRICTLY on the ads row's month + campaign_id. Two gaps
# made whole months collapse to a single "(Unmapped)" bucket:
#   1) A month's mapping not uploaded yet (e.g. June before its rows are added).
#   2) Campaigns recreated each month with NEW campaign_ids but the SAME name
#      (Swiggy's June campaigns: new ids, names identical to mapped May ones).
#
# Fix: resolve each row's SKU via a LATERAL that considers the row's OWN
# campaign_id AND any campaign sharing the same campaign_name, preferring
#   own campaign_id  ->  exact month  ->  most recent (updated_at/created_at).
# So June inherits May's mapping by id when possible, otherwise by campaign name.

SWIGGY_FALLBACK = """
CREATE OR REPLACE VIEW public.swiggy_ads_master AS
SELECT
    s.date                                          AS date,
    s.campaign_id                                   AS campaign_id,
    s.campaign_name                                 AS campaign_name,
    s.total_conversions                             AS direct_qty_sold,
    s.total_impressions                             AS impressions,
    s.total_budget_burnt                            AS ad_spent,
    s.total_gmv                                     AS direct_gmv,
    s.format                                        AS format,
    amb.sku_id                                      AS format_sku_code,
    ms.sku_sap_name                                 AS sap_sku_name,
    ms.category                                     AS category,
    ms.sub_category                                 AS sub_category,
    ms.item                                         AS item,
    ms.item_head                                    AS item_head,
    ms.per_unit                                     AS per_unit,
    ms.per_unit_value                               AS per_ltr,
    (COALESCE(ms.per_unit_value, 0)
       * COALESCE(s.total_conversions, 0))          AS ads_ltr_sold,
    UPPER(TO_CHAR(s.date, 'FMMonth'))               AS month,
    EXTRACT(YEAR FROM s.date)::integer              AS year,
    (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0')
       || '-' || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day
FROM public.swiggy_ads s
LEFT JOIN LATERAL (
    SELECT amb.sku_id
    FROM public.ads_master_bs amb
    JOIN public.swiggy_ads s2 ON s2.campaign_id = amb.campaign_id
    WHERE UPPER(TRIM(amb.format::text)) = 'SWIGGY'
      AND (amb.campaign_id = s.campaign_id
           OR UPPER(TRIM(s2.campaign_name::text)) = UPPER(TRIM(s.campaign_name::text)))
    ORDER BY (amb.campaign_id = s.campaign_id) DESC,
             (UPPER(TRIM(amb.month::text)) = UPPER(TO_CHAR(s.date, 'FMMonth'))) DESC,
             amb.updated_at DESC NULLS LAST,
             amb.created_at DESC NULLS LAST
    LIMIT 1
) amb ON true
LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(amb.sku_id))
      AND UPPER(TRIM(ms.format::text)) = 'SWIGGY';
"""

BLINKIT_FALLBACK = """
CREATE OR REPLACE VIEW public.blinkit_ads_master AS
SELECT
    b.date                                          AS date,
    b.campaign_id                                   AS campaign_id,
    b.campaign_name                                 AS campaign_name,
    b.direct_qty_sold                               AS direct_qty_sold,
    b.indirect_qty_sold                             AS indirect_qty_sold,
    b.impression                                    AS impressions,
    b.ad_spent                                      AS ad_spent,
    b.direct_gmv                                    AS direct_gmv,
    b.indirect_gmv                                  AS indirect_gmv,
    b.format                                        AS format,
    amb.sku_id                                      AS format_sku_code,
    ms.sku_sap_name                                 AS sap_sku_name,
    ms.category                                     AS category,
    ms.sub_category                                 AS sub_category,
    ms.item                                         AS item,
    ms.item_head                                    AS item_head,
    ms.per_unit                                     AS per_unit,
    ms.per_unit_value                               AS per_ltr,
    (COALESCE(ms.per_unit_value, 0)
       * COALESCE(b.direct_qty_sold, 0))            AS ads_ltr_sold,
    UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
    EXTRACT(YEAR FROM b.date)::integer              AS year,
    (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
       || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day
FROM public.blinkit_ads b
LEFT JOIN LATERAL (
    SELECT amb.sku_id
    FROM public.ads_master_bs amb
    JOIN public.blinkit_ads b2 ON b2.campaign_id = amb.campaign_id
    WHERE REPLACE(UPPER(TRIM(amb.format::text)), ' ', '') = 'BLINKIT'
      AND (amb.campaign_id = b.campaign_id
           OR UPPER(TRIM(b2.campaign_name::text)) = UPPER(TRIM(b.campaign_name::text)))
    ORDER BY (amb.campaign_id = b.campaign_id) DESC,
             (UPPER(TRIM(amb.month::text)) = UPPER(TO_CHAR(b.date, 'FMMonth'))) DESC,
             amb.updated_at DESC NULLS LAST,
             amb.created_at DESC NULLS LAST
    LIMIT 1
) amb ON true
LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(amb.sku_id))
      AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BLINKIT';
"""

# --- Reverse: the original strict (exact-month + exact-campaign-id) join. ---

SWIGGY_STRICT = """
CREATE OR REPLACE VIEW public.swiggy_ads_master AS
SELECT
    s.date AS date, s.campaign_id AS campaign_id, s.campaign_name AS campaign_name,
    s.total_conversions AS direct_qty_sold, s.total_impressions AS impressions,
    s.total_budget_burnt AS ad_spent, s.total_gmv AS direct_gmv, s.format AS format,
    amb.sku_id AS format_sku_code, ms.sku_sap_name AS sap_sku_name,
    ms.category AS category, ms.sub_category AS sub_category, ms.item AS item,
    ms.item_head AS item_head, ms.per_unit AS per_unit, ms.per_unit_value AS per_ltr,
    (COALESCE(ms.per_unit_value, 0) * COALESCE(s.total_conversions, 0)) AS ads_ltr_sold,
    UPPER(TO_CHAR(s.date, 'FMMonth')) AS month,
    EXTRACT(YEAR FROM s.date)::integer AS year,
    (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0') || '-' || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day
FROM public.swiggy_ads s
LEFT JOIN public.ads_master_bs amb
       ON amb.campaign_id = s.campaign_id
      AND amb.month = UPPER(TO_CHAR(s.date, 'FMMonth'))
      AND UPPER(TRIM(amb.format::text)) = 'SWIGGY'
LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(amb.sku_id))
      AND UPPER(TRIM(ms.format::text)) = 'SWIGGY';
"""

BLINKIT_STRICT = """
CREATE OR REPLACE VIEW public.blinkit_ads_master AS
SELECT
    b.date AS date, b.campaign_id AS campaign_id, b.campaign_name AS campaign_name,
    b.direct_qty_sold AS direct_qty_sold, b.indirect_qty_sold AS indirect_qty_sold,
    b.impression AS impressions, b.ad_spent AS ad_spent, b.direct_gmv AS direct_gmv,
    b.indirect_gmv AS indirect_gmv, b.format AS format,
    amb.sku_id AS format_sku_code, ms.sku_sap_name AS sap_sku_name,
    ms.category AS category, ms.sub_category AS sub_category, ms.item AS item,
    ms.item_head AS item_head, ms.per_unit AS per_unit, ms.per_unit_value AS per_ltr,
    (COALESCE(ms.per_unit_value, 0) * COALESCE(b.direct_qty_sold, 0)) AS ads_ltr_sold,
    UPPER(TO_CHAR(b.date, 'FMMonth')) AS month,
    EXTRACT(YEAR FROM b.date)::integer AS year,
    (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0') || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day
FROM public.blinkit_ads b
LEFT JOIN public.ads_master_bs amb
       ON amb.campaign_id = b.campaign_id
      AND amb.month = UPPER(TO_CHAR(b.date, 'FMMonth'))
      AND REPLACE(UPPER(TRIM(amb.format::text)), ' ', '') = 'BLINKIT'
LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(amb.sku_id))
      AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BLINKIT';
"""


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0047_appointment_commit_updated_by"),
    ]

    operations = [
        migrations.RunSQL(sql=SWIGGY_FALLBACK, reverse_sql=SWIGGY_STRICT),
        migrations.RunSQL(sql=BLINKIT_FALLBACK, reverse_sql=BLINKIT_STRICT),
    ]
