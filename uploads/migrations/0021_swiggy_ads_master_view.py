from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `swiggy_ads_master`.

    Replicates the "ADS MASTER RANGE" sheet of ADs SPENT (1).xlsx for the
    Swiggy slice. Joins `swiggy_ads` to `ads_master_bs` (campaign → SKU map,
    per month) and then to `master_sheet` (SKU metadata).

    Column mappings — see SWIGGY_ADS_MASTER_VIEW_IMPLEMENTATION_PLAN.md
    for the full rationale and Excel formula reverse-engineering.

    Source column decisions (locked in by the user):
      direct_qty_sold ← swiggy_ads.total_conversions
      direct_gmv      ← swiggy_ads.total_gmv          (NOT total_direct_gmv_*)
      ad_spent        ← swiggy_ads.total_budget_burnt
      impressions     ← swiggy_ads.total_impressions

    Join strategy:
      swiggy_ads (s) ⟕ ads_master_bs (amb)
          ON amb.campaign_id = s.campaign_id
         AND amb.month       = UPPER(TO_CHAR(s.date, 'FMMonth'))
         AND amb.format      = 'SWIGGY'
      ⟕ master_sheet (ms)
          ON UPPER(TRIM(ms.format_sku_code::text))
           = UPPER(TRIM(amb.sku_id))
         AND ms.format       = 'SWIGGY'

    The (campaign_id, month) join lets the SKU mapping evolve over time —
    a campaign's May-2026 row looks up the May-2026 mapping in
    ads_master_bs, not whichever row happens to come first.
    """

    dependencies = [
        ("uploads", "0020_ads_master_bs"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.swiggy_ads_master AS
            SELECT
                -- Source columns from swiggy_ads
                s.date                                          AS date,
                s.campaign_id                                   AS campaign_id,
                s.campaign_name                                 AS campaign_name,
                s.total_conversions                             AS direct_qty_sold,
                s.total_impressions                             AS impressions,
                s.total_budget_burnt                            AS ad_spent,
                s.total_gmv                                     AS direct_gmv,
                s.format                                        AS format,

                -- Joined from ads_master_bs -> master_sheet
                amb.sku_id                                      AS format_sku_code,
                ms.sku_sap_name                                 AS sap_sku_name,
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,
                ms.per_unit                                     AS per_unit,
                ms.per_unit_value                               AS per_ltr,

                -- Derived
                (COALESCE(ms.per_unit_value, 0)
                   * COALESCE(s.total_conversions, 0))          AS ads_ltr_sold,
                UPPER(TO_CHAR(s.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM s.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day

            FROM public.swiggy_ads s

            LEFT JOIN public.ads_master_bs amb
                   ON amb.campaign_id = s.campaign_id
                  AND amb.month       = UPPER(TO_CHAR(s.date, 'FMMonth'))
                  AND UPPER(TRIM(amb.format::text)) = 'SWIGGY'

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(amb.sku_id))
                  AND UPPER(TRIM(ms.format::text)) = 'SWIGGY';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.swiggy_ads_master;
            """,
        ),
    ]
