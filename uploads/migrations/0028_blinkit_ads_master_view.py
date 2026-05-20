from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `blinkit_ads_master`.

    Mirrors the `swiggy_ads_master` view shape but sourced from `blinkit_ads`
    via the `ads_master_bs` bridge (Blinkit's export has no row-level SKU
    identifier — only campaign_id — so the campaign → SKU map is required).

    Column mappings — see BLINKIT_ADS_MASTER_VIEW_IMPLEMENTATION_PLAN.md
    for the full rationale.

    Final decisions taken before implementation:
      - format      ← blinkit_ads.format  (Choice A in §6.1 — constant 'BLINKIT')
      - join        ← Design B: (campaign_id, month_of_date) + format filter
                      (§6.2 — same as swiggy_ads_master, lets SKU mapping
                      evolve per month)
      - format filter uses REPLACE(UPPER(TRIM(...)), ' ', '') — defensive
        against the BIG-BASKET-style space-variance trap we hit earlier

    Source aliases (per user spec):
      direct_qty_sold      → direct_qty_sold
      indirect_qty_sold    → indirect_qty_sold
      impression           → impressions  (NB: DB col is singular; view col is plural)
      ad_spent             → ad_spent     (stores "Estimated Budget Consumed")
      direct_gmv           → direct_gmv
      indirect_gmv         → indirect_gmv

    Unlike the other 4 ad master views, this one keeps `direct_gmv` and
    `indirect_gmv` as separate columns rather than collapsing into a single
    `gmv` — matches the user's spec exactly.

    No DISTINCT ON wrapper needed because
    `master_sheet_format_sku_unique_idx` (migration 0018) is UNIQUE on
    (format, format_sku_code) — guarantees at most one BLINKIT row per SKU.
    """

    dependencies = [
        ("uploads", "0027_blinkit_ads_drop_keyword_cols"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.blinkit_ads_master AS
            SELECT
                -- Source columns from blinkit_ads
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
                   * COALESCE(b.direct_qty_sold, 0))            AS ads_ltr_sold,
                UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM b.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

            FROM public.blinkit_ads b

            LEFT JOIN public.ads_master_bs amb
                   ON amb.campaign_id = b.campaign_id
                  AND amb.month       = UPPER(TO_CHAR(b.date, 'FMMonth'))
                  AND REPLACE(UPPER(TRIM(amb.format::text)), ' ', '') = 'BLINKIT'

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(amb.sku_id))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BLINKIT';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.blinkit_ads_master;
            """,
        ),
    ]
