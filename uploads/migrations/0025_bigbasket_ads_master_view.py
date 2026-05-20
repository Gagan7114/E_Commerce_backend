from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `bigbasket_ads_master`.

    Mirrors the `zepto_ads_master` view shape but sourced from `bigbasket_ads`
    and filtered to `master_sheet.format = 'BIGBASKET'`. The join is direct —
    `bigbasket_ads.product_id` is already the SKU-level identifier, so no
    `ads_master_bs` bridge is needed.

    Column mappings — see BIGBASKET_ADS_MASTER_VIEW_IMPLEMENTATION_PLAN.md
    for the full rationale.

    Final decisions taken before implementation:
      - format       ← master_sheet.format     (Choice A in §5.1)
      - impressions  ← bigbasket_ads.ad_impressions
      - join         ← bigbasket_ads.product_id = master_sheet.format_sku_code
                       AND master_sheet.format = 'BIGBASKET'

    Source aliases (per user spec):
      product_id       → sku_id
      product_name     → sku_name
      orders_sku       → direct_qty_sold
      other_sku_orders → indirect_qty_sold
      ad_impressions   → impressions
      ad_spend         → ad_spent
      ad_revenue       → gmv

    No DISTINCT ON wrapper needed because
    `master_sheet_format_sku_unique_idx` (migration 0018) is UNIQUE on
    (format, format_sku_code) — guarantees at most one BIGBASKET row per SKU.
    """

    dependencies = [
        ("uploads", "0024_bigbasket_ads"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.bigbasket_ads_master AS
            SELECT
                -- Source columns from bigbasket_ads (renamed to reporting vocabulary)
                b.date                                          AS date,
                b.product_id                                    AS sku_id,
                b.product_name                                  AS sku_name,
                b.orders_sku                                    AS direct_qty_sold,
                b.other_sku_orders                              AS indirect_qty_sold,
                b.ad_impressions                                AS impressions,
                b.ad_spend                                      AS ad_spent,
                b.ad_revenue                                    AS gmv,

                -- Joined from master_sheet (BIGBASKET rows only)
                ms.format                                       AS format,
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,
                ms.per_unit                                     AS per_unit,
                ms.per_unit_value                               AS per_ltr,

                -- Derived (identical formulas to zepto_ads_master)
                (COALESCE(ms.per_unit_value, 0)
                   * COALESCE(b.orders_sku, 0))                 AS ads_ltr_sold,
                UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM b.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

            FROM public.bigbasket_ads b

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(b.product_id))
                  AND UPPER(TRIM(ms.format::text)) = 'BIGBASKET';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.bigbasket_ads_master;
            """,
        ),
    ]
