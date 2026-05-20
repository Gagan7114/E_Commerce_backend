from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `zepto_ads_master`.

    Replicates the "ZEPTO ADS RANGE" sheet of ADs SPENT (1).xlsx. Joins
    `zepto_ads` directly to `master_sheet` by `product_id = format_sku_code`,
    filtered to `master_sheet.format = 'ZEPTO'` so a SKU registered for
    multiple platforms only returns its Zepto-side row.

    Column mappings — see ZEPTO_ADS_MASTER_VIEW_IMPLEMENTATION_PLAN.md
    for the full rationale and Excel formula reverse-engineering.

    Final decisions taken before implementation:
      - format       ← master_sheet.format     (Choice A in §5.2)
      - impressions  ← zepto_ads.impressions   (added; Excel sheet omits)
      - join         ← zepto_ads.product_id = master_sheet.format_sku_code
                       AND master_sheet.format = 'ZEPTO'
      - No DISTINCT ON wrapper needed because
        `master_sheet_format_sku_unique_idx` (migration 0018) is UNIQUE on
        (format, format_sku_code) — guarantees at most one ZEPTO row per SKU.

    Source aliases (per user spec):
      same_skus    → direct_qty_sold
      other_skus   → indirect_qty_sold
      spend        → ad_spent
      revenue      → gmv
      product_id   → sku_id
      product_name → sku_name
    """

    dependencies = [
        ("uploads", "0022_zepto_ads"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.zepto_ads_master AS
            SELECT
                -- Source columns from zepto_ads (renamed to reporting vocabulary)
                z.date                                          AS date,
                z.product_id                                    AS sku_id,
                z.product_name                                  AS sku_name,
                z.same_skus                                     AS direct_qty_sold,
                z.other_skus                                    AS indirect_qty_sold,
                z.impressions                                   AS impressions,
                z.spend                                         AS ad_spent,
                z.revenue                                       AS gmv,

                -- Joined from master_sheet (ZEPTO rows only)
                ms.format                                       AS format,
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,
                ms.per_unit                                     AS per_unit,
                ms.per_unit_value                               AS per_ltr,

                -- Derived
                (COALESCE(ms.per_unit_value, 0)
                   * COALESCE(z.same_skus, 0))                  AS ads_ltr_sold,
                UPPER(TO_CHAR(z.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM z.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM z.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(z.date, 'FMMonth'))) AS month_day

            FROM public.zepto_ads z

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(z.product_id))
                  AND UPPER(TRIM(ms.format::text)) = 'ZEPTO';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.zepto_ads_master;
            """,
        ),
    ]
