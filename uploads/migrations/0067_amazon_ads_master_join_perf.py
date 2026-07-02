from django.db import migrations

# Perf: migration 0062 gave amazon_ads_master a portfolio-fallback join that
# ran regexp_replace(... '\s+' ...) on EVERY amazon_ads row (a nested loop that
# evaluated the regex ~400k times per query — ~0.45s, and it runs 12x in the
# Realise trend). Only ONE portfolio ('TESTING  CAMPAIGN', double space) actually
# needed whitespace-collapse, so replace the per-row regex with a plain
# upper(trim(...)) equality and store that one seed row double-spaced. Output is
# identical; the join is ~2x faster.
FIX_SEED = """
UPDATE public.amazon_portfolio_head
   SET portfolio_name = 'TESTING  CAMPAIGN'
 WHERE portfolio_name = 'TESTING CAMPAIGN';
"""
REVERT_SEED = """
UPDATE public.amazon_portfolio_head
   SET portfolio_name = 'TESTING CAMPAIGN'
 WHERE portfolio_name = 'TESTING  CAMPAIGN';
"""

_VIEW_HEAD = """
CREATE OR REPLACE VIEW public.amazon_ads_master AS
SELECT
    a.id,
    a.campaign_id,
    a.ad_group_id,
    a.advertised_product_id,
    a.budget_currency,
    a.campaign_name,
    a.ad_group_name,
    a.advertised_product_sku,
    a.portfolio_id,
    a.portfolio_name,
    (a.impressions         * COALESCE(ph.weight, 1))::numeric AS impressions,
    (a.clicks              * COALESCE(ph.weight, 1))::numeric AS clicks,
    a.ctr,
    (a.total_cost          * COALESCE(ph.weight, 1))::numeric AS total_cost,
    (a.purchases           * COALESCE(ph.weight, 1))::numeric AS purchases,
    (a.sales               * COALESCE(ph.weight, 1))::numeric AS sales,
    (a.units_sold          * COALESCE(ph.weight, 1))::numeric AS units_sold,
    a.cost_per_purchase,
    a.purchase_rate,
    a.roas,
    (a.purchases_promoted  * COALESCE(ph.weight, 1))::numeric AS purchases_promoted,
    (a.sales_promoted      * COALESCE(ph.weight, 1))::numeric AS sales_promoted,
    (a.units_sold_promoted * COALESCE(ph.weight, 1))::numeric AS units_sold_promoted,
    a.cost_per_purchase_promoted,
    a.purchase_rate_promoted,
    a.roas_promoted,
    (a.purchases_halo      * COALESCE(ph.weight, 1))::numeric AS purchases_halo,
    (a.sales_halo          * COALESCE(ph.weight, 1))::numeric AS sales_halo,
    (a.units_sold_halo     * COALESCE(ph.weight, 1))::numeric AS units_sold_halo,
    (a.purchases_ntb       * COALESCE(ph.weight, 1))::numeric AS purchases_ntb,
    (a.sales_ntb           * COALESCE(ph.weight, 1))::numeric AS sales_ntb,
    (a.units_sold_ntb      * COALESCE(ph.weight, 1))::numeric AS units_sold_ntb,
    a.cost_per_purchase_ntb,
    a.purchase_rate_ntb,
    a.roas_ntb,
    (a.detail_page_views   * COALESCE(ph.weight, 1))::numeric AS detail_page_views,
    a.cost_per_detail_page_view,
    a.detail_page_view_rate,
    a.format,
    a.uploaded_at,
    a.date,
    EXTRACT(year FROM a.date)::integer AS year,
    upper(to_char(a.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    m.category,
    m.sub_category,
    COALESCE(m.item_head, ph.item_head) AS item_head
FROM public.amazon_ads a
LEFT JOIN public.master_sheet m
    ON NULLIF(TRIM(a.advertised_product_id), '') IS NOT NULL
   AND upper(TRIM(a.advertised_product_id)) = upper(TRIM(m.format_sku_code::text))
LEFT JOIN public.amazon_portfolio_head ph
    ON m.item_head IS NULL
   AND {portfolio_expr} = ph.portfolio_name;
"""

NEW_VIEW = _VIEW_HEAD.format(
    portfolio_expr="upper(TRIM(COALESCE(a.portfolio_name, '')))"
)
OLD_VIEW = _VIEW_HEAD.format(
    portfolio_expr=(
        "regexp_replace(upper(TRIM(COALESCE(a.portfolio_name, ''))), "
        "'\\s+', ' ', 'g')"
    )
)


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0066_pincode_mapping"),
    ]

    operations = [
        migrations.RunSQL(sql=FIX_SEED, reverse_sql=REVERT_SEED),
        migrations.RunSQL(sql=NEW_VIEW, reverse_sql=OLD_VIEW),
    ]
