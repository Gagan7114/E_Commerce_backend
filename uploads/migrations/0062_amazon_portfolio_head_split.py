from django.db import migrations

# Portfolio -> item_head mapping for Amazon ads that carry NO advertised ASIN
# (Sponsored Brands / Store / Video campaigns). Those rows can never map through
# the ASIN -> master_sheet join, so without this they sit in "(Unmapped)". Each
# portfolio maps to Premium/Commodity; the three ambiguous portfolios
# (Mix Campigns / blank / Testing) are split PROPORTIONALLY to the current mapped
# Amazon spend share (~84.4% Premium / 15.6% Commodity as of Jun-2026) via two
# rows whose weights sum to 1. Edit this table any time to re-classify.
SEED = [
    # portfolio (UPPER, single-spaced),  item_head,   weight
    ("EXTRA VIRGIN",     "PREMIUM",   "1.000"),
    ("EXTRA LIGHT",      "PREMIUM",   "1.000"),
    ("POMACE",           "PREMIUM",   "1.000"),
    ("SANO POMACE",      "PREMIUM",   "1.000"),
    ("CANOLA",           "PREMIUM",   "1.000"),
    ("JIVO MUSTARD",     "COMMODITY", "1.000"),
    ("SUNFLOWER OIL",    "COMMODITY", "1.000"),
    # Ambiguous -> proportional split (weights sum to 1 so the spend total is
    # preserved, only reallocated across the two heads).
    ("MIX CAMPIGNS",     "PREMIUM",   "0.844"),
    ("MIX CAMPIGNS",     "COMMODITY", "0.156"),
    ("",                 "PREMIUM",   "0.844"),   # blank portfolio = "(none)"
    ("",                 "COMMODITY", "0.156"),
    ("TESTING CAMPAIGN", "PREMIUM",   "0.844"),
    ("TESTING CAMPAIGN", "COMMODITY", "0.156"),
]

_seed_values = ",\n            ".join(
    f"('{pf}', '{ih}', {w})" for pf, ih, w in SEED
)

CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS public.amazon_portfolio_head (
    id            bigserial PRIMARY KEY,
    portfolio_name text    NOT NULL,
    item_head     text     NOT NULL,
    weight        numeric  NOT NULL DEFAULT 1.0,
    UNIQUE (portfolio_name, item_head)
);

TRUNCATE public.amazon_portfolio_head;
INSERT INTO public.amazon_portfolio_head (portfolio_name, item_head, weight) VALUES
    {_seed_values};
"""

# Recreated amazon_ads_master: same columns/order/types as before, but
#   * the additive metric columns are multiplied by the fallback weight, and
#   * item_head = COALESCE(ASIN head, portfolio head).
# The portfolio join fires ONLY when the ASIN join yielded no item_head, and it
# fans a row out into 2 weighted rows for the ambiguous portfolios (weights sum
# to 1, so every SUM(metric) is preserved — the spend is reallocated across
# Premium/Commodity, never created or lost). Consumers all aggregate with
# SUM()/recomputed ratios, so weighted rows are safe.
CREATE_VIEW = """
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
   AND regexp_replace(upper(TRIM(COALESCE(a.portfolio_name, ''))), '\\s+', ' ', 'g')
       = ph.portfolio_name;
"""

# Reverse: the pre-split definition (plain ASIN join, no weighting).
RESTORE_VIEW = """
CREATE OR REPLACE VIEW public.amazon_ads_master AS
SELECT
    a.*,
    EXTRACT(year FROM a.date)::integer AS year,
    upper(to_char(a.date::timestamp with time zone, 'FMMonth'::text)) AS month,
    m.category,
    m.sub_category,
    m.item_head
FROM public.amazon_ads a
LEFT JOIN public.master_sheet m
    ON upper(TRIM(a.advertised_product_id)) = upper(TRIM(m.format_sku_code::text));
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0061_total_po_zbs_grn_code"),
    ]

    operations = [
        migrations.RunSQL(sql=CREATE_TABLE, reverse_sql="DROP TABLE IF EXISTS public.amazon_portfolio_head;"),
        migrations.RunSQL(sql=CREATE_VIEW, reverse_sql=RESTORE_VIEW),
    ]
