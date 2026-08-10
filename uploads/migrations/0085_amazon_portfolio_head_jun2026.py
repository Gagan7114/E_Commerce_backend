from django.db import migrations

# Map the Amazon ad portfolios created around 12-Jun-2026 that were still
# landing in "(Unmapped)". Like the ones 0062/0063 already cover, every one of
# their rows carries NO advertised ASIN (Sponsored Brands / Store / Video), so
# the ASIN -> master_sheet join can never classify them and only a portfolio
# rule can. Together they held ₹2,07,201 — 2.1% of all-time Amazon ad spend.
#
# All are unambiguous, so weight 1.0 (no proportional split). Deliberately NOT
# using the 0062 split trick here: that 0.844/0.156 reallocation exists for
# genuinely mixed portfolios ("Mix Campigns"), and applying it to a portfolio
# whose real head is knowable would invent an allocation — e.g. it would file
# ~₹25k of Groundnut spend under Commodity — which is worse than an honest
# "(Unmapped)" row.
#
# Heads follow master_sheet's own classification of each family:
#   GROUNDNUT        -> PREMIUM   (GROUNDNUT category = PREMIUM, 31 SKUs)
#   PREMIUM CATEGORY -> PREMIUM   (self-evident)
#   SANO CANOLA      -> PREMIUM   (CANOLA = PREMIUM, 129 SKUs)
#   SO OLIVE         -> PREMIUM   (OLIVE = PREMIUM, 281 SKUs)
#   SESAME OIL       -> PREMIUM   (SESAME / SESAME OIL = PREMIUM, 8 SKUs)
#   WHEATGRASS MANGO -> OTHER     (user's call; OTHER is a live head, as
#                                  master_sheet already uses for GIFT PACK)
#
# `portfolio_name` must be stored EXACTLY as the deployed view's join produces
# it — `upper(trim(coalesce(portfolio_name,'')))`, which does NOT collapse
# internal whitespace. All six normalise to single-spaced text, verified against
# amazon_ads before writing this. (The pre-existing 'TESTING  CAMPAIGN' row is
# double-spaced on purpose: its source rows are too, and "tidying" it would
# break a working mapping.)
INSERT = """
INSERT INTO public.amazon_portfolio_head (portfolio_name, item_head, weight) VALUES
    ('GROUNDNUT',        'PREMIUM', 1.000),
    ('PREMIUM CATEGORY', 'PREMIUM', 1.000),
    ('SANO CANOLA',      'PREMIUM', 1.000),
    ('SO OLIVE',         'PREMIUM', 1.000),
    ('SESAME OIL',       'PREMIUM', 1.000),
    ('WHEATGRASS MANGO', 'OTHER',   1.000)
ON CONFLICT (portfolio_name, item_head) DO UPDATE SET weight = EXCLUDED.weight;
"""

REVERSE = """
DELETE FROM public.amazon_portfolio_head
 WHERE portfolio_name IN (
    'GROUNDNUT', 'PREMIUM CATEGORY', 'SANO CANOLA',
    'SO OLIVE', 'SESAME OIL', 'WHEATGRASS MANGO'
 );
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0084_platform_city_universe"),
    ]

    operations = [
        migrations.RunSQL(sql=INSERT, reverse_sql=REVERSE),
    ]
