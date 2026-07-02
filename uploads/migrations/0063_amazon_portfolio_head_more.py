from django.db import migrations

# Map the last Amazon ad portfolios that were still landing in "(Unmapped)"
# (they carry no ASIN, so only a portfolio -> item_head rule can classify them):
#   * COCONUT        -> PREMIUM   (coconut oil; master_sheet classes it Premium)
#   * YELLOW MUSTARD -> COMMODITY (mustard line)
# Both are unambiguous, so weight 1.0 (no proportional split).
INSERT = """
INSERT INTO public.amazon_portfolio_head (portfolio_name, item_head, weight) VALUES
    ('COCONUT', 'PREMIUM', 1.000),
    ('YELLOW MUSTARD', 'COMMODITY', 1.000)
ON CONFLICT (portfolio_name, item_head) DO UPDATE SET weight = EXCLUDED.weight;
"""

REVERSE = """
DELETE FROM public.amazon_portfolio_head
 WHERE portfolio_name IN ('COCONUT', 'YELLOW MUSTARD');
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0062_amazon_portfolio_head_split"),
    ]

    operations = [
        migrations.RunSQL(sql=INSERT, reverse_sql=REVERSE),
    ]
