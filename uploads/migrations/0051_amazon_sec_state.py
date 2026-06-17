from django.db import migrations


class Migration(migrations.Migration):
    """State-wise Amazon Secondary table.

    Stores the "Sales by State" Manufacturing/Retail export (View By=[State]) —
    same Amazon secondary report family as amazon_sec_range, but carrying a
    `state` dimension and only the shipped metrics the State export provides
    (no product title / brand / ordered / returns). One table (no Daily/Range
    split); the report's viewing range is captured in from_date/to_date,
    auto-detected from the file metadata. Upsert dedup key matches the frontend
    `amazon_state` config: (business, state, asin, from_date, to_date).
    """

    dependencies = [
        ("uploads", "0050_ads_daily_copy_tables"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.amazon_sec_state (
                id              bigserial PRIMARY KEY,
                business        text,
                state           text,
                asin            varchar,
                shipped_revenue numeric,
                shipped_cogs    numeric,
                shipped_units   integer,
                from_date       date,
                to_date         date,
                created_at      timestamp without time zone DEFAULT now()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS amazon_sec_state_business_state_asin_from_to_key
                ON public.amazon_sec_state (business, state, asin, from_date, to_date);
            CREATE INDEX IF NOT EXISTS idx_amazon_sec_state_dates
                ON public.amazon_sec_state (from_date, to_date);
            CREATE INDEX IF NOT EXISTS idx_amazon_sec_state_asin
                ON public.amazon_sec_state (asin);
            CREATE INDEX IF NOT EXISTS idx_amazon_sec_state_state
                ON public.amazon_sec_state (state);
            """,
            reverse_sql=r"""
            DROP TABLE IF EXISTS public.amazon_sec_state;
            """,
        ),
    ]
