from django.db import migrations


class Migration(migrations.Migration):
    """Create flipkart_ads table for Flipkart Ads report ingestion.

    Source: Flipkart Ads "Campaign Performance" CSV export. The file's first
    two rows are metadata ("Start Time, ..." and "End Time, ..."); the real
    column headers begin at row 3 (Campaign ID, Campaign Name, …).

    The user picks a single upload `date` in the UI which is written to
    every row (same pattern as Amazon / Swiggy / Zepto / BigBasket / Blinkit).

    Unique key: (date, campaign_id). `format` defaults to 'FLIPKART' so a
    direct INSERT without that column still tags the row correctly.
    """

    dependencies = [
        ("uploads", "0029_amazon_ads_master_drop_month_day"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.flipkart_ads (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key
                date            DATE NOT NULL DEFAULT '1970-01-01',
                campaign_id     TEXT NOT NULL DEFAULT '',

                -- Other text columns
                campaign_name   TEXT,
                campaign_status TEXT,
                campaign_type   TEXT,
                budgeting_type  TEXT,

                -- Numeric metrics
                campaign_budget         NUMERIC,
                ad_spend                NUMERIC,
                views                   NUMERIC,
                clicks                  NUMERIC,
                total_converted_units   NUMERIC,
                total_revenue           NUMERIC,
                roi                     NUMERIC,
                click_through_rate      NUMERIC,
                conversion_rate         NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'FLIPKART',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS flipkart_ads_dedup_idx
                ON public.flipkart_ads (date, campaign_id);

            CREATE INDEX IF NOT EXISTS flipkart_ads_campaign_id_idx
                ON public.flipkart_ads (campaign_id);

            CREATE INDEX IF NOT EXISTS flipkart_ads_date_idx
                ON public.flipkart_ads (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.flipkart_ads;
            """,
        ),
    ]
