from django.db import migrations


class Migration(migrations.Migration):
    """Create zepto_ads table for Zepto Ads report ingestion.

    Source: Zepto Ads Console export (20 cols). The user picks a single
    upload `date` in the UI, mirroring the Amazon / Swiggy Ads flow.

    Unique key: (date, product_id, campaign_id). Verified against the
    sample export (47 rows, 47 unique (product_id, campaign_id) combos).

    `cpc` is optional — when missing from the source row it is stored as NULL.
    """

    dependencies = [
        ("uploads", "0021_swiggy_ads_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.zepto_ads (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key columns
                date            DATE NOT NULL DEFAULT '1970-01-01',
                product_id      TEXT NOT NULL DEFAULT '',
                campaign_id     TEXT NOT NULL DEFAULT '',

                -- Other text columns
                product_name    TEXT,
                brand_id        TEXT,
                brand_name      TEXT,
                campaign_name   TEXT,
                category        TEXT,

                -- Metrics
                atc             NUMERIC,
                clicks          NUMERIC,
                cpc             NUMERIC,
                cpm             NUMERIC,
                ctr             NUMERIC,
                impressions     NUMERIC,
                orders          NUMERIC,
                other_skus      NUMERIC,
                revenue         NUMERIC,
                roas            NUMERIC,
                robas           NUMERIC,
                same_skus       NUMERIC,
                spend           NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'ZEPTO',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS zepto_ads_dedup_idx
                ON public.zepto_ads (date, product_id, campaign_id);

            CREATE INDEX IF NOT EXISTS zepto_ads_campaign_id_idx
                ON public.zepto_ads (campaign_id);

            CREATE INDEX IF NOT EXISTS zepto_ads_product_id_idx
                ON public.zepto_ads (product_id);

            CREATE INDEX IF NOT EXISTS zepto_ads_date_idx
                ON public.zepto_ads (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.zepto_ads;
            """,
        ),
    ]
