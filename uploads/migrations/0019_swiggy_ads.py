from django.db import migrations


class Migration(migrations.Migration):
    """Create swiggy_ads table for Swiggy Instamart Ads report ingestion.

    Source: Swiggy Instamart Ads Console CSV (28 cols incl. CAMPAIGN_START_DATE /
    CAMPAIGN_END_DATE which are dropped on ingest). The user picks a single
    upload `date` in the UI, mirroring the Amazon Ads flow.

    Unique key: (date, campaign_id, keyword_count). keyword_count is the only
    dimension in the Swiggy export that distinguishes the two segment rows
    (keyword vs non-keyword placement) of the same campaign — verified against
    the May 2026 export which has zero duplicates under this key.
    """

    dependencies = [
        ("uploads", "0018_master_sheet_format_sku_unique"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.swiggy_ads (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key columns
                date            DATE NOT NULL DEFAULT '1970-01-01',
                campaign_id     TEXT NOT NULL DEFAULT '',
                keyword_count   NUMERIC NOT NULL DEFAULT 0,

                -- Other text columns
                campaign_name   TEXT,
                campaign_status TEXT,
                bidding_type    TEXT,
                budget_type     TEXT,
                brand_name      TEXT,

                -- Count / aggregate metrics
                ad_property_count           NUMERIC,
                city_count                  NUMERIC,
                product_count               NUMERIC,
                ecpm                        NUMERIC,
                ecpc                        NUMERIC,
                total_impressions           NUMERIC,
                total_budget                NUMERIC,
                total_budget_burnt          NUMERIC,
                total_clicks                NUMERIC,
                total_ctr                   NUMERIC,
                total_a2c                   NUMERIC,
                a2c_rate                    NUMERIC,
                total_gmv                   NUMERIC,
                total_conversions           NUMERIC,
                total_roi                   NUMERIC,
                total_direct_gmv_7_days     NUMERIC,
                total_direct_roi_7_days     NUMERIC,
                total_direct_gmv_14_days    NUMERIC,
                total_direct_roi_14_days    NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'SWIGGY',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS swiggy_ads_dedup_idx
                ON public.swiggy_ads (date, campaign_id, keyword_count);

            CREATE INDEX IF NOT EXISTS swiggy_ads_campaign_id_idx
                ON public.swiggy_ads (campaign_id);

            CREATE INDEX IF NOT EXISTS swiggy_ads_date_idx
                ON public.swiggy_ads (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.swiggy_ads;
            """,
        ),
    ]
