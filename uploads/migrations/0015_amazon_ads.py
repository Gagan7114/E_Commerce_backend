from django.db import migrations


class Migration(migrations.Migration):
    """Create amazon_ads table for Amazon Ads (AMS) report ingestion.

    Source: Amazon Ads Console CSV (e.g., AMS_DATA_MAY.csv) — 38 columns.
    Unique key: (date_range, campaign_id, ad_group_id, advertised_product_id).
    All four key columns are TEXT NOT NULL DEFAULT '' so empty product-ID
    rows (Sponsored-Brands campaigns) still dedupe correctly — Postgres
    treats NULL as distinct in unique indexes.
    """

    dependencies = [
        ("uploads", "0014_blinkit_ads_full_dedup"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.amazon_ads (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key columns (4)
                date_range              TEXT NOT NULL DEFAULT '',
                campaign_id             TEXT NOT NULL DEFAULT '',
                ad_group_id             TEXT NOT NULL DEFAULT '',
                advertised_product_id   TEXT NOT NULL DEFAULT '',

                -- Other identifying text columns
                budget_currency         TEXT,
                campaign_name           TEXT,
                ad_group_name           TEXT,
                advertised_product_sku  TEXT,
                portfolio_id            TEXT,
                portfolio_name          TEXT,

                -- Volume metrics
                impressions             NUMERIC,
                clicks                  NUMERIC,
                ctr                     NUMERIC,
                total_cost              NUMERIC,
                purchases               NUMERIC,
                sales                   NUMERIC,
                units_sold              NUMERIC,
                cost_per_purchase       NUMERIC,
                purchase_rate           NUMERIC,
                roas                    NUMERIC,

                -- Promoted metrics
                purchases_promoted              NUMERIC,
                sales_promoted                  NUMERIC,
                units_sold_promoted             NUMERIC,
                cost_per_purchase_promoted      NUMERIC,
                purchase_rate_promoted          NUMERIC,
                roas_promoted                   NUMERIC,

                -- Halo metrics
                purchases_halo          NUMERIC,
                sales_halo              NUMERIC,
                units_sold_halo         NUMERIC,

                -- New-to-brand metrics
                purchases_ntb           NUMERIC,
                sales_ntb               NUMERIC,
                units_sold_ntb          NUMERIC,
                cost_per_purchase_ntb   NUMERIC,
                purchase_rate_ntb       NUMERIC,
                roas_ntb                NUMERIC,

                -- Detail page metrics
                detail_page_views               NUMERIC,
                cost_per_detail_page_view       NUMERIC,
                detail_page_view_rate           NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'AMAZON',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS amazon_ads_dedup_idx
                ON public.amazon_ads (
                    date_range,
                    campaign_id,
                    ad_group_id,
                    advertised_product_id
                );

            CREATE INDEX IF NOT EXISTS amazon_ads_campaign_id_idx
                ON public.amazon_ads (campaign_id);

            CREATE INDEX IF NOT EXISTS amazon_ads_date_range_idx
                ON public.amazon_ads (date_range);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.amazon_ads;
            """,
        ),
    ]
