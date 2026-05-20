from django.db import migrations


class Migration(migrations.Migration):
    """Create bigbasket_ads table for BigBasket Ads report ingestion.

    Source: BigBasket Ads Console export — "Campaign Performance Report SPA"
    sheet, 16 cols. The user picks a single upload `date` in the UI,
    mirroring the Amazon / Swiggy / Zepto Ads flow.

    Unique key: (date, product_id, campaign_id). Verified against the sample
    export (10 rows, all distinct product_id values — so the composite is
    trivially unique today, and stays unique under future multi-campaign-type
    reports like Display/Sponsored Brands where product_id alone could collide).

    `format` is auto-filled with 'BIGBASKET' (DB default + uploader payload).
    """

    dependencies = [
        ("uploads", "0023_zepto_ads_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.bigbasket_ads (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key columns
                date            DATE NOT NULL DEFAULT '1970-01-01',
                product_id      TEXT NOT NULL DEFAULT '',
                campaign_id     TEXT NOT NULL DEFAULT '',

                -- Other text columns
                product_name    TEXT,
                campaign_name   TEXT,
                brand_name      TEXT,
                category        TEXT,

                -- Metrics
                ad_spend              NUMERIC,
                ad_impressions        NUMERIC,
                cpm                   NUMERIC,
                add_to_cart           NUMERIC,
                orders_sku            NUMERIC,
                ad_revenue            NUMERIC,
                roas                  NUMERIC,
                other_sku_orders      NUMERIC,
                same_category_orders  NUMERIC,
                other_sku_ad_revenue  NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'BIGBASKET',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS bigbasket_ads_dedup_idx
                ON public.bigbasket_ads (date, product_id, campaign_id);

            CREATE INDEX IF NOT EXISTS bigbasket_ads_campaign_id_idx
                ON public.bigbasket_ads (campaign_id);

            CREATE INDEX IF NOT EXISTS bigbasket_ads_product_id_idx
                ON public.bigbasket_ads (product_id);

            CREATE INDEX IF NOT EXISTS bigbasket_ads_date_idx
                ON public.bigbasket_ads (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.bigbasket_ads;
            """,
        ),
    ]
