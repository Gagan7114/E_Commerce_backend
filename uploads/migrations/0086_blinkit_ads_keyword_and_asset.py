from django.db import migrations


class Migration(migrations.Migration):
    """Keep the keyword and asset detail the Blinkit ads report already carries.

    Flow 8 (`blinkit-cli ads pull`) downloads one .xlsx with two sheets:

        PRODUCT_LISTING         one row per campaign + keyword + match type
        PRODUCT_RECOMMENDATION  one row per campaign + subcampaign + asset

    Until now the uploader merged every one of those rows down to a campaign-day
    aggregate before insert, so `blinkit_ads` holds ~26 rows a day and the
    keyword / asset / CPM / budget columns were thrown away. On the August 2026
    report that is 5,811 of 6,019 rows discarded.

    (Migration 0027 dropped five keyword-named columns from `blinkit_ads`; those
    were always empty strings, because the merge already ran before the insert.
    There is no historical detail to recover — it starts accumulating from the
    next upload after this migration.)

    These two tables store the report as delivered. `blinkit_ads` is untouched
    and keeps receiving the merged campaign-day rows exactly as before, so every
    existing dashboard reading it is unaffected.

    Column naming follows `blinkit_ads` where the two overlap (ad_spent,
    direct_gmv, indirect_gmv, direct_qty_sold, indirect_qty_sold, impression) so
    a reader does not have to learn two vocabularies.
    """

    dependencies = [
        ("uploads", "0085_amazon_portfolio_head_jun2026"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- PRODUCT_LISTING: keyword-targeted (Product Booster) ads.
            CREATE TABLE IF NOT EXISTS public.blinkit_ads_keyword (
                id                    BIGSERIAL PRIMARY KEY,
                format                TEXT        NOT NULL DEFAULT 'BLINKIT',
                date                  DATE        NOT NULL,
                campaign_id           TEXT        NOT NULL DEFAULT '',
                campaign_name         TEXT        NOT NULL DEFAULT '',
                keyword               TEXT        NOT NULL DEFAULT '',
                match_type            TEXT        NOT NULL DEFAULT '',
                most_viewed_position  TEXT        NOT NULL DEFAULT '',
                pacing_type           TEXT,
                cpm                   NUMERIC,
                total_budget          NUMERIC,
                impression            NUMERIC,
                direct_atc            NUMERIC,
                indirect_atc          NUMERIC,
                new_users             NUMERIC,
                direct_gmv            NUMERIC,
                indirect_gmv          NUMERIC,
                direct_qty_sold       NUMERIC,
                indirect_qty_sold     NUMERIC,
                ad_spent              NUMERIC,
                uploaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- A campaign can run the same keyword under several match types and
            -- positions, and each is its own line in the report. All four text
            -- parts are NOT NULL DEFAULT '' so the index actually dedupes —
            -- Postgres treats NULL as distinct, which would let re-uploads pile
            -- up silently (the same trap migration 0014 fixed on blinkit_ads).
            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_keyword_dedup_idx
                ON public.blinkit_ads_keyword
                (date, campaign_id, keyword, match_type, most_viewed_position);

            CREATE INDEX IF NOT EXISTS blinkit_ads_keyword_date_idx
                ON public.blinkit_ads_keyword (date);

            -- PRODUCT_RECOMMENDATION: asset-placed (Recommendation) ads.
            CREATE TABLE IF NOT EXISTS public.blinkit_ads_asset (
                id                  BIGSERIAL PRIMARY KEY,
                format              TEXT        NOT NULL DEFAULT 'BLINKIT',
                date                DATE        NOT NULL,
                campaign_id         TEXT        NOT NULL DEFAULT '',
                subcampaign_id      TEXT        NOT NULL DEFAULT '',
                campaign_name       TEXT        NOT NULL DEFAULT '',
                asset               TEXT        NOT NULL DEFAULT '',
                title               TEXT,
                pacing_type         TEXT,
                cpm                 NUMERIC,
                total_budget        NUMERIC,
                impression          NUMERIC,
                direct_atc          NUMERIC,
                indirect_atc        NUMERIC,
                new_users           NUMERIC,
                direct_gmv          NUMERIC,
                indirect_gmv        NUMERIC,
                direct_qty_sold     NUMERIC,
                indirect_qty_sold   NUMERIC,
                ad_spent            NUMERIC,
                uploaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_asset_dedup_idx
                ON public.blinkit_ads_asset
                (date, campaign_id, subcampaign_id, asset);

            CREATE INDEX IF NOT EXISTS blinkit_ads_asset_date_idx
                ON public.blinkit_ads_asset (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.blinkit_ads_keyword;
            DROP TABLE IF EXISTS public.blinkit_ads_asset;
            """,
        ),
    ]
