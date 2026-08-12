from django.db import migrations


class Migration(migrations.Migration):
    """Add cpm to the detail tables' dedup keys.

    The Blinkit ads report emits a separate line per CPM bid: the same campaign,
    keyword, match type and position can appear twice on one day at two
    different CPMs, each carrying its own impressions and spend. Example, from
    the August export:

        2026-08-01  campaign 45139  "pomace olive"  SMART  pos 1
            cpm 200 -> 2 impressions, Rs 0.40
            cpm 210 -> 3 impressions, Rs 0.60

    The 0086 key did not include cpm, so the second line overwrote the first:
    102 of 5,550 August rows collapsed and Rs 6,115 of spend went missing.
    With cpm in the key, 5,549 of 5,550 rows survive.

    The remaining pair is identical on every exported column and still differs
    in metrics (4 vs 5 impressions, Rs 0.80 vs Rs 1.00), so nothing in the file
    can separate them — no column-based key can, and they merge. That is Rs 0.80
    of Rs 4.81 lakh, 0.0002%.

    cpm becomes NOT NULL DEFAULT 0 because Postgres treats NULL as distinct in a
    unique index: leaving it nullable would let rows with no CPM pile up on
    every re-upload, which is the trap migration 0014 fixed on blinkit_ads.
    """

    dependencies = [
        ("uploads", "0086_blinkit_ads_keyword_and_asset"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            UPDATE public.blinkit_ads_keyword SET cpm = 0 WHERE cpm IS NULL;
            UPDATE public.blinkit_ads_asset   SET cpm = 0 WHERE cpm IS NULL;

            ALTER TABLE public.blinkit_ads_keyword
                ALTER COLUMN cpm SET DEFAULT 0,
                ALTER COLUMN cpm SET NOT NULL;
            ALTER TABLE public.blinkit_ads_asset
                ALTER COLUMN cpm SET DEFAULT 0,
                ALTER COLUMN cpm SET NOT NULL;

            DROP INDEX IF EXISTS public.blinkit_ads_keyword_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_keyword_dedup_idx
                ON public.blinkit_ads_keyword
                (date, campaign_id, keyword, match_type, most_viewed_position, cpm);

            DROP INDEX IF EXISTS public.blinkit_ads_asset_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_asset_dedup_idx
                ON public.blinkit_ads_asset
                (date, campaign_id, subcampaign_id, asset, cpm);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_keyword_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_keyword_dedup_idx
                ON public.blinkit_ads_keyword
                (date, campaign_id, keyword, match_type, most_viewed_position);

            DROP INDEX IF EXISTS public.blinkit_ads_asset_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_asset_dedup_idx
                ON public.blinkit_ads_asset
                (date, campaign_id, subcampaign_id, asset);

            ALTER TABLE public.blinkit_ads_keyword ALTER COLUMN cpm DROP NOT NULL;
            ALTER TABLE public.blinkit_ads_asset   ALTER COLUMN cpm DROP NOT NULL;
            """,
        ),
    ]
