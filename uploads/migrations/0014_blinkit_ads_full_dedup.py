from django.db import migrations


class Migration(migrations.Migration):
    """Expand blinkit_ads dedup key to all eight business columns.

    Adds: targeting_value, match_type, most_viewed_position, pacing_type.
    Tightens existing nullable columns (campaign_name, targeting_type) to
    NOT NULL DEFAULT '' so the plain unique index can include them
    (PostgreSQL treats NULLs as distinct in indexes, which would defeat
    dedup for PRODUCT_RECOMMENDATION rows that lack some of these fields).
    """

    dependencies = [
        ("uploads", "0013_blinkit_ads_dedup_key"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- Backfill any existing NULLs so SET NOT NULL succeeds.
            UPDATE public.blinkit_ads SET campaign_name  = '' WHERE campaign_name  IS NULL;
            UPDATE public.blinkit_ads SET targeting_type = '' WHERE targeting_type IS NULL;

            ALTER TABLE public.blinkit_ads
                ALTER COLUMN campaign_name  SET DEFAULT '',
                ALTER COLUMN campaign_name  SET NOT NULL,
                ALTER COLUMN targeting_type SET DEFAULT '',
                ALTER COLUMN targeting_type SET NOT NULL;

            ALTER TABLE public.blinkit_ads
                ADD COLUMN IF NOT EXISTS targeting_value       TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS match_type            TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS most_viewed_position  TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS pacing_type           TEXT NOT NULL DEFAULT '';

            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_dedup_idx
                ON public.blinkit_ads (
                    date,
                    campaign_id,
                    campaign_name,
                    targeting_type,
                    targeting_value,
                    match_type,
                    most_viewed_position,
                    pacing_type
                );
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id);

            ALTER TABLE public.blinkit_ads
                DROP COLUMN IF EXISTS pacing_type,
                DROP COLUMN IF EXISTS most_viewed_position,
                DROP COLUMN IF EXISTS match_type,
                DROP COLUMN IF EXISTS targeting_value;

            ALTER TABLE public.blinkit_ads
                ALTER COLUMN campaign_name  DROP NOT NULL,
                ALTER COLUMN campaign_name  DROP DEFAULT,
                ALTER COLUMN targeting_type DROP NOT NULL,
                ALTER COLUMN targeting_type DROP DEFAULT;
            """,
        ),
    ]
