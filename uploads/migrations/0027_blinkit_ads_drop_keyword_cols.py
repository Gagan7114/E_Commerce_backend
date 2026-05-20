from django.db import migrations


class Migration(migrations.Migration):
    """Drop the 5 keyword-level columns from blinkit_ads and shrink the
    unique index to (date, campaign_id, campaign_name).

    After the uploader's keyword-merge change, every row in blinkit_ads is a
    campaign-day aggregate — these 5 columns were always empty strings, just
    occupying space and confusing readers.

    Columns dropped:
      - targeting_type
      - targeting_value
      - match_type
      - most_viewed_position
      - pacing_type

    Index change:
      blinkit_ads_dedup_idx UNIQUE (date, campaign_id, campaign_name,
                                    targeting_type, targeting_value,
                                    match_type, most_viewed_position,
                                    pacing_type)
        ───►
      blinkit_ads_dedup_idx UNIQUE (date, campaign_id, campaign_name)
    """

    dependencies = [
        ("uploads", "0026_bigbasket_ads_master_format_fix"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;

            ALTER TABLE public.blinkit_ads
                DROP COLUMN IF EXISTS targeting_type,
                DROP COLUMN IF EXISTS targeting_value,
                DROP COLUMN IF EXISTS match_type,
                DROP COLUMN IF EXISTS most_viewed_position,
                DROP COLUMN IF EXISTS pacing_type;

            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id, campaign_name);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;

            ALTER TABLE public.blinkit_ads
                ADD COLUMN IF NOT EXISTS targeting_type        TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS targeting_value       TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS match_type            TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS most_viewed_position  TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS pacing_type           TEXT NOT NULL DEFAULT '';

            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_dedup_idx
                ON public.blinkit_ads (
                    date, campaign_id, campaign_name,
                    targeting_type, targeting_value,
                    match_type, most_viewed_position, pacing_type
                );
            """,
        ),
    ]
