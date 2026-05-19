from django.db import migrations


class Migration(migrations.Migration):
    """Replace date_range (text span) with date (single user-picked date).

    The uploader now requires the user to pick a single date before pasting
    rows; that date is written to every row of the upload. Removes the
    free-text date_range column entirely.
    """

    dependencies = [
        ("uploads", "0015_amazon_ads"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.amazon_ads_dedup_idx;
            DROP INDEX IF EXISTS public.amazon_ads_date_range_idx;

            ALTER TABLE public.amazon_ads
                DROP COLUMN IF EXISTS date_range;

            ALTER TABLE public.amazon_ads
                ADD COLUMN IF NOT EXISTS date DATE NOT NULL DEFAULT '1970-01-01';

            CREATE UNIQUE INDEX IF NOT EXISTS amazon_ads_dedup_idx
                ON public.amazon_ads (
                    date,
                    campaign_id,
                    ad_group_id,
                    advertised_product_id
                );

            CREATE INDEX IF NOT EXISTS amazon_ads_date_idx
                ON public.amazon_ads (date);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.amazon_ads_dedup_idx;
            DROP INDEX IF EXISTS public.amazon_ads_date_idx;

            ALTER TABLE public.amazon_ads
                DROP COLUMN IF EXISTS date;

            ALTER TABLE public.amazon_ads
                ADD COLUMN IF NOT EXISTS date_range TEXT NOT NULL DEFAULT '';

            CREATE UNIQUE INDEX IF NOT EXISTS amazon_ads_dedup_idx
                ON public.amazon_ads (date_range, campaign_id, ad_group_id, advertised_product_id);

            CREATE INDEX IF NOT EXISTS amazon_ads_date_range_idx
                ON public.amazon_ads (date_range);
            """,
        ),
    ]
