from django.db import migrations


class Migration(migrations.Migration):
    """Switch blinkit_ads dedup key to (date, campaign_id).

    The earlier index keyed on (date, campaign_id, targeting_type) doesn't
    match the way the source Google Apps Script identifies Blinkit ad rows.
    The script tracks rows by (date, platform=BLINKIT) + campaign_id and does
    not persist targeting_type at all. Aligning our DB key with the script.
    """

    dependencies = [
        ("uploads", "0012_blinkit_ads"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;
            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;
            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id, targeting_type);
            """,
        ),
    ]
