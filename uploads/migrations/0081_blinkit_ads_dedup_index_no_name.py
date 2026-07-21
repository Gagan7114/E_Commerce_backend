from django.db import migrations


class Migration(migrations.Migration):
    """Part B of the Blinkit rename-duplicate fix: swap the unique index to
    (date, campaign_id).

    ⚠️ DEPLOY-TOGETHER-WITH-FRONTEND ⚠️
    This changes the dedup index from (date, campaign_id, campaign_name) to
    (date, campaign_id). The uploader's `ON CONFLICT` target must match the
    index, so this migration MUST go live at the same time as the frontend
    change in Frontend/src/pages/uploader/hub/lib/configs/ads.js (blinkit
    uniqueKey/uniqueKeyFields/mergeBy.keyFields -> drop campaign_name).
    Applying this BEFORE that frontend is deployed makes live Blinkit uploads
    fail with "no unique or exclusion constraint matching the ON CONFLICT
    specification". Depends on 0080 having removed the duplicate rows (else the
    unique index can't build).
    """

    dependencies = [
        ("uploads", "0080_blinkit_ads_dedup_drop_campaign_name"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.blinkit_ads_dedup_idx;
            CREATE UNIQUE INDEX blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id, campaign_name);
            """,
        ),
    ]
