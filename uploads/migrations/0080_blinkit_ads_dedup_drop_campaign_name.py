from django.db import migrations


class Migration(migrations.Migration):
    """Part A of the Blinkit rename-duplicate fix: REMOVE the duplicate rows.

    Every blinkit_ads row is a campaign-day aggregate, uniquely a
    (date, campaign_id). But the dedup index also included campaign_name (left
    that way by 0027 when keyword cols were dropped), so a Blinkit *rename*
    (same id, new name) inserted a SECOND row instead of updating the first —
    double-counting spend. In Jul 2026 this inflated Blinkit spend by ~68k
    (123 duplicate campaign-days), the entire gap vs the source sheet.

    This migration only DELETES the duplicates (keeping the newest upload per
    (date, campaign_id)), backing them up first. The index swap that prevents
    recurrence is a SEPARATE migration (0081) that MUST be deployed together
    with the frontend uploader change (configs/ads.js uniqueKey -> date,
    campaign_id); shipping the index change before the frontend would make the
    old-key ON CONFLICT upsert fail on live. Keeping the data cleanup here means
    it can go out safely on its own.
    """

    dependencies = [
        ("uploads", "0079_ads_master_basic_rate_carry_forward"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- Back up the older duplicate rows we are about to delete.
            DROP TABLE IF EXISTS public.blinkit_ads_dup_backup_0080;
            CREATE TABLE public.blinkit_ads_dup_backup_0080 AS
            SELECT b.*
            FROM public.blinkit_ads b
            JOIN (
                SELECT id,
                       row_number() OVER (
                           PARTITION BY date, campaign_id
                           ORDER BY uploaded_at DESC NULLS LAST, id DESC
                       ) AS rn
                FROM public.blinkit_ads
            ) r ON r.id = b.id
            WHERE r.rn > 1;

            -- Delete the older duplicate rows, keeping the newest per (date, campaign_id).
            DELETE FROM public.blinkit_ads
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           row_number() OVER (
                               PARTITION BY date, campaign_id
                               ORDER BY uploaded_at DESC NULLS LAST, id DESC
                           ) AS rn
                    FROM public.blinkit_ads
                ) t WHERE t.rn > 1
            );
            """,
            reverse_sql="""
            INSERT INTO public.blinkit_ads
            SELECT * FROM public.blinkit_ads_dup_backup_0080
            ON CONFLICT DO NOTHING;

            DROP TABLE IF EXISTS public.blinkit_ads_dup_backup_0080;
            """,
        ),
    ]
