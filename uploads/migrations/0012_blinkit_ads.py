from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0011_upload_file_processing_statuses"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.blinkit_ads (
                id BIGSERIAL PRIMARY KEY,
                date DATE,
                campaign_id TEXT,
                campaign_name TEXT,
                direct_qty_sold NUMERIC,
                indirect_qty_sold NUMERIC,
                ad_spent NUMERIC,
                direct_gmv NUMERIC,
                indirect_gmv NUMERIC,
                impression NUMERIC,
                targeting_type TEXT,
                format TEXT NOT NULL DEFAULT 'BLINKIT',
                uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Allow safe upserts on (date, campaign_id, targeting_type).
            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_ads_dedup_idx
                ON public.blinkit_ads (date, campaign_id, targeting_type);

            CREATE INDEX IF NOT EXISTS blinkit_ads_date_idx
                ON public.blinkit_ads (date);

            CREATE INDEX IF NOT EXISTS blinkit_ads_campaign_id_idx
                ON public.blinkit_ads (campaign_id);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.blinkit_ads;
            """,
        ),
    ]
