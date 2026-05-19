from django.db import migrations


class Migration(migrations.Migration):
    """Create ads_master_bs — manual mapping of campaign/SKU per month.

    Five columns: month, campaign_id, sku_id, item, format.
    Unique key: (month, campaign_id, sku_id). All three key columns are
    TEXT NOT NULL DEFAULT '' so Postgres dedupes correctly even when a
    caller submits blank cells (NULL is treated as distinct in unique
    indexes).
    """

    dependencies = [
        ("uploads", "0019_swiggy_ads"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.ads_master_bs (
                id BIGSERIAL PRIMARY KEY,

                month        TEXT NOT NULL DEFAULT '',
                campaign_id  TEXT NOT NULL DEFAULT '',
                sku_id       TEXT NOT NULL DEFAULT '',
                item         TEXT,
                format       TEXT,

                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ads_master_bs_dedup_idx
                ON public.ads_master_bs (month, campaign_id, sku_id);

            CREATE INDEX IF NOT EXISTS ads_master_bs_campaign_idx
                ON public.ads_master_bs (campaign_id);

            CREATE INDEX IF NOT EXISTS ads_master_bs_sku_idx
                ON public.ads_master_bs (sku_id);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.ads_master_bs;
            """,
        ),
    ]
