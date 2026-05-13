from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0011_secmaster_blinkit_rate_join"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS month_landingrate_logs (
                id BIGSERIAL PRIMARY KEY,
                sku_code VARCHAR NOT NULL,
                sku_name VARCHAR NOT NULL,
                format VARCHAR NOT NULL,
                month VARCHAR NOT NULL,
                old_landing_rate NUMERIC,
                old_basic_rate NUMERIC,
                new_landing_rate NUMERIC,
                new_basic_rate NUMERIC,
                reason TEXT NOT NULL,
                updated_by_id BIGINT,
                updated_by_email VARCHAR,
                source_created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS month_landingrate_logs_lookup_idx
            ON month_landingrate_logs (format, sku_code, month, updated_at DESC);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS month_landingrate_logs;
            """,
        )
    ]
