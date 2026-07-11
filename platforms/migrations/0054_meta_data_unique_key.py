from django.db import migrations


# Unique key for the Upload Hub's ON CONFLICT upsert on meta_data. The Meta
# export is one row per campaign per reporting period, so (date, campaign_name)
# — date being the reporting-end date — uniquely identifies a row. Re-uploading
# the same period updates instead of duplicating.
class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0053_meta_data_table"),
    ]

    operations = [
        migrations.RunSQL(
            sql='CREATE UNIQUE INDEX IF NOT EXISTS meta_data_date_campaign_uidx '
                'ON meta_data ("date", campaign_name);',
            reverse_sql="DROP INDEX IF EXISTS meta_data_date_campaign_uidx;",
        ),
    ]
