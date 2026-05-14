from django.db import migrations


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE IF EXISTS reporting."Amazon PO"
                ADD COLUMN IF NOT EXISTS cost_price NUMERIC,
                ADD COLUMN IF NOT EXISTS per_liter NUMERIC,
                ADD COLUMN IF NOT EXISTS year INTEGER,
                ADD COLUMN IF NOT EXISTS tax NUMERIC,
                ADD COLUMN IF NOT EXISTS brand TEXT;

            ALTER TABLE IF EXISTS reporting."Amazon PO"
                ALTER COLUMN po_window TYPE INTEGER USING NULL,
                ALTER COLUMN per_ltr_unit TYPE TEXT USING per_ltr_unit::text;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]
