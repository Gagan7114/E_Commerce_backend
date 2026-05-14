from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0002_appointment_sheet_shape"),
    ]

    operations = [
        migrations.RunSQL(
            sql='''
            ALTER TABLE IF EXISTS reporting."Amazon PO"
                DROP COLUMN IF EXISTS vendor_new,
                DROP COLUMN IF EXISTS delivery_month,
                DROP COLUMN IF EXISTS unit_of_measure;

            ALTER TABLE IF EXISTS reporting."appointment"
                DROP COLUMN IF EXISTS asn;

            ALTER TABLE IF EXISTS staging."appointment data"
                DROP COLUMN IF EXISTS asn;
            ''',
            reverse_sql='''
            ALTER TABLE IF EXISTS reporting."Amazon PO"
                ADD COLUMN IF NOT EXISTS vendor_new TEXT,
                ADD COLUMN IF NOT EXISTS delivery_month INTEGER,
                ADD COLUMN IF NOT EXISTS unit_of_measure TEXT;

            ALTER TABLE IF EXISTS reporting."appointment"
                ADD COLUMN IF NOT EXISTS asn TEXT;

            ALTER TABLE IF EXISTS staging."appointment data"
                ADD COLUMN IF NOT EXISTS asn TEXT;
            ''',
        ),
    ]
