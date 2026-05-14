from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0003_remove_unused_upload_columns"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE IF EXISTS public.master_sheet
                ADD COLUMN IF NOT EXISTS tax_rate NUMERIC;
            """,
            reverse_sql="""
            ALTER TABLE IF EXISTS public.master_sheet
                DROP COLUMN IF EXISTS tax_rate;
            """,
        ),
    ]
