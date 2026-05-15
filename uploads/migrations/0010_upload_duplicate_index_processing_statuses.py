from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0009_amazon_upload_lookup_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DO $$
            BEGIN
                IF to_regclass('raw.upload_file') IS NOT NULL THEN
                    EXECUTE 'DROP INDEX IF EXISTS raw.upload_file_duplicate_lookup_idx';
                    EXECUTE 'CREATE INDEX upload_file_duplicate_lookup_idx
                        ON raw.upload_file (file_hash, main_table_name, raw_file_name, uploaded_at DESC)
                        WHERE status IN (
                            ''completed'', ''partially_successful'', ''staged'',
                            ''uploaded'', ''validating'', ''queued'', ''processing''
                        )';
                END IF;
            END $$;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS raw.upload_file_duplicate_lookup_idx;
            CREATE INDEX IF NOT EXISTS upload_file_duplicate_lookup_idx
                ON raw.upload_file (file_hash, main_table_name, raw_file_name, uploaded_at DESC)
                WHERE status IN (
                    'completed', 'partially_successful', 'staged',
                    'uploaded', 'validating'
                );
            """,
        ),
    ]
