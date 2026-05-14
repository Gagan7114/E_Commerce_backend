from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0004_master_sheet_tax_rate"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DO $$
            BEGIN
                IF to_regclass('reporting."Amazon PO"') IS NULL THEN
                    RETURN;
                END IF;

                -- Add column if it was never created
                EXECUTE 'ALTER TABLE reporting."Amazon PO"
                             ADD COLUMN IF NOT EXISTS source_line_key TEXT';

                -- Backfill rows that are missing a key
                EXECUTE $sql$
                    UPDATE reporting."Amazon PO"
                       SET source_line_key = md5(concat_ws('|',
                           COALESCE(po_number, ''),
                           COALESCE(external_id, ''),
                           COALESCE(merchant_sku, ''),
                           COALESCE(fulfillment_center, '')
                       ))
                     WHERE source_line_key IS NULL
                $sql$;

                -- Remove duplicate rows, keeping the most recently updated one
                EXECUTE $sql$
                    DELETE FROM reporting."Amazon PO"
                    WHERE source_line_key IS NOT NULL
                      AND ctid NOT IN (
                          SELECT DISTINCT ON (source_line_key) ctid
                            FROM reporting."Amazon PO"
                           WHERE source_line_key IS NOT NULL
                           ORDER BY source_line_key, updated_at DESC NULLS LAST
                      )
                $sql$;

                -- Create the unique index the ON CONFLICT clause depends on
                EXECUTE $sql$
                    CREATE UNIQUE INDEX IF NOT EXISTS amazon_po_source_line_key_uq
                        ON reporting."Amazon PO" (source_line_key)
                $sql$;
            END $$;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS reporting.amazon_po_source_line_key_uq;
            """,
        ),
    ]
