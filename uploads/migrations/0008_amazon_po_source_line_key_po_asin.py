from django.db import migrations


NEW_KEY_SQL = """
md5(concat_ws('|',
    LOWER(TRIM(COALESCE(po_number, ''))),
    LOWER(TRIM(COALESCE(asin, '')))
))
"""


OLD_KEY_SQL = """
md5(concat_ws('|',
    COALESCE(po_number, ''),
    COALESCE(external_id, ''),
    COALESCE(merchant_sku, ''),
    COALESCE(fulfillment_center, '')
))
"""


def _rebuild_source_line_key(expression: str) -> str:
    return f"""
    DO $$
    BEGIN
        IF to_regclass('reporting."Amazon PO"') IS NULL THEN
            RETURN;
        END IF;

        DROP INDEX IF EXISTS reporting.amazon_po_source_line_key_uq;

        EXECUTE 'ALTER TABLE reporting."Amazon PO"
                     ADD COLUMN IF NOT EXISTS source_line_key TEXT';

        EXECUTE $sql$
            UPDATE reporting."Amazon PO"
               SET source_line_key = {expression}
        $sql$;

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

        EXECUTE $sql$
            CREATE UNIQUE INDEX IF NOT EXISTS amazon_po_source_line_key_uq
                ON reporting."Amazon PO" (source_line_key)
        $sql$;
    END $$;
    """


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0007_split_appointment_po_rows"),
    ]

    operations = [
        migrations.RunSQL(
            sql=_rebuild_source_line_key(NEW_KEY_SQL),
            reverse_sql=_rebuild_source_line_key(OLD_KEY_SQL),
        ),
    ]
