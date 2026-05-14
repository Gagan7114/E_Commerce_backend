from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0005_amazon_po_source_line_key_index"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            DO $$
            BEGIN
                IF to_regclass('reporting."appointment"') IS NULL THEN
                    RETURN;
                END IF;

                -- Drop old index that was built on the wrong key (included pos/destination_fc/pro)
                DROP INDEX IF EXISTS reporting.appointment_line_key_uq;

                -- Recompute appointment_line_key using only appointment_id (the true unique identifier).
                -- Old key included pos/destination_fc/pro, which meant re-uploading the same appointment
                -- with updated PO data inserted a duplicate row instead of updating the existing one.
                EXECUTE $sql$
                    UPDATE reporting."appointment"
                       SET appointment_line_key = md5(LOWER(TRIM(COALESCE(appointment_id, ''))))
                     WHERE appointment_id IS NOT NULL
                $sql$;

                -- Deduplicate: if two rows now share the same appointment_line_key (because they were
                -- the same appointment uploaded with different pos values), keep only the most recently
                -- updated one.
                EXECUTE $sql$
                    DELETE FROM reporting."appointment"
                    WHERE appointment_line_key IS NOT NULL
                      AND ctid NOT IN (
                          SELECT DISTINCT ON (appointment_line_key) ctid
                            FROM reporting."appointment"
                           WHERE appointment_line_key IS NOT NULL
                           ORDER BY appointment_line_key, updated_at DESC NULLS LAST
                      )
                $sql$;

                -- Recreate the unique index on the corrected key
                EXECUTE $sql$
                    CREATE UNIQUE INDEX appointment_line_key_uq
                        ON reporting."appointment" (appointment_line_key)
                $sql$;
            END $$;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]
