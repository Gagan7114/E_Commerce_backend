from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0006_fix_appointment_line_key"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            DO $$
            BEGIN
                IF to_regclass('reporting."appointment"') IS NULL THEN
                    RETURN;
                END IF;

                DROP TABLE IF EXISTS pg_temp._appointment_split_rows;

                CREATE TEMP TABLE _appointment_split_rows ON COMMIT DROP AS
                WITH expanded AS (
                    SELECT md5(concat_ws('|',
                               LOWER(TRIM(COALESCE(a.appointment_id, ''))),
                               LOWER(TRIM(COALESCE(split_pos, '')))
                           )) AS appointment_line_key,
                           a.appointment_id,
                           a.status,
                           a.appointment_time,
                           a.creation_date,
                           split_pos AS pos,
                           a.destination_fc,
                           a.pro,
                           a.month,
                           a.year,
                           a.upload_id,
                           a.created_at,
                           a.updated_at,
                           a.ctid AS source_ctid
                      FROM reporting."appointment" a
                      CROSS JOIN LATERAL (
                          SELECT NULLIF(TRIM(po_value), '') AS split_pos
                            FROM unnest(
                                CASE
                                    WHEN NULLIF(TRIM(COALESCE(a.pos, '')), '') IS NULL
                                        THEN ARRAY[NULL::text]
                                    ELSE regexp_split_to_array(a.pos, '\s*[,;]\s*')
                                END
                            ) AS parts(po_value)
                           WHERE NULLIF(TRIM(COALESCE(a.pos, '')), '') IS NULL
                              OR NULLIF(TRIM(po_value), '') IS NOT NULL
                      ) po_parts
                     WHERE a.appointment_id IS NOT NULL
                ),
                ranked AS (
                    SELECT *,
                           row_number() OVER (
                               PARTITION BY appointment_line_key
                               ORDER BY updated_at DESC NULLS LAST, source_ctid DESC
                           ) AS rn
                      FROM expanded
                )
                SELECT appointment_line_key, appointment_id, status, appointment_time,
                       creation_date, pos, destination_fc, pro, month, year, upload_id,
                       created_at, updated_at
                  FROM ranked
                 WHERE rn = 1;

                DELETE FROM reporting."appointment";

                INSERT INTO reporting."appointment" (
                    appointment_line_key, appointment_id, status, appointment_time,
                    creation_date, pos, destination_fc, pro, month, year, upload_id,
                    created_at, updated_at
                )
                SELECT appointment_line_key, appointment_id, status, appointment_time,
                       creation_date, pos, destination_fc, pro, month, year, upload_id,
                       COALESCE(created_at, now()), COALESCE(updated_at, now())
                  FROM _appointment_split_rows;

                CREATE UNIQUE INDEX IF NOT EXISTS appointment_line_key_uq
                    ON reporting."appointment" (appointment_line_key);
            END $$;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]
