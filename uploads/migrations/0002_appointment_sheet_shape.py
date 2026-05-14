from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0001_amazon_po_report_shape"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r'''
            DO $$
            DECLARE
                pk_name text;
            BEGIN
                IF to_regclass('reporting."appointment"') IS NULL THEN
                    RETURN;
                END IF;

                EXECUTE 'ALTER TABLE reporting."appointment" ADD COLUMN IF NOT EXISTS appointment_line_key TEXT';

                EXECUTE $sql$
                    UPDATE reporting."appointment"
                       SET appointment_line_key = md5(concat_ws('|',
                           COALESCE(appointment_id, ''),
                           COALESCE(pos, ''),
                           COALESCE(destination_fc, ''),
                           COALESCE(pro, '')
                       ))
                     WHERE appointment_line_key IS NULL
                $sql$;

                SELECT conname
                  INTO pk_name
                  FROM pg_constraint
                 WHERE conrelid = 'reporting."appointment"'::regclass
                   AND contype = 'p'
                 LIMIT 1;

                IF pk_name IS NOT NULL THEN
                    EXECUTE format('ALTER TABLE reporting."appointment" DROP CONSTRAINT %I', pk_name);
                END IF;

                EXECUTE $sql$
                    ALTER TABLE reporting."appointment"
                        ALTER COLUMN month TYPE TEXT
                        USING CASE month::text
                            WHEN '1' THEN 'JANUARY'
                            WHEN '2' THEN 'FEBRUARY'
                            WHEN '3' THEN 'MARCH'
                            WHEN '4' THEN 'APRIL'
                            WHEN '5' THEN 'MAY'
                            WHEN '6' THEN 'JUNE'
                            WHEN '7' THEN 'JULY'
                            WHEN '8' THEN 'AUGUST'
                            WHEN '9' THEN 'SEPTEMBER'
                            WHEN '10' THEN 'OCTOBER'
                            WHEN '11' THEN 'NOVEMBER'
                            WHEN '12' THEN 'DECEMBER'
                            ELSE NULLIF(UPPER(month::text), '')
                        END
                $sql$;

                EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS appointment_line_key_uq ON reporting."appointment" (appointment_line_key)';
            END $$;
            ''',
            reverse_sql=migrations.RunSQL.noop,
        )
    ]
