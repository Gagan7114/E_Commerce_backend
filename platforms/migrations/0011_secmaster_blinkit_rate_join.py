from django.db import migrations


FORWARD_SQL = r"""
DO $$
DECLARE
    view_sql text;
BEGIN
    SELECT pg_get_viewdef(c.oid, true)
      INTO view_sql
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = 'SecMaster';

    IF view_sql IS NULL THEN
        RAISE NOTICE 'SecMaster view does not exist; skipping Blinkit rate join patch.';
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = b.item_id::text AND mlr.format::text = ''BLINKIT''::text',
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(b.item_id::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''blinkit''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', b.date::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true'
    );

    EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;
END $$;
"""


REVERSE_SQL = r"""
DO $$
DECLARE
    view_sql text;
BEGIN
    SELECT pg_get_viewdef(c.oid, true)
      INTO view_sql
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = 'SecMaster';

    IF view_sql IS NULL THEN
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(b.item_id::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''blinkit''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', b.date::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true',
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = b.item_id::text AND mlr.format::text = ''BLINKIT''::text'
    );

    EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;
END $$;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0010_secmaster_zepto_rate_join"),
    ]

    operations = [migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL)]
