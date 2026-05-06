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
        RAISE NOTICE 'SecMaster view does not exist; skipping rate join patch.';
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = z."SKU Number" AND mlr.format::text = ''ZEPTO''::text',
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(z."SKU Number"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''zepto''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', z."Date"::timestamp), ''YYYY-MM-DD'')
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
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(z."SKU Number"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''zepto''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', z."Date"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true',
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = z."SKU Number" AND mlr.format::text = ''ZEPTO''::text'
    );

    EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;
END $$;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0009_month_target_logs_new_targets"),
        ("platforms", "0009_zomato_citymall_uploader_tables"),
    ]

    operations = [migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL)]
