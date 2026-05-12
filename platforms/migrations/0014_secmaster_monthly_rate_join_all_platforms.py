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
     WHERE c.relname = 'SecMaster'
       AND n.nspname = 'public';

    IF view_sql IS NULL THEN
        RAISE NOTICE 'SecMaster view does not exist; skipping monthly rate join patch.';
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = s."ITEM_CODE" AND mlr.format::text = ''SWIGGY''::text',
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(s."ITEM_CODE"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''swiggy''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', s."ORDERED_DATE"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true'
    );

    view_sql := replace(
        view_sql,
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = bb.source_sku_id AND mlr.format::text = ''BIGBASKET''::text',
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(bb.source_sku_id::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''bigbasket''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', bb.date_range::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true'
    );

    view_sql := replace(
        view_sql,
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = jm."SKU" AND mlr.format::text = ''JIO MART''::text',
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(jm."SKU"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''jiomart''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', jm."ORDER_DATE"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true'
    );

    view_sql := replace(
        view_sql,
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = fk."Product Id" AND mlr.format::text = ''FLIPKART''::text',
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(fk."Product Id"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''flipkart''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', fk."Order Date"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true'
    );

    IF view_sql LIKE '%JOIN monthly_landing_rate mlr ON%' THEN
        RAISE EXCEPTION 'SecMaster still contains a non-monthly monthly_landing_rate join.';
    END IF;

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
     WHERE c.relname = 'SecMaster'
       AND n.nspname = 'public';

    IF view_sql IS NULL THEN
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(s."ITEM_CODE"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''swiggy''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', s."ORDERED_DATE"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true',
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = s."ITEM_CODE" AND mlr.format::text = ''SWIGGY''::text'
    );

    view_sql := replace(
        view_sql,
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(bb.source_sku_id::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''bigbasket''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', bb.date_range::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true',
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = bb.source_sku_id AND mlr.format::text = ''BIGBASKET''::text'
    );

    view_sql := replace(
        view_sql,
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(jm."SKU"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''jiomart''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', jm."ORDER_DATE"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true',
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = jm."SKU" AND mlr.format::text = ''JIO MART''::text'
    );

    view_sql := replace(
        view_sql,
        'LEFT JOIN LATERAL (
            SELECT rate.landing_rate, rate.basic_rate
              FROM monthly_landing_rate rate
             WHERE UPPER(TRIM(rate.sku_code::text)) = UPPER(TRIM(fk."Product Id"::text))
               AND REGEXP_REPLACE(LOWER(TRIM(rate.format::text)), ''[^a-z0-9]+'', '''', ''g'') = ''flipkart''
               AND rate.month = TO_CHAR(DATE_TRUNC(''month'', fk."Order Date"::timestamp), ''YYYY-MM-DD'')
             ORDER BY rate.created_at DESC
             LIMIT 1
        ) mlr ON true',
        'LEFT JOIN monthly_landing_rate mlr ON mlr.sku_code::text = fk."Product Id" AND mlr.format::text = ''FLIPKART''::text'
    );

    EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;
END $$;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0013_monthly_landing_rate_basic_precision"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
