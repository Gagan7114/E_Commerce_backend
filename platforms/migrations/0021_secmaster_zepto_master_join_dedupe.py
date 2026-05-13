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
        RAISE NOTICE 'SecMaster view does not exist; skipping Zepto master-sheet de-dupe patch.';
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN master_sheet m ON upper(TRIM(BOTH FROM m.format_sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number"))',
        'LEFT JOIN LATERAL (
            SELECT ms.*
              FROM master_sheet ms
             WHERE upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number"))
               AND regexp_replace(lower(TRIM(BOTH FROM ms.format::text)), ''[^a-z0-9]+'', '''', ''g''::text) = ''zepto''::text
             ORDER BY ms.product_name, ms.item, ms.per_unit
             LIMIT 1
        ) m ON true'
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
     WHERE c.relname = 'SecMaster'
       AND n.nspname = 'public';

    IF view_sql IS NULL THEN
        RETURN;
    END IF;

    view_sql := replace(
        view_sql,
        'LEFT JOIN LATERAL (
            SELECT ms.*
              FROM master_sheet ms
             WHERE upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number"))
               AND regexp_replace(lower(TRIM(BOTH FROM ms.format::text)), ''[^a-z0-9]+'', '''', ''g''::text) = ''zepto''::text
             ORDER BY ms.product_name, ms.item, ms.per_unit
             LIMIT 1
        ) m ON true',
        'LEFT JOIN master_sheet m ON upper(TRIM(BOTH FROM m.format_sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number"))'
    );

    EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;
END $$;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0020_secmaster_zepto_master_join_case_insensitive"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
