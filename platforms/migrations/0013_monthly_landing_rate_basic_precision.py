from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0012_month_landingrate_logs"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DO $$
            DECLARE
                view_sql text;
                mat_sql text;
                mat_index_sql text[];
                idx_sql text;
            BEGIN
                SELECT pg_get_viewdef('"SecMaster"'::regclass, true)
                  INTO view_sql;

                IF to_regclass('public."SecMaster_Mat"') IS NOT NULL THEN
                    SELECT pg_get_viewdef('"SecMaster_Mat"'::regclass, true)
                      INTO mat_sql;
                    SELECT array_agg(indexdef ORDER BY indexname)
                      INTO mat_index_sql
                      FROM pg_indexes
                     WHERE schemaname = 'public'
                       AND tablename = 'SecMaster_Mat';
                    DROP MATERIALIZED VIEW "SecMaster_Mat";
                END IF;

                DROP VIEW "SecMaster";

                ALTER TABLE monthly_landing_rate
                ALTER COLUMN basic_rate TYPE NUMERIC
                USING basic_rate::numeric;

                EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;

                IF mat_sql IS NOT NULL THEN
                    EXECUTE 'CREATE MATERIALIZED VIEW "SecMaster_Mat" AS ' || mat_sql;
                    FOREACH idx_sql IN ARRAY COALESCE(mat_index_sql, ARRAY[]::text[]) LOOP
                        EXECUTE idx_sql;
                    END LOOP;
                END IF;
            END $$;
            """,
            reverse_sql="""
            DO $$
            DECLARE
                view_sql text;
                mat_sql text;
                mat_index_sql text[];
                idx_sql text;
            BEGIN
                SELECT pg_get_viewdef('"SecMaster"'::regclass, true)
                  INTO view_sql;

                IF to_regclass('public."SecMaster_Mat"') IS NOT NULL THEN
                    SELECT pg_get_viewdef('"SecMaster_Mat"'::regclass, true)
                      INTO mat_sql;
                    SELECT array_agg(indexdef ORDER BY indexname)
                      INTO mat_index_sql
                      FROM pg_indexes
                     WHERE schemaname = 'public'
                       AND tablename = 'SecMaster_Mat';
                    DROP MATERIALIZED VIEW "SecMaster_Mat";
                END IF;

                DROP VIEW "SecMaster";

                ALTER TABLE monthly_landing_rate
                ALTER COLUMN basic_rate TYPE NUMERIC(12, 2)
                USING basic_rate::numeric(12, 2);

                EXECUTE 'CREATE OR REPLACE VIEW "SecMaster" AS ' || view_sql;

                IF mat_sql IS NOT NULL THEN
                    EXECUTE 'CREATE MATERIALIZED VIEW "SecMaster_Mat" AS ' || mat_sql;
                    FOREACH idx_sql IN ARRAY COALESCE(mat_index_sql, ARRAY[]::text[]) LOOP
                        EXECUTE idx_sql;
                    END LOOP;
                END IF;
            END $$;
            """,
        ),
    ]
