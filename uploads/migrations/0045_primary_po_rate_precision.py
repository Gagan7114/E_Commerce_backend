from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0044_amazon_coupon_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            """
            DO $$
            DECLARE
                master_po_base_def text;
                master_po_raw_def text;
                master_po_def text;
            BEGIN
                IF to_regclass('public.master_po_base') IS NOT NULL THEN
                    SELECT pg_get_viewdef('public.master_po_base'::regclass, true)
                      INTO master_po_base_def;
                END IF;
                IF to_regclass('public.master_po_raw') IS NOT NULL THEN
                    SELECT pg_get_viewdef('public.master_po_raw'::regclass, true)
                      INTO master_po_raw_def;
                END IF;
                IF to_regclass('public.master_po') IS NOT NULL
                   AND EXISTS (
                       SELECT 1
                         FROM pg_class c
                         JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public'
                          AND c.relname = 'master_po'
                          AND c.relkind = 'v'
                   ) THEN
                    SELECT pg_get_viewdef('public.master_po'::regclass, true)
                      INTO master_po_def;
                END IF;

                DROP VIEW IF EXISTS public.master_po;
                DROP VIEW IF EXISTS public.master_po_raw;
                DROP VIEW IF EXISTS public.master_po_base;

                IF to_regclass('public.total_po') IS NOT NULL THEN
                    ALTER TABLE public.total_po
                    ALTER COLUMN basic_rate TYPE NUMERIC USING basic_rate::numeric,
                    ALTER COLUMN landing_rate TYPE NUMERIC USING landing_rate::numeric;
                END IF;

                IF to_regclass('public.total_po_zbs') IS NOT NULL THEN
                    ALTER TABLE public.total_po_zbs
                    ALTER COLUMN basic_rate TYPE NUMERIC USING basic_rate::numeric,
                    ALTER COLUMN landing_rate TYPE NUMERIC USING landing_rate::numeric;
                END IF;

                IF to_regclass('public.master_po') IS NOT NULL THEN
                    ALTER TABLE public.master_po
                    ALTER COLUMN basic_rate TYPE NUMERIC USING basic_rate::numeric,
                    ALTER COLUMN landing_rate TYPE NUMERIC USING landing_rate::numeric;
                END IF;

                IF master_po_base_def IS NOT NULL THEN
                    EXECUTE 'CREATE VIEW public.master_po_base AS ' || master_po_base_def;
                END IF;
                IF master_po_raw_def IS NOT NULL THEN
                    EXECUTE 'CREATE VIEW public.master_po_raw AS ' || master_po_raw_def;
                END IF;
                IF master_po_def IS NOT NULL THEN
                    EXECUTE 'CREATE VIEW public.master_po AS ' || master_po_def;
                END IF;
            END $$;
            """,
            migrations.RunSQL.noop,
        ),
    ]
