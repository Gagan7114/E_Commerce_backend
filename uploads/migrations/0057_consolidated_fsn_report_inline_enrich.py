from django.db import migrations


class Migration(migrations.Migration):
    """Move the 5 master_sheet columns INTO consolidated_fsn_report (instead of the
    separate _master view) and fill them at upload time.

    - Drops the now-redundant consolidated_fsn_report_master view.
    - Adds item / sku_code / category / sub_category / item_head columns and drops
      uploaded_at.
    - Adds a BEFORE INSERT trigger that resolves those 5 from master_sheet by
      matching sku_id = master_sheet.product_name (FLIPKART row preferred), so each
      uploaded row stores the values directly (frozen at upload; re-upload to
      refresh after a master_sheet change).
    - Backfills any rows already in the table.
    """

    dependencies = [
        ("uploads", "0056_consolidated_fsn_report_master"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            DROP VIEW IF EXISTS public.consolidated_fsn_report_master;

            ALTER TABLE public.consolidated_fsn_report
                ADD COLUMN IF NOT EXISTS item         TEXT,
                ADD COLUMN IF NOT EXISTS sku_code     TEXT,
                ADD COLUMN IF NOT EXISTS category     TEXT,
                ADD COLUMN IF NOT EXISTS sub_category TEXT,
                ADD COLUMN IF NOT EXISTS item_head    TEXT,
                DROP COLUMN IF EXISTS uploaded_at;

            CREATE OR REPLACE FUNCTION public.consolidated_fsn_report_enrich()
            RETURNS trigger AS $$
            BEGIN
                SELECT m.item, m.format_sku_code, m.category, m.sub_category, m.item_head
                  INTO NEW.item, NEW.sku_code, NEW.category, NEW.sub_category, NEW.item_head
                FROM public.master_sheet m
                WHERE UPPER(TRIM(m.product_name::text)) = UPPER(TRIM(NEW.sku_id))
                ORDER BY (UPPER(TRIM(m.format::text)) = 'FLIPKART') DESC,
                         m.format_sku_code
                LIMIT 1;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS consolidated_fsn_report_enrich_trg
                ON public.consolidated_fsn_report;
            CREATE TRIGGER consolidated_fsn_report_enrich_trg
                BEFORE INSERT ON public.consolidated_fsn_report
                FOR EACH ROW EXECUTE FUNCTION public.consolidated_fsn_report_enrich();

            -- Backfill rows already present (the trigger only fires on new
            -- inserts). One master_sheet row per product_name (FLIPKART first),
            -- joined on the normalised sku_id key.
            UPDATE public.consolidated_fsn_report c
               SET item         = ms.item,
                   sku_code     = ms.format_sku_code,
                   category     = ms.category,
                   sub_category = ms.sub_category,
                   item_head    = ms.item_head
              FROM (
                   SELECT DISTINCT ON (UPPER(TRIM(product_name::text)))
                          UPPER(TRIM(product_name::text)) AS pn_key,
                          item, format_sku_code, category, sub_category, item_head
                   FROM public.master_sheet
                   WHERE product_name IS NOT NULL AND TRIM(product_name::text) <> ''
                   ORDER BY UPPER(TRIM(product_name::text)),
                            (UPPER(TRIM(format::text)) = 'FLIPKART') DESC,
                            format_sku_code
              ) ms
             WHERE ms.pn_key = UPPER(TRIM(c.sku_id));
            """,
            reverse_sql=r"""
            DROP TRIGGER IF EXISTS consolidated_fsn_report_enrich_trg
                ON public.consolidated_fsn_report;
            DROP FUNCTION IF EXISTS public.consolidated_fsn_report_enrich();
            ALTER TABLE public.consolidated_fsn_report
                DROP COLUMN IF EXISTS item,
                DROP COLUMN IF EXISTS sku_code,
                DROP COLUMN IF EXISTS category,
                DROP COLUMN IF EXISTS sub_category,
                DROP COLUMN IF EXISTS item_head,
                ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """,
        ),
    ]
