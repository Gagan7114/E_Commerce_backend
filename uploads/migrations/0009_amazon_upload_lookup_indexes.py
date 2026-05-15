from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0008_amazon_po_source_line_key_po_asin"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DO $$
            BEGIN
                IF to_regclass('public.master_sheet') IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS master_sheet_format_sku_code_norm_idx
                        ON public.master_sheet (UPPER(TRIM(format_sku_code::text)))';
                    EXECUTE 'CREATE INDEX IF NOT EXISTS master_sheet_item_norm_idx
                        ON public.master_sheet (UPPER(TRIM(item::text)))';
                    EXECUTE 'CREATE INDEX IF NOT EXISTS master_sheet_product_name_norm_idx
                        ON public.master_sheet (LOWER(TRIM(product_name::text)))';
                END IF;

                IF to_regclass('public.amazon_asin_margin') IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS amazon_asin_margin_asin_norm_idx
                        ON public.amazon_asin_margin (UPPER(TRIM(asin::text)))';
                END IF;

                IF to_regclass('public.fc_city_state_channel_master') IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS fc_city_state_channel_fc_norm_idx
                        ON public.fc_city_state_channel_master (UPPER(TRIM(fc::text)))';
                END IF;

                IF to_regclass('master.fc_master') IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS fc_master_active_fc_code_idx
                        ON master.fc_master (fc_code)
                        WHERE is_active = true';
                END IF;

                IF to_regclass('staging."amazon data"') IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS amazon_data_upload_id_idx
                        ON staging."amazon data" (upload_id)';
                END IF;

                IF to_regclass('raw.upload_file') IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS upload_file_duplicate_lookup_idx
                        ON raw.upload_file (file_hash, main_table_name, raw_file_name, uploaded_at DESC)
                        WHERE status IN (
                            ''completed'', ''partially_successful'', ''staged'',
                            ''uploaded'', ''validating''
                        )';
                END IF;
            END $$;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.master_sheet_format_sku_code_norm_idx;
            DROP INDEX IF EXISTS public.master_sheet_item_norm_idx;
            DROP INDEX IF EXISTS public.master_sheet_product_name_norm_idx;
            DROP INDEX IF EXISTS public.amazon_asin_margin_asin_norm_idx;
            DROP INDEX IF EXISTS public.fc_city_state_channel_fc_norm_idx;
            DROP INDEX IF EXISTS master.fc_master_active_fc_code_idx;
            DROP INDEX IF EXISTS staging.amazon_data_upload_id_idx;
            DROP INDEX IF EXISTS raw.upload_file_duplicate_lookup_idx;
            """,
        ),
    ]
