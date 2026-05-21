from django.db import migrations


class Migration(migrations.Migration):
    """Create blinkit_brandfund table for Blinkit Brand Fund report ingestion.

    Source: Blinkit Brand Fund CSV export (22 columns). Per the spec, we drop
    the source `date` column and replace it with a user-picked date stamped
    on every row — same pattern as the zepto / swiggy brand-fund uploaders.

    `format` is auto-tagged 'BLINKIT' (DB default + uploader payload).

    Unique key: (date, city, item_id, p_type). The same item in the same
    city can have multiple offer types (p_type) on the same date, so all
    four are required to dedupe cleanly.
    """

    dependencies = [
        ("uploads", "0036_swiggy_brandfund_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.blinkit_brandfund (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key columns (NOT NULL DEFAULT '' so Postgres dedupes
                -- correctly — NULLs would be treated as distinct).
                date            DATE NOT NULL DEFAULT '1970-01-01',
                city            TEXT NOT NULL DEFAULT '',
                item_id         TEXT NOT NULL DEFAULT '',
                p_type          TEXT NOT NULL DEFAULT '',

                -- Other text descriptors
                product_id          TEXT,
                offer_type          TEXT,
                product_name        TEXT,
                l0_category_name    TEXT,
                l1_category_name    TEXT,
                l2_category_name    TEXT,
                brand_name          TEXT,
                system_sheet_id     TEXT,
                upload_source       TEXT,
                user_email          TEXT,

                -- Numeric metrics
                multiplier                       NUMERIC,
                item_mrp                         NUMERIC,
                brandfund_absolute_value         NUMERIC,
                brandfund_absolute_input_value   NUMERIC,
                brandfund_percentage_value       NUMERIC,
                qty_sold                         NUMERIC,
                total_brand_fund                 NUMERIC,
                mrp_gmv                          NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'BLINKIT',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_brandfund_dedup_idx
                ON public.blinkit_brandfund (date, city, item_id, p_type);

            CREATE INDEX IF NOT EXISTS blinkit_brandfund_item_id_idx
                ON public.blinkit_brandfund (item_id);

            CREATE INDEX IF NOT EXISTS blinkit_brandfund_brand_name_idx
                ON public.blinkit_brandfund (brand_name);

            CREATE INDEX IF NOT EXISTS blinkit_brandfund_date_idx
                ON public.blinkit_brandfund (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.blinkit_brandfund;
            """,
        ),
    ]
