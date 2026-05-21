from django.db import migrations


class Migration(migrations.Migration):
    """Create swiggy_brandfund table for Swiggy Brand Fund report ingestion.

    Source: Swiggy Brand Fund CSV export (18 columns). Per the spec, we drop
    the `ORDER_DATE` column — replaced by the user-picked `date` stamped on
    every row, same pattern as the other ads/brand-fund uploaders.

    Format is auto-tagged 'SWIGGY' via the column default + uploader payload.

    Unique key: (date, store_id, item_code, combo_item_code). The same item
    can sell in many stores on the same day, and a regular ITEM_CODE may have
    a matching COMBO_ITEM_CODE when the row tracks combo sales, so all four
    are needed to dedupe cleanly.
    """

    dependencies = [
        ("uploads", "0034_zepto_brandfund_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.swiggy_brandfund (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key columns (NOT NULL DEFAULT '' so combos without
                -- a COMBO_ITEM_CODE still dedupe correctly).
                date              DATE NOT NULL DEFAULT '1970-01-01',
                store_id          TEXT NOT NULL DEFAULT '',
                item_code         TEXT NOT NULL DEFAULT '',
                combo_item_code   TEXT NOT NULL DEFAULT '',

                -- Other text columns
                brand             TEXT,
                city              TEXT,
                area_name         TEXT,
                l1_category       TEXT,
                l2_category       TEXT,
                l3_category       TEXT,
                product_name      TEXT,
                variant           TEXT,
                combo             TEXT,

                -- Numeric metrics
                combo_units_sold  NUMERIC,
                base_mrp          NUMERIC,
                units_sold        NUMERIC,
                gmv               NUMERIC,
                discount_spend    NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'SWIGGY',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS swiggy_brandfund_dedup_idx
                ON public.swiggy_brandfund (date, store_id, item_code, combo_item_code);

            CREATE INDEX IF NOT EXISTS swiggy_brandfund_store_id_idx
                ON public.swiggy_brandfund (store_id);

            CREATE INDEX IF NOT EXISTS swiggy_brandfund_item_code_idx
                ON public.swiggy_brandfund (item_code);

            CREATE INDEX IF NOT EXISTS swiggy_brandfund_date_idx
                ON public.swiggy_brandfund (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.swiggy_brandfund;
            """,
        ),
    ]
