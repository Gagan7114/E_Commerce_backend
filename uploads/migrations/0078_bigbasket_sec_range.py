from django.db import migrations


class Migration(migrations.Migration):
    """Range-report BigBasket Secondary table.

    Stores the BigBasket "manufacturer sales report" downloaded for a date
    RANGE (date_range like '20260701 - 20260717') — the Range option of the
    BigBasket Secondary uploader's Daily/Range toggle. The existing single-day
    uploads keep writing to bigbasketSec; range snapshots land here so the two
    granularities never mix.

    Unlike bigbasketSec, this table carries the report's business_type column
    (b2c / bbdaily), so the two channels are separate rows keyed properly
    instead of colliding. leaf_slug is part of the key because the export can
    repeat a SKU + city + business under two category spellings (title-case
    'Cold Pressed Oil' vs kebab 'cold-pressed-oil') with different quantities.
    Upsert dedup key matches the frontend bigbasket `range` mode config:
    (source_sku_id, source_city_name, business_type, leaf_slug, date_range).
    """

    dependencies = [
        ("uploads", "0077_bigbasketsec_unique_include_qty_sales"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.bigbasket_sec_range (
                id               bigserial PRIMARY KEY,
                date_range       text,
                source_city_name text,
                business_type    text,
                brand_name       text,
                top_slug         text,
                mid_slug         text,
                leaf_slug        text,
                source_sku_id    text,
                sku_description  text,
                sku_weight       text,
                total_quantity   numeric,
                total_mrp        numeric,
                total_sales      numeric,
                created_at       timestamp without time zone DEFAULT now()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS bigbasket_sec_range_sku_city_biz_leaf_range_key
                ON public.bigbasket_sec_range
                (source_sku_id, source_city_name, business_type, leaf_slug, date_range);
            CREATE INDEX IF NOT EXISTS idx_bigbasket_sec_range_range
                ON public.bigbasket_sec_range (date_range);
            CREATE INDEX IF NOT EXISTS idx_bigbasket_sec_range_sku
                ON public.bigbasket_sec_range (source_sku_id);
            """,
            reverse_sql=r"""
            DROP TABLE IF EXISTS public.bigbasket_sec_range;
            """,
        ),
    ]
