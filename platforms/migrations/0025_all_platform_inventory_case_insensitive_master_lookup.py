from django.db import migrations


FORWARD_SQL = """
CREATE OR REPLACE VIEW public.all_platform_inventory AS
WITH master_lookup AS (
    SELECT
        format,
        format_sku_code,
        item,
        item_head,
        brand,
        per_unit_value,
        format_key,
        sku_key
    FROM (
        SELECT
            ms.format,
            ms.format_sku_code,
            ms.item,
            ms.item_head,
            ms.brand,
            ms.per_unit_value,
            REGEXP_REPLACE(LOWER(TRIM(COALESCE(ms.format, '')::text)), '[^a-z0-9]+', '', 'g') AS format_key,
            UPPER(TRIM(ms.format_sku_code::text)) AS sku_key,
            ROW_NUMBER() OVER (
                PARTITION BY
                    REGEXP_REPLACE(LOWER(TRIM(COALESCE(ms.format, '')::text)), '[^a-z0-9]+', '', 'g'),
                    UPPER(TRIM(ms.format_sku_code::text))
                ORDER BY
                    CASE WHEN NULLIF(TRIM(COALESCE(ms.item, '')::text), '') IS NULL THEN 1 ELSE 0 END,
                    CASE WHEN ms.per_unit_value IS NULL THEN 1 ELSE 0 END,
                    COALESCE(ms.product_name, ''),
                    ms.ctid
            ) AS rn
        FROM public.master_sheet ms
        WHERE NULLIF(TRIM(ms.format_sku_code::text), '') IS NOT NULL
    ) ranked
    WHERE rn = 1
)
SELECT
    bi.inventory_date,
    bi.item_id AS sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    bi.total_inv_qty AS soh_unit,
    bi.total_inv_qty::double precision * ms.per_unit_value AS soh_ltr,
    bi.backend_facility_name AS location,
    'BLINKIT'::text AS format
FROM public.blinkit_inventory bi
JOIN master_lookup ms
  ON UPPER(TRIM(bi.item_id::text)) = ms.sku_key
 AND ms.format_key = 'blinkit'
UNION ALL
SELECT
    zi.inventory_date,
    zi.sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    zi.units AS soh_unit,
    zi.units::double precision * ms.per_unit_value AS soh_ltr,
    zi.city AS location,
    'ZEPTO'::text AS format
FROM public.zepto_inventory zi
JOIN master_lookup ms
  ON UPPER(TRIM(zi.sku_code::text)) = ms.sku_key
 AND ms.format_key = 'zepto'
UNION ALL
SELECT
    sw.inventory_date,
    sw.sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    sw.warehouse_qty_available AS soh_unit,
    sw.warehouse_qty_available::double precision * ms.per_unit_value AS soh_ltr,
    sw.city AS location,
    'SWIGGY'::text AS format
FROM public.swiggy_inventory sw
JOIN master_lookup ms
  ON UPPER(TRIM(sw.sku_code::text)) = ms.sku_key
 AND ms.format_key = 'swiggy'
UNION ALL
SELECT
    bb.inventory_date,
    bb.sku_id AS sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    bb.soh AS soh_unit,
    bb.soh::double precision * ms.per_unit_value AS soh_ltr,
    bb.city AS location,
    'BIG BASKET'::text AS format
FROM public.bigbasket_inventory bb
JOIN master_lookup ms
  ON UPPER(TRIM(bb.sku_id::text)) = ms.sku_key
 AND ms.format_key = 'bigbasket'
UNION ALL
SELECT
    jio.inventory_date,
    jio.sku_id AS sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    jio.total_sellable_inv AS soh_unit,
    jio.total_sellable_inv::double precision * ms.per_unit_value AS soh_ltr,
    jio.rfc_name AS location,
    'JIO MART'::text AS format
FROM public.jiomart_inventory jio
JOIN master_lookup ms
  ON UPPER(TRIM(jio.sku_id::text)) = ms.sku_key
 AND ms.format_key = 'jiomart';
"""


REVERSE_SQL = """
CREATE OR REPLACE VIEW public.all_platform_inventory AS
SELECT
    bi.inventory_date,
    bi.item_id AS sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    bi.total_inv_qty AS soh_unit,
    bi.total_inv_qty::double precision * ms.per_unit_value AS soh_ltr,
    bi.backend_facility_name AS location,
    'BLINKIT'::text AS format
FROM public.blinkit_inventory bi
JOIN public.master_sheet ms ON ms.format_sku_code::text = bi.item_id
UNION ALL
SELECT
    zi.inventory_date,
    zi.sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    zi.units AS soh_unit,
    zi.units::double precision * ms.per_unit_value AS soh_ltr,
    zi.city AS location,
    'ZEPTO'::text AS format
FROM public.zepto_inventory zi
JOIN public.master_sheet ms ON zi.sku_code = ms.format_sku_code::text
UNION ALL
SELECT
    sw.inventory_date,
    sw.sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    sw.warehouse_qty_available AS soh_unit,
    sw.warehouse_qty_available::double precision * ms.per_unit_value AS soh_ltr,
    sw.city AS location,
    'SWIGGY'::text AS format
FROM public.swiggy_inventory sw
JOIN public.master_sheet ms ON ms.format_sku_code::text = sw.sku_code
UNION ALL
SELECT
    bb.inventory_date,
    bb.sku_id AS sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    bb.soh AS soh_unit,
    bb.soh::double precision * ms.per_unit_value AS soh_ltr,
    bb.city AS location,
    'BIG BASKET'::text AS format
FROM public.bigbasket_inventory bb
JOIN public.master_sheet ms ON ms.format_sku_code::text = bb.sku_id
UNION ALL
SELECT
    jio.inventory_date,
    jio.sku_id AS sku_code,
    ms.item,
    ms.item_head,
    ms.brand,
    jio.total_sellable_inv AS soh_unit,
    jio.total_sellable_inv::double precision * ms.per_unit_value AS soh_ltr,
    jio.rfc_name AS location,
    'JIO MART'::text AS format
FROM public.jiomart_inventory jio
JOIN public.master_sheet ms ON ms.format_sku_code::text = jio.sku_id;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0024_amazon_master_inventory_sheet_shape"),
    ]

    operations = [
        migrations.RunSQL(FORWARD_SQL, REVERSE_SQL),
    ]
