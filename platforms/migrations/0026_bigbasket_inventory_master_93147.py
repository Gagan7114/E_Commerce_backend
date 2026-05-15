from django.db import migrations


FORWARD_SQL = """
INSERT INTO public.master_sheet (
    format_sku_code,
    product_name,
    item,
    format,
    sku_sap_code,
    sku_sap_name,
    category,
    sub_category,
    case_pack,
    per_unit,
    item_head,
    brand,
    uom,
    per_unit_value,
    category_head,
    is_litre,
    is_litre_oil,
    packaging_cost,
    tax_rate
)
SELECT
    '93147' AS format_sku_code,
    'Jivo Canola Oil 5 L Jar' AS product_name,
    item,
    format,
    sku_sap_code,
    sku_sap_name,
    category,
    sub_category,
    case_pack,
    per_unit,
    item_head,
    brand,
    uom,
    per_unit_value,
    category_head,
    is_litre,
    is_litre_oil,
    packaging_cost,
    tax_rate
FROM public.master_sheet
WHERE REGEXP_REPLACE(LOWER(TRIM(COALESCE(format, '')::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
  AND UPPER(TRIM(format_sku_code::text)) = '282780'
  AND NOT EXISTS (
      SELECT 1
      FROM public.master_sheet existing
      WHERE REGEXP_REPLACE(LOWER(TRIM(COALESCE(existing.format, '')::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
        AND UPPER(TRIM(existing.format_sku_code::text)) = '93147'
  )
LIMIT 1;
"""


REVERSE_SQL = """
DELETE FROM public.master_sheet
WHERE REGEXP_REPLACE(LOWER(TRIM(COALESCE(format, '')::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
  AND UPPER(TRIM(format_sku_code::text)) = '93147'
  AND item = 'CANOLA 5L'
  AND item_head = 'PREMIUM'
  AND COALESCE(per_unit_value, 0) = 5;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0025_all_platform_inventory_case_insensitive_master_lookup"),
    ]

    operations = [
        migrations.RunSQL(FORWARD_SQL, REVERSE_SQL),
    ]
