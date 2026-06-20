from django.db import migrations


# Re-creates flipkart_state_sales_master (see 0046) with `is_litre` exposed at the
# end of the column list, so the State-wise Sales map can compute litres for
# Flipkart the same way SecMaster does: item_quantity * per_unit_value, but only
# for litre SKUs (is_litre = 'Y'). CREATE OR REPLACE VIEW only permits appending
# new columns after the existing ones, which is exactly what this does.
FORWARD_SQL = r"""
CREATE OR REPLACE VIEW public.flipkart_state_sales_master AS
WITH master_lookup AS (
    SELECT DISTINCT ON (regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g'))
        regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g') AS fsn_key,
        brand,
        category,
        sub_category,
        item_head,
        per_unit_value,
        per_unit,
        item,
        is_litre
    FROM public.master_sheet
    WHERE upper(trim(format::text)) = 'FLIPKART'
      AND NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g')
)
SELECT
    f.*,
    ms.brand,
    ms.category,
    ms.sub_category,
    ms.item_head,
    ms.per_unit_value,
    ms.per_unit,
    ms.item,
    ms.is_litre
FROM public.flipkart_state_sales f
LEFT JOIN master_lookup ms
    ON ms.fsn_key = regexp_replace(upper(f.fsn), '[^A-Z0-9]+', '', 'g');
"""

# Reverse drops the column again by restoring the 0046 definition.
REVERSE_SQL = r"""
CREATE OR REPLACE VIEW public.flipkart_state_sales_master AS
WITH master_lookup AS (
    SELECT DISTINCT ON (regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g'))
        regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g') AS fsn_key,
        brand,
        category,
        sub_category,
        item_head,
        per_unit_value,
        per_unit,
        item
    FROM public.master_sheet
    WHERE upper(trim(format::text)) = 'FLIPKART'
      AND NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g')
)
SELECT
    f.*,
    ms.brand,
    ms.category,
    ms.sub_category,
    ms.item_head,
    ms.per_unit_value,
    ms.per_unit,
    ms.item
FROM public.flipkart_state_sales f
LEFT JOIN master_lookup ms
    ON ms.fsn_key = regexp_replace(upper(f.fsn), '[^A-Z0-9]+', '', 'g');
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0046_flipkart_state_sales_master_view"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
