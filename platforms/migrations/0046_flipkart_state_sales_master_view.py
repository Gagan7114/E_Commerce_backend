from django.db import migrations


# flipkart_state_sales enriched with master_sheet catalogue attributes, joined on
# the Flipkart FSN. flipkart_state_sales.fsn is stored with embedded quote chars
# (e.g. ""EDOG...""), so both sides are normalised to alphanumerics before
# matching; the join is scoped to master_sheet.format = 'FLIPKART'. See
# FLIPKART_STATE_SALES_MASTER_VIEW_PLAN.md (verified 100% FSN coverage).
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

REVERSE_SQL = "DROP VIEW IF EXISTS public.flipkart_state_sales_master;"


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0045_secmaster_state_from_city_mapping"),
        ("uploads", "0054_flipkart_state_sales"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
