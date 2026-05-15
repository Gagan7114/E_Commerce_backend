from django.db import migrations


FORWARD_SQL = """
CREATE OR REPLACE VIEW public.amazon_master_inventory AS
WITH master_lookup AS (
    SELECT DISTINCT ON (UPPER(TRIM(format_sku_code::text)))
        format_sku_code,
        brand,
        item_head,
        category_head,
        per_unit_value,
        category,
        sub_category,
        per_unit
    FROM public.master_sheet
    WHERE NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY
        UPPER(TRIM(format_sku_code::text)),
        CASE
            WHEN REGEXP_REPLACE(LOWER(TRIM(COALESCE(format, '')::text)), '[^a-z0-9]+', '', 'g') = 'amazon'
                THEN 0
            ELSE 1
        END,
        COALESCE(item_head, ''),
        COALESCE(category, ''),
        COALESCE(product_name, '')
),
closest_inventory_date AS (
    SELECT inventory_date
    FROM public.amazon_inventory
    WHERE inventory_date IS NOT NULL
    ORDER BY ABS(inventory_date - CURRENT_DATE), inventory_date DESC
    LIMIT 1
)
SELECT
    ai.id,
    ai.inventory_date,
    ai.raw_viewing_range,
    ai.uploaded_at,
    ai.business,
    ai.asin,
    ai.product_title,
    ai.brand,
    ai.sourceable_product_oos_pct,
    ai.vendor_confirmation_pct,
    ai.net_received,
    ai.net_received_units,
    ai.open_purchase_order_quantity,
    ai.receive_fill_pct,
    ai.overall_vendor_lead_time_days,
    ai.unfilled_customer_ordered_units,
    ai.aged_90_days_sellable_inventory,
    ai.aged_90_days_sellable_units,
    ai.sellable_on_hand_inventory,
    ai.sellable_on_hand_units,
    ai.unsellable_on_hand_inventory,
    ai.unsellable_on_hand_units,
    CASE
        WHEN ai.business ILIKE '%wellness%' OR ai.business ILIKE '%jwpl%' THEN 'WELLNESS'
        WHEN ai.business ILIKE '%mart%' OR ai.business ILIKE '%jmpl%' THEN 'MART'
        ELSE ai.business
    END AS format_type,
    CASE
        WHEN ai.inventory_date = cid.inventory_date THEN 'INCLUDE'
        ELSE 'EXCLUDE'
    END AS include_exclude,
    ml.brand AS brand_2,
    ml.item_head,
    ml.category_head,
    ml.per_unit_value::numeric AS per_ltr,
    COALESCE(ml.per_unit_value::numeric, 0)
        * COALESCE(ai.sellable_on_hand_units, 0)::numeric AS soh_ltr,
    ml.category,
    ml.sub_category,
    ml.per_unit,
    CASE
        WHEN ai.inventory_date IS NOT NULL THEN UPPER(TO_CHAR(ai.inventory_date, 'FMMonth'))
        ELSE NULL
    END AS month,
    EXTRACT(YEAR FROM ai.inventory_date)::int AS year
FROM public.amazon_inventory ai
LEFT JOIN closest_inventory_date cid ON TRUE
LEFT JOIN master_lookup ml
    ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(ai.asin::text));
"""


REVERSE_SQL = """
DROP VIEW IF EXISTS public.amazon_master_inventory;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0022_amazon_sec_daily_master_view"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
