from django.db import migrations


FORWARD_SQL = r"""
CREATE OR REPLACE VIEW amazon_mp_master AS
WITH master_lookup AS (
    SELECT DISTINCT ON (UPPER(TRIM(format_sku_code::text)))
        format_sku_code,
        brand,
        item_head,
        category,
        sub_category,
        per_unit,
        per_unit_value::numeric AS per_unit_value
    FROM master_sheet
    WHERE NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY
        UPPER(TRIM(format_sku_code::text)),
        CASE
            WHEN REGEXP_REPLACE(LOWER(TRIM(COALESCE(format, '')::text)), '[^a-z0-9]+', '', 'g') = 'amazon'
                THEN 0
            ELSE 1
        END
),
base AS (
    SELECT
        a.invoice_date,
        a.invoice_number,
        a.transaction_type,
        a.order_id,
        a.shipment_id,
        a.shipment_date,
        a.order_date,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.quantity), '')::numeric)
            ELSE NULLIF(TRIM(a.quantity), '')::numeric
        END AS quantity,
        a.item_description,
        a.asin,
        a.sku,
        a.ship_to_city,
        a.ship_to_state,
        a.ship_to_country,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.invoice_amount), '')::numeric)
            ELSE NULLIF(TRIM(a.invoice_amount), '')::numeric
        END AS invoice_amount,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.tax_exclusive_gross), '')::numeric)
            ELSE NULLIF(TRIM(a.tax_exclusive_gross), '')::numeric
        END AS tax_exclusive_gross,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.total_tax_amount), '')::numeric)
            ELSE NULLIF(TRIM(a.total_tax_amount), '')::numeric
        END AS total_tax_amount,
        ms.brand,
        ms.item_head,
        ms.category,
        ms.sub_category,
        ms.per_unit AS per_ltr_unit,
        ms.per_unit_value AS per_liter,
        CASE
            WHEN a.shipment_date ~ '^\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}'
                THEN TO_DATE(
                    (regexp_match(a.shipment_date, '^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})'))[1] || '/' ||
                    (regexp_match(a.shipment_date, '^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})'))[2] || '/' ||
                    (regexp_match(a.shipment_date, '^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})'))[3],
                    'YYYY/MM/DD')
            WHEN a.shipment_date ~ '^\s*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}'
                THEN TO_DATE(
                    (regexp_match(a.shipment_date, '^\s*(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})'))[1] || '/' ||
                    (regexp_match(a.shipment_date, '^\s*(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})'))[2] || '/' ||
                    CASE
                        WHEN length((regexp_match(a.shipment_date, '^\s*\d{1,2}[-/]\d{1,2}[-/](\d{2,4})'))[1]) = 2
                            THEN '20' || (regexp_match(a.shipment_date, '^\s*\d{1,2}[-/]\d{1,2}[-/](\d{2,4})'))[1]
                        ELSE (regexp_match(a.shipment_date, '^\s*\d{1,2}[-/]\d{1,2}[-/](\d{2,4})'))[1]
                    END,
                    'DD/MM/YYYY')
        END AS shipment_dt
    FROM amazon_mp a
    LEFT JOIN master_lookup ms
        ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(a.asin::text))
)
SELECT
    invoice_date,
    invoice_number,
    transaction_type,
    order_id,
    shipment_id,
    shipment_date,
    order_date,
    quantity,
    item_description,
    asin,
    sku,
    ship_to_city,
    ship_to_state,
    ship_to_country,
    invoice_amount,
    tax_exclusive_gross,
    total_tax_amount,
    brand,
    item_head,
    category,
    sub_category,
    per_ltr_unit,
    per_liter,
    COALESCE(quantity * per_liter, 0) AS delivered_ltr,
    UPPER(TO_CHAR(shipment_dt, 'FMMonth')) AS shipment_month,
    EXTRACT(YEAR FROM shipment_dt)::int AS shipment_year
FROM base;
"""


REVERSE_SQL = r"""
CREATE OR REPLACE VIEW amazon_mp_master AS
WITH master_lookup AS (
    SELECT DISTINCT ON (UPPER(TRIM(format_sku_code::text)))
        format_sku_code,
        brand,
        item_head,
        category,
        sub_category,
        per_unit,
        per_unit_value::numeric AS per_unit_value
    FROM master_sheet
    WHERE NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY
        UPPER(TRIM(format_sku_code::text)),
        CASE
            WHEN REGEXP_REPLACE(LOWER(TRIM(COALESCE(format, '')::text)), '[^a-z0-9]+', '', 'g') = 'amazon'
                THEN 0
            ELSE 1
        END
),
base AS (
    SELECT
        a.invoice_date,
        a.invoice_number,
        a.transaction_type,
        a.order_id,
        a.shipment_id,
        a.shipment_date,
        a.order_date,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.quantity), '')::numeric)
            ELSE NULLIF(TRIM(a.quantity), '')::numeric
        END AS quantity,
        a.item_description,
        a.asin,
        a.sku,
        a.ship_to_city,
        a.ship_to_state,
        a.ship_to_country,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.invoice_amount), '')::numeric)
            ELSE NULLIF(TRIM(a.invoice_amount), '')::numeric
        END AS invoice_amount,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.tax_exclusive_gross), '')::numeric)
            ELSE NULLIF(TRIM(a.tax_exclusive_gross), '')::numeric
        END AS tax_exclusive_gross,
        CASE
            WHEN UPPER(TRIM(a.transaction_type)) = 'REFUND'
                THEN -ABS(NULLIF(TRIM(a.total_tax_amount), '')::numeric)
            ELSE NULLIF(TRIM(a.total_tax_amount), '')::numeric
        END AS total_tax_amount,
        ms.brand,
        ms.item_head,
        ms.category,
        ms.sub_category,
        ms.per_unit AS per_ltr_unit,
        ms.per_unit_value AS per_liter,
        CASE
            WHEN a.shipment_date ~ '^\s*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}'
                THEN TO_DATE(
                    (regexp_match(a.shipment_date, '^\s*(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})'))[1] || '/' ||
                    (regexp_match(a.shipment_date, '^\s*(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})'))[2] || '/' ||
                    CASE
                        WHEN length((regexp_match(a.shipment_date, '^\s*\d{1,2}[-/]\d{1,2}[-/](\d{2,4})'))[1]) = 2
                            THEN '20' || (regexp_match(a.shipment_date, '^\s*\d{1,2}[-/]\d{1,2}[-/](\d{2,4})'))[1]
                        ELSE (regexp_match(a.shipment_date, '^\s*\d{1,2}[-/]\d{1,2}[-/](\d{2,4})'))[1]
                    END,
                    'DD/MM/YYYY')
        END AS shipment_dt
    FROM amazon_mp a
    LEFT JOIN master_lookup ms
        ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(a.asin::text))
)
SELECT
    invoice_date,
    invoice_number,
    transaction_type,
    order_id,
    shipment_id,
    shipment_date,
    order_date,
    quantity,
    item_description,
    asin,
    sku,
    ship_to_city,
    ship_to_state,
    ship_to_country,
    invoice_amount,
    tax_exclusive_gross,
    total_tax_amount,
    brand,
    item_head,
    category,
    sub_category,
    per_ltr_unit,
    per_liter,
    COALESCE(quantity * per_liter, 0) AS delivered_ltr,
    UPPER(TO_CHAR(shipment_dt, 'FMMonth')) AS shipment_month,
    EXTRACT(YEAR FROM shipment_dt)::int AS shipment_year
FROM base;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0037_master_po_normalized_format_index"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
