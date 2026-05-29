from django.db import migrations


# amazon_mp_master: enriches the raw amazon_mp (GST MTR) table with master-sheet
# attributes (joined on ASIN = format_sku_code, AMAZON format preferred) plus
# derived columns. Mirrors the amazon_sec_daily_master_view conventions.
#
# Business rules (confirmed):
#   - Refund rows: Quantity AND all monetary columns (Invoice Amount, Tax
#     Exclusive Gross, Total Tax Amount) are sign-flipped negative. Cancel /
#     FreeReplacement / Shipment stay as-is (positive).
#   - Delivered Ltr = signed_quantity * per_liter, COALESCEd to 0 when NULL
#     (non-litre SKUs / unmatched ASINs).
#   - Shipment Month = month NAME (e.g. MAY); Shipment Year = 4-digit year.
#     Both NULL when shipment_date is empty (e.g. Cancel rows).
#   - shipment_date is TEXT and arrives in two formats depending on load path
#     (file upload "DD/MM/YY HH:MM" vs Excel paste "DD-MM-YYYY HH:MM"); parsed
#     defensively with a regex accepting -/ separators and 2- or 4-digit years.
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

REVERSE_SQL = """
DROP VIEW IF EXISTS amazon_mp_master;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0034_amazon_mp_table"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
