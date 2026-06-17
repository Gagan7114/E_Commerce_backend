"""Fill SecMaster.state from city_state_mapping (the base view, not the matview).

SecMaster's per-platform UNION leaves `state` NULL for every platform except Jio
Mart. This wraps the existing definition in an outer SELECT that LEFT JOINs
city_state_mapping on the normalised `location` (city) and COALESCEs the state,
so SecMaster.state is filled for the QC platforms too. Only the `state` column
changes; all other columns pass through unchanged. city_state_mapping.city_key is
unique, so the join never multiplies rows. secmaster_mv (a plain SELECT * snapshot
of SecMaster) is refreshed so it inherits the filled state.

Reverse restores the original SecMaster definition (so uploads.0052 can later drop
city_state_mapping). NOTE: any future CREATE OR REPLACE of SecMaster must keep
this outer city_state_mapping wrap, or re-apply this migration.
"""
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0044_call_center_done_date"),
        ("uploads", "0052_city_state_mapping"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r'''CREATE OR REPLACE VIEW "SecMaster" AS
SELECT u.date, u.sku_code, u.sku_name, u.item, u.quantity, u.gmv, u.mrp, u.amount, u.location, u.brand, u.format, COALESCE(NULLIF(TRIM(u.state::text), ''), csm.state) AS state, u.sap_sku_code, u.sap_sku_name, u.category, u.sub_category, u.case_pack, u.per_ltr_unit, u.ltr_sold, u.item_head, u.category_head, u.uom, u.month, u.year, u.per_ltr, u.city, u.landing_rate, u.sales_amt, u.real_date, u.basic_rate, u.sales_amt_exc
FROM (SELECT z."Date" AS date,
    z."SKU Number" AS sku_code,
    COALESCE(m.product_name, z."SKU Name") AS sku_name,
    m.item,
    z."Sales (Qty) - Units" AS quantity,
    z."Gross Merchandise Value" AS gmv,
    z."MRP" AS mrp,
    z."Gross Merchandise Value" AS amount,
    z."City" AS location,
    COALESCE(m.brand, z."Brand Name") AS brand,
    'ZEPTO'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN z."Sales (Qty) - Units"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(z."Date"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM z."Date") AS year,
    m.per_unit_value AS per_ltr,
    z."City" AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * z."Sales (Qty) - Units"::numeric, 0::numeric) AS sales_amt,
    to_char(z."Date"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * z."Sales (Qty) - Units"::numeric, 0::numeric) AS sales_amt_exc
   FROM "zeptoSec" z
     LEFT JOIN LATERAL ( SELECT ms.format_sku_code,
            ms.product_name,
            ms.item,
            ms.format,
            ms.sku_sap_code,
            ms.sku_sap_name,
            ms.category,
            ms.sub_category,
            ms.case_pack,
            ms.per_unit,
            ms.item_head,
            ms.brand,
            ms.uom,
            ms.per_unit_value,
            ms.category_head,
            ms.is_litre,
            ms.is_litre_oil,
            ms.packaging_cost,
            ms.tax_rate
           FROM master_sheet ms
          WHERE upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number")) AND regexp_replace(lower(TRIM(BOTH FROM ms.format)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'zepto'::text
          ORDER BY ms.product_name, ms.item, ms.per_unit
         LIMIT 1) m ON true
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'zepto'::text AND rate.month::text = to_char(date_trunc('month'::text, z."Date"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT b.date,
    b.item_id::text AS sku_code,
    COALESCE(m.product_name, b.item_name) AS sku_name,
    m.item,
    b.qty_sold AS quantity,
    b.mrp * b.qty_sold AS gmv,
    b.mrp,
    b.mrp AS amount,
    b.city_name AS location,
    m.brand,
    'BLINKIT'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN b.qty_sold::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(b.date::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM b.date) AS year,
    m.per_unit_value AS per_ltr,
    b.city_name AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * b.qty_sold::numeric, 0::numeric) AS sales_amt,
    to_char(b.date::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * b.qty_sold::numeric, 0::numeric) AS sales_amt_exc
   FROM "blinkitSec" b
     LEFT JOIN master_sheet m ON m.format_sku_code::text = b.item_id::text
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM b.item_id::text)) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'blinkit'::text AND rate.month::text = to_char(date_trunc('month'::text, b.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT s."ORDERED_DATE" AS date,
    s."ITEM_CODE" AS sku_code,
    COALESCE(m.product_name, s."PRODUCT_NAME") AS sku_name,
    m.item,
    COALESCE(s."COMBO_UNITS_SOLD", 0) + s."UNITS_SOLD" AS quantity,
    s."GMV" AS gmv,
    s."BASE_MRP" AS mrp,
    s."GMV" AS amount,
    s."CITY" AS location,
    COALESCE(m.brand, s."BRAND") AS brand,
    'SWIGGY'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN s."UNITS_SOLD"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(s."ORDERED_DATE"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM s."ORDERED_DATE") AS year,
    m.per_unit_value AS per_ltr,
    s."CITY" AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * (COALESCE(s."COMBO_UNITS_SOLD", 0) + s."UNITS_SOLD")::numeric, 0::numeric) AS sales_amt,
    to_char(s."ORDERED_DATE"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * (COALESCE(s."COMBO_UNITS_SOLD", 0) + s."UNITS_SOLD")::numeric, 0::numeric) AS sales_amt_exc
   FROM "swiggySec" s
     LEFT JOIN master_sheet m ON m.format_sku_code::text = s."ITEM_CODE"
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM s."ITEM_CODE")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'swiggy'::text AND rate.month::text = to_char(date_trunc('month'::text, s."ORDERED_DATE"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT bb.date_range AS date,
    bb.source_sku_id AS sku_code,
    COALESCE(m.product_name, bb.sku_description) AS sku_name,
    m.item,
    bb.total_quantity AS quantity,
    bb.total_sales AS gmv,
    bb.total_mrp AS mrp,
    bb.total_sales AS amount,
    bb.source_city_name AS location,
    COALESCE(m.brand, bb.brand_name) AS brand,
    'BIG BASKET'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN bb.total_quantity::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(bb.date_range::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM bb.date_range) AS year,
    m.per_unit_value AS per_ltr,
    bb.source_city_name AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * bb.total_quantity::numeric, 0::numeric) AS sales_amt,
    to_char(bb.date_range::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * bb.total_quantity::numeric, 0::numeric) AS sales_amt_exc
   FROM "bigbasketSec" bb
     LEFT JOIN master_sheet m ON m.format_sku_code::text = bb.source_sku_id
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM bb.source_sku_id)) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'bigbasket'::text AND rate.month::text = to_char(date_trunc('month'::text, bb.date_range::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT jm."ORDER_DATE" AS date,
    jm."SKU" AS sku_code,
    COALESCE(m.product_name, jm."PRODUCT_TITLE") AS sku_name,
    m.item,
    jm."ITEM_QUANTITY" AS quantity,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN jm."FINAL_INVOICE_AMOUNT"
            ELSE NULL::numeric
        END AS gmv,
    NULL::real AS mrp,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN jm."TAXABLE_VALUE"
            ELSE NULL::numeric
        END AS amount,
    jm."DELIVERY_STATE" AS location,
    m.brand,
    'JIO MART'::text AS format,
    jm."DELIVERY_STATE" AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text AND jm."EVENT_TYPE" = 'sale'::text THEN jm."ITEM_QUANTITY"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(jm."ORDER_DATE"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM jm."ORDER_DATE") AS year,
    m.per_unit_value AS per_ltr,
    jm."DELIVERY_PINCODE" AS city,
    mlr.landing_rate,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN COALESCE(mlr.landing_rate * jm."ITEM_QUANTITY"::numeric, 0::numeric)
            ELSE 0::numeric
        END AS sales_amt,
    to_char(jm."ORDER_DATE"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN COALESCE(mlr.basic_rate * jm."ITEM_QUANTITY"::numeric, 0::numeric)
            ELSE 0::numeric
        END AS sales_amt_exc
   FROM "jiomartSec" jm
     LEFT JOIN master_sheet m ON m.format_sku_code::text = jm."FSN_PRODUCT_ID"
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM jm."SKU")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'jiomart'::text AND rate.month::text = to_char(date_trunc('month'::text, jm."ORDER_DATE"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT fk."Order Date" AS date,
    fk."Product Id" AS sku_code,
    COALESCE(m.product_name, fk."SKU ID") AS sku_name,
    m.item,
    fk."Final Sale Units" AS quantity,
    fk."GMV" AS gmv,
    NULL::real AS mrp,
    fk."Final Sale Amount" AS amount,
    fk."Location Id" AS location,
    COALESCE(m.brand, fk."Brand") AS brand,
    'FLIPKART'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN fk."Final Sale Units"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(fk."Order Date"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM fk."Order Date") AS year,
    m.per_unit_value AS per_ltr,
    fk."Location Id" AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * fk."Final Sale Units"::numeric, 0::numeric) AS sales_amt,
    to_char(fk."Order Date"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * fk."Final Sale Units"::numeric, 0::numeric) AS sales_amt_exc
   FROM "flipkartSec" fk
     LEFT JOIN master_sheet m ON m.format = 'FLIPKART'::text AND m.format_sku_code::text = fk."Product Id"
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM fk."Product Id")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'flipkart'::text AND rate.month::text = to_char(date_trunc('month'::text, fk."Order Date"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
) u
LEFT JOIN public.city_state_mapping csm
    ON csm.city_key = btrim(regexp_replace(upper(u.location::text), '[^A-Z0-9]+', ' ', 'g'));

REFRESH MATERIALIZED VIEW public.secmaster_mv;''',
            reverse_sql=r'''CREATE OR REPLACE VIEW "SecMaster" ASSELECT z."Date" AS date,
    z."SKU Number" AS sku_code,
    COALESCE(m.product_name, z."SKU Name") AS sku_name,
    m.item,
    z."Sales (Qty) - Units" AS quantity,
    z."Gross Merchandise Value" AS gmv,
    z."MRP" AS mrp,
    z."Gross Merchandise Value" AS amount,
    z."City" AS location,
    COALESCE(m.brand, z."Brand Name") AS brand,
    'ZEPTO'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN z."Sales (Qty) - Units"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(z."Date"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM z."Date") AS year,
    m.per_unit_value AS per_ltr,
    z."City" AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * z."Sales (Qty) - Units"::numeric, 0::numeric) AS sales_amt,
    to_char(z."Date"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * z."Sales (Qty) - Units"::numeric, 0::numeric) AS sales_amt_exc
   FROM "zeptoSec" z
     LEFT JOIN LATERAL ( SELECT ms.format_sku_code,
            ms.product_name,
            ms.item,
            ms.format,
            ms.sku_sap_code,
            ms.sku_sap_name,
            ms.category,
            ms.sub_category,
            ms.case_pack,
            ms.per_unit,
            ms.item_head,
            ms.brand,
            ms.uom,
            ms.per_unit_value,
            ms.category_head,
            ms.is_litre,
            ms.is_litre_oil,
            ms.packaging_cost,
            ms.tax_rate
           FROM master_sheet ms
          WHERE upper(TRIM(BOTH FROM ms.format_sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number")) AND regexp_replace(lower(TRIM(BOTH FROM ms.format)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'zepto'::text
          ORDER BY ms.product_name, ms.item, ms.per_unit
         LIMIT 1) m ON true
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM z."SKU Number")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'zepto'::text AND rate.month::text = to_char(date_trunc('month'::text, z."Date"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT b.date,
    b.item_id::text AS sku_code,
    COALESCE(m.product_name, b.item_name) AS sku_name,
    m.item,
    b.qty_sold AS quantity,
    b.mrp * b.qty_sold AS gmv,
    b.mrp,
    b.mrp AS amount,
    b.city_name AS location,
    m.brand,
    'BLINKIT'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN b.qty_sold::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(b.date::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM b.date) AS year,
    m.per_unit_value AS per_ltr,
    b.city_name AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * b.qty_sold::numeric, 0::numeric) AS sales_amt,
    to_char(b.date::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * b.qty_sold::numeric, 0::numeric) AS sales_amt_exc
   FROM "blinkitSec" b
     LEFT JOIN master_sheet m ON m.format_sku_code::text = b.item_id::text
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM b.item_id::text)) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'blinkit'::text AND rate.month::text = to_char(date_trunc('month'::text, b.date::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT s."ORDERED_DATE" AS date,
    s."ITEM_CODE" AS sku_code,
    COALESCE(m.product_name, s."PRODUCT_NAME") AS sku_name,
    m.item,
    COALESCE(s."COMBO_UNITS_SOLD", 0) + s."UNITS_SOLD" AS quantity,
    s."GMV" AS gmv,
    s."BASE_MRP" AS mrp,
    s."GMV" AS amount,
    s."CITY" AS location,
    COALESCE(m.brand, s."BRAND") AS brand,
    'SWIGGY'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN s."UNITS_SOLD"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(s."ORDERED_DATE"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM s."ORDERED_DATE") AS year,
    m.per_unit_value AS per_ltr,
    s."CITY" AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * (COALESCE(s."COMBO_UNITS_SOLD", 0) + s."UNITS_SOLD")::numeric, 0::numeric) AS sales_amt,
    to_char(s."ORDERED_DATE"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * (COALESCE(s."COMBO_UNITS_SOLD", 0) + s."UNITS_SOLD")::numeric, 0::numeric) AS sales_amt_exc
   FROM "swiggySec" s
     LEFT JOIN master_sheet m ON m.format_sku_code::text = s."ITEM_CODE"
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM s."ITEM_CODE")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'swiggy'::text AND rate.month::text = to_char(date_trunc('month'::text, s."ORDERED_DATE"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT bb.date_range AS date,
    bb.source_sku_id AS sku_code,
    COALESCE(m.product_name, bb.sku_description) AS sku_name,
    m.item,
    bb.total_quantity AS quantity,
    bb.total_sales AS gmv,
    bb.total_mrp AS mrp,
    bb.total_sales AS amount,
    bb.source_city_name AS location,
    COALESCE(m.brand, bb.brand_name) AS brand,
    'BIG BASKET'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN bb.total_quantity::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(bb.date_range::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM bb.date_range) AS year,
    m.per_unit_value AS per_ltr,
    bb.source_city_name AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * bb.total_quantity::numeric, 0::numeric) AS sales_amt,
    to_char(bb.date_range::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * bb.total_quantity::numeric, 0::numeric) AS sales_amt_exc
   FROM "bigbasketSec" bb
     LEFT JOIN master_sheet m ON m.format_sku_code::text = bb.source_sku_id
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM bb.source_sku_id)) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'bigbasket'::text AND rate.month::text = to_char(date_trunc('month'::text, bb.date_range::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT jm."ORDER_DATE" AS date,
    jm."SKU" AS sku_code,
    COALESCE(m.product_name, jm."PRODUCT_TITLE") AS sku_name,
    m.item,
    jm."ITEM_QUANTITY" AS quantity,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN jm."FINAL_INVOICE_AMOUNT"
            ELSE NULL::numeric
        END AS gmv,
    NULL::real AS mrp,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN jm."TAXABLE_VALUE"
            ELSE NULL::numeric
        END AS amount,
    jm."DELIVERY_STATE" AS location,
    m.brand,
    'JIO MART'::text AS format,
    jm."DELIVERY_STATE" AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text AND jm."EVENT_TYPE" = 'sale'::text THEN jm."ITEM_QUANTITY"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(jm."ORDER_DATE"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM jm."ORDER_DATE") AS year,
    m.per_unit_value AS per_ltr,
    jm."DELIVERY_PINCODE" AS city,
    mlr.landing_rate,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN COALESCE(mlr.landing_rate * jm."ITEM_QUANTITY"::numeric, 0::numeric)
            ELSE 0::numeric
        END AS sales_amt,
    to_char(jm."ORDER_DATE"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
        CASE
            WHEN jm."EVENT_TYPE" = 'sale'::text THEN COALESCE(mlr.basic_rate * jm."ITEM_QUANTITY"::numeric, 0::numeric)
            ELSE 0::numeric
        END AS sales_amt_exc
   FROM "jiomartSec" jm
     LEFT JOIN master_sheet m ON m.format_sku_code::text = jm."FSN_PRODUCT_ID"
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM jm."SKU")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'jiomart'::text AND rate.month::text = to_char(date_trunc('month'::text, jm."ORDER_DATE"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true
UNION ALL
 SELECT fk."Order Date" AS date,
    fk."Product Id" AS sku_code,
    COALESCE(m.product_name, fk."SKU ID") AS sku_name,
    m.item,
    fk."Final Sale Units" AS quantity,
    fk."GMV" AS gmv,
    NULL::real AS mrp,
    fk."Final Sale Amount" AS amount,
    fk."Location Id" AS location,
    COALESCE(m.brand, fk."Brand") AS brand,
    'FLIPKART'::text AS format,
    NULL::text AS state,
    m.sku_sap_code AS sap_sku_code,
    m.sku_sap_name AS sap_sku_name,
    m.category,
    m.sub_category,
    m.case_pack,
    m.per_unit AS per_ltr_unit,
        CASE
            WHEN m.is_litre = 'Y'::text THEN fk."Final Sale Units"::double precision * m.per_unit_value
            ELSE NULL::double precision
        END AS ltr_sold,
    m.item_head,
    m.category_head,
    m.uom,
    TRIM(BOTH FROM to_char(fk."Order Date"::timestamp with time zone, 'MONTH'::text)) AS month,
    EXTRACT(year FROM fk."Order Date") AS year,
    m.per_unit_value AS per_ltr,
    fk."Location Id" AS city,
    mlr.landing_rate,
    COALESCE(mlr.landing_rate * fk."Final Sale Units"::numeric, 0::numeric) AS sales_amt,
    to_char(fk."Order Date"::timestamp with time zone, 'DD-MM-YYYY'::text) AS real_date,
    mlr.basic_rate,
    COALESCE(mlr.basic_rate * fk."Final Sale Units"::numeric, 0::numeric) AS sales_amt_exc
   FROM "flipkartSec" fk
     LEFT JOIN master_sheet m ON m.format = 'FLIPKART'::text AND m.format_sku_code::text = fk."Product Id"
     LEFT JOIN LATERAL ( SELECT rate.landing_rate,
            rate.basic_rate
           FROM monthly_landing_rate rate
          WHERE upper(TRIM(BOTH FROM rate.sku_code::text)) = upper(TRIM(BOTH FROM fk."Product Id")) AND regexp_replace(lower(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+'::text, ''::text, 'g'::text) = 'flipkart'::text AND rate.month::text = to_char(date_trunc('month'::text, fk."Order Date"::timestamp without time zone), 'YYYY-MM-DD'::text)
          ORDER BY rate.created_at DESC
         LIMIT 1) mlr ON true;

REFRESH MATERIALIZED VIEW public.secmaster_mv;''',
        ),
    ]
