from django.db import migrations


FORWARD_SQL = """
CREATE OR REPLACE VIEW amazon_sec_range_master_view AS
WITH master_lookup AS (
    SELECT DISTINCT ON (format_sku_code)
        format_sku_code,
        item,
        category,
        sub_category,
        item_head,
        per_unit,
        brand,
        per_unit_value
    FROM master_sheet
    WHERE format_sku_code IS NOT NULL
      AND format_sku_code <> ''
    ORDER BY format_sku_code
),
base AS (
    SELECT
        r.id AS source_id,
        r.from_date,
        r.to_date,
        r.business,
        r.asin,
        r.product_title,
        r.brand,
        COALESCE(r.ordered_revenue, 0)::numeric AS ordered_revenue,
        COALESCE(r.ordered_units, 0)::numeric AS ordered_units,
        COALESCE(r.shipped_revenue, 0)::numeric AS source_shipped_revenue,
        r.shipped_cogs,
        COALESCE(r.shipped_units, 0)::numeric AS shipped_units,
        COALESCE(r.customer_returns, 0)::numeric AS return_units,
        ms.per_unit_value::numeric AS unit_size,
        ms.item,
        ms.category,
        ms.sub_category,
        ms.item_head,
        ms.per_unit,
        ms.brand AS brand_2,
        m.margin_category,
        m.margin_pct,
        (m.margin_pct / 100.0) AS margin_rate,
        CASE
            WHEN r.business ILIKE '%jmpl%' OR r.business ILIKE '%mart%' THEN 'RK JMPL'
            WHEN r.business ILIKE '%jwpl%' OR r.business ILIKE '%wellness%' THEN 'RK JWPL'
            ELSE r.business
        END AS sales_type
    FROM amazon_sec_range r
    LEFT JOIN master_lookup ms
        ON ms.format_sku_code = r.asin
    LEFT JOIN amazon_sec_range_margins m
        ON m.asin = r.asin
),
calc AS (
    SELECT
        *,
        ordered_units * COALESCE(unit_size, 0) AS ordered_litres,
        shipped_units * COALESCE(unit_size, 0) AS shipped_litres,
        return_units * COALESCE(unit_size, 0) AS return_litres,
        (ordered_revenue / NULLIF(ordered_units, 0)) * return_units AS return_value,
        (ordered_revenue / NULLIF(ordered_units, 0)) * shipped_units AS calculated_shipped_revenue,
        ordered_units - shipped_units - return_units AS canceled_units
    FROM base
),
final_calc AS (
    SELECT
        *,
        ordered_revenue
            - COALESCE(calculated_shipped_revenue, 0)
            - COALESCE(return_value, 0) AS canceled_value,
        ordered_litres - shipped_litres - canceled_units AS canceled_litres,
        COALESCE(calculated_shipped_revenue, 0)
            - COALESCE(calculated_shipped_revenue, 0) * COALESCE(margin_rate, 0)
            AS shipped_revenue_after_margin,
        calculated_shipped_revenue / NULLIF(shipped_litres, 0) AS selling_price
    FROM calc
)
SELECT
    source_id,
    from_date,
    to_date,
    asin,
    product_title,
    brand,
    unit_size,
    ordered_revenue,
    ordered_units,
    ordered_litres,
    source_shipped_revenue,
    shipped_units,
    shipped_litres,
    return_value,
    return_units,
    return_litres,
    canceled_value,
    canceled_units,
    canceled_litres,
    sales_type,
    calculated_shipped_revenue,
    item,
    category,
    sub_category,
    margin_pct,
    shipped_revenue_after_margin,
    item_head,
    EXTRACT(YEAR FROM from_date)::int AS year,
    UPPER(TO_CHAR(from_date, 'FMMonth')) AS month,
    TO_CHAR(to_date, 'DD') || '-' || UPPER(TO_CHAR(from_date, 'FMMonth')) AS month_day,
    per_unit,
    brand_2,
    selling_price,
    shipped_revenue_after_margin / NULLIF(shipped_litres, 0) AS selling_price_after_margin,
    COALESCE(selling_price - selling_price * COALESCE(margin_rate, 0), 0) AS realise,
    business,
    shipped_cogs,
    margin_category
FROM final_calc;
"""


REVERSE_SQL = """
CREATE OR REPLACE VIEW amazon_sec_range_master_view AS
WITH master_lookup AS (
    SELECT DISTINCT ON (format_sku_code)
        format_sku_code,
        item,
        category,
        sub_category,
        item_head,
        per_unit,
        brand,
        per_unit_value
    FROM master_sheet
    WHERE format_sku_code IS NOT NULL
      AND format_sku_code <> ''
    ORDER BY format_sku_code
),
base AS (
    SELECT
        r.id AS source_id,
        r.from_date,
        r.to_date,
        r.business,
        r.asin,
        r.product_title,
        r.brand,
        COALESCE(r.ordered_revenue, 0)::numeric AS ordered_revenue,
        COALESCE(r.ordered_units, 0)::numeric AS ordered_units,
        COALESCE(r.shipped_revenue, 0)::numeric AS source_shipped_revenue,
        r.shipped_cogs,
        COALESCE(r.shipped_units, 0)::numeric AS shipped_units,
        COALESCE(r.customer_returns, 0)::numeric AS return_units,
        ms.per_unit_value::numeric AS unit_size,
        ms.item,
        ms.category,
        ms.sub_category,
        ms.item_head,
        ms.per_unit,
        ms.brand AS brand_2,
        m.margin_category,
        m.margin_pct,
        (m.margin_pct / 100.0) AS margin_rate,
        CASE
            WHEN r.business ILIKE '%jmpl%' OR r.business ILIKE '%mart%' THEN 'RK JMPL'
            WHEN r.business ILIKE '%jwpl%' OR r.business ILIKE '%wellness%' THEN 'RK JWPL'
            ELSE r.business
        END AS sales_type
    FROM amazon_sec_range r
    LEFT JOIN master_lookup ms
        ON ms.format_sku_code = r.asin
    LEFT JOIN amazon_sec_range_margins m
        ON m.asin = r.asin
),
calc AS (
    SELECT
        *,
        ordered_units * unit_size AS ordered_litres,
        shipped_units * unit_size AS shipped_litres,
        return_units * unit_size AS return_litres,
        (ordered_revenue / NULLIF(ordered_units, 0)) * return_units AS return_value,
        (ordered_revenue / NULLIF(ordered_units, 0)) * shipped_units AS calculated_shipped_revenue,
        ordered_units - shipped_units - return_units AS canceled_units
    FROM base
),
final_calc AS (
    SELECT
        *,
        ordered_revenue
            - COALESCE(calculated_shipped_revenue, 0)
            - COALESCE(return_value, 0) AS canceled_value,
        ordered_litres - shipped_litres - canceled_units AS canceled_litres,
        COALESCE(calculated_shipped_revenue, 0)
            - COALESCE(calculated_shipped_revenue, 0) * COALESCE(margin_rate, 0)
            AS shipped_revenue_after_margin,
        calculated_shipped_revenue / NULLIF(shipped_litres, 0) AS selling_price
    FROM calc
)
SELECT
    source_id,
    from_date,
    to_date,
    asin,
    product_title,
    brand,
    unit_size,
    ordered_revenue,
    ordered_units,
    ordered_litres,
    source_shipped_revenue,
    shipped_units,
    shipped_litres,
    return_value,
    return_units,
    return_litres,
    canceled_value,
    canceled_units,
    canceled_litres,
    sales_type,
    calculated_shipped_revenue,
    item,
    category,
    sub_category,
    margin_pct,
    shipped_revenue_after_margin,
    item_head,
    EXTRACT(YEAR FROM from_date)::int AS year,
    UPPER(TO_CHAR(from_date, 'FMMonth')) AS month,
    TO_CHAR(to_date, 'DD') || '-' || UPPER(TO_CHAR(from_date, 'FMMonth')) AS month_day,
    per_unit,
    brand_2,
    selling_price,
    shipped_revenue_after_margin / NULLIF(shipped_litres, 0) AS selling_price_after_margin,
    COALESCE(selling_price - selling_price * COALESCE(margin_rate, 0), 0) AS realise,
    business,
    shipped_cogs,
    margin_category
FROM final_calc;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0018_amazon_sec_range_margins_view"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
