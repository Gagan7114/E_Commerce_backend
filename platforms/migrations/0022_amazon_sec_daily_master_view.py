from django.db import migrations


FORWARD_SQL = """
CREATE OR REPLACE VIEW amazon_sec_daily_master_view AS
WITH master_lookup AS (
    SELECT DISTINCT ON (UPPER(TRIM(format_sku_code::text)))
        format_sku_code,
        item,
        category,
        sub_category,
        item_head,
        per_unit,
        per_unit_value,
        category_head,
        tax_rate
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
        d.report_date AS from_date,
        d.report_date AS to_date,
        d.asin,
        d.product_title,
        d.brand,
        ms.per_unit_value::numeric AS unit_size,
        COALESCE(d.ordered_revenue, 0)::numeric AS ordered_revenue,
        COALESCE(d.ordered_units, 0)::numeric AS ordered_units,
        COALESCE(d.shipped_revenue, 0)::numeric AS shipped_revenue,
        COALESCE(d.shipped_units, 0)::numeric AS shipped_units,
        COALESCE(d.customer_returns, 0)::numeric AS return_units,
        CASE
            WHEN d.business ILIKE '%jmpl%' OR d.business ILIKE '%mart%' THEN 'RK JMPL'
            WHEN d.business ILIKE '%jwpl%' OR d.business ILIKE '%wellness%' THEN 'RK JWPL'
            ELSE d.business
        END AS sales_type,
        ms.item,
        ms.item_head,
        ms.category,
        ms.sub_category,
        ms.per_unit,
        ms.category_head,
        (m.margin_pct / 100.0)::numeric AS distributor_margin,
        ms.tax_rate::numeric AS tax
    FROM amazon_sec_daily d
    LEFT JOIN master_lookup ms
        ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(d.asin::text))
    LEFT JOIN amazon_sec_range_margins m
        ON UPPER(TRIM(m.asin::text)) = UPPER(TRIM(d.asin::text))
),
calc AS (
    SELECT
        *,
        ordered_units * COALESCE(unit_size, 0) AS ordered_litres,
        shipped_units * COALESCE(unit_size, 0) AS shipped_litres,
        return_units * COALESCE(unit_size, 0) AS return_litres,
        (ordered_revenue / NULLIF(ordered_units, 0)) * return_units AS return_value,
        (ordered_revenue / NULLIF(ordered_units, 0)) * shipped_units AS shipped_revenue_2,
        ordered_units - shipped_units - return_units AS canceled_units
    FROM base
),
final_calc AS (
    SELECT
        *,
        ordered_revenue
            - COALESCE(shipped_revenue_2, 0)
            - COALESCE(return_value, 0) AS canceled_value,
        ordered_litres - shipped_litres - return_litres AS canceled_litres
    FROM calc
)
SELECT
    from_date,
    to_date,
    asin,
    product_title,
    brand,
    unit_size,
    ordered_revenue,
    ordered_units,
    ordered_litres,
    shipped_revenue,
    shipped_units,
    shipped_litres,
    return_value,
    return_units,
    return_litres,
    canceled_value,
    canceled_units,
    canceled_litres,
    sales_type,
    shipped_revenue_2,
    item,
    item_head,
    category,
    sub_category,
    CASE
        WHEN from_date IS NOT NULL THEN UPPER(TO_CHAR(from_date, 'FMMonth'))
        ELSE NULL
    END AS month,
    CASE
        WHEN from_date IS NOT NULL
            THEN UPPER(TO_CHAR(from_date, 'FMMonth')) || '-' || TO_CHAR(from_date, 'DD')
        ELSE NULL
    END AS month_num,
    EXTRACT(YEAR FROM from_date)::int AS year,
    per_unit,
    category_head,
    distributor_margin,
    tax
FROM final_calc;
"""


REVERSE_SQL = """
DROP VIEW IF EXISTS amazon_sec_daily_master_view;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0021_secmaster_zepto_master_join_dedupe"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
