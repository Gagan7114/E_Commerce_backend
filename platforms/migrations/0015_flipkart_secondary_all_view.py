from django.db import migrations


FORWARD_SQL = r"""
CREATE OR REPLACE VIEW public.flipkart_secondary_all AS
SELECT
    fk.id,
    fk."Product Id",
    fk."SKU ID",
    fk."Category" AS "Platform Category",
    fk."Brand",
    fk."Vertical",
    fk."Order Date",
    fk."Fulfillment Type",
    fk."Location Id",
    fk."Gross Units",
    fk."GMV",
    fk."Cancellation Units",
    fk."Cancellation Amount",
    fk."Return Units",
    fk."Return Amount",
    fk."Final Sale Units",
    fk."Final Sale Amount",
    COALESCE(ms.item_head::text, '') AS item_head,
    COALESCE(ms.category::text, '') AS mapped_category,
    COALESCE(ms.sub_category::text, '') AS sub_category,
    ms.per_unit_value::numeric AS per_ltr,
    CASE
        WHEN ms.per_unit_value IS NULL OR fk."Gross Units" IS NULL THEN NULL
        ELSE ms.per_unit_value::numeric * fk."Gross Units"
    END AS ltr_ordered,
    CASE
        WHEN ms.per_unit_value IS NULL OR fk."Final Sale Units" IS NULL THEN NULL
        ELSE ms.per_unit_value::numeric * fk."Final Sale Units"
    END AS ltr_sold,
    CASE
        WHEN ms.per_unit_value IS NULL OR fk."Cancellation Units" IS NULL THEN NULL
        ELSE ms.per_unit_value::numeric * fk."Cancellation Units"
    END AS cancellation_ltr,
    CASE
        WHEN ms.per_unit_value IS NULL OR fk."Return Units" IS NULL THEN NULL
        ELSE ms.per_unit_value::numeric * fk."Return Units"
    END AS return_ltr,
    CASE
        WHEN fk."Order Date" IS NULL THEN NULL
        ELSE UPPER(TO_CHAR(fk."Order Date", 'FMMonth'))
    END AS month,
    CASE
        WHEN fk."Order Date" IS NULL THEN NULL
        ELSE EXTRACT(YEAR FROM fk."Order Date")::integer
    END AS year,
    COALESCE(ms.item::text, '') AS item,
    CASE
        WHEN fk."Order Date" IS NULL THEN NULL
        ELSE TO_CHAR(fk."Order Date", 'DD-MM-YYYY')
    END AS real_date,
    NULL::numeric AS bank_settlement,
    NULL::numeric AS total_settlement_value,
    ms.packaging_cost AS packaging_cost_ltr,
    CASE
        WHEN ms.packaging_cost IS NULL OR fk."Final Sale Units" IS NULL THEN NULL
        ELSE ms.packaging_cost * fk."Final Sale Units"
    END AS total_packaging_cost,
    fk.created_at
FROM public."flipkartSec" fk
LEFT JOIN LATERAL (
    SELECT
        m.item_head,
        m.category,
        m.sub_category,
        m.per_unit_value,
        m.item,
        m.packaging_cost
    FROM public.master_sheet m
    WHERE UPPER(TRIM(m.format_sku_code::text)) = UPPER(TRIM(fk."Product Id"::text))
      AND REGEXP_REPLACE(LOWER(TRIM(m.format::text)), '[^a-z0-9]+', '', 'g') = 'flipkart'
    ORDER BY m.format_sku_code
    LIMIT 1
) ms ON true;
"""


REVERSE_SQL = r"""
DROP VIEW IF EXISTS public.flipkart_secondary_all;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0014_secmaster_monthly_rate_join_all_platforms"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD_SQL, reverse_sql=REVERSE_SQL),
    ]
