from django.db import migrations


class Migration(migrations.Migration):
    """SecMaster-style enrichment view over bigbasket_sec_range.

    Mirrors the "BIG BASKET" branch of the SecMaster view (master_sheet join
    for item/item_head/litres + carry-forward monthly_landing_rate for
    sales_amt / sales_amt_exc), but sources the RANGE upload table instead of
    the daily bigbasketSec. date_range is text 'DD-MM-YYYY - DD-MM-YYYY'
    (e.g. '01-07-2026 - 17-07-2026'); rows with any other shape are excluded.

    Each upload is a cumulative month-to-date snapshot identified by its
    to_date. Consumers must NOT sum across snapshots of the same month — the
    BigBasket SEC Dashboard reads only the freshest snapshot per month
    (to_date = MAX(to_date)). month/year are derived from from_date.

    Plain (non-materialized) view: the source table is small (a few hundred
    rows per snapshot), so live reads are cheap and no refresh wiring exists.
    """

    dependencies = [
        ("platforms", "0057_master_po_appointment_date"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE OR REPLACE VIEW public.bigbasket_sec_range_master AS
            SELECT
                d.from_date,
                d.to_date,
                d.to_date AS date,
                r.date_range,
                r.business_type,
                r.top_slug,
                r.mid_slug,
                r.leaf_slug,
                r.source_sku_id AS sku_code,
                COALESCE(m.product_name, r.sku_description) AS sku_name,
                m.item,
                r.total_quantity AS quantity,
                r.total_sales AS gmv,
                r.total_mrp AS mrp,
                r.total_sales AS amount,
                r.source_city_name AS location,
                r.source_city_name AS city,
                COALESCE(m.brand, r.brand_name) AS brand,
                'BIG BASKET'::text AS format,
                m.sku_sap_code AS sap_sku_code,
                m.sku_sap_name AS sap_sku_name,
                m.category,
                m.sub_category,
                m.case_pack,
                m.per_unit AS per_ltr_unit,
                CASE
                    WHEN m.is_litre = 'Y'::text
                        THEN r.total_quantity::double precision * m.per_unit_value
                    ELSE NULL::double precision
                END AS ltr_sold,
                m.item_head,
                m.category_head,
                m.uom,
                TRIM(BOTH FROM TO_CHAR(d.from_date::timestamp, 'MONTH')) AS month,
                EXTRACT(year FROM d.from_date) AS year,
                m.per_unit_value AS per_ltr,
                mlr.landing_rate,
                COALESCE(mlr.landing_rate * r.total_quantity::numeric, 0::numeric) AS sales_amt,
                TO_CHAR(d.to_date::timestamp, 'DD-MM-YYYY') AS real_date,
                mlr.basic_rate,
                COALESCE(mlr.basic_rate * r.total_quantity::numeric, 0::numeric) AS sales_amt_exc
            FROM public.bigbasket_sec_range r
            CROSS JOIN LATERAL (
                SELECT
                    TO_DATE(SPLIT_PART(TRIM(r.date_range), ' - ', 1), 'DD-MM-YYYY') AS from_date,
                    TO_DATE(SPLIT_PART(TRIM(r.date_range), ' - ', 2), 'DD-MM-YYYY') AS to_date
            ) d
            LEFT JOIN master_sheet m
                ON m.format_sku_code::text = r.source_sku_id
            LEFT JOIN LATERAL (
                SELECT rate.landing_rate, rate.basic_rate
                FROM monthly_landing_rate rate
                WHERE UPPER(TRIM(BOTH FROM rate.sku_code::text)) = UPPER(TRIM(BOTH FROM r.source_sku_id))
                  AND REGEXP_REPLACE(LOWER(TRIM(BOTH FROM rate.format::text)), '[^a-z0-9]+', '', 'g') = 'bigbasket'
                  AND rate.month::date <= DATE_TRUNC('month', d.from_date)::date
                ORDER BY rate.month::date DESC, rate.created_at DESC
                LIMIT 1
            ) mlr ON true
            WHERE TRIM(COALESCE(r.date_range, ''))
                  ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4} - [0-9]{2}-[0-9]{2}-[0-9]{4}$';
            """,
            reverse_sql=r"""
            DROP VIEW IF EXISTS public.bigbasket_sec_range_master;
            """,
        ),
    ]
