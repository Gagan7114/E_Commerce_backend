"""
Materialize the PRIMARY SUMMARY's derived/normalized layer.

The Primary Summary dashboard aggregates every primary platform. Computing that
live meant running the heavy pack-size regex + item-head/per-litre derivation
over all master_po rows on every request (~5s). This matview precomputes that
derived layer ONCE (at refresh time) so the summary endpoint only has to do fast
GROUP BYs over it (~100ms).

Built on top of `master_po_mv` (the raw snapshot) and refreshed right after it
via platforms.master_po_refresh.refresh_primary_summary_mv(), which is wired
into the same upload/refresh hook. Mirrors the exact derivation in
platforms.views._primary_summary_cte so the numbers are identical.
"""

from django.db import migrations


_FORWARD = r"""
DROP MATERIALIZED VIEW IF EXISTS public.primary_summary_mv;

CREATE MATERIALIZED VIEW public.primary_summary_mv AS
WITH base AS (
    SELECT
        p.*,
        CASE WHEN TRIM(p.po_date::text) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(TRIM(p.po_date::text),'DD-MM-YYYY')
             WHEN TRIM(p.po_date::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.po_date::text)::date ELSE NULL END AS po_dt,
        CASE WHEN TRIM(p.delivery_date::text) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(TRIM(p.delivery_date::text),'DD-MM-YYYY')
             WHEN TRIM(p.delivery_date::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.delivery_date::text)::date ELSE NULL END AS delivery_dt,
        REGEXP_REPLACE(LOWER(TRIM(p.format::text)), '[^a-z0-9]+', '', 'g') AS format_key
    FROM public.master_po_mv p
),
with_pack_text AS (
    SELECT *, UPPER(CONCAT_WS(' ', item::text, sap_sku_name::text, sku_name::text, unit_of_measure::text)) AS pack_text
    FROM base
),
with_pack_matches AS (
    SELECT *,
        regexp_match(pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)') AS combo_full_match,
        regexp_match(pack_text, '([0-9]+(?:\.[0-9]+)?)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)') AS combo_compact_match,
        regexp_match(pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:ML|MLS|M)(?:[^A-Z0-9]|$)') AS ml_match,
        regexp_match(pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER)(?:[^A-Z0-9]|$)') AS ltr_match,
        regexp_match(pack_text, '([0-9]+(?:\.[0-9]+)?)\s*L(?:[^A-Z0-9]|$)') AS l_match
    FROM with_pack_text
),
metric_base AS (
    SELECT *, COALESCE(
        CASE WHEN combo_full_match IS NOT NULL THEN combo_full_match[1]::numeric + combo_full_match[2]::numeric
             WHEN combo_compact_match IS NOT NULL THEN combo_compact_match[1]::numeric + combo_compact_match[2]::numeric
             WHEN ml_match IS NOT NULL THEN ml_match[1]::numeric/1000
             WHEN ltr_match IS NOT NULL THEN ltr_match[1]::numeric
             WHEN l_match IS NOT NULL THEN l_match[1]::numeric ELSE NULL END,
        NULLIF(per_liter,0), 1) AS effective_per_liter
    FROM with_pack_matches
),
normalized AS (
    SELECT *,
        COALESCE(NULLIF(UPPER(TRIM(po_status::text)),''),'OTHER') AS status_key,
        CASE WHEN UPPER(TRIM(item_head::text))='PREMIUM' THEN 'PREMIUM' WHEN UPPER(TRIM(item_head::text))='COMMODITY' THEN 'COMMODITY' ELSE 'OTHER' END AS item_head_key,
        COALESCE(NULLIF(UPPER(TRIM(item::text)),''),NULLIF(UPPER(TRIM(sku_name::text)),''),'OTHER') AS item_key,
        COALESCE(NULLIF(UPPER(TRIM(category::text)),''),'OTHER') AS category_key,
        COALESCE(NULLIF(UPPER(TRIM(sub_category::text)),''),'OTHER') AS sub_category_key,
        COALESCE(NULLIF(UPPER(TRIM(open_close::text)),''),'CLOSED') AS open_close_key,
        COALESCE(NULLIF(UPPER(TRIM(po_month::text)),''),UPPER(TRIM(TO_CHAR(po_dt,'FMMONTH')))) AS po_month_key,
        COALESCE(NULLIF(UPPER(TRIM(delivery_month::text)),''),UPPER(TRIM(TO_CHAR(delivery_dt,'FMMONTH')))) AS delivery_month_key,
        EXTRACT(YEAR FROM delivery_dt)::integer AS delivery_year,
        CASE WHEN effective_per_liter IS NULL THEN UPPER(TRIM(unit_of_measure::text))
             WHEN effective_per_liter<1 THEN UPPER(TRIM(TO_CHAR(effective_per_liter*1000,'FM999999990.###')))||' MLS'
             ELSE UPPER(TRIM(TO_CHAR(effective_per_liter,'FM999999990.###')))||' LTR' END AS per_ltr_key,
        COALESCE(total_order_liters,0) AS metric_order_liters,
        COALESCE(total_delivered_liters,0) AS metric_delivered_liters,
        COALESCE(total_order_amt_inclusive,0) AS metric_order_value,
        COALESCE(total_deliver_amt_inclusive,0) AS metric_delivered_value,
        COALESCE(order_qty,0) AS metric_order_qty,
        COALESCE(delivered_qty,0) AS metric_delivered_qty,
        COALESCE(missed_ltrs,0) AS metric_pending_liters,
        COALESCE(missed_qty,0) AS metric_pending_qty,
        COALESCE(COALESCE(missed_qty,0)*CASE WHEN NULLIF(TRIM(basic_rate::text),'') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN NULLIF(TRIM(basic_rate::text),'')::numeric ELSE 0 END,0) AS metric_pending_value
    FROM metric_base
)
SELECT
    format_key, item_head_key, item_key, category_key, sub_category_key, per_ltr_key,
    status_key, open_close_key, po_month_key, delivery_month_key, delivery_year,
    po_year, po_dt, delivery_dt, vendor_new, vendor_name, lead_time,
    metric_order_liters, metric_delivered_liters, metric_order_value, metric_delivered_value,
    metric_order_qty, metric_delivered_qty, metric_pending_liters, metric_pending_qty, metric_pending_value,
    0::numeric AS metric_projection_value, 0::numeric AS metric_projection_ltrs, 0::numeric AS metric_projection_qty
FROM normalized;

CREATE INDEX IF NOT EXISTS primary_summary_mv_del_idx ON public.primary_summary_mv (delivery_month_key, delivery_year);
CREATE INDEX IF NOT EXISTS primary_summary_mv_po_idx ON public.primary_summary_mv (po_month_key, po_year);
CREATE INDEX IF NOT EXISTS primary_summary_mv_fmt_idx ON public.primary_summary_mv (format_key);
"""

_REVERSE = "DROP MATERIALIZED VIEW IF EXISTS public.primary_summary_mv;"


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0055_daily_ads_master_matviews"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
