"""Phase 2 - make master_po.po_status fact-aware (auto-expiry + auto-appointment).

Replaces the po_status logic in master_po_raw with the settled priority cascade
(decisions locked 2026-07-22). First match wins:

  1. any delivery on the PO (max delivered over PO > 0) -> COMPLETED
  2. mapped status = CANCELLED                          -> CANCELLED
  3. past expiry (po_expiry_date < today, nothing delivered) -> EXPIRED
  4. appointment_date is set                            -> APPOINTMENT DONE
  5. else status_mapping[raw]; unmapped -> 'NEEDS MAPPING' (never blank)

Also recomputes missed_qty / missed_ltrs from the NEW status (they were passed
through from master_po_base, which computes a separate, now-stale po_status - so
without this a newly-EXPIRED row would show the wrong missed figures). item_status
and open_close already derive from the recomputed status and are unchanged.

Impact (raw pre-dedup grain, measured 2026-07-22): 1,189 rows (2.0%) change status;
mainly PENDING->EXPIRED (auto-expiry) and *->COMPLETED (any-delivery-wins).

SAFETY
------
  * Column list is byte-identical to the prior definition, so this is a plain
    CREATE OR REPLACE VIEW (no DROP, dependents untouched), then a REFRESH of
    master_po_mv + primary_summary_mv so reads reflect it.
  * backwards() restores 0057's exact master_po_raw body and refreshes again.
"""
from django.db import migrations

NEW_RAW = r''' WITH mapped AS (
         SELECT b.po_number,
            b.po_date,
            b.po_expiry_date,
            b.delivery_date,
            b.appointment_date,
            b.vendor_name,
            b.status,
            b.sku_code,
            b.sku_name,
            b.order_qty,
            b.delivered_qty,
            b.basic_rate,
            b.landing_rate,
            b.location,
            b.format,
            b.remark,
            b.lead_time,
            b.days_to_expiry,
            b.po_window,
            b.po_status,
            b.item_status,
            b.vendor_new,
            b.item,
            b.sap_sku_name,
            b.category,
            b.sub_category,
            b.case_pack,
            b.per_liter,
            b.total_order_liters,
            b.total_delivered_liters,
            b.total_order_amt_inclusive,
            b.total_deliver_amt_inclusive,
            b.po_month,
            b.delivery_month,
            b.po_year,
            b.delivered_year,
            b.item_head,
            b.city,
            b.state,
            b.distributor_margin,
            b.realise,
            b.distributor_commission_per_unit,
            b.total_distributor_commission,
            b.brand,
            b.category_head,
            b.unit_of_measure,
            b.open_close,
            b.total_order_amt_exclusive,
            b.total_delivered_amt_exclusive,
            b.total_order_amt_without_margin,
            b.total_delivered_amt_without_margin,
            b.missed_qty,
            b.filled_qty,
            b.missed_ltrs,
            b.filled_ltrs,
                CASE
                    WHEN max(COALESCE(b.delivered_qty, 0::numeric)) OVER (PARTITION BY upper(TRIM(BOTH FROM b.format)), TRIM(BOTH FROM b.po_number)) > 0::numeric THEN 'COMPLETED'::text
                    WHEN upper(TRIM(BOTH FROM COALESCE(sm.status_new, ''::text))) = 'CANCELLED'::text THEN 'CANCELLED'::text
                    WHEN _mp_to_date(b.po_expiry_date::text) IS NOT NULL AND _mp_to_date(b.po_expiry_date::text) < CURRENT_DATE THEN 'EXPIRED'::text
                    WHEN NULLIF(TRIM(BOTH FROM b.appointment_date::text), ''::text) IS NOT NULL THEN 'APPOINTMENT DONE'::text
                    ELSE COALESCE(NULLIF(sm.status_new::text, ''::text), 'NEEDS MAPPING'::text)
                END AS calculated_po_status
           FROM master_po_base b
             LEFT JOIN status_mapping sm ON upper(TRIM(BOTH FROM b.status)) = upper(TRIM(BOTH FROM sm.status::text))
        )
 SELECT po_number,
    po_date,
    po_expiry_date,
    delivery_date,
    appointment_date,
    vendor_name,
    status,
    sku_code,
    sku_name,
    order_qty,
    delivered_qty,
    basic_rate,
    landing_rate,
    location,
    format,
    remark,
    lead_time,
    days_to_expiry,
    po_window,
    calculated_po_status AS po_status,
        CASE
            WHEN calculated_po_status = 'COMPLETED'::text AND COALESCE(delivered_qty, 0::numeric) < COALESCE(order_qty, 0::numeric) THEN 'SHORT SUPPLIED'::text
            WHEN calculated_po_status <> 'COMPLETED'::text THEN calculated_po_status
            ELSE 'FULL SUPPLIED'::text
        END AS item_status,
    vendor_new,
    item,
    sap_sku_name,
    category,
    sub_category,
    case_pack,
    per_liter,
    total_order_liters,
    total_delivered_liters,
    total_order_amt_inclusive,
    total_deliver_amt_inclusive,
    po_month,
    delivery_month,
    po_year,
    delivered_year,
    item_head,
    city,
    state,
    distributor_margin,
    realise,
    distributor_commission_per_unit,
    total_distributor_commission,
    brand,
    category_head,
    unit_of_measure,
        CASE
            WHEN NULLIF(TRIM(BOTH FROM po_number), ''::text) IS NULL THEN ''::text
            WHEN calculated_po_status = ANY (ARRAY['APPOINTMENT DONE'::text, 'PENDING'::text]) THEN 'OPEN'::text
            ELSE 'CLOSED'::text
        END AS open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
        CASE
            WHEN calculated_po_status = ANY (ARRAY['PENDING'::text, 'CANCELLED'::text, 'APPOINTMENT DONE'::text, 'NEEDS MAPPING'::text]) THEN 0::numeric
            WHEN calculated_po_status = ANY (ARRAY['COMPLETED'::text, 'EXPIRED'::text]) THEN COALESCE(order_qty, 0::numeric) - COALESCE(delivered_qty, 0::numeric)
            ELSE NULL::numeric
        END AS missed_qty,
    filled_qty,
        CASE
            WHEN calculated_po_status = ANY (ARRAY['PENDING'::text, 'CANCELLED'::text, 'APPOINTMENT DONE'::text, 'NEEDS MAPPING'::text]) THEN 0::numeric
            WHEN calculated_po_status = ANY (ARRAY['COMPLETED'::text, 'EXPIRED'::text]) THEN COALESCE(total_order_liters, 0::numeric) - COALESCE(total_delivered_liters, 0::numeric)
            ELSE NULL::numeric
        END AS missed_ltrs,
    filled_ltrs
   FROM mapped'''

OLD_RAW = r''' WITH mapped AS (
         SELECT b.po_number,
            b.po_date,
            b.po_expiry_date,
            b.delivery_date,
            b.appointment_date,
            b.vendor_name,
            b.status,
            b.sku_code,
            b.sku_name,
            b.order_qty,
            b.delivered_qty,
            b.basic_rate,
            b.landing_rate,
            b.location,
            b.format,
            b.remark,
            b.lead_time,
            b.days_to_expiry,
            b.po_window,
            b.po_status,
            b.item_status,
            b.vendor_new,
            b.item,
            b.sap_sku_name,
            b.category,
            b.sub_category,
            b.case_pack,
            b.per_liter,
            b.total_order_liters,
            b.total_delivered_liters,
            b.total_order_amt_inclusive,
            b.total_deliver_amt_inclusive,
            b.po_month,
            b.delivery_month,
            b.po_year,
            b.delivered_year,
            b.item_head,
            b.city,
            b.state,
            b.distributor_margin,
            b.realise,
            b.distributor_commission_per_unit,
            b.total_distributor_commission,
            b.brand,
            b.category_head,
            b.unit_of_measure,
            b.open_close,
            b.total_order_amt_exclusive,
            b.total_delivered_amt_exclusive,
            b.total_order_amt_without_margin,
            b.total_delivered_amt_without_margin,
            b.missed_qty,
            b.filled_qty,
            b.missed_ltrs,
            b.filled_ltrs,
            COALESCE(
                CASE
                    WHEN upper(TRIM(BOTH FROM b.format)) = 'BLINKIT'::text AND upper(TRIM(BOTH FROM b.status)) = 'EXPIRED'::text AND COALESCE(b.delivered_qty, 0::numeric) <> 0::numeric THEN 'COMPLETED'::text
                    WHEN upper(TRIM(BOTH FROM b.format)) = 'BLINKIT'::text AND upper(TRIM(BOTH FROM b.status)) = 'EXPIRED'::text AND COALESCE(b.delivered_qty, 0::numeric) = 0::numeric THEN 'EXPIRED'::text
                    WHEN upper(TRIM(BOTH FROM b.format)) = 'SWIGGY'::text AND upper(TRIM(BOTH FROM b.status)) = 'CONFIRMED'::text THEN
                    CASE
                        WHEN max(COALESCE(b.delivered_qty, 0::numeric)) OVER (PARTITION BY b.po_number) > 0::numeric THEN 'COMPLETED'::text
                        ELSE 'PENDING'::text
                    END
                    ELSE sm.status_new::text
                END, ''::text) AS calculated_po_status
           FROM master_po_base b
             LEFT JOIN status_mapping sm ON upper(TRIM(BOTH FROM b.status)) = upper(TRIM(BOTH FROM sm.status::text))
        )
 SELECT po_number,
    po_date,
    po_expiry_date,
    delivery_date,
    appointment_date,
    vendor_name,
    status,
    sku_code,
    sku_name,
    order_qty,
    delivered_qty,
    basic_rate,
    landing_rate,
    location,
    format,
    remark,
    lead_time,
    days_to_expiry,
    po_window,
    calculated_po_status AS po_status,
        CASE
            WHEN calculated_po_status = 'COMPLETED'::text AND COALESCE(delivered_qty, 0::numeric) < COALESCE(order_qty, 0::numeric) THEN 'SHORT SUPPLIED'::text
            WHEN calculated_po_status <> 'COMPLETED'::text THEN calculated_po_status
            ELSE 'FULL SUPPLIED'::text
        END AS item_status,
    vendor_new,
    item,
    sap_sku_name,
    category,
    sub_category,
    case_pack,
    per_liter,
    total_order_liters,
    total_delivered_liters,
    total_order_amt_inclusive,
    total_deliver_amt_inclusive,
    po_month,
    delivery_month,
    po_year,
    delivered_year,
    item_head,
    city,
    state,
    distributor_margin,
    realise,
    distributor_commission_per_unit,
    total_distributor_commission,
    brand,
    category_head,
    unit_of_measure,
        CASE
            WHEN NULLIF(TRIM(BOTH FROM po_number), ''::text) IS NULL THEN ''::text
            WHEN calculated_po_status = ANY (ARRAY['APPOINTMENT DONE'::text, 'PENDING'::text]) THEN 'OPEN'::text
            ELSE 'CLOSED'::text
        END AS open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM mapped'''


def _refresh(apps, schema_editor):
    cur = schema_editor.connection.cursor()
    cur.execute("REFRESH MATERIALIZED VIEW public.master_po_mv")
    cur.execute("SELECT to_regclass('public.primary_summary_mv')")
    if cur.fetchone()[0] is not None:
        cur.execute("REFRESH MATERIALIZED VIEW public.primary_summary_mv")


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0060_status_mapping_table"),
    ]

    operations = [
        migrations.RunSQL(
            sql="CREATE OR REPLACE VIEW master_po_raw AS" + NEW_RAW,
            reverse_sql="CREATE OR REPLACE VIEW master_po_raw AS" + OLD_RAW,
        ),
        migrations.RunPython(_refresh, _refresh),
    ]
