"""ZOMATO opts out of auto-expiry — its po_status comes from the uploaded file.

0061 made master_po.po_status fact-aware for every primary platform with one
platform-agnostic cascade. Rule 3 of that cascade (past expiry + nothing
delivered -> EXPIRED) is now gated to skip ZOMATO, on request: Zomato POs go
back to the pre-0061 behaviour where the status the user uploads (mapped through
status_mapping) is what the dashboards show, and correcting a Zomato PO is a
manual edit — fix Status in the Master PO sheet (sync_po_from_sheet writes it
back to total_po) or re-upload the file — rather than something the view
overrides against today's date.

Cascade after this migration (first match wins):

  1. any delivery on the PO (max delivered over PO > 0) -> COMPLETED
  2. mapped status = CANCELLED                          -> CANCELLED
  3. past expiry, nothing delivered, format <> ZOMATO    -> EXPIRED   <-- gated
  4. appointment_date is set                            -> APPOINTMENT DONE
  5. else status_mapping[raw]; unmapped -> 'NEEDS MAPPING' (never blank)

ONLY rule 3 changes, and only for ZOMATO. Every other platform keeps auto-expiry
exactly as 0061 shipped it, and Zomato keeps rules 1/2/4/5 (a delivered Zomato PO
still reads COMPLETED — that is a recorded fact, not a date guess).

Knock-on effects for Zomato rows that were auto-EXPIRED and now fall through to
their uploaded status (typically PENDING):
  * open_close flips CLOSED -> OPEN, so they re-enter the pendency backlog.
  * missed_qty / missed_ltrs go from (order - delivered) to 0 — the undelivered
    quantity stops being booked as missed. Both columns derive from
    calculated_po_status, so this follows automatically with no extra edit.
A Zomato PO whose uploaded status IS 'EXPIRED' still maps to EXPIRED as before;
the file remains the source of truth either way.

`days_to_expiry` and `po_expiry_date` are untouched — the expiry date is still
carried and still drives the expiry-alert widgets; it just no longer rewrites
po_status for this one platform.

SAFETY
------
  * Column list is byte-identical to 0061, so this is a plain CREATE OR REPLACE
    VIEW (no DROP, dependents untouched), then a REFRESH of master_po_mv +
    primary_summary_mv so reads reflect it.
  * backwards() restores 0061's exact master_po_raw body and refreshes again.
"""
from django.db import migrations

# 0061's body with ONE line changed: the EXPIRED arm now skips ZOMATO.
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
                    WHEN upper(TRIM(BOTH FROM COALESCE(b.format, ''::text))) <> 'ZOMATO'::text AND _mp_to_date(b.po_expiry_date::text) IS NOT NULL AND _mp_to_date(b.po_expiry_date::text) < CURRENT_DATE THEN 'EXPIRED'::text
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

# 0061's body verbatim — ungated EXPIRED arm (auto-expiry for every platform).
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


def _refresh(apps, schema_editor):
    cur = schema_editor.connection.cursor()
    cur.execute("REFRESH MATERIALIZED VIEW public.master_po_mv")
    cur.execute("SELECT to_regclass('public.primary_summary_mv')")
    if cur.fetchone()[0] is not None:
        cur.execute("REFRESH MATERIALIZED VIEW public.primary_summary_mv")


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0066_flipkart_grocery_inventory"),
    ]

    operations = [
        migrations.RunSQL(
            sql="CREATE OR REPLACE VIEW master_po_raw AS" + NEW_RAW,
            reverse_sql="CREATE OR REPLACE VIEW master_po_raw AS" + OLD_RAW,
        ),
        migrations.RunPython(_refresh, _refresh),
    ]
