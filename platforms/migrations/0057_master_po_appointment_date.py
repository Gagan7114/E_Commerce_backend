"""Add appointment_date to the master_po view chain (all primary platforms except Amazon).

WHY
---
The primary Master PO sheet needs an "Appointment Date" column. Its source
columns (total_po.appointment_date / total_po_zbs.appointment_date) are added by
uploads migration 0076. This migration surfaces that value through the live view
chain so it shows in the dashboard sheet and Excel export.

Amazon is NOT in total_po/total_po_zbs (it uses the separate amazon_po pipeline),
so this covers exactly the primary platforms and never touches Amazon.

WHAT
----
The chain master_po_base (view) -> master_po_raw (view) -> master_po_mv
(materialized) -> master_po (view), plus the dependent primary_summary_mv
(materialized), only exist in the live DB (never in the repo). Adding a column
mid-list to a view/matview needs a full DROP + recreate of the whole chain in
dependency order. The bodies below are the exact live definitions (pulled via
pg_get_viewdef) with a single appointment_date projection threaded in right after
delivery_date at every layer. primary_summary_mv is recreated verbatim (it never
references appointment_date; it is only dropped because it depends on
master_po_mv).

SAFETY / REVERSIBLE
-------------------
  * Validated: the exact forward SQL was executed in a rolled-back transaction on
    the live DB - master_po gains appointment_date after delivery_date, row count
    unchanged (47,578), primary_summary_mv rebuilds identically.
  * backwards() recreates the original (pre-appointment_date) bodies, restoring
    the chain byte-for-byte. The appointment_date columns themselves are dropped
    by reversing uploads 0076.

ROLL BACK
---------
  python manage.py migrate platforms 0056
"""

from django.db import migrations

MPMV_INDEXES = [
    'CREATE INDEX idx_mpmv_days_to_expiry_1_5 ON public.master_po_mv USING btree (upper(TRIM(BOTH FROM po_status))) WHERE ((days_to_expiry >= 1) AND (days_to_expiry <= 5))',
    'CREATE INDEX idx_mpmv_delivmonth_year_head ON public.master_po_mv USING btree (upper(TRIM(BOTH FROM delivery_month)), delivered_year, upper(TRIM(BOTH FROM item_head)))',
    'CREATE INDEX idx_mpmv_format ON public.master_po_mv USING btree (upper(TRIM(BOTH FROM format)))',
    "CREATE INDEX idx_mpmv_format_norm ON public.master_po_mv USING btree (regexp_replace(lower(TRIM(BOTH FROM format)), '[^a-z0-9]+'::text, ''::text, 'g'::text))",
    'CREATE INDEX idx_mpmv_pendency ON public.master_po_mv USING btree (upper(TRIM(BOTH FROM open_close)), upper(TRIM(BOTH FROM format)), po_year, upper(TRIM(BOTH FROM po_month)))',
]

PSMV_INDEXES = [
    'CREATE INDEX primary_summary_mv_del_idx ON public.primary_summary_mv USING btree (delivery_month_key, delivery_year)',
    'CREATE INDEX primary_summary_mv_fmt_idx ON public.primary_summary_mv USING btree (format_key)',
    'CREATE INDEX primary_summary_mv_po_idx ON public.primary_summary_mv USING btree (po_month_key, po_year)',
]

FWD_BASE = r""" WITH base AS (
         SELECT total_po_zbs.po_number,
            total_po_zbs.po_date,
            total_po_zbs.po_expiry_date,
            total_po_zbs.grn_date AS delivery_date,
            total_po_zbs.appointment_date,
            total_po_zbs.vendor_name,
            total_po_zbs.status,
            total_po_zbs.sku_code,
            total_po_zbs.sku_name,
            total_po_zbs.order_qty,
            total_po_zbs.delivered_qty,
            total_po_zbs.basic_rate,
            total_po_zbs.landing_rate,
            total_po_zbs.location,
            total_po_zbs.format,
            total_po_zbs.remark
           FROM total_po_zbs
        UNION ALL
         SELECT total_po.po_number,
            total_po.po_date,
            total_po.po_expiry_date,
            total_po.grn_date AS delivery_date,
            total_po.appointment_date,
            total_po.vendor_name,
            total_po.status,
            total_po.sku_code,
            total_po.sku_name,
            total_po.order_qty,
            total_po.delivered_qty,
            total_po.basic_rate,
            total_po.landing_rate,
            total_po.location,
            total_po.format,
            total_po.remark
           FROM total_po
        ), ms_by_format AS (
         SELECT DISTINCT ON ((upper(btrim(master_sheet.format_sku_code::text)))) upper(btrim(master_sheet.format_sku_code::text)) AS k,
            master_sheet.item,
            master_sheet.sku_sap_name,
            master_sheet.category,
            master_sheet.sub_category,
            master_sheet.per_unit_value,
            master_sheet.item_head
           FROM master_sheet
          WHERE NULLIF(btrim(master_sheet.format_sku_code::text), ''::text) IS NOT NULL
          ORDER BY (upper(btrim(master_sheet.format_sku_code::text)))
        ), ms_by_sap AS (
         SELECT DISTINCT ON ((upper(btrim(master_sheet.sku_sap_name::text)))) upper(btrim(master_sheet.sku_sap_name::text)) AS k,
            master_sheet.case_pack,
            master_sheet.brand,
            master_sheet.category_head,
            master_sheet.per_unit AS uom
           FROM master_sheet
          WHERE NULLIF(btrim(master_sheet.sku_sap_name::text), ''::text) IS NOT NULL
          ORDER BY (upper(btrim(master_sheet.sku_sap_name::text)))
        ), prep AS (
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
            _mp_to_date(b.po_date::text) AS pod,
            _mp_to_date(b.po_expiry_date::text) AS poe,
            _mp_to_date(b.delivery_date::text) AS deld,
            NULLIF(btrim(b.order_qty::text), ''::text)::numeric AS oq,
            NULLIF(btrim(b.delivered_qty::text), ''::text)::numeric AS dq,
            NULLIF(btrim(b.basic_rate::text), ''::text)::numeric AS br,
            NULLIF(btrim(b.landing_rate::text), ''::text)::numeric AS lr,
            upper(btrim(b.po_number)) AS pokey,
            upper(btrim(b.sku_code)) AS skukey
           FROM base b
        ), enr AS (
         SELECT p.po_number,
            p.po_date,
            p.po_expiry_date,
            p.delivery_date,
            p.appointment_date,
            p.vendor_name,
            p.status,
            p.sku_code,
            p.sku_name,
            p.order_qty,
            p.delivered_qty,
            p.basic_rate,
            p.landing_rate,
            p.location,
            p.format,
            p.remark,
            p.pod,
            p.poe,
            p.deld,
            p.oq,
            p.dq,
            p.br,
            p.lr,
            p.pokey,
            p.skukey,
            f.item,
            f.sku_sap_name AS sap_sku_name,
            f.category,
            f.sub_category,
            f.per_unit_value::numeric AS per_liter,
            f.item_head,
                CASE
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%KNOWTABLE%'::text THEN 'KNOWTABLE ONLINE SERVICES PRIVATE LIMITED'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%CHIRAG%'::text THEN 'CHIRAG ENTERPRISES MUMBAI'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%BABA LOKENATH%'::text THEN 'BABA LOKENATH TRADERS'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%JIVO MART%'::text THEN 'JIVO MART PRIVATE LIMITED'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%EVARA%'::text THEN 'EVARA ENTERPRISES'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%SUSTAINQUEST%'::text THEN 'SUSTAINQUEST PRIVATE LIMITED'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%ANTIZE%'::text THEN 'ANTIZE FOODS PVT LTD'::text
                    ELSE p.vendor_name
                END AS vendor_new,
                CASE
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%BENGALURU%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BANGALORE%'::text THEN 'BENGALURU'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%MUMBAI%'::text THEN 'MUMBAI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%PUNE%'::text THEN 'PUNE'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%DELHI%'::text THEN 'DELHI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%NOIDA%'::text THEN 'NOIDA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%GURGAON%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%GURUGRAM%'::text THEN 'GURUGRAM'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%KOLKATA%'::text THEN 'KOLKATA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%HYDERABAD%'::text THEN 'HYDERABAD'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%CHENNAI%'::text THEN 'CHENNAI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%LUCKNOW%'::text THEN 'LUCKNOW'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%LUDHIANA%'::text THEN 'LUDHIANA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%FARUKHNAGAR%'::text THEN 'FARUKHNAGAR'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%DASNA%'::text THEN 'DASNA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%RAJPURA%'::text THEN 'RAJPURA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%BALLABHGARH%'::text THEN 'BALLABHGARH'::text
                    ELSE p.location
                END AS city,
                CASE
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%BENGALURU%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BANGALORE%'::text THEN 'KARNATAKA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%MUMBAI%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%PUNE%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BHIWANDI%'::text THEN 'MAHARASHTRA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%DELHI%'::text THEN 'DELHI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%NOIDA%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%DASNA%'::text THEN 'UTTAR PRADESH'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%GURGAON%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%GURUGRAM%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%FARUKHNAGAR%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BALLABHGARH%'::text THEN 'HARYANA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%KOLKATA%'::text THEN 'WEST BENGAL'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%HYDERABAD%'::text THEN 'TELANGANA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%CHENNAI%'::text THEN 'TAMIL NADU'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%LUDHIANA%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%RAJPURA%'::text THEN 'PUNJAB'::text
                    ELSE NULL::text
                END AS state,
                CASE
                    WHEN upper(COALESCE(p.status, ''::text)) = ANY (ARRAY['COMPLETED'::text, 'COMPLETE'::text, 'FULFILLED'::text, 'GRN DONE'::text, 'GRN_DONE'::text]) THEN 'COMPLETED'::text
                    WHEN upper(COALESCE(p.status, ''::text)) = ANY (ARRAY['CANCELLED'::text, 'CANCELED'::text, 'CANCEL'::text]) THEN 'CANCELLED'::text
                    WHEN upper(COALESCE(p.status, ''::text)) = ANY (ARRAY['PENDING'::text, 'CONFIRMED'::text, 'SCHEDULED'::text, 'APPOINTMENT DONE'::text, 'PENDING_ACKNOWLEDGEMENT'::text, 'PENDING_ASN_CREATION'::text, 'PENDING_GRN'::text, 'ASN_CREATED'::text]) THEN 'PENDING'::text
                    WHEN upper(COALESCE(p.status, ''::text)) = 'EXPIRED'::text THEN 'EXPIRED'::text
                    ELSE NULLIF(upper(COALESCE(p.status, ''::text)), ''::text)
                END AS status_mapped,
            max(p.dq) OVER (PARTITION BY p.pokey) AS po_max_delivered
           FROM prep p
             LEFT JOIN ms_by_format f ON f.k = p.skukey
        ), enr2 AS (
         SELECT e.po_number,
            e.po_date,
            e.po_expiry_date,
            e.delivery_date,
            e.appointment_date,
            e.vendor_name,
            e.status,
            e.sku_code,
            e.sku_name,
            e.order_qty,
            e.delivered_qty,
            e.basic_rate,
            e.landing_rate,
            e.location,
            e.format,
            e.remark,
            e.pod,
            e.poe,
            e.deld,
            e.oq,
            e.dq,
            e.br,
            e.lr,
            e.pokey,
            e.skukey,
            e.item,
            e.sap_sku_name,
            e.category,
            e.sub_category,
            e.per_liter,
            e.item_head,
            e.vendor_new,
            e.city,
            e.state,
            e.status_mapped,
            e.po_max_delivered,
            s.case_pack,
            s.brand,
            s.category_head,
            s.uom AS unit_of_measure
           FROM enr e
             LEFT JOIN ms_by_sap s ON s.k = upper(btrim(e.sap_sku_name::text))
        ), c1 AS (
         SELECT e.po_number,
            e.po_date,
            e.po_expiry_date,
            e.delivery_date,
            e.appointment_date,
            e.vendor_name,
            e.status,
            e.sku_code,
            e.sku_name,
            e.order_qty,
            e.delivered_qty,
            e.basic_rate,
            e.landing_rate,
            e.location,
            e.format,
            e.remark,
            e.pod,
            e.poe,
            e.deld,
            e.oq,
            e.dq,
            e.br,
            e.lr,
            e.pokey,
            e.skukey,
            e.item,
            e.sap_sku_name,
            e.category,
            e.sub_category,
            e.per_liter,
            e.item_head,
            e.vendor_new,
            e.city,
            e.state,
            e.status_mapped,
            e.po_max_delivered,
            e.case_pack,
            e.brand,
            e.category_head,
            e.unit_of_measure,
                CASE
                    WHEN upper(COALESCE(e.format, ''::text)) = 'BLINKIT'::text AND upper(COALESCE(e.status, ''::text)) = 'EXPIRED'::text AND COALESCE(e.dq, 0::numeric) <> 0::numeric THEN 'COMPLETED'::text
                    WHEN upper(COALESCE(e.format, ''::text)) = 'BLINKIT'::text AND upper(COALESCE(e.status, ''::text)) = 'EXPIRED'::text AND COALESCE(e.dq, 0::numeric) = 0::numeric THEN 'EXPIRED'::text
                    WHEN upper(COALESCE(e.status, ''::text)) = 'CONFIRMED'::text AND upper(COALESCE(e.format, ''::text)) = 'SWIGGY'::text THEN
                    CASE
                        WHEN COALESCE(e.po_max_delivered, 0::numeric) > 0::numeric THEN 'COMPLETED'::text
                        ELSE 'PENDING'::text
                    END
                    ELSE upper(COALESCE(e.status_mapped, 'EXPIRED'::text))
                END AS po_status
           FROM enr2 e
        ), c2 AS (
         SELECT c1.po_number,
            c1.po_date,
            c1.po_expiry_date,
            c1.delivery_date,
            c1.appointment_date,
            c1.vendor_name,
            c1.status,
            c1.sku_code,
            c1.sku_name,
            c1.order_qty,
            c1.delivered_qty,
            c1.basic_rate,
            c1.landing_rate,
            c1.location,
            c1.format,
            c1.remark,
            c1.pod,
            c1.poe,
            c1.deld,
            c1.oq,
            c1.dq,
            c1.br,
            c1.lr,
            c1.pokey,
            c1.skukey,
            c1.item,
            c1.sap_sku_name,
            c1.category,
            c1.sub_category,
            c1.per_liter,
            c1.item_head,
            c1.vendor_new,
            c1.city,
            c1.state,
            c1.status_mapped,
            c1.po_max_delivered,
            c1.case_pack,
            c1.brand,
            c1.category_head,
            c1.unit_of_measure,
            c1.po_status,
                CASE
                    WHEN c1.deld IS NOT NULL AND c1.pod IS NOT NULL THEN c1.deld - c1.pod
                    ELSE NULL::integer
                END AS lead_time,
                CASE
                    WHEN c1.poe IS NOT NULL THEN GREATEST(c1.poe - CURRENT_DATE, 0)
                    ELSE NULL::integer
                END AS days_to_expiry,
                CASE
                    WHEN c1.poe IS NOT NULL AND c1.pod IS NOT NULL THEN c1.poe - c1.pod
                    ELSE NULL::integer
                END AS po_window,
                CASE
                    WHEN c1.po_status = 'COMPLETED'::text AND COALESCE(c1.dq, 0::numeric) < COALESCE(c1.oq, 0::numeric) THEN 'SHORT SUPPLIED'::text
                    WHEN c1.po_status <> 'COMPLETED'::text THEN c1.po_status
                    ELSE 'FULL SUPPLIED'::text
                END AS item_status,
            COALESCE(c1.oq, 0::numeric) * COALESCE(c1.per_liter, 0::numeric) AS total_order_liters,
            COALESCE(c1.dq, 0::numeric) * COALESCE(c1.per_liter, 0::numeric) AS total_delivered_liters,
            COALESCE(c1.oq, 0::numeric) * COALESCE(c1.lr, 0::numeric) AS total_order_amt_inclusive,
            COALESCE(c1.dq, 0::numeric) * COALESCE(c1.lr, 0::numeric) AS total_deliver_amt_inclusive,
            COALESCE(c1.oq, 0::numeric) * COALESCE(c1.br, 0::numeric) AS total_order_amt_exclusive,
            COALESCE(c1.dq, 0::numeric) * COALESCE(c1.br, 0::numeric) AS total_delivered_amt_exclusive,
                CASE
                    WHEN c1.pod IS NULL THEN NULL::text
                    ELSE upper(to_char(c1.pod::timestamp with time zone, 'FMMonth'::text))
                END AS po_month,
                CASE
                    WHEN c1.deld IS NULL THEN NULL::text
                    ELSE upper(to_char(c1.deld::timestamp with time zone, 'FMMonth'::text))
                END AS delivery_month,
            EXTRACT(year FROM c1.pod)::integer AS po_year,
            EXTRACT(year FROM c1.deld)::integer AS delivered_year,
                CASE
                    WHEN c1.po_status = ANY (ARRAY['APPOINTMENT DONE'::text, 'PENDING'::text]) THEN 'OPEN'::text
                    ELSE 'CLOSED'::text
                END AS open_close,
                CASE
                    WHEN c1.po_status = ANY (ARRAY['PENDING'::text, 'CANCELLED'::text, 'APPOINTMENT DONE'::text]) THEN 0::numeric
                    WHEN c1.po_status = ANY (ARRAY['COMPLETED'::text, 'EXPIRED'::text]) THEN COALESCE(c1.oq, 0::numeric) - COALESCE(c1.dq, 0::numeric)
                    ELSE NULL::numeric
                END AS missed_qty,
            COALESCE(c1.dq, 0::numeric) AS filled_qty,
                CASE
                    WHEN c1.vendor_new = 'KNOWTABLE ONLINE SERVICES PRIVATE LIMITED'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.city, ''::text)) = 'BENGALURU'::text THEN 0.055
                        ELSE 0.065
                    END
                    WHEN c1.vendor_new = 'CHIRAG ENTERPRISES MUMBAI'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.06
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'COMMODITY'::text THEN 0.04
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'BABA LOKENATH TRADERS'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.06
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'COMMODITY'::text THEN 0.03
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'JIVO MART PRIVATE LIMITED'::text THEN 0.045
                    WHEN c1.vendor_new = 'EVARA ENTERPRISES'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.045
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'COMMODITY'::text THEN 0.04
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'SUSTAINQUEST PRIVATE LIMITED'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.05
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'ANTIZE FOODS PVT LTD'::text THEN 0.055
                    ELSE 0.045
                END AS distributor_margin
           FROM c1
        ), c3 AS (
         SELECT c2.po_number,
            c2.po_date,
            c2.po_expiry_date,
            c2.delivery_date,
            c2.appointment_date,
            c2.vendor_name,
            c2.status,
            c2.sku_code,
            c2.sku_name,
            c2.order_qty,
            c2.delivered_qty,
            c2.basic_rate,
            c2.landing_rate,
            c2.location,
            c2.format,
            c2.remark,
            c2.pod,
            c2.poe,
            c2.deld,
            c2.oq,
            c2.dq,
            c2.br,
            c2.lr,
            c2.pokey,
            c2.skukey,
            c2.item,
            c2.sap_sku_name,
            c2.category,
            c2.sub_category,
            c2.per_liter,
            c2.item_head,
            c2.vendor_new,
            c2.city,
            c2.state,
            c2.status_mapped,
            c2.po_max_delivered,
            c2.case_pack,
            c2.brand,
            c2.category_head,
            c2.unit_of_measure,
            c2.po_status,
            c2.lead_time,
            c2.days_to_expiry,
            c2.po_window,
            c2.item_status,
            c2.total_order_liters,
            c2.total_delivered_liters,
            c2.total_order_amt_inclusive,
            c2.total_deliver_amt_inclusive,
            c2.total_order_amt_exclusive,
            c2.total_delivered_amt_exclusive,
            c2.po_month,
            c2.delivery_month,
            c2.po_year,
            c2.delivered_year,
            c2.open_close,
            c2.missed_qty,
            c2.filled_qty,
            c2.distributor_margin,
                CASE
                    WHEN c2.total_delivered_liters = 0::numeric THEN 0::numeric
                    ELSE COALESCE(c2.br, 0::numeric) / (1::numeric + c2.distributor_margin) / NULLIF(c2.per_liter, 0::numeric)
                END AS realise,
            c2.total_order_amt_exclusive / (1::numeric + c2.distributor_margin) AS total_order_amt_without_margin,
            c2.total_delivered_amt_exclusive / (1::numeric + c2.distributor_margin) AS total_delivered_amt_without_margin,
                CASE
                    WHEN c2.po_status = ANY (ARRAY['PENDING'::text, 'CANCELLED'::text, 'APPOINTMENT DONE'::text]) THEN 0::numeric
                    WHEN c2.po_status = ANY (ARRAY['COMPLETED'::text, 'EXPIRED'::text]) THEN c2.total_order_liters - c2.total_delivered_liters
                    ELSE NULL::numeric
                END AS missed_ltrs,
            c2.total_delivered_liters AS filled_ltrs
           FROM c2
        ), c4 AS (
         SELECT c3.po_number,
            c3.po_date,
            c3.po_expiry_date,
            c3.delivery_date,
            c3.appointment_date,
            c3.vendor_name,
            c3.status,
            c3.sku_code,
            c3.sku_name,
            c3.order_qty,
            c3.delivered_qty,
            c3.basic_rate,
            c3.landing_rate,
            c3.location,
            c3.format,
            c3.remark,
            c3.pod,
            c3.poe,
            c3.deld,
            c3.oq,
            c3.dq,
            c3.br,
            c3.lr,
            c3.pokey,
            c3.skukey,
            c3.item,
            c3.sap_sku_name,
            c3.category,
            c3.sub_category,
            c3.per_liter,
            c3.item_head,
            c3.vendor_new,
            c3.city,
            c3.state,
            c3.status_mapped,
            c3.po_max_delivered,
            c3.case_pack,
            c3.brand,
            c3.category_head,
            c3.unit_of_measure,
            c3.po_status,
            c3.lead_time,
            c3.days_to_expiry,
            c3.po_window,
            c3.item_status,
            c3.total_order_liters,
            c3.total_delivered_liters,
            c3.total_order_amt_inclusive,
            c3.total_deliver_amt_inclusive,
            c3.total_order_amt_exclusive,
            c3.total_delivered_amt_exclusive,
            c3.po_month,
            c3.delivery_month,
            c3.po_year,
            c3.delivered_year,
            c3.open_close,
            c3.missed_qty,
            c3.filled_qty,
            c3.distributor_margin,
            c3.realise,
            c3.total_order_amt_without_margin,
            c3.total_delivered_amt_without_margin,
            c3.missed_ltrs,
            c3.filled_ltrs,
                CASE
                    WHEN COALESCE(c3.realise, 0::numeric) = 0::numeric THEN 0::numeric
                    ELSE COALESCE(c3.br, 0::numeric) * c3.distributor_margin
                END AS distributor_commission_per_unit
           FROM c3
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
    po_status,
    item_status,
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
    distributor_commission_per_unit * COALESCE(dq, 0::numeric) AS total_distributor_commission,
    brand,
    category_head,
    unit_of_measure,
    open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM c4"""

FWD_RAW = r""" WITH mapped AS (
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
   FROM mapped"""

FWD_MV = r""" SELECT po_number,
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
    po_status,
    item_status,
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
    open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM ( SELECT r.po_number,
            r.po_date,
            r.po_expiry_date,
            r.delivery_date,
            r.appointment_date,
            r.vendor_name,
            r.status,
            r.sku_code,
            r.sku_name,
            r.order_qty,
            r.delivered_qty,
            r.basic_rate,
            r.landing_rate,
            r.location,
            r.format,
            r.remark,
            r.lead_time,
            r.days_to_expiry,
            r.po_window,
            r.po_status,
            r.item_status,
            r.vendor_new,
            r.item,
            r.sap_sku_name,
            r.category,
            r.sub_category,
            r.case_pack,
            r.per_liter,
            r.total_order_liters,
            r.total_delivered_liters,
            r.total_order_amt_inclusive,
            r.total_deliver_amt_inclusive,
            r.po_month,
            r.delivery_month,
            r.po_year,
            r.delivered_year,
            r.item_head,
            r.city,
            r.state,
            r.distributor_margin,
            r.realise,
            r.distributor_commission_per_unit,
            r.total_distributor_commission,
            r.brand,
            r.category_head,
            r.unit_of_measure,
            r.open_close,
            r.total_order_amt_exclusive,
            r.total_delivered_amt_exclusive,
            r.total_order_amt_without_margin,
            r.total_delivered_amt_without_margin,
            r.missed_qty,
            r.filled_qty,
            r.missed_ltrs,
            r.filled_ltrs,
            row_number() OVER (PARTITION BY (upper(TRIM(BOTH FROM r.format))), (TRIM(BOTH FROM r.po_number)), (upper(TRIM(BOTH FROM r.sku_code))) ORDER BY (
                CASE
                    WHEN upper(TRIM(BOTH FROM r.open_close)) = 'OPEN'::text THEN 0
                    ELSE 1
                END), r.delivery_date DESC NULLS LAST, r.po_date DESC NULLS LAST) AS rn
           FROM master_po_raw r) x
  WHERE rn = 1"""

FWD_MASTER = r""" SELECT po_number,
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
    po_status,
    item_status,
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
    open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM master_po_mv"""

FWD_SUMMARY = r""" WITH base AS (
         SELECT p.po_number,
            p.po_date,
            p.po_expiry_date,
            p.delivery_date,
            p.vendor_name,
            p.status,
            p.sku_code,
            p.sku_name,
            p.order_qty,
            p.delivered_qty,
            p.basic_rate,
            p.landing_rate,
            p.location,
            p.format,
            p.remark,
            p.lead_time,
            p.days_to_expiry,
            p.po_window,
            p.po_status,
            p.item_status,
            p.vendor_new,
            p.item,
            p.sap_sku_name,
            p.category,
            p.sub_category,
            p.case_pack,
            p.per_liter,
            p.total_order_liters,
            p.total_delivered_liters,
            p.total_order_amt_inclusive,
            p.total_deliver_amt_inclusive,
            p.po_month,
            p.delivery_month,
            p.po_year,
            p.delivered_year,
            p.item_head,
            p.city,
            p.state,
            p.distributor_margin,
            p.realise,
            p.distributor_commission_per_unit,
            p.total_distributor_commission,
            p.brand,
            p.category_head,
            p.unit_of_measure,
            p.open_close,
            p.total_order_amt_exclusive,
            p.total_delivered_amt_exclusive,
            p.total_order_amt_without_margin,
            p.total_delivered_amt_without_margin,
            p.missed_qty,
            p.filled_qty,
            p.missed_ltrs,
            p.filled_ltrs,
                CASE
                    WHEN TRIM(BOTH FROM p.po_date::text) ~ '^\d{2}-\d{2}-\d{4}$'::text THEN to_date(TRIM(BOTH FROM p.po_date::text), 'DD-MM-YYYY'::text)
                    WHEN TRIM(BOTH FROM p.po_date::text) ~ '^\d{4}-\d{2}-\d{2}$'::text THEN TRIM(BOTH FROM p.po_date::text)::date
                    ELSE NULL::date
                END AS po_dt,
                CASE
                    WHEN TRIM(BOTH FROM p.delivery_date::text) ~ '^\d{2}-\d{2}-\d{4}$'::text THEN to_date(TRIM(BOTH FROM p.delivery_date::text), 'DD-MM-YYYY'::text)
                    WHEN TRIM(BOTH FROM p.delivery_date::text) ~ '^\d{4}-\d{2}-\d{2}$'::text THEN TRIM(BOTH FROM p.delivery_date::text)::date
                    ELSE NULL::date
                END AS delivery_dt,
            regexp_replace(lower(TRIM(BOTH FROM p.format)), '[^a-z0-9]+'::text, ''::text, 'g'::text) AS format_key
           FROM master_po_mv p
        ), with_pack_text AS (
         SELECT base.po_number,
            base.po_date,
            base.po_expiry_date,
            base.delivery_date,
            base.vendor_name,
            base.status,
            base.sku_code,
            base.sku_name,
            base.order_qty,
            base.delivered_qty,
            base.basic_rate,
            base.landing_rate,
            base.location,
            base.format,
            base.remark,
            base.lead_time,
            base.days_to_expiry,
            base.po_window,
            base.po_status,
            base.item_status,
            base.vendor_new,
            base.item,
            base.sap_sku_name,
            base.category,
            base.sub_category,
            base.case_pack,
            base.per_liter,
            base.total_order_liters,
            base.total_delivered_liters,
            base.total_order_amt_inclusive,
            base.total_deliver_amt_inclusive,
            base.po_month,
            base.delivery_month,
            base.po_year,
            base.delivered_year,
            base.item_head,
            base.city,
            base.state,
            base.distributor_margin,
            base.realise,
            base.distributor_commission_per_unit,
            base.total_distributor_commission,
            base.brand,
            base.category_head,
            base.unit_of_measure,
            base.open_close,
            base.total_order_amt_exclusive,
            base.total_delivered_amt_exclusive,
            base.total_order_amt_without_margin,
            base.total_delivered_amt_without_margin,
            base.missed_qty,
            base.filled_qty,
            base.missed_ltrs,
            base.filled_ltrs,
            base.po_dt,
            base.delivery_dt,
            base.format_key,
            upper(concat_ws(' '::text, base.item, base.sap_sku_name::text, base.sku_name, base.unit_of_measure::text)) AS pack_text
           FROM base
        ), with_pack_matches AS (
         SELECT with_pack_text.po_number,
            with_pack_text.po_date,
            with_pack_text.po_expiry_date,
            with_pack_text.delivery_date,
            with_pack_text.vendor_name,
            with_pack_text.status,
            with_pack_text.sku_code,
            with_pack_text.sku_name,
            with_pack_text.order_qty,
            with_pack_text.delivered_qty,
            with_pack_text.basic_rate,
            with_pack_text.landing_rate,
            with_pack_text.location,
            with_pack_text.format,
            with_pack_text.remark,
            with_pack_text.lead_time,
            with_pack_text.days_to_expiry,
            with_pack_text.po_window,
            with_pack_text.po_status,
            with_pack_text.item_status,
            with_pack_text.vendor_new,
            with_pack_text.item,
            with_pack_text.sap_sku_name,
            with_pack_text.category,
            with_pack_text.sub_category,
            with_pack_text.case_pack,
            with_pack_text.per_liter,
            with_pack_text.total_order_liters,
            with_pack_text.total_delivered_liters,
            with_pack_text.total_order_amt_inclusive,
            with_pack_text.total_deliver_amt_inclusive,
            with_pack_text.po_month,
            with_pack_text.delivery_month,
            with_pack_text.po_year,
            with_pack_text.delivered_year,
            with_pack_text.item_head,
            with_pack_text.city,
            with_pack_text.state,
            with_pack_text.distributor_margin,
            with_pack_text.realise,
            with_pack_text.distributor_commission_per_unit,
            with_pack_text.total_distributor_commission,
            with_pack_text.brand,
            with_pack_text.category_head,
            with_pack_text.unit_of_measure,
            with_pack_text.open_close,
            with_pack_text.total_order_amt_exclusive,
            with_pack_text.total_delivered_amt_exclusive,
            with_pack_text.total_order_amt_without_margin,
            with_pack_text.total_delivered_amt_without_margin,
            with_pack_text.missed_qty,
            with_pack_text.filled_qty,
            with_pack_text.missed_ltrs,
            with_pack_text.filled_ltrs,
            with_pack_text.po_dt,
            with_pack_text.delivery_dt,
            with_pack_text.format_key,
            with_pack_text.pack_text,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'::text) AS combo_full_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'::text) AS combo_compact_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:ML|MLS|M)(?:[^A-Z0-9]|$)'::text) AS ml_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER)(?:[^A-Z0-9]|$)'::text) AS ltr_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*L(?:[^A-Z0-9]|$)'::text) AS l_match
           FROM with_pack_text
        ), metric_base AS (
         SELECT with_pack_matches.po_number,
            with_pack_matches.po_date,
            with_pack_matches.po_expiry_date,
            with_pack_matches.delivery_date,
            with_pack_matches.vendor_name,
            with_pack_matches.status,
            with_pack_matches.sku_code,
            with_pack_matches.sku_name,
            with_pack_matches.order_qty,
            with_pack_matches.delivered_qty,
            with_pack_matches.basic_rate,
            with_pack_matches.landing_rate,
            with_pack_matches.location,
            with_pack_matches.format,
            with_pack_matches.remark,
            with_pack_matches.lead_time,
            with_pack_matches.days_to_expiry,
            with_pack_matches.po_window,
            with_pack_matches.po_status,
            with_pack_matches.item_status,
            with_pack_matches.vendor_new,
            with_pack_matches.item,
            with_pack_matches.sap_sku_name,
            with_pack_matches.category,
            with_pack_matches.sub_category,
            with_pack_matches.case_pack,
            with_pack_matches.per_liter,
            with_pack_matches.total_order_liters,
            with_pack_matches.total_delivered_liters,
            with_pack_matches.total_order_amt_inclusive,
            with_pack_matches.total_deliver_amt_inclusive,
            with_pack_matches.po_month,
            with_pack_matches.delivery_month,
            with_pack_matches.po_year,
            with_pack_matches.delivered_year,
            with_pack_matches.item_head,
            with_pack_matches.city,
            with_pack_matches.state,
            with_pack_matches.distributor_margin,
            with_pack_matches.realise,
            with_pack_matches.distributor_commission_per_unit,
            with_pack_matches.total_distributor_commission,
            with_pack_matches.brand,
            with_pack_matches.category_head,
            with_pack_matches.unit_of_measure,
            with_pack_matches.open_close,
            with_pack_matches.total_order_amt_exclusive,
            with_pack_matches.total_delivered_amt_exclusive,
            with_pack_matches.total_order_amt_without_margin,
            with_pack_matches.total_delivered_amt_without_margin,
            with_pack_matches.missed_qty,
            with_pack_matches.filled_qty,
            with_pack_matches.missed_ltrs,
            with_pack_matches.filled_ltrs,
            with_pack_matches.po_dt,
            with_pack_matches.delivery_dt,
            with_pack_matches.format_key,
            with_pack_matches.pack_text,
            with_pack_matches.combo_full_match,
            with_pack_matches.combo_compact_match,
            with_pack_matches.ml_match,
            with_pack_matches.ltr_match,
            with_pack_matches.l_match,
            COALESCE(
                CASE
                    WHEN with_pack_matches.combo_full_match IS NOT NULL THEN with_pack_matches.combo_full_match[1]::numeric + with_pack_matches.combo_full_match[2]::numeric
                    WHEN with_pack_matches.combo_compact_match IS NOT NULL THEN with_pack_matches.combo_compact_match[1]::numeric + with_pack_matches.combo_compact_match[2]::numeric
                    WHEN with_pack_matches.ml_match IS NOT NULL THEN with_pack_matches.ml_match[1]::numeric / 1000::numeric
                    WHEN with_pack_matches.ltr_match IS NOT NULL THEN with_pack_matches.ltr_match[1]::numeric
                    WHEN with_pack_matches.l_match IS NOT NULL THEN with_pack_matches.l_match[1]::numeric
                    ELSE NULL::numeric
                END, NULLIF(with_pack_matches.per_liter, 0::numeric), 1::numeric) AS effective_per_liter
           FROM with_pack_matches
        ), normalized AS (
         SELECT metric_base.po_number,
            metric_base.po_date,
            metric_base.po_expiry_date,
            metric_base.delivery_date,
            metric_base.vendor_name,
            metric_base.status,
            metric_base.sku_code,
            metric_base.sku_name,
            metric_base.order_qty,
            metric_base.delivered_qty,
            metric_base.basic_rate,
            metric_base.landing_rate,
            metric_base.location,
            metric_base.format,
            metric_base.remark,
            metric_base.lead_time,
            metric_base.days_to_expiry,
            metric_base.po_window,
            metric_base.po_status,
            metric_base.item_status,
            metric_base.vendor_new,
            metric_base.item,
            metric_base.sap_sku_name,
            metric_base.category,
            metric_base.sub_category,
            metric_base.case_pack,
            metric_base.per_liter,
            metric_base.total_order_liters,
            metric_base.total_delivered_liters,
            metric_base.total_order_amt_inclusive,
            metric_base.total_deliver_amt_inclusive,
            metric_base.po_month,
            metric_base.delivery_month,
            metric_base.po_year,
            metric_base.delivered_year,
            metric_base.item_head,
            metric_base.city,
            metric_base.state,
            metric_base.distributor_margin,
            metric_base.realise,
            metric_base.distributor_commission_per_unit,
            metric_base.total_distributor_commission,
            metric_base.brand,
            metric_base.category_head,
            metric_base.unit_of_measure,
            metric_base.open_close,
            metric_base.total_order_amt_exclusive,
            metric_base.total_delivered_amt_exclusive,
            metric_base.total_order_amt_without_margin,
            metric_base.total_delivered_amt_without_margin,
            metric_base.missed_qty,
            metric_base.filled_qty,
            metric_base.missed_ltrs,
            metric_base.filled_ltrs,
            metric_base.po_dt,
            metric_base.delivery_dt,
            metric_base.format_key,
            metric_base.pack_text,
            metric_base.combo_full_match,
            metric_base.combo_compact_match,
            metric_base.ml_match,
            metric_base.ltr_match,
            metric_base.l_match,
            metric_base.effective_per_liter,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.po_status)), ''::text), 'OTHER'::text) AS status_key,
                CASE
                    WHEN upper(TRIM(BOTH FROM metric_base.item_head)) = 'PREMIUM'::text THEN 'PREMIUM'::text
                    WHEN upper(TRIM(BOTH FROM metric_base.item_head)) = 'COMMODITY'::text THEN 'COMMODITY'::text
                    ELSE 'OTHER'::text
                END AS item_head_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.item)), ''::text), NULLIF(upper(TRIM(BOTH FROM metric_base.sku_name)), ''::text), 'OTHER'::text) AS item_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.category)), ''::text), 'OTHER'::text) AS category_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.sub_category)), ''::text), 'OTHER'::text) AS sub_category_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.open_close)), ''::text), 'CLOSED'::text) AS open_close_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.po_month)), ''::text), upper(TRIM(BOTH FROM to_char(metric_base.po_dt::timestamp with time zone, 'FMMONTH'::text)))) AS po_month_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.delivery_month)), ''::text), upper(TRIM(BOTH FROM to_char(metric_base.delivery_dt::timestamp with time zone, 'FMMONTH'::text)))) AS delivery_month_key,
            EXTRACT(year FROM metric_base.delivery_dt)::integer AS delivery_year,
                CASE
                    WHEN metric_base.effective_per_liter IS NULL THEN upper(TRIM(BOTH FROM metric_base.unit_of_measure::text))
                    WHEN metric_base.effective_per_liter < 1::numeric THEN upper(TRIM(BOTH FROM to_char(metric_base.effective_per_liter * 1000::numeric, 'FM999999990.###'::text))) || ' MLS'::text
                    ELSE upper(TRIM(BOTH FROM to_char(metric_base.effective_per_liter, 'FM999999990.###'::text))) || ' LTR'::text
                END AS per_ltr_key,
            COALESCE(metric_base.total_order_liters, 0::numeric) AS metric_order_liters,
            COALESCE(metric_base.total_delivered_liters, 0::numeric) AS metric_delivered_liters,
            COALESCE(metric_base.total_order_amt_inclusive, 0::numeric) AS metric_order_value,
            COALESCE(metric_base.total_deliver_amt_inclusive, 0::numeric) AS metric_delivered_value,
            COALESCE(metric_base.order_qty, 0::numeric) AS metric_order_qty,
            COALESCE(metric_base.delivered_qty, 0::numeric) AS metric_delivered_qty,
            COALESCE(metric_base.missed_ltrs, 0::numeric) AS metric_pending_liters,
            COALESCE(metric_base.missed_qty, 0::numeric) AS metric_pending_qty,
            COALESCE(COALESCE(metric_base.missed_qty, 0::numeric) *
                CASE
                    WHEN NULLIF(TRIM(BOTH FROM metric_base.basic_rate::text), ''::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text THEN NULLIF(TRIM(BOTH FROM metric_base.basic_rate::text), ''::text)::numeric
                    ELSE 0::numeric
                END, 0::numeric) AS metric_pending_value
           FROM metric_base
        )
 SELECT format_key,
    item_head_key,
    item_key,
    category_key,
    sub_category_key,
    per_ltr_key,
    status_key,
    open_close_key,
    po_month_key,
    delivery_month_key,
    delivery_year,
    po_year,
    po_dt,
    delivery_dt,
    vendor_new,
    vendor_name,
    lead_time,
    metric_order_liters,
    metric_delivered_liters,
    metric_order_value,
    metric_delivered_value,
    metric_order_qty,
    metric_delivered_qty,
    metric_pending_liters,
    metric_pending_qty,
    metric_pending_value,
    0::numeric AS metric_projection_value,
    0::numeric AS metric_projection_ltrs,
    0::numeric AS metric_projection_qty
   FROM normalized"""

ORIG_BASE = r""" WITH base AS (
         SELECT total_po_zbs.po_number,
            total_po_zbs.po_date,
            total_po_zbs.po_expiry_date,
            total_po_zbs.grn_date AS delivery_date,
            total_po_zbs.vendor_name,
            total_po_zbs.status,
            total_po_zbs.sku_code,
            total_po_zbs.sku_name,
            total_po_zbs.order_qty,
            total_po_zbs.delivered_qty,
            total_po_zbs.basic_rate,
            total_po_zbs.landing_rate,
            total_po_zbs.location,
            total_po_zbs.format,
            total_po_zbs.remark
           FROM total_po_zbs
        UNION ALL
         SELECT total_po.po_number,
            total_po.po_date,
            total_po.po_expiry_date,
            total_po.grn_date AS delivery_date,
            total_po.vendor_name,
            total_po.status,
            total_po.sku_code,
            total_po.sku_name,
            total_po.order_qty,
            total_po.delivered_qty,
            total_po.basic_rate,
            total_po.landing_rate,
            total_po.location,
            total_po.format,
            total_po.remark
           FROM total_po
        ), ms_by_format AS (
         SELECT DISTINCT ON ((upper(btrim(master_sheet.format_sku_code::text)))) upper(btrim(master_sheet.format_sku_code::text)) AS k,
            master_sheet.item,
            master_sheet.sku_sap_name,
            master_sheet.category,
            master_sheet.sub_category,
            master_sheet.per_unit_value,
            master_sheet.item_head
           FROM master_sheet
          WHERE NULLIF(btrim(master_sheet.format_sku_code::text), ''::text) IS NOT NULL
          ORDER BY (upper(btrim(master_sheet.format_sku_code::text)))
        ), ms_by_sap AS (
         SELECT DISTINCT ON ((upper(btrim(master_sheet.sku_sap_name::text)))) upper(btrim(master_sheet.sku_sap_name::text)) AS k,
            master_sheet.case_pack,
            master_sheet.brand,
            master_sheet.category_head,
            master_sheet.per_unit AS uom
           FROM master_sheet
          WHERE NULLIF(btrim(master_sheet.sku_sap_name::text), ''::text) IS NOT NULL
          ORDER BY (upper(btrim(master_sheet.sku_sap_name::text)))
        ), prep AS (
         SELECT b.po_number,
            b.po_date,
            b.po_expiry_date,
            b.delivery_date,
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
            _mp_to_date(b.po_date::text) AS pod,
            _mp_to_date(b.po_expiry_date::text) AS poe,
            _mp_to_date(b.delivery_date::text) AS deld,
            NULLIF(btrim(b.order_qty::text), ''::text)::numeric AS oq,
            NULLIF(btrim(b.delivered_qty::text), ''::text)::numeric AS dq,
            NULLIF(btrim(b.basic_rate::text), ''::text)::numeric AS br,
            NULLIF(btrim(b.landing_rate::text), ''::text)::numeric AS lr,
            upper(btrim(b.po_number)) AS pokey,
            upper(btrim(b.sku_code)) AS skukey
           FROM base b
        ), enr AS (
         SELECT p.po_number,
            p.po_date,
            p.po_expiry_date,
            p.delivery_date,
            p.vendor_name,
            p.status,
            p.sku_code,
            p.sku_name,
            p.order_qty,
            p.delivered_qty,
            p.basic_rate,
            p.landing_rate,
            p.location,
            p.format,
            p.remark,
            p.pod,
            p.poe,
            p.deld,
            p.oq,
            p.dq,
            p.br,
            p.lr,
            p.pokey,
            p.skukey,
            f.item,
            f.sku_sap_name AS sap_sku_name,
            f.category,
            f.sub_category,
            f.per_unit_value::numeric AS per_liter,
            f.item_head,
                CASE
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%KNOWTABLE%'::text THEN 'KNOWTABLE ONLINE SERVICES PRIVATE LIMITED'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%CHIRAG%'::text THEN 'CHIRAG ENTERPRISES MUMBAI'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%BABA LOKENATH%'::text THEN 'BABA LOKENATH TRADERS'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%JIVO MART%'::text THEN 'JIVO MART PRIVATE LIMITED'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%EVARA%'::text THEN 'EVARA ENTERPRISES'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%SUSTAINQUEST%'::text THEN 'SUSTAINQUEST PRIVATE LIMITED'::text
                    WHEN upper(COALESCE(p.vendor_name, ''::text)) ~~ '%ANTIZE%'::text THEN 'ANTIZE FOODS PVT LTD'::text
                    ELSE p.vendor_name
                END AS vendor_new,
                CASE
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%BENGALURU%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BANGALORE%'::text THEN 'BENGALURU'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%MUMBAI%'::text THEN 'MUMBAI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%PUNE%'::text THEN 'PUNE'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%DELHI%'::text THEN 'DELHI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%NOIDA%'::text THEN 'NOIDA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%GURGAON%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%GURUGRAM%'::text THEN 'GURUGRAM'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%KOLKATA%'::text THEN 'KOLKATA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%HYDERABAD%'::text THEN 'HYDERABAD'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%CHENNAI%'::text THEN 'CHENNAI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%LUCKNOW%'::text THEN 'LUCKNOW'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%LUDHIANA%'::text THEN 'LUDHIANA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%FARUKHNAGAR%'::text THEN 'FARUKHNAGAR'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%DASNA%'::text THEN 'DASNA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%RAJPURA%'::text THEN 'RAJPURA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%BALLABHGARH%'::text THEN 'BALLABHGARH'::text
                    ELSE p.location
                END AS city,
                CASE
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%BENGALURU%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BANGALORE%'::text THEN 'KARNATAKA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%MUMBAI%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%PUNE%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BHIWANDI%'::text THEN 'MAHARASHTRA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%DELHI%'::text THEN 'DELHI'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%NOIDA%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%DASNA%'::text THEN 'UTTAR PRADESH'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%GURGAON%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%GURUGRAM%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%FARUKHNAGAR%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%BALLABHGARH%'::text THEN 'HARYANA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%KOLKATA%'::text THEN 'WEST BENGAL'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%HYDERABAD%'::text THEN 'TELANGANA'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%CHENNAI%'::text THEN 'TAMIL NADU'::text
                    WHEN upper(COALESCE(p.location, ''::text)) ~~ '%LUDHIANA%'::text OR upper(COALESCE(p.location, ''::text)) ~~ '%RAJPURA%'::text THEN 'PUNJAB'::text
                    ELSE NULL::text
                END AS state,
                CASE
                    WHEN upper(COALESCE(p.status, ''::text)) = ANY (ARRAY['COMPLETED'::text, 'COMPLETE'::text, 'FULFILLED'::text, 'GRN DONE'::text, 'GRN_DONE'::text]) THEN 'COMPLETED'::text
                    WHEN upper(COALESCE(p.status, ''::text)) = ANY (ARRAY['CANCELLED'::text, 'CANCELED'::text, 'CANCEL'::text]) THEN 'CANCELLED'::text
                    WHEN upper(COALESCE(p.status, ''::text)) = ANY (ARRAY['PENDING'::text, 'CONFIRMED'::text, 'SCHEDULED'::text, 'APPOINTMENT DONE'::text, 'PENDING_ACKNOWLEDGEMENT'::text, 'PENDING_ASN_CREATION'::text, 'PENDING_GRN'::text, 'ASN_CREATED'::text]) THEN 'PENDING'::text
                    WHEN upper(COALESCE(p.status, ''::text)) = 'EXPIRED'::text THEN 'EXPIRED'::text
                    ELSE NULLIF(upper(COALESCE(p.status, ''::text)), ''::text)
                END AS status_mapped,
            max(p.dq) OVER (PARTITION BY p.pokey) AS po_max_delivered
           FROM prep p
             LEFT JOIN ms_by_format f ON f.k = p.skukey
        ), enr2 AS (
         SELECT e.po_number,
            e.po_date,
            e.po_expiry_date,
            e.delivery_date,
            e.vendor_name,
            e.status,
            e.sku_code,
            e.sku_name,
            e.order_qty,
            e.delivered_qty,
            e.basic_rate,
            e.landing_rate,
            e.location,
            e.format,
            e.remark,
            e.pod,
            e.poe,
            e.deld,
            e.oq,
            e.dq,
            e.br,
            e.lr,
            e.pokey,
            e.skukey,
            e.item,
            e.sap_sku_name,
            e.category,
            e.sub_category,
            e.per_liter,
            e.item_head,
            e.vendor_new,
            e.city,
            e.state,
            e.status_mapped,
            e.po_max_delivered,
            s.case_pack,
            s.brand,
            s.category_head,
            s.uom AS unit_of_measure
           FROM enr e
             LEFT JOIN ms_by_sap s ON s.k = upper(btrim(e.sap_sku_name::text))
        ), c1 AS (
         SELECT e.po_number,
            e.po_date,
            e.po_expiry_date,
            e.delivery_date,
            e.vendor_name,
            e.status,
            e.sku_code,
            e.sku_name,
            e.order_qty,
            e.delivered_qty,
            e.basic_rate,
            e.landing_rate,
            e.location,
            e.format,
            e.remark,
            e.pod,
            e.poe,
            e.deld,
            e.oq,
            e.dq,
            e.br,
            e.lr,
            e.pokey,
            e.skukey,
            e.item,
            e.sap_sku_name,
            e.category,
            e.sub_category,
            e.per_liter,
            e.item_head,
            e.vendor_new,
            e.city,
            e.state,
            e.status_mapped,
            e.po_max_delivered,
            e.case_pack,
            e.brand,
            e.category_head,
            e.unit_of_measure,
                CASE
                    WHEN upper(COALESCE(e.format, ''::text)) = 'BLINKIT'::text AND upper(COALESCE(e.status, ''::text)) = 'EXPIRED'::text AND COALESCE(e.dq, 0::numeric) <> 0::numeric THEN 'COMPLETED'::text
                    WHEN upper(COALESCE(e.format, ''::text)) = 'BLINKIT'::text AND upper(COALESCE(e.status, ''::text)) = 'EXPIRED'::text AND COALESCE(e.dq, 0::numeric) = 0::numeric THEN 'EXPIRED'::text
                    WHEN upper(COALESCE(e.status, ''::text)) = 'CONFIRMED'::text AND upper(COALESCE(e.format, ''::text)) = 'SWIGGY'::text THEN
                    CASE
                        WHEN COALESCE(e.po_max_delivered, 0::numeric) > 0::numeric THEN 'COMPLETED'::text
                        ELSE 'PENDING'::text
                    END
                    ELSE upper(COALESCE(e.status_mapped, 'EXPIRED'::text))
                END AS po_status
           FROM enr2 e
        ), c2 AS (
         SELECT c1.po_number,
            c1.po_date,
            c1.po_expiry_date,
            c1.delivery_date,
            c1.vendor_name,
            c1.status,
            c1.sku_code,
            c1.sku_name,
            c1.order_qty,
            c1.delivered_qty,
            c1.basic_rate,
            c1.landing_rate,
            c1.location,
            c1.format,
            c1.remark,
            c1.pod,
            c1.poe,
            c1.deld,
            c1.oq,
            c1.dq,
            c1.br,
            c1.lr,
            c1.pokey,
            c1.skukey,
            c1.item,
            c1.sap_sku_name,
            c1.category,
            c1.sub_category,
            c1.per_liter,
            c1.item_head,
            c1.vendor_new,
            c1.city,
            c1.state,
            c1.status_mapped,
            c1.po_max_delivered,
            c1.case_pack,
            c1.brand,
            c1.category_head,
            c1.unit_of_measure,
            c1.po_status,
                CASE
                    WHEN c1.deld IS NOT NULL AND c1.pod IS NOT NULL THEN c1.deld - c1.pod
                    ELSE NULL::integer
                END AS lead_time,
                CASE
                    WHEN c1.poe IS NOT NULL THEN GREATEST(c1.poe - CURRENT_DATE, 0)
                    ELSE NULL::integer
                END AS days_to_expiry,
                CASE
                    WHEN c1.poe IS NOT NULL AND c1.pod IS NOT NULL THEN c1.poe - c1.pod
                    ELSE NULL::integer
                END AS po_window,
                CASE
                    WHEN c1.po_status = 'COMPLETED'::text AND COALESCE(c1.dq, 0::numeric) < COALESCE(c1.oq, 0::numeric) THEN 'SHORT SUPPLIED'::text
                    WHEN c1.po_status <> 'COMPLETED'::text THEN c1.po_status
                    ELSE 'FULL SUPPLIED'::text
                END AS item_status,
            COALESCE(c1.oq, 0::numeric) * COALESCE(c1.per_liter, 0::numeric) AS total_order_liters,
            COALESCE(c1.dq, 0::numeric) * COALESCE(c1.per_liter, 0::numeric) AS total_delivered_liters,
            COALESCE(c1.oq, 0::numeric) * COALESCE(c1.lr, 0::numeric) AS total_order_amt_inclusive,
            COALESCE(c1.dq, 0::numeric) * COALESCE(c1.lr, 0::numeric) AS total_deliver_amt_inclusive,
            COALESCE(c1.oq, 0::numeric) * COALESCE(c1.br, 0::numeric) AS total_order_amt_exclusive,
            COALESCE(c1.dq, 0::numeric) * COALESCE(c1.br, 0::numeric) AS total_delivered_amt_exclusive,
                CASE
                    WHEN c1.pod IS NULL THEN NULL::text
                    ELSE upper(to_char(c1.pod::timestamp with time zone, 'FMMonth'::text))
                END AS po_month,
                CASE
                    WHEN c1.deld IS NULL THEN NULL::text
                    ELSE upper(to_char(c1.deld::timestamp with time zone, 'FMMonth'::text))
                END AS delivery_month,
            EXTRACT(year FROM c1.pod)::integer AS po_year,
            EXTRACT(year FROM c1.deld)::integer AS delivered_year,
                CASE
                    WHEN c1.po_status = ANY (ARRAY['APPOINTMENT DONE'::text, 'PENDING'::text]) THEN 'OPEN'::text
                    ELSE 'CLOSED'::text
                END AS open_close,
                CASE
                    WHEN c1.po_status = ANY (ARRAY['PENDING'::text, 'CANCELLED'::text, 'APPOINTMENT DONE'::text]) THEN 0::numeric
                    WHEN c1.po_status = ANY (ARRAY['COMPLETED'::text, 'EXPIRED'::text]) THEN COALESCE(c1.oq, 0::numeric) - COALESCE(c1.dq, 0::numeric)
                    ELSE NULL::numeric
                END AS missed_qty,
            COALESCE(c1.dq, 0::numeric) AS filled_qty,
                CASE
                    WHEN c1.vendor_new = 'KNOWTABLE ONLINE SERVICES PRIVATE LIMITED'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.city, ''::text)) = 'BENGALURU'::text THEN 0.055
                        ELSE 0.065
                    END
                    WHEN c1.vendor_new = 'CHIRAG ENTERPRISES MUMBAI'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.06
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'COMMODITY'::text THEN 0.04
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'BABA LOKENATH TRADERS'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.06
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'COMMODITY'::text THEN 0.03
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'JIVO MART PRIVATE LIMITED'::text THEN 0.045
                    WHEN c1.vendor_new = 'EVARA ENTERPRISES'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.045
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'COMMODITY'::text THEN 0.04
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'SUSTAINQUEST PRIVATE LIMITED'::text THEN
                    CASE
                        WHEN upper(COALESCE(c1.item_head, ''::text)) = 'PREMIUM'::text THEN 0.05
                        ELSE 0.045
                    END
                    WHEN c1.vendor_new = 'ANTIZE FOODS PVT LTD'::text THEN 0.055
                    ELSE 0.045
                END AS distributor_margin
           FROM c1
        ), c3 AS (
         SELECT c2.po_number,
            c2.po_date,
            c2.po_expiry_date,
            c2.delivery_date,
            c2.vendor_name,
            c2.status,
            c2.sku_code,
            c2.sku_name,
            c2.order_qty,
            c2.delivered_qty,
            c2.basic_rate,
            c2.landing_rate,
            c2.location,
            c2.format,
            c2.remark,
            c2.pod,
            c2.poe,
            c2.deld,
            c2.oq,
            c2.dq,
            c2.br,
            c2.lr,
            c2.pokey,
            c2.skukey,
            c2.item,
            c2.sap_sku_name,
            c2.category,
            c2.sub_category,
            c2.per_liter,
            c2.item_head,
            c2.vendor_new,
            c2.city,
            c2.state,
            c2.status_mapped,
            c2.po_max_delivered,
            c2.case_pack,
            c2.brand,
            c2.category_head,
            c2.unit_of_measure,
            c2.po_status,
            c2.lead_time,
            c2.days_to_expiry,
            c2.po_window,
            c2.item_status,
            c2.total_order_liters,
            c2.total_delivered_liters,
            c2.total_order_amt_inclusive,
            c2.total_deliver_amt_inclusive,
            c2.total_order_amt_exclusive,
            c2.total_delivered_amt_exclusive,
            c2.po_month,
            c2.delivery_month,
            c2.po_year,
            c2.delivered_year,
            c2.open_close,
            c2.missed_qty,
            c2.filled_qty,
            c2.distributor_margin,
                CASE
                    WHEN c2.total_delivered_liters = 0::numeric THEN 0::numeric
                    ELSE COALESCE(c2.br, 0::numeric) / (1::numeric + c2.distributor_margin) / NULLIF(c2.per_liter, 0::numeric)
                END AS realise,
            c2.total_order_amt_exclusive / (1::numeric + c2.distributor_margin) AS total_order_amt_without_margin,
            c2.total_delivered_amt_exclusive / (1::numeric + c2.distributor_margin) AS total_delivered_amt_without_margin,
                CASE
                    WHEN c2.po_status = ANY (ARRAY['PENDING'::text, 'CANCELLED'::text, 'APPOINTMENT DONE'::text]) THEN 0::numeric
                    WHEN c2.po_status = ANY (ARRAY['COMPLETED'::text, 'EXPIRED'::text]) THEN c2.total_order_liters - c2.total_delivered_liters
                    ELSE NULL::numeric
                END AS missed_ltrs,
            c2.total_delivered_liters AS filled_ltrs
           FROM c2
        ), c4 AS (
         SELECT c3.po_number,
            c3.po_date,
            c3.po_expiry_date,
            c3.delivery_date,
            c3.vendor_name,
            c3.status,
            c3.sku_code,
            c3.sku_name,
            c3.order_qty,
            c3.delivered_qty,
            c3.basic_rate,
            c3.landing_rate,
            c3.location,
            c3.format,
            c3.remark,
            c3.pod,
            c3.poe,
            c3.deld,
            c3.oq,
            c3.dq,
            c3.br,
            c3.lr,
            c3.pokey,
            c3.skukey,
            c3.item,
            c3.sap_sku_name,
            c3.category,
            c3.sub_category,
            c3.per_liter,
            c3.item_head,
            c3.vendor_new,
            c3.city,
            c3.state,
            c3.status_mapped,
            c3.po_max_delivered,
            c3.case_pack,
            c3.brand,
            c3.category_head,
            c3.unit_of_measure,
            c3.po_status,
            c3.lead_time,
            c3.days_to_expiry,
            c3.po_window,
            c3.item_status,
            c3.total_order_liters,
            c3.total_delivered_liters,
            c3.total_order_amt_inclusive,
            c3.total_deliver_amt_inclusive,
            c3.total_order_amt_exclusive,
            c3.total_delivered_amt_exclusive,
            c3.po_month,
            c3.delivery_month,
            c3.po_year,
            c3.delivered_year,
            c3.open_close,
            c3.missed_qty,
            c3.filled_qty,
            c3.distributor_margin,
            c3.realise,
            c3.total_order_amt_without_margin,
            c3.total_delivered_amt_without_margin,
            c3.missed_ltrs,
            c3.filled_ltrs,
                CASE
                    WHEN COALESCE(c3.realise, 0::numeric) = 0::numeric THEN 0::numeric
                    ELSE COALESCE(c3.br, 0::numeric) * c3.distributor_margin
                END AS distributor_commission_per_unit
           FROM c3
        )
 SELECT po_number,
    po_date,
    po_expiry_date,
    delivery_date,
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
    po_status,
    item_status,
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
    distributor_commission_per_unit * COALESCE(dq, 0::numeric) AS total_distributor_commission,
    brand,
    category_head,
    unit_of_measure,
    open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM c4"""

ORIG_RAW = r""" WITH mapped AS (
         SELECT b.po_number,
            b.po_date,
            b.po_expiry_date,
            b.delivery_date,
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
   FROM mapped"""

ORIG_MV = r""" SELECT po_number,
    po_date,
    po_expiry_date,
    delivery_date,
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
    po_status,
    item_status,
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
    open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM ( SELECT r.po_number,
            r.po_date,
            r.po_expiry_date,
            r.delivery_date,
            r.vendor_name,
            r.status,
            r.sku_code,
            r.sku_name,
            r.order_qty,
            r.delivered_qty,
            r.basic_rate,
            r.landing_rate,
            r.location,
            r.format,
            r.remark,
            r.lead_time,
            r.days_to_expiry,
            r.po_window,
            r.po_status,
            r.item_status,
            r.vendor_new,
            r.item,
            r.sap_sku_name,
            r.category,
            r.sub_category,
            r.case_pack,
            r.per_liter,
            r.total_order_liters,
            r.total_delivered_liters,
            r.total_order_amt_inclusive,
            r.total_deliver_amt_inclusive,
            r.po_month,
            r.delivery_month,
            r.po_year,
            r.delivered_year,
            r.item_head,
            r.city,
            r.state,
            r.distributor_margin,
            r.realise,
            r.distributor_commission_per_unit,
            r.total_distributor_commission,
            r.brand,
            r.category_head,
            r.unit_of_measure,
            r.open_close,
            r.total_order_amt_exclusive,
            r.total_delivered_amt_exclusive,
            r.total_order_amt_without_margin,
            r.total_delivered_amt_without_margin,
            r.missed_qty,
            r.filled_qty,
            r.missed_ltrs,
            r.filled_ltrs,
            row_number() OVER (PARTITION BY (upper(TRIM(BOTH FROM r.format))), (TRIM(BOTH FROM r.po_number)), (upper(TRIM(BOTH FROM r.sku_code))) ORDER BY (
                CASE
                    WHEN upper(TRIM(BOTH FROM r.open_close)) = 'OPEN'::text THEN 0
                    ELSE 1
                END), r.delivery_date DESC NULLS LAST, r.po_date DESC NULLS LAST) AS rn
           FROM master_po_raw r) x
  WHERE rn = 1"""

ORIG_MASTER = r""" SELECT po_number,
    po_date,
    po_expiry_date,
    delivery_date,
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
    po_status,
    item_status,
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
    open_close,
    total_order_amt_exclusive,
    total_delivered_amt_exclusive,
    total_order_amt_without_margin,
    total_delivered_amt_without_margin,
    missed_qty,
    filled_qty,
    missed_ltrs,
    filled_ltrs
   FROM master_po_mv"""

ORIG_SUMMARY = r""" WITH base AS (
         SELECT p.po_number,
            p.po_date,
            p.po_expiry_date,
            p.delivery_date,
            p.vendor_name,
            p.status,
            p.sku_code,
            p.sku_name,
            p.order_qty,
            p.delivered_qty,
            p.basic_rate,
            p.landing_rate,
            p.location,
            p.format,
            p.remark,
            p.lead_time,
            p.days_to_expiry,
            p.po_window,
            p.po_status,
            p.item_status,
            p.vendor_new,
            p.item,
            p.sap_sku_name,
            p.category,
            p.sub_category,
            p.case_pack,
            p.per_liter,
            p.total_order_liters,
            p.total_delivered_liters,
            p.total_order_amt_inclusive,
            p.total_deliver_amt_inclusive,
            p.po_month,
            p.delivery_month,
            p.po_year,
            p.delivered_year,
            p.item_head,
            p.city,
            p.state,
            p.distributor_margin,
            p.realise,
            p.distributor_commission_per_unit,
            p.total_distributor_commission,
            p.brand,
            p.category_head,
            p.unit_of_measure,
            p.open_close,
            p.total_order_amt_exclusive,
            p.total_delivered_amt_exclusive,
            p.total_order_amt_without_margin,
            p.total_delivered_amt_without_margin,
            p.missed_qty,
            p.filled_qty,
            p.missed_ltrs,
            p.filled_ltrs,
                CASE
                    WHEN TRIM(BOTH FROM p.po_date::text) ~ '^\d{2}-\d{2}-\d{4}$'::text THEN to_date(TRIM(BOTH FROM p.po_date::text), 'DD-MM-YYYY'::text)
                    WHEN TRIM(BOTH FROM p.po_date::text) ~ '^\d{4}-\d{2}-\d{2}$'::text THEN TRIM(BOTH FROM p.po_date::text)::date
                    ELSE NULL::date
                END AS po_dt,
                CASE
                    WHEN TRIM(BOTH FROM p.delivery_date::text) ~ '^\d{2}-\d{2}-\d{4}$'::text THEN to_date(TRIM(BOTH FROM p.delivery_date::text), 'DD-MM-YYYY'::text)
                    WHEN TRIM(BOTH FROM p.delivery_date::text) ~ '^\d{4}-\d{2}-\d{2}$'::text THEN TRIM(BOTH FROM p.delivery_date::text)::date
                    ELSE NULL::date
                END AS delivery_dt,
            regexp_replace(lower(TRIM(BOTH FROM p.format)), '[^a-z0-9]+'::text, ''::text, 'g'::text) AS format_key
           FROM master_po_mv p
        ), with_pack_text AS (
         SELECT base.po_number,
            base.po_date,
            base.po_expiry_date,
            base.delivery_date,
            base.vendor_name,
            base.status,
            base.sku_code,
            base.sku_name,
            base.order_qty,
            base.delivered_qty,
            base.basic_rate,
            base.landing_rate,
            base.location,
            base.format,
            base.remark,
            base.lead_time,
            base.days_to_expiry,
            base.po_window,
            base.po_status,
            base.item_status,
            base.vendor_new,
            base.item,
            base.sap_sku_name,
            base.category,
            base.sub_category,
            base.case_pack,
            base.per_liter,
            base.total_order_liters,
            base.total_delivered_liters,
            base.total_order_amt_inclusive,
            base.total_deliver_amt_inclusive,
            base.po_month,
            base.delivery_month,
            base.po_year,
            base.delivered_year,
            base.item_head,
            base.city,
            base.state,
            base.distributor_margin,
            base.realise,
            base.distributor_commission_per_unit,
            base.total_distributor_commission,
            base.brand,
            base.category_head,
            base.unit_of_measure,
            base.open_close,
            base.total_order_amt_exclusive,
            base.total_delivered_amt_exclusive,
            base.total_order_amt_without_margin,
            base.total_delivered_amt_without_margin,
            base.missed_qty,
            base.filled_qty,
            base.missed_ltrs,
            base.filled_ltrs,
            base.po_dt,
            base.delivery_dt,
            base.format_key,
            upper(concat_ws(' '::text, base.item, base.sap_sku_name::text, base.sku_name, base.unit_of_measure::text)) AS pack_text
           FROM base
        ), with_pack_matches AS (
         SELECT with_pack_text.po_number,
            with_pack_text.po_date,
            with_pack_text.po_expiry_date,
            with_pack_text.delivery_date,
            with_pack_text.vendor_name,
            with_pack_text.status,
            with_pack_text.sku_code,
            with_pack_text.sku_name,
            with_pack_text.order_qty,
            with_pack_text.delivered_qty,
            with_pack_text.basic_rate,
            with_pack_text.landing_rate,
            with_pack_text.location,
            with_pack_text.format,
            with_pack_text.remark,
            with_pack_text.lead_time,
            with_pack_text.days_to_expiry,
            with_pack_text.po_window,
            with_pack_text.po_status,
            with_pack_text.item_status,
            with_pack_text.vendor_new,
            with_pack_text.item,
            with_pack_text.sap_sku_name,
            with_pack_text.category,
            with_pack_text.sub_category,
            with_pack_text.case_pack,
            with_pack_text.per_liter,
            with_pack_text.total_order_liters,
            with_pack_text.total_delivered_liters,
            with_pack_text.total_order_amt_inclusive,
            with_pack_text.total_deliver_amt_inclusive,
            with_pack_text.po_month,
            with_pack_text.delivery_month,
            with_pack_text.po_year,
            with_pack_text.delivered_year,
            with_pack_text.item_head,
            with_pack_text.city,
            with_pack_text.state,
            with_pack_text.distributor_margin,
            with_pack_text.realise,
            with_pack_text.distributor_commission_per_unit,
            with_pack_text.total_distributor_commission,
            with_pack_text.brand,
            with_pack_text.category_head,
            with_pack_text.unit_of_measure,
            with_pack_text.open_close,
            with_pack_text.total_order_amt_exclusive,
            with_pack_text.total_delivered_amt_exclusive,
            with_pack_text.total_order_amt_without_margin,
            with_pack_text.total_delivered_amt_without_margin,
            with_pack_text.missed_qty,
            with_pack_text.filled_qty,
            with_pack_text.missed_ltrs,
            with_pack_text.filled_ltrs,
            with_pack_text.po_dt,
            with_pack_text.delivery_dt,
            with_pack_text.format_key,
            with_pack_text.pack_text,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'::text) AS combo_full_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'::text) AS combo_compact_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:ML|MLS|M)(?:[^A-Z0-9]|$)'::text) AS ml_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER)(?:[^A-Z0-9]|$)'::text) AS ltr_match,
            regexp_match(with_pack_text.pack_text, '([0-9]+(?:\.[0-9]+)?)\s*L(?:[^A-Z0-9]|$)'::text) AS l_match
           FROM with_pack_text
        ), metric_base AS (
         SELECT with_pack_matches.po_number,
            with_pack_matches.po_date,
            with_pack_matches.po_expiry_date,
            with_pack_matches.delivery_date,
            with_pack_matches.vendor_name,
            with_pack_matches.status,
            with_pack_matches.sku_code,
            with_pack_matches.sku_name,
            with_pack_matches.order_qty,
            with_pack_matches.delivered_qty,
            with_pack_matches.basic_rate,
            with_pack_matches.landing_rate,
            with_pack_matches.location,
            with_pack_matches.format,
            with_pack_matches.remark,
            with_pack_matches.lead_time,
            with_pack_matches.days_to_expiry,
            with_pack_matches.po_window,
            with_pack_matches.po_status,
            with_pack_matches.item_status,
            with_pack_matches.vendor_new,
            with_pack_matches.item,
            with_pack_matches.sap_sku_name,
            with_pack_matches.category,
            with_pack_matches.sub_category,
            with_pack_matches.case_pack,
            with_pack_matches.per_liter,
            with_pack_matches.total_order_liters,
            with_pack_matches.total_delivered_liters,
            with_pack_matches.total_order_amt_inclusive,
            with_pack_matches.total_deliver_amt_inclusive,
            with_pack_matches.po_month,
            with_pack_matches.delivery_month,
            with_pack_matches.po_year,
            with_pack_matches.delivered_year,
            with_pack_matches.item_head,
            with_pack_matches.city,
            with_pack_matches.state,
            with_pack_matches.distributor_margin,
            with_pack_matches.realise,
            with_pack_matches.distributor_commission_per_unit,
            with_pack_matches.total_distributor_commission,
            with_pack_matches.brand,
            with_pack_matches.category_head,
            with_pack_matches.unit_of_measure,
            with_pack_matches.open_close,
            with_pack_matches.total_order_amt_exclusive,
            with_pack_matches.total_delivered_amt_exclusive,
            with_pack_matches.total_order_amt_without_margin,
            with_pack_matches.total_delivered_amt_without_margin,
            with_pack_matches.missed_qty,
            with_pack_matches.filled_qty,
            with_pack_matches.missed_ltrs,
            with_pack_matches.filled_ltrs,
            with_pack_matches.po_dt,
            with_pack_matches.delivery_dt,
            with_pack_matches.format_key,
            with_pack_matches.pack_text,
            with_pack_matches.combo_full_match,
            with_pack_matches.combo_compact_match,
            with_pack_matches.ml_match,
            with_pack_matches.ltr_match,
            with_pack_matches.l_match,
            COALESCE(
                CASE
                    WHEN with_pack_matches.combo_full_match IS NOT NULL THEN with_pack_matches.combo_full_match[1]::numeric + with_pack_matches.combo_full_match[2]::numeric
                    WHEN with_pack_matches.combo_compact_match IS NOT NULL THEN with_pack_matches.combo_compact_match[1]::numeric + with_pack_matches.combo_compact_match[2]::numeric
                    WHEN with_pack_matches.ml_match IS NOT NULL THEN with_pack_matches.ml_match[1]::numeric / 1000::numeric
                    WHEN with_pack_matches.ltr_match IS NOT NULL THEN with_pack_matches.ltr_match[1]::numeric
                    WHEN with_pack_matches.l_match IS NOT NULL THEN with_pack_matches.l_match[1]::numeric
                    ELSE NULL::numeric
                END, NULLIF(with_pack_matches.per_liter, 0::numeric), 1::numeric) AS effective_per_liter
           FROM with_pack_matches
        ), normalized AS (
         SELECT metric_base.po_number,
            metric_base.po_date,
            metric_base.po_expiry_date,
            metric_base.delivery_date,
            metric_base.vendor_name,
            metric_base.status,
            metric_base.sku_code,
            metric_base.sku_name,
            metric_base.order_qty,
            metric_base.delivered_qty,
            metric_base.basic_rate,
            metric_base.landing_rate,
            metric_base.location,
            metric_base.format,
            metric_base.remark,
            metric_base.lead_time,
            metric_base.days_to_expiry,
            metric_base.po_window,
            metric_base.po_status,
            metric_base.item_status,
            metric_base.vendor_new,
            metric_base.item,
            metric_base.sap_sku_name,
            metric_base.category,
            metric_base.sub_category,
            metric_base.case_pack,
            metric_base.per_liter,
            metric_base.total_order_liters,
            metric_base.total_delivered_liters,
            metric_base.total_order_amt_inclusive,
            metric_base.total_deliver_amt_inclusive,
            metric_base.po_month,
            metric_base.delivery_month,
            metric_base.po_year,
            metric_base.delivered_year,
            metric_base.item_head,
            metric_base.city,
            metric_base.state,
            metric_base.distributor_margin,
            metric_base.realise,
            metric_base.distributor_commission_per_unit,
            metric_base.total_distributor_commission,
            metric_base.brand,
            metric_base.category_head,
            metric_base.unit_of_measure,
            metric_base.open_close,
            metric_base.total_order_amt_exclusive,
            metric_base.total_delivered_amt_exclusive,
            metric_base.total_order_amt_without_margin,
            metric_base.total_delivered_amt_without_margin,
            metric_base.missed_qty,
            metric_base.filled_qty,
            metric_base.missed_ltrs,
            metric_base.filled_ltrs,
            metric_base.po_dt,
            metric_base.delivery_dt,
            metric_base.format_key,
            metric_base.pack_text,
            metric_base.combo_full_match,
            metric_base.combo_compact_match,
            metric_base.ml_match,
            metric_base.ltr_match,
            metric_base.l_match,
            metric_base.effective_per_liter,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.po_status)), ''::text), 'OTHER'::text) AS status_key,
                CASE
                    WHEN upper(TRIM(BOTH FROM metric_base.item_head)) = 'PREMIUM'::text THEN 'PREMIUM'::text
                    WHEN upper(TRIM(BOTH FROM metric_base.item_head)) = 'COMMODITY'::text THEN 'COMMODITY'::text
                    ELSE 'OTHER'::text
                END AS item_head_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.item)), ''::text), NULLIF(upper(TRIM(BOTH FROM metric_base.sku_name)), ''::text), 'OTHER'::text) AS item_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.category)), ''::text), 'OTHER'::text) AS category_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.sub_category)), ''::text), 'OTHER'::text) AS sub_category_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.open_close)), ''::text), 'CLOSED'::text) AS open_close_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.po_month)), ''::text), upper(TRIM(BOTH FROM to_char(metric_base.po_dt::timestamp with time zone, 'FMMONTH'::text)))) AS po_month_key,
            COALESCE(NULLIF(upper(TRIM(BOTH FROM metric_base.delivery_month)), ''::text), upper(TRIM(BOTH FROM to_char(metric_base.delivery_dt::timestamp with time zone, 'FMMONTH'::text)))) AS delivery_month_key,
            EXTRACT(year FROM metric_base.delivery_dt)::integer AS delivery_year,
                CASE
                    WHEN metric_base.effective_per_liter IS NULL THEN upper(TRIM(BOTH FROM metric_base.unit_of_measure::text))
                    WHEN metric_base.effective_per_liter < 1::numeric THEN upper(TRIM(BOTH FROM to_char(metric_base.effective_per_liter * 1000::numeric, 'FM999999990.###'::text))) || ' MLS'::text
                    ELSE upper(TRIM(BOTH FROM to_char(metric_base.effective_per_liter, 'FM999999990.###'::text))) || ' LTR'::text
                END AS per_ltr_key,
            COALESCE(metric_base.total_order_liters, 0::numeric) AS metric_order_liters,
            COALESCE(metric_base.total_delivered_liters, 0::numeric) AS metric_delivered_liters,
            COALESCE(metric_base.total_order_amt_inclusive, 0::numeric) AS metric_order_value,
            COALESCE(metric_base.total_deliver_amt_inclusive, 0::numeric) AS metric_delivered_value,
            COALESCE(metric_base.order_qty, 0::numeric) AS metric_order_qty,
            COALESCE(metric_base.delivered_qty, 0::numeric) AS metric_delivered_qty,
            COALESCE(metric_base.missed_ltrs, 0::numeric) AS metric_pending_liters,
            COALESCE(metric_base.missed_qty, 0::numeric) AS metric_pending_qty,
            COALESCE(COALESCE(metric_base.missed_qty, 0::numeric) *
                CASE
                    WHEN NULLIF(TRIM(BOTH FROM metric_base.basic_rate::text), ''::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text THEN NULLIF(TRIM(BOTH FROM metric_base.basic_rate::text), ''::text)::numeric
                    ELSE 0::numeric
                END, 0::numeric) AS metric_pending_value
           FROM metric_base
        )
 SELECT format_key,
    item_head_key,
    item_key,
    category_key,
    sub_category_key,
    per_ltr_key,
    status_key,
    open_close_key,
    po_month_key,
    delivery_month_key,
    delivery_year,
    po_year,
    po_dt,
    delivery_dt,
    vendor_new,
    vendor_name,
    lead_time,
    metric_order_liters,
    metric_delivered_liters,
    metric_order_value,
    metric_delivered_value,
    metric_order_qty,
    metric_delivered_qty,
    metric_pending_liters,
    metric_pending_qty,
    metric_pending_value,
    0::numeric AS metric_projection_value,
    0::numeric AS metric_projection_ltrs,
    0::numeric AS metric_projection_qty
   FROM normalized"""


def _rebuild(schema_editor, base, raw, mv, master, summary):
    # params=None so psycopg sends the SQL verbatim. The view bodies contain
    # literal LIKE patterns ('%KNOWTABLE%', ...); with the default params=()
    # psycopg would parse '%' as a placeholder and fail.
    def ex(stmt):
        schema_editor.execute(stmt, params=None)
    # Drop the whole chain top-down (dependents first).
    ex("DROP VIEW IF EXISTS public.master_po")
    ex("DROP MATERIALIZED VIEW IF EXISTS public.primary_summary_mv")
    ex("DROP MATERIALIZED VIEW IF EXISTS public.master_po_mv")
    ex("DROP VIEW IF EXISTS public.master_po_raw")
    ex("DROP VIEW IF EXISTS public.master_po_base")
    # Recreate bottom-up.
    ex("CREATE VIEW public.master_po_base AS " + base)
    ex("CREATE VIEW public.master_po_raw AS " + raw)
    ex("CREATE MATERIALIZED VIEW public.master_po_mv AS " + mv + " WITH DATA")
    for stmt in MPMV_INDEXES:
        ex(stmt)
    ex("CREATE VIEW public.master_po AS " + master)
    ex("CREATE MATERIALIZED VIEW public.primary_summary_mv AS " + summary + " WITH DATA")
    for stmt in PSMV_INDEXES:
        ex(stmt)


def forwards(apps, schema_editor):
    _rebuild(schema_editor, FWD_BASE, FWD_RAW, FWD_MV, FWD_MASTER, FWD_SUMMARY)


def backwards(apps, schema_editor):
    _rebuild(schema_editor, ORIG_BASE, ORIG_RAW, ORIG_MV, ORIG_MASTER, ORIG_SUMMARY)


class Migration(migrations.Migration):

    atomic = True

    dependencies = [
        ("platforms", "0056_primary_summary_matview"),
        ("uploads", "0076_primary_po_appointment_date"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
