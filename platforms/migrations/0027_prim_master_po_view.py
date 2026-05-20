"""
Replace the physical `prim_master_po` table with a SQL VIEW that unions every
platform's `<slug>_prim` table, applies the per-platform column mapping the old
uploader sync used to do, joins the master sheet for SKU attributes, and
computes the derived columns (po_status, item_status, total_order_liters,
fill-rate percentages, etc.) inline.

After this migration:
  * Every primary upload lands in `<slug>_prim` and is INSTANTLY visible to the
    dashboard via the view — no more sync drift.
  * The old `_update_master_po_if_present` Python sync is permanently
    unnecessary.
  * The original physical table is preserved as `prim_master_po_legacy` so the
    data isn't lost. After verifying the view, you can drop the legacy table
    manually:  DROP TABLE public.prim_master_po_legacy;

Notes on date handling:
  Every platform `_prim` table stores `po_date` / expiry / delivery as TEXT in
  `DD-MM-YYYY` format — except Zepto which sometimes has
  `'07 May 2026 12:48 pm'`. The view keeps these columns as TEXT in
  `DD-MM-YYYY` format (normalising Zepto's variant) so the dashboard's existing
  text-date regex parsing (_prim_safe_date_expr in platforms/views.py) keeps
  working unchanged. A small helper function `_pm_parse_date(text)` is created
  for internal derived computations (year, po_month, lead_time, etc.).
"""

from django.db import migrations


_FORWARD = r"""
-- 1. Backup the existing prim_master_po table so no data is lost.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'public' AND tablename = 'prim_master_po'
    ) THEN
        EXECUTE 'ALTER TABLE public.prim_master_po RENAME TO prim_master_po_legacy';
    END IF;
END
$$;

-- 2. Drop any existing view of the same name (idempotent).
DROP VIEW IF EXISTS public.prim_master_po;

-- 3. Helper: parse messy text dates from primary tables. Handles:
--      DD-MM-YYYY           (the canonical format)
--      'DD Mon YYYY [hh:mm am]'   (Zepto sometimes ships this)
--      YYYY-MM-DD           (ISO, as a fallback)
--    Returns NULL for anything else, including the few garbage rows like
--    '01-11.2025' and '22-11-20255'.
CREATE OR REPLACE FUNCTION public._pm_parse_date(s text)
RETURNS date AS $$
DECLARE
    t text := NULLIF(TRIM(s), '');
BEGIN
    IF t IS NULL THEN RETURN NULL; END IF;
    IF t ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
        BEGIN RETURN TO_DATE(t, 'DD-MM-YYYY'); EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
    END IF;
    IF t ~ '^[0-9]{1,2} [A-Za-z]{3} [0-9]{4}' THEN
        BEGIN
            RETURN TO_DATE(SUBSTRING(t FROM '^[0-9]{1,2} [A-Za-z]{3} [0-9]{4}'), 'DD Mon YYYY');
        EXCEPTION WHEN OTHERS THEN RETURN NULL;
        END;
    END IF;
    IF t ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN
        BEGIN RETURN TO_DATE(LEFT(t, 10), 'YYYY-MM-DD'); EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Helper: normalise any of the above text formats back to canonical DD-MM-YYYY
-- (or NULL). The view emits text dates so the dashboard's existing regex
-- parsing keeps working.
CREATE OR REPLACE FUNCTION public._pm_dmy_text(s text)
RETURNS text AS $$
DECLARE
    d date := public._pm_parse_date(s);
BEGIN
    IF d IS NULL THEN RETURN NULL; END IF;
    RETURN TO_CHAR(d, 'DD-MM-YYYY');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- 4. Build the view. Each platform contributes a SELECT with native columns
--    mapped to the canonical names; date fields are normalised to DD-MM-YYYY
--    text. Derived columns are then computed inline.

CREATE OR REPLACE VIEW public.prim_master_po AS
WITH master_lookup AS (
    SELECT DISTINCT ON (UPPER(TRIM(format_sku_code::text)))
        format_sku_code,
        item_head,
        category,
        sub_category,
        per_unit_value,
        per_unit,
        brand,
        product_name AS master_item,
        sku_sap_name,
        format AS master_format
    FROM public.master_sheet
    WHERE NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY
        UPPER(TRIM(format_sku_code::text)),
        COALESCE(item_head, ''),
        COALESCE(category, ''),
        COALESCE(product_name, '')
),
unified AS (
    -- BLINKIT ---------------------------------------------------------
    SELECT
        'BLINKIT'::text                                AS format,
        b.po_number::text                              AS po_number,
        b.item_id::text                                AS sku_code,
        b.name::text                                   AS sku_name,
        b.vendor_name::text                            AS vendor_name,
        b.manufacturer_name::text                      AS manufacturer_name,
        b.facility_name::text                          AS location,
        b.po_state::text                               AS status,
        public._pm_dmy_text(b.order_date)              AS po_date,
        public._pm_dmy_text(b.expiry_date)             AS po_expiry_date,
        public._pm_dmy_text(b.appointment_date)        AS delivery_date,
        b.units_ordered::numeric                       AS order_qty,
        b.delivered_qty::numeric                       AS delivered_qty,
        b.cost_price::numeric                          AS basic_rate,
        b.landing_rate::numeric                        AS landing_rate,
        NULL::text                                     AS remarks
    FROM public.blinkit_prim b
    UNION ALL
    -- ZEPTO -----------------------------------------------------------
    SELECT
        'ZEPTO'::text,
        z.po_no::text,
        z.sku_code::text,
        z.sku_desc::text,
        z.vendor_name::text,
        NULL::text,
        z.del_location::text,
        z.status::text,
        public._pm_dmy_text(z.po_date),
        public._pm_dmy_text(z.po_expiry_date),
        public._pm_dmy_text(z.grn_date),
        z.qty::numeric,
        z.grn_quantity::numeric,
        z.unit_base_cost::numeric,
        z.landing_cost::numeric,
        NULL::text
    FROM public.zepto_prim z
    UNION ALL
    -- SWIGGY ----------------------------------------------------------
    SELECT
        'SWIGGY'::text,
        s.po_number::text,
        s.sku_code::text,
        s.sku_description::text,
        s.vendor_name::text,
        NULL::text,
        s.facility_name::text,
        s.status::text,
        public._pm_dmy_text(s.po_created_at),
        public._pm_dmy_text(s.po_expiry_date),
        public._pm_dmy_text(s.expected_delivery_date),
        s.ordered_qty::numeric,
        s.received_qty::numeric,
        s.unit_based_cost::numeric,
        s.landing_rate::numeric,
        NULL::text
    FROM public.swiggy_prim s
    UNION ALL
    -- BIGBASKET -------------------------------------------------------
    SELECT
        'BIG BASKET'::text,
        bb.po_number::text,
        bb.sku_code::text,
        bb.sku_name::text,
        bb.vendor::text,
        NULL::text,
        bb.location::text,
        bb.status::text,
        public._pm_dmy_text(bb.po_date),
        public._pm_dmy_text(bb.po_expiry_date),
        public._pm_dmy_text(bb.delivery_date),
        bb.order_qty::numeric,
        bb.delivered_qty::numeric,
        bb.basic_cost::numeric,
        bb.landing_cost::numeric,
        bb.remarks::text
    FROM public.bigbasket_prim bb
    UNION ALL
    -- FLIPKART GROCERY ------------------------------------------------
    SELECT
        'FLIPKART GROCERY'::text,
        fg.po_number::text,
        fg.sku_code::text,
        fg.sku_name::text,
        fg.vendor::text,
        NULL::text,
        fg.location::text,
        fg.status::text,
        public._pm_dmy_text(fg.po_date),
        public._pm_dmy_text(fg.po_expiry_date),
        public._pm_dmy_text(fg.delivery_date),
        fg.order_qty::numeric,
        fg.delivered_qty::numeric,
        fg.basic_rate::numeric,
        fg.landing_rate::numeric,
        fg.remark::text
    FROM public.flipkart_grocery_prim fg
    UNION ALL
    -- ZOMATO ----------------------------------------------------------
    SELECT
        'ZOMATO'::text,
        zm.po_number::text,
        zm.sku_code::text,
        zm.sku_name::text,
        zm.vendor::text,
        NULL::text,
        zm.location::text,
        zm.status::text,
        public._pm_dmy_text(zm.po_date),
        public._pm_dmy_text(zm.po_expiry_date),
        public._pm_dmy_text(COALESCE(NULLIF(TRIM(zm.delivery_date), ''), zm.appointment_date)),
        zm.order_qty::numeric,
        zm.delivered_qty::numeric,
        zm.basic_rate::numeric,
        zm.landing_rate::numeric,
        zm.remark::text
    FROM public.zomato_prim zm
    UNION ALL
    -- CITYMALL --------------------------------------------------------
    SELECT
        'CITY MALL'::text,
        cm.po_number::text,
        cm.sku_code::text,
        cm.sku_name::text,
        cm.vendor::text,
        NULL::text,
        cm.location::text,
        cm.status::text,
        public._pm_dmy_text(cm.po_date),
        public._pm_dmy_text(cm.po_expiry_date),
        public._pm_dmy_text(cm.delivery_date),
        cm.order_qty::numeric,
        cm.delivered_qty::numeric,
        cm.base_cost_price::numeric,
        cm.landing_rate::numeric,
        cm.remark::text
    FROM public.citymall_prim cm
),
joined AS (
    SELECT
        u.*,
        public._pm_parse_date(u.po_date)         AS _po_date_d,
        public._pm_parse_date(u.po_expiry_date)  AS _po_expiry_date_d,
        public._pm_parse_date(u.delivery_date)   AS _delivery_date_d,
        ml.item_head,
        ml.category,
        ml.sub_category,
        ml.per_unit_value::numeric AS _ms_per_liter,
        ml.per_unit                AS unit_of_measure,
        ml.brand,
        COALESCE(ml.master_item, u.sku_name) AS item,
        ml.sku_sap_name            AS sap_sku_name,
        -- Pack-text used to regex-parse litres from SKU/product names when
        -- master_sheet has no matching entry (Zepto UUIDs, etc.).
        UPPER(CONCAT_WS(' ',
            COALESCE(ml.master_item, u.sku_name)::text,
            ml.sku_sap_name::text,
            u.sku_name::text,
            ml.per_unit::text
        )) AS _pack_text
    FROM unified u
    LEFT JOIN master_lookup ml
        ON UPPER(TRIM(u.sku_code::text)) = UPPER(TRIM(ml.format_sku_code::text))
),
pack_parsed AS (
    SELECT
        j.*,
        -- Mirror the regex fallback used by platforms.views._primary_master_po_cte
        -- so SKUs without a master_sheet match still get a usable per_liter.
        regexp_match(j._pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'
        ) AS _combo_full_match,
        regexp_match(j._pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*\+\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER|L)(?:[^A-Z0-9]|$)'
        ) AS _combo_compact_match,
        regexp_match(j._pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*(?:ML|MLS|M)(?:[^A-Z0-9]|$)'
        ) AS _ml_match,
        regexp_match(j._pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*(?:LTR|LITRE|LITER)(?:[^A-Z0-9]|$)'
        ) AS _ltr_match,
        regexp_match(j._pack_text,
            '([0-9]+(?:\.[0-9]+)?)\s*L(?:[^A-Z0-9]|$)'
        ) AS _l_match
    FROM joined j
),
with_per_liter AS (
    SELECT
        p.*,
        COALESCE(
            NULLIF(p._ms_per_liter, 0),
            CASE
                WHEN p._combo_full_match IS NOT NULL
                    THEN p._combo_full_match[1]::numeric + p._combo_full_match[2]::numeric
                WHEN p._combo_compact_match IS NOT NULL
                    THEN p._combo_compact_match[1]::numeric + p._combo_compact_match[2]::numeric
                WHEN p._ltr_match IS NOT NULL THEN p._ltr_match[1]::numeric
                WHEN p._l_match IS NOT NULL   THEN p._l_match[1]::numeric
                WHEN p._ml_match IS NOT NULL  THEN p._ml_match[1]::numeric / 1000
                ELSE NULL
            END
        ) AS per_liter
    FROM pack_parsed p
),
calc AS (
    SELECT
        w.*,

        -- status normalization (matches old RECALC SQL) ----------------
        CASE
            WHEN UPPER(COALESCE(format, '')) = 'BLINKIT'
             AND UPPER(COALESCE(status, '')) = 'EXPIRED'
             AND COALESCE(delivered_qty, 0) <> 0 THEN 'COMPLETED'
            WHEN UPPER(COALESCE(format, '')) = 'BLINKIT'
             AND UPPER(COALESCE(status, '')) = 'EXPIRED'
             AND COALESCE(delivered_qty, 0) = 0 THEN 'EXPIRED'
            WHEN UPPER(COALESCE(status, '')) IN ('COMPLETED', 'COMPLETE', 'FULFILLED', 'GRN DONE', 'GRN_DONE')
                THEN 'COMPLETED'
            WHEN UPPER(COALESCE(status, '')) IN ('CANCELLED', 'CANCELED', 'CANCEL') THEN 'CANCELLED'
            WHEN UPPER(COALESCE(status, '')) IN (
                'PENDING', 'CONFIRMED', 'SCHEDULED', 'APPOINTMENT DONE',
                'PENDING_ACKNOWLEDGEMENT', 'PENDING_ASN_CREATION',
                'PENDING_GRN', 'ASN_CREATED'
            ) THEN 'PENDING'
            WHEN UPPER(COALESCE(status, '')) = 'EXPIRED' THEN 'EXPIRED'
            ELSE NULLIF(UPPER(COALESCE(status, '')), '')
        END AS po_status,

        -- vendor margin lookup (same hard-coded list as the old recalc) ---
        CASE
            WHEN UPPER(COALESCE(vendor_name, '')) = 'KNOWTABLE ONLINE SERVICES PRIVATE LIMITED'
                THEN CASE WHEN UPPER(COALESCE(location, '')) = 'BENGALURU' THEN 0.055 ELSE 0.065 END
            WHEN UPPER(COALESCE(vendor_name, '')) = 'CHIRAG ENTERPRISES MUMBAI'
                THEN CASE
                    WHEN UPPER(COALESCE(item_head, '')) = 'PREMIUM' THEN 0.06
                    WHEN UPPER(COALESCE(item_head, '')) = 'COMMODITY' THEN 0.04
                    ELSE 0.045
                END
            WHEN UPPER(COALESCE(vendor_name, '')) = 'BABA LOKENATH TRADERS'
                THEN CASE
                    WHEN UPPER(COALESCE(item_head, '')) = 'PREMIUM' THEN 0.06
                    WHEN UPPER(COALESCE(item_head, '')) = 'COMMODITY' THEN 0.03
                    ELSE 0.045
                END
            WHEN UPPER(COALESCE(vendor_name, '')) = 'JIVO MART PRIVATE LIMITED' THEN 0.045
            WHEN UPPER(COALESCE(vendor_name, '')) = 'EVARA ENTERPRISES'
                THEN CASE
                    WHEN UPPER(COALESCE(item_head, '')) = 'PREMIUM' THEN 0.045
                    WHEN UPPER(COALESCE(item_head, '')) = 'COMMODITY' THEN 0.04
                    ELSE 0.045
                END
            WHEN UPPER(COALESCE(vendor_name, '')) = 'SUSTAINQUEST PRIVATE LIMITED'
                THEN CASE WHEN UPPER(COALESCE(item_head, '')) = 'PREMIUM' THEN 0.05 ELSE 0.045 END
            WHEN UPPER(COALESCE(vendor_name, '')) = 'ANTIZE FOODS PVT LTD' THEN 0.055
            ELSE 0.045
        END AS distributor_margin
    FROM with_per_liter w
),
final AS (
    SELECT
        c.format,
        c.po_number,
        c.sku_code,
        c.sku_name,
        c.vendor_name,
        c.manufacturer_name,
        c.location,
        c.status,
        c.po_date,
        c.po_expiry_date,
        c.delivery_date,
        c.order_qty,
        c.delivered_qty,
        c.basic_rate,
        c.landing_rate,
        c.remarks,
        c.item_head,
        c.category,
        c.sub_category,
        c.per_liter,
        c.unit_of_measure,
        c.brand,
        c.item,
        c.sap_sku_name,
        c.po_status,
        c.distributor_margin,

        -- vendor_new mirrors vendor_name (the recalc used COALESCE(vendor_new, vendor_name))
        c.vendor_name AS vendor_new,

        -- per-row metrics --------------------------------------------------
        COALESCE(c.order_qty, 0) * COALESCE(c.per_liter, 0)        AS total_order_liters,
        COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0)    AS total_delivered_liters,
        COALESCE(c.order_qty, 0) * COALESCE(c.basic_rate, 0)       AS total_order_amt_exclusive,
        COALESCE(c.delivered_qty, 0) * COALESCE(c.basic_rate, 0)   AS total_delivered_amt_exclusive,
        COALESCE(c.order_qty, 0) * COALESCE(c.landing_rate, 0)     AS total_order_amt_inclusive,
        COALESCE(c.delivered_qty, 0) * COALESCE(c.landing_rate, 0) AS total_deliver_amt_inclusive,

        -- item status (FULL SUPPLIED / SHORT SUPPLIED / passthrough) -----
        CASE
            WHEN c.po_status = 'COMPLETED' AND COALESCE(c.delivered_qty, 0) < COALESCE(c.order_qty, 0)
                THEN 'SHORT SUPPLIED'
            WHEN c.po_status = 'COMPLETED' THEN 'FULL SUPPLIED'
            ELSE c.po_status
        END AS item_status,

        -- date helpers (from parsed date) --------------------------------
        CASE
            WHEN c._delivery_date_d IS NOT NULL AND c._po_date_d IS NOT NULL
                THEN (c._delivery_date_d - c._po_date_d)::integer
        END AS lead_time,
        CASE
            WHEN c._po_expiry_date_d IS NOT NULL
                THEN GREATEST((c._po_expiry_date_d - CURRENT_DATE)::integer, 0)
        END AS days_to_expiry,
        CASE
            WHEN c._po_expiry_date_d IS NOT NULL AND c._po_date_d IS NOT NULL
                THEN (c._po_expiry_date_d - c._po_date_d)::text
        END AS po_window,
        CASE WHEN c._po_date_d IS NULL THEN NULL
             ELSE UPPER(TO_CHAR(c._po_date_d, 'FMMonth')) END                              AS po_month,
        CASE WHEN c._delivery_date_d IS NULL THEN NULL
             ELSE UPPER(TO_CHAR(c._delivery_date_d, 'FMMonth')) END                        AS delivery_month,
        EXTRACT(YEAR FROM c._po_date_d)::integer                                           AS year,

        CASE
            WHEN c.po_status IN ('PENDING', 'APPOINTMENT DONE') THEN 'OPEN'
            ELSE 'CLOSED'
        END AS open_close,

        -- missed / filled --------------------------------------------------
        CASE
            WHEN c.po_status IN ('PENDING', 'CANCELLED', 'APPOINTMENT DONE') THEN 0
            WHEN c.po_status IN ('COMPLETED', 'EXPIRED')
                THEN COALESCE(c.order_qty, 0) - COALESCE(c.delivered_qty, 0)
        END AS missed_qty,
        COALESCE(c.delivered_qty, 0)                                                       AS filled_qty,
        CASE
            WHEN c.po_status IN ('PENDING', 'CANCELLED', 'APPOINTMENT DONE') THEN 0
            WHEN c.po_status IN ('COMPLETED', 'EXPIRED')
                THEN COALESCE(c.order_qty, 0) * COALESCE(c.per_liter, 0)
                   - COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0)
        END AS missed_ltrs,
        COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0)                            AS filled_ltrs,
        CASE
            WHEN c.po_status IN ('PENDING', 'CANCELLED', 'APPOINTMENT DONE') THEN 0
            WHEN c.po_status IN ('COMPLETED', 'EXPIRED')
                THEN COALESCE(c.order_qty, 0) * COALESCE(c.basic_rate, 0)
                   - COALESCE(c.delivered_qty, 0) * COALESCE(c.basic_rate, 0)
        END AS missed_amt,
        COALESCE(c.delivered_qty, 0) * COALESCE(c.basic_rate, 0)                           AS filled_amt,

        -- cancelled-aware metrics -----------------------------------------
        CASE WHEN c.po_status = 'CANCELLED' THEN 0 ELSE COALESCE(c.order_qty, 0) END       AS order_qty_cl,
        CASE WHEN c.po_status = 'CANCELLED' THEN 0
             ELSE COALESCE(c.order_qty, 0) * COALESCE(c.per_liter, 0) END                  AS order_ltrs_cl,
        CASE WHEN c.po_status = 'CANCELLED' THEN 0
             ELSE COALESCE(c.order_qty, 0) * COALESCE(c.basic_rate, 0) END                 AS order_amt_cl,

        -- without-margin amounts ------------------------------------------
        (COALESCE(c.order_qty, 0) * COALESCE(c.basic_rate, 0))
            / NULLIF(1 + c.distributor_margin, 0)                                          AS total_order_amt_without_margin,
        (COALESCE(c.delivered_qty, 0) * COALESCE(c.basic_rate, 0))
            / NULLIF(1 + c.distributor_margin, 0)                                          AS total_delivered_amt_without_margin,

        -- realise & commission --------------------------------------------
        CASE
            WHEN COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0) = 0 THEN 0
            ELSE (COALESCE(c.basic_rate, 0) / NULLIF(1 + c.distributor_margin, 0))
                 / NULLIF(c.per_liter, 0)
        END AS realise,
        CASE
            WHEN COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0) = 0 THEN 0
            ELSE COALESCE(c.basic_rate, 0) * c.distributor_margin
        END AS distributor_commission_per_unit,
        CASE
            WHEN COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0) = 0 THEN 0
            ELSE COALESCE(c.basic_rate, 0) * c.distributor_margin * COALESCE(c.delivered_qty, 0)
        END AS total_distributor_commission,

        -- city defaults to location -------------------------------------
        c.location AS city,

        -- fill / miss rate % (raw fractions; views.py multiplies if needed)
        COALESCE(c.delivered_qty, 0)
            / NULLIF(CASE WHEN c.po_status = 'CANCELLED' THEN 0 ELSE c.order_qty END, 0)
            AS qty_fill_rate_pct,
        (CASE
            WHEN c.po_status IN ('PENDING', 'CANCELLED', 'APPOINTMENT DONE') THEN 0
            WHEN c.po_status IN ('COMPLETED', 'EXPIRED')
                THEN COALESCE(c.order_qty, 0) - COALESCE(c.delivered_qty, 0)
        END) / NULLIF(CASE WHEN c.po_status = 'CANCELLED' THEN 0 ELSE c.order_qty END, 0)
            AS qty_miss_rate_pct,
        (COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0))
            / NULLIF(CASE WHEN c.po_status = 'CANCELLED'
                          THEN 0
                          ELSE COALESCE(c.order_qty, 0) * COALESCE(c.per_liter, 0)
                     END, 0)
            AS ltrs_fill_rate_pct,
        (CASE
            WHEN c.po_status IN ('PENDING', 'CANCELLED', 'APPOINTMENT DONE') THEN 0
            WHEN c.po_status IN ('COMPLETED', 'EXPIRED')
                THEN COALESCE(c.order_qty, 0) * COALESCE(c.per_liter, 0)
                   - COALESCE(c.delivered_qty, 0) * COALESCE(c.per_liter, 0)
        END) / NULLIF(CASE WHEN c.po_status = 'CANCELLED'
                          THEN 0
                          ELSE COALESCE(c.order_qty, 0) * COALESCE(c.per_liter, 0)
                     END, 0)
            AS ltrs_miss_rate_pct
    FROM calc c
)
SELECT * FROM final;

COMMENT ON VIEW public.prim_master_po IS
    'Live view over every <slug>_prim table with master-sheet attributes and derived metrics. Replaces the previous physical table; original snapshot lives in prim_master_po_legacy.';
"""


_REVERSE = r"""
DROP VIEW IF EXISTS public.prim_master_po;
DROP FUNCTION IF EXISTS public._pm_dmy_text(text);
DROP FUNCTION IF EXISTS public._pm_parse_date(text);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'public' AND tablename = 'prim_master_po_legacy'
    ) THEN
        EXECUTE 'ALTER TABLE public.prim_master_po_legacy RENAME TO prim_master_po';
    END IF;
END
$$;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0026_bigbasket_inventory_master_93147"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
