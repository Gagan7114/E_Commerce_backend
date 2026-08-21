# Flipkart State-Sales Master View — plan & mapping

## Goal
Create a database view **`flipkart_state_sales_master`** over the
**`flipkart_state_sales`** upload table that enriches every row with catalogue
attributes from **`master_sheet`**, joined on the Flipkart product id.

- Join key: `flipkart_state_sales.fsn`  ↔  `master_sheet.format_sku_code`
  (scoped to `master_sheet.format = 'FLIPKART'`).
- New columns added from `master_sheet`: **category, sub_category, item_head,
  per_unit_value, per_unit, item**.
- Everything already on `flipkart_state_sales` passes through unchanged.

This mirrors the existing master views (e.g. `amazon_sec_daily_master_view`),
which enrich an upload table with `master_sheet` attributes via a de-duplicated
lookup CTE.

---

## Source tables

### `flipkart_state_sales` (upload table — 60 data columns + `id`, `created_at`)
Flipkart B2C "Sales Report" GST export, stored verbatim as TEXT. Relevant here:
- **`fsn`** — Flipkart product id (the join key).
- plus `order_id, order_item_id, event_type, …, customer_delivery_state,
  item_quantity, final_invoice_amount, …` (all 60 pass through to the view).

### `master_sheet` (catalogue) — the six attributes to add
| `master_sheet` column | type | → view column |
|---|---|---|
| `category`        | text              | `category` |
| `sub_category`    | text              | `sub_category` |
| `item_head`       | text              | `item_head` |
| `per_unit_value`  | real              | `per_unit_value` |
| `per_unit`        | character varying | `per_unit` |
| `item`            | text              | `item` |

`master_sheet.format_sku_code` (varchar) is the per-format SKU code; for
`format = 'FLIPKART'` rows it holds the **FSN** (e.g. `EDOGTKKPP3TWKYSP`).

---

## The mapping (join key) — and the FSN-quote gotcha

`flipkart_state_sales.fsn` is stored **with embedded quote characters** from the
source file — e.g. `""EDOFZFUFCQDJHTQF""` — whereas `master_sheet.format_sku_code`
is the bare code `EDOFZFUFCQDJHTQF`. A literal/`TRIM` join therefore matches
**0 rows**.

Fix: normalise **both** sides to alphanumerics-only before comparing (FSNs are
strictly `[A-Z0-9]`, so this is lossless and also absorbs case/whitespace):

```sql
regexp_replace(upper(<col>::text), '[^A-Z0-9]+', '', 'g')
```

**Join condition**
```sql
regexp_replace(upper(f.fsn),                '[^A-Z0-9]+','','g')
  = regexp_replace(upper(m.format_sku_code), '[^A-Z0-9]+','','g')
AND upper(trim(m.format::text)) = 'FLIPKART'
```

### Coverage (measured on live data, 2026-06-18)
- `flipkart_state_sales`: **13,256 rows**, **102 distinct FSNs**.
- `master_sheet` FLIPKART rows: 276; `(format, format_sku_code)` is **unique**
  (792/792), so the join **cannot multiply rows**.
- Literal join: **0 / 13,256 (0%)** — blocked by the embedded quotes.
- Normalised join: **13,256 / 13,256 rows (100%)** and **102 / 102 FSNs (100%)** —
  zero unmatched.

> Optional follow-up (not required for the view): strip the surrounding quotes
> from `fsn` at upload time (the parser's `stripExcelQuote` left a doubled `""`),
> so the stored value is clean. The view's normalised join makes this cosmetic.

---

## New columns added in the master view
Exactly the six requested, all sourced from the matched `master_sheet` row
(NULL when an FSN has no FLIPKART catalogue entry — currently none):

`category`, `sub_category`, `item_head`, `per_unit_value`, `per_unit`, `item`.

## View shape
`flipkart_state_sales.*` (all 60 columns + `id`, `created_at`) **+** the six new
columns above.

---

## Proposed DDL

```sql
CREATE OR REPLACE VIEW public.flipkart_state_sales_master AS
WITH master_lookup AS (
    -- One catalogue row per normalised FLIPKART FSN. DISTINCT ON is a safety net;
    -- (format, format_sku_code) is already unique in master_sheet.
    SELECT DISTINCT ON (regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g'))
        regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g') AS fsn_key,
        category,
        sub_category,
        item_head,
        per_unit_value,
        per_unit,
        item
    FROM public.master_sheet
    WHERE upper(trim(format::text)) = 'FLIPKART'
      AND NULLIF(TRIM(format_sku_code::text), '') IS NOT NULL
    ORDER BY regexp_replace(upper(format_sku_code::text), '[^A-Z0-9]+', '', 'g')
)
SELECT
    f.*,
    ms.category,
    ms.sub_category,
    ms.item_head,
    ms.per_unit_value,
    ms.per_unit,
    ms.item
FROM public.flipkart_state_sales f
LEFT JOIN master_lookup ms
    ON ms.fsn_key = regexp_replace(upper(f.fsn), '[^A-Z0-9]+', '', 'g');
```

`LEFT JOIN` keeps every sales row even if its FSN is missing from the catalogue
(the six columns come back NULL).

---

## Migration plan
- New migration **`platforms/migrations/0046_flipkart_state_sales_master_view.py`**
  (master views live in `platforms`; the latest is `0045_secmaster_state_from_city_mapping`).
- `dependencies = [("platforms", "0045_secmaster_state_from_city_mapping"),
  ("uploads", "0054_flipkart_state_sales")]` so the base table exists.
- `RunSQL(forward = CREATE OR REPLACE VIEW …, reverse = DROP VIEW IF EXISTS …)`.
- Plain view (not materialised) — matches `amazon_sec_daily_master_view`; can be
  promoted to a matview later if a dashboard needs it.

## Notes / decisions
1. **Scope to `format = 'FLIPKART'`** so an FSN can't accidentally match a
   different platform's `format_sku_code`. (Confirmed unique within the format.)
2. **Numeric types:** `per_unit_value` stays `real` and `item_quantity` /
   amounts remain TEXT (as stored). Cast in downstream queries if you need to
   aggregate sales by state.
3. **No row multiplication** — verified the join is 1:1 on the catalogue side.
4. Not in scope unless requested: a dashboard endpoint over this view, or a
   materialised variant.

## Verification done for this plan
- Confirmed all six `master_sheet` columns exist with the types above.
- Confirmed 276 FLIPKART catalogue rows; `(format, format_sku_code)` unique.
- Confirmed the FSN-quote issue and that the normalised join yields **100%**
  coverage (13,256/13,256 rows; 102/102 FSNs; 0 unmatched).
