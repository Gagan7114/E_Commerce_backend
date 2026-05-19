# `swiggy_ads_master` View — Implementation Plan

**Status:** ✅ **IMPLEMENTED** as `backend/uploads/migrations/0021_swiggy_ads_master_view.py`.

Final decisions taken before implementation:
- `direct_qty_sold` ← `total_conversions` (§5.1)
- `direct_gmv` ← `total_gmv` (§5.2, user-confirmed)
- `ads_master_bs` join uses `(campaign_id, month)` — Design B (§5.3)
- Both bridge/master joins filter to `format = 'SWIGGY'` so mappings for other platforms cannot duplicate Swiggy rows.
- View column names are lowercase snake_case (`format_sku_code`, `sap_sku_name`, `month_day`, …) matching the `amazon_ads_master` convention.

## 1. Goal

Reproduce the **"ADS MASTER RANGE"** sheet of `ADs SPENT (1).xlsx` as a Postgres view named `swiggy_ads_master`, restricted to Swiggy. The view aggregates spend / sales metrics from `swiggy_ads`, attaches SKU metadata from `master_sheet` via the `ads_master_bs` bridge table, and adds derived date and per-litre columns.

The user-facing data model becomes:

```
swiggy_ads (raw upload, per upload date)
    ▼ campaign_id  ─────────►  ads_master_bs (campaign → sku mapping, per month)
                                    ▼ sku_id  ─────────►  master_sheet (SKU metadata)
                                                              │
                                                              ▼
                                                       swiggy_ads_master (this view)
```

## 2. Source tables

| Table              | Used for                                                                  |
| ------------------ | -------------------------------------------------------------------------- |
| `public.swiggy_ads` | Date, campaign id/name, impressions, ad spent, direct GMV, qty, format     |
| `public.ads_master_bs` | Campaign-id → SKU-id mapping (added per month)                          |
| `public.master_sheet`  | SKU-level metadata (SAP name, category, item, item head, per-unit, per-ltr) |

### Excel → DB column mapping (verified against the sheet's formulas)

The Excel formulas in row 2 of `ADS MASTER RANGE` were inspected directly:

| Excel cell (ADS MASTER RANGE) | Formula                                                | Translates to (in our schema)                    |
| ----------------------------- | ------------------------------------------------------ | ------------------------------------------------ |
| `J2` (FORMAT SKU CODE)        | `=XLOOKUP(B2, 'MASTER SHEET'!V:V, 'MASTER SHEET'!W:W)` | `ads_master_bs.sku_id` keyed by `campaign_id`    |
| `K2` (SAP SKU NAME)           | `=XLOOKUP(J2, 'MASTER SHEET'!A:A, 'MASTER SHEET'!F:F)` | `master_sheet.sku_sap_name`                      |
| `L2` (CATEGORY)               | `=XLOOKUP(J2, A:A, G:G)`                               | `master_sheet.category`                          |
| `M2` (SUB-CATEGORY)           | `=XLOOKUP(J2, A:A, H:H)`                               | `master_sheet.sub_category`                      |
| `N2` (ITEM)                   | `=XLOOKUP(J2, A:A, C:C)`                               | `master_sheet.item`                              |
| `O2` (ITEM HEAD)              | `=XLOOKUP(J2, A:A, K:K)`                               | `master_sheet.item_head`                         |
| `P2` (PER UNIT)               | `=XLOOKUP(J2, A:A, J:J)`                               | `master_sheet.per_unit` (text, e.g. `6 LTR`)     |
| `Q2` (PER LTR)                | `=XLOOKUP(J2, A:A, N:N)`                               | `master_sheet.per_unit_value` (numeric, e.g. `6`)|
| `R2` (ADS LTR SOLD)           | `=Q2 * D2`                                             | `per_unit_value × direct_qty_sold`               |
| `S2` (REAL DATE)              | `=DATE(RIGHT(A2,4), MID(A2,4,2), LEFT(A2,2))`          | `swiggy_ads.date` (already a `DATE`)             |
| `T2` (MONTH)                  | `=UPPER(TEXT(S2,"MMMM"))`                              | `UPPER(TO_CHAR(date, 'FMMonth'))`                |
| `U2` (YEAR)                   | `=RIGHT(S2,4)`                                         | `EXTRACT(YEAR FROM date)`                        |
| `V2` (MONTH-DAY)              | `=LEFT(A2,2) & "-" & T2`                               | `LPAD(DAY,2,'0') || '-' || MONTH`                |

**Note on the BS lookup:** the Excel's `MASTER SHEET!V:V`/`W:W` columns are NOT part of the standard master_sheet schema in our DB — they are an Excel-only side-table (`CAMPAIGN ID` → `SKU ID`). We replicate this side-table as the dedicated `ads_master_bs` table that we already created.

## 3. Output columns (final view shape)

| #  | Column           | Type    | Source                                                              |
| -- | ---------------- | ------- | ------------------------------------------------------------------- |
| 1  | `date`           | DATE    | `swiggy_ads.date`                                                   |
| 2  | `campaign_id`    | TEXT    | `swiggy_ads.campaign_id`                                            |
| 3  | `campaign_name`  | TEXT    | `swiggy_ads.campaign_name`                                          |
| 4  | `direct_qty_sold`| NUMERIC | `swiggy_ads.total_conversions` *(see §5.1 — needs user confirmation)* |
| 5  | `impressions`    | NUMERIC | `swiggy_ads.total_impressions`                                      |
| 6  | `ad_spent`       | NUMERIC | `swiggy_ads.total_budget_burnt`                                     |
| 7  | `direct_gmv`     | NUMERIC | `swiggy_ads.total_gmv` *(user-decided — see §5.2)*                  |
| 8  | `format`         | TEXT    | `swiggy_ads.format` (always `'SWIGGY'`)                             |
| 9  | `format_sku_code`| TEXT    | `ads_master_bs.sku_id` (joined by `campaign_id`)                    |
| 10 | `sap_sku_name`   | TEXT    | `master_sheet.sku_sap_name` (joined by `format_sku_code`)           |
| 11 | `category`       | TEXT    | `master_sheet.category`                                             |
| 12 | `sub_category`   | TEXT    | `master_sheet.sub_category`                                         |
| 13 | `item`           | TEXT    | `master_sheet.item`                                                 |
| 14 | `item_head`      | TEXT    | `master_sheet.item_head`                                            |
| 15 | `per_unit`       | TEXT    | `master_sheet.per_unit`                                             |
| 16 | `per_ltr`        | NUMERIC | `master_sheet.per_unit_value`                                       |
| 17 | `ads_ltr_sold`   | NUMERIC | `per_unit_value × direct_qty_sold`                                  |
| 18 | `month`          | TEXT    | `UPPER(TO_CHAR(date, 'FMMonth'))` → `'FEBRUARY'`                    |
| 19 | `year`           | INT     | `EXTRACT(YEAR FROM date)::int`                                      |
| 20 | `month_day`      | TEXT    | `LPAD(EXTRACT(DAY FROM date)::text, 2, '0') || '-' || month` → `'12-FEBRUARY'` |

Column order intentionally follows the user's spec (Excel grouping), not the underlying tables.

## 4. SQL skeleton (DRAFT — for review, not for execution)

```sql
CREATE OR REPLACE VIEW public.swiggy_ads_master AS
SELECT
    -- ── Source columns from swiggy_ads ──
    s.date                                          AS date,
    s.campaign_id                                   AS campaign_id,
    s.campaign_name                                 AS campaign_name,
    s.total_conversions                             AS direct_qty_sold,   -- §5.1
    s.total_impressions                             AS impressions,
    s.total_budget_burnt                            AS ad_spent,
    s.total_gmv                                     AS direct_gmv,        -- §5.2 (total_gmv per user decision)
    s.format                                        AS format,

    -- ── Joined from ads_master_bs → master_sheet ──
    amb.sku_id                                      AS format_sku_code,
    ms.sku_sap_name                                 AS sap_sku_name,
    ms.category                                     AS category,
    ms.sub_category                                 AS sub_category,
    ms.item                                         AS item,
    ms.item_head                                    AS item_head,
    ms.per_unit                                     AS per_unit,
    ms.per_unit_value                               AS per_ltr,

    -- ── Derived ──
    (COALESCE(ms.per_unit_value, 0)
       * COALESCE(s.total_conversions, 0))          AS ads_ltr_sold,
    UPPER(TO_CHAR(s.date, 'FMMonth'))               AS month,
    EXTRACT(YEAR FROM s.date)::integer              AS year,
    (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0')
       || '-' || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day

FROM public.swiggy_ads s

LEFT JOIN public.ads_master_bs amb
       ON amb.campaign_id = s.campaign_id
      AND amb.month       = UPPER(TO_CHAR(s.date, 'FMMonth'))   -- §5.3
      AND UPPER(TRIM(amb.format::text)) = 'SWIGGY'

LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text))
        = UPPER(TRIM(amb.sku_id))
      AND UPPER(TRIM(ms.format::text)) = 'SWIGGY';
```

### Reverse SQL
```sql
DROP VIEW IF EXISTS public.swiggy_ads_master;
```

## 5. Edge cases & open questions

### 5.1 — `direct_qty_sold` ambiguity ⚠️

The Excel column `DIRECT QTY SOLD` does not have a 1:1 source in `swiggy_ads`. Swiggy Instamart's ads export does not include a literal "units sold" column. The candidates are:

| Candidate column                | Semantics                                          | Best for "DIRECT QTY SOLD"? |
| ------------------------------- | -------------------------------------------------- | --------------------------- |
| `total_conversions`             | Direct conversions in the attribution window       | ✅ Most likely               |
| `total_a2c`                     | Add-to-cart events (not actual sales)              | ❌ Pre-purchase              |

Recommendation: use `total_conversions`. **User to confirm before migration is written.**

### 5.2 — `direct_gmv` source ✅ DECIDED

`direct_gmv` ← `swiggy_ads.total_gmv`.

Swiggy exports also have `total_direct_gmv_7_days` and `total_direct_gmv_14_days` for attributed-window GMV, but the user chose the headline `total_gmv` column (campaign-level total GMV, no attribution window) to match what's reported externally.

### 5.3 — `ads_master_bs` join key: month or no month?

`ads_master_bs` has a composite unique key `(month, campaign_id, sku_id)`. The Excel does a simple `XLOOKUP(campaign_id, …)` — first match wins regardless of month. Two designs:

| Design | Behavior | Risk |
| ------ | -------- | ---- |
| **A. Join by `campaign_id` only** | Simpler. Matches Excel exactly. | If the same `campaign_id` is mapped to different SKUs in different months, the row "wins" arbitrarily and SKU metadata can drift. |
| **B. Join by `(campaign_id, month_of_date)`** *(preferred — used in §4 draft)* | A campaign's row for May 2026 looks up the May 2026 mapping. Lets the SKU mapping evolve over time. | Requires a BS row for every (campaign_id, month) that appears in `swiggy_ads`; otherwise the SKU columns are NULL. |

The draft uses **B**. If we go with **A**, change the `LEFT JOIN ads_master_bs amb` clause to drop the month filter and add `DISTINCT ON (campaign_id)` semantics (e.g. via a subquery or window).

### 5.4 — Multiple `swiggy_ads` rows per (campaign_id, date)

`swiggy_ads`'s unique key is `(date, campaign_id, keyword_count)`. A single campaign can have two rows for the same date — one with keywords and one without. The view will emit both rows (correct — they represent different placements). Any downstream dashboard that wants "one row per campaign per date" must do its own `SUM(...) GROUP BY` on top of the view; the view does NOT pre-aggregate.

### 5.5 — `master_sheet.format_sku_code` is a JSON-like field in some rows

`master_sheet.format_sku_code` is stored as TEXT. The `UPPER(TRIM(x::text))` cast handles non-text inputs defensively. This matches the existing `amazon_ads_master` view's join (migration `0017_amazon_ads_master_view.py`).

### 5.6 — Empty / missing mappings

- If a campaign has no `ads_master_bs` entry → `format_sku_code` is NULL, all `master_sheet` columns are NULL, `ads_ltr_sold` is 0.
- If a campaign maps to a `sku_id` not present in `master_sheet` → same as above for `master_sheet` columns; `ads_ltr_sold` is 0.

`COALESCE(ms.per_unit_value, 0)` keeps `ads_ltr_sold` non-NULL so downstream SUMs don't break.

### 5.7 — Performance & indexes

Existing indexes already cover the joins:

- `ads_master_bs_campaign_idx` on `ads_master_bs.campaign_id` ✓
- `ads_master_bs_dedup_idx` UNIQUE on `(month, campaign_id, sku_id)` ✓ (covers Design B)
- `master_sheet_format_sku_code_unique` (created in migration `0018`) ✓

No new indexes needed for the view to perform.

## 6. File / migration structure

Following the established pattern (e.g. `0017_amazon_ads_master_view.py`):

| File                                                                         | Contents                                                       |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `backend/uploads/migrations/0021_swiggy_ads_master_view.py`                 | `RunSQL` with `CREATE OR REPLACE VIEW` + `DROP VIEW` reverse.  |

Dependencies: `('uploads', '0020_ads_master_bs')`.

No model class is needed — the view is reached via raw SQL (the existing `_dict_rows` helper in `platforms/views.py` already handles this for the Amazon equivalent).

## 7. Wiring (next-phase, out of scope for this plan)

After the view is created, the standard wiring for the dashboard side would be:

1. Add `swiggy_ads_dashboard` endpoint in `platforms/views.py` querying `public.swiggy_ads_master` — KPIs (Total Spend, Units, Sales, ROAS) plus by-portfolio / by-category breakdowns.
2. Add the route to `platforms/urls.py` as `<slug:slug>/swiggy-ads-dashboard`.
3. Frontend: `PlatformSwiggyAdsDashboard.jsx` + new entry under Swiggy's "ADS" nav group.

These are explicitly **out of scope** for this document — this plan covers only the view.

## 8. Testing

Once the migration is applied:

| Check | SQL / expected |
| ----- | -------------- |
| View exists | `\d+ public.swiggy_ads_master` (or `SELECT relname FROM pg_class WHERE relname='swiggy_ads_master'`) |
| Row count matches base table | `SELECT COUNT(*) FROM swiggy_ads_master;` equals `SELECT COUNT(*) FROM swiggy_ads;` |
| Spot-check a known campaign | Pick a `(campaign_id, date)` you mapped in `ads_master_bs` → verify `format_sku_code`, `sap_sku_name`, `category` populated. |
| Compute parity with Excel | For 1–2 sample rows from the user's Excel: `ads_ltr_sold = per_ltr × direct_qty_sold`. Excel row 2: `6 × 174 = 1044` ✓ |
| Null fallbacks | A campaign with no BS mapping → metadata columns NULL, `ads_ltr_sold = 0`. |
| Month/year strings | `2026-02-12 → month='FEBRUARY', year=2026, month_day='12-FEBRUARY'` |

## 9. Decision summary — final

1. ✅ `direct_qty_sold` ← `total_conversions` (§5.1).
2. ✅ `direct_gmv` ← `total_gmv` (§5.2, user-confirmed).
3. ✅ `ads_master_bs` join uses `(campaign_id, month)` — Design B (§5.3).
4. ✅ View columns are lowercase snake_case, matching `amazon_ads_master`.

Implemented as migration `0021_swiggy_ads_master_view.py`.
