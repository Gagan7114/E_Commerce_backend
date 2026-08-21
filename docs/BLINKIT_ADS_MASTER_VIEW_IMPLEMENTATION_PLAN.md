# `blinkit_ads_master` View — Implementation Plan

**Status:** ✅ **IMPLEMENTED** as `backend/uploads/migrations/0028_blinkit_ads_master_view.py`.

> The plan originally proposed migration number 0027, but `0027_blinkit_ads_drop_keyword_cols` was created in the meantime (to clean up the 5 empty keyword columns from `blinkit_ads`). This view is therefore migration **0028** with dependency `('uploads', '0027_blinkit_ads_drop_keyword_cols')`.

Final decisions taken before implementation:
- `format` ← `blinkit_ads.format` constant `'BLINKIT'` (Choice A, §6.1)
- `ads_master_bs` join uses `(campaign_id, month_of_date)` — Design B (§6.2)
- View column names lowercase snake_case (§6.3)
- No `DISTINCT ON` wrapper — `master_sheet_format_sku_unique_idx` (migration 0018) is UNIQUE on `(format, format_sku_code)`, so the platform filter alone guarantees at most one BLINKIT row per SKU.

Verified live: 22 columns, row count parity with `blinkit_ads` (252/252), `SUM(ad_spent) = ₹906,814.20` matches the raw table total, **36 of 252 rows are mapped** (up from the expected ~14 — multiple months of mapping data are now in `ads_master_bs`). Sample row `(2026-05-17, campaign 45139 "Pomace Oil 1L")` enriches correctly: `format_sku_code=10143020`, `item='JIVO POMACE 1L'`, `item_head='PREMIUM'`, `per_ltr=1`, `ads_ltr_sold=270` (= 1 × 270 ✓).

## 1. Goal

Build a reporting view named `blinkit_ads_master` over the (now keyword-merged) `blinkit_ads` table. The view renames Blinkit's raw ad columns to the **same reporting vocabulary** used by `swiggy_ads_master`, attaches SKU metadata from `master_sheet` via the `ads_master_bs` bridge table, and adds derived date and per-litre columns.

This is the **fifth view in the family** — and uses the *bridge join* pattern (same as Swiggy), not the *direct join* pattern (Zepto / BigBasket). Why: Blinkit's ads export does NOT carry a SKU identifier at the row level — only `campaign_id` and `campaign_name`. So we need the `ads_master_bs` table to translate campaign → SKU.

| Platform   | View                  | Bridge needed?       | Join key                                    |
| ---------- | --------------------- | -------------------- | ------------------------------------------- |
| Amazon     | `amazon_ads_master`   | No                   | `advertised_product_id` → `format_sku_code` |
| **Swiggy** | **`swiggy_ads_master`** | **Yes (`ads_master_bs`)** | **`campaign_id` → bridge → `format_sku_code`** |
| Zepto      | `zepto_ads_master`    | No                   | `product_id` → `format_sku_code`            |
| BigBasket  | `bigbasket_ads_master`| No                   | `product_id` → `format_sku_code`            |
| **Blinkit**| **`blinkit_ads_master`** *(this plan)* | **Yes (`ads_master_bs`)** | **`campaign_id` → bridge → `format_sku_code`** |

```
blinkit_ads (merged: 1 row per campaign-day)
    ▼ campaign_id  ─────────►  ads_master_bs (campaign → sku mapping, per month)
                                    ▼ sku_id  ─────────►  master_sheet (SKU metadata)
                                                              │
                                                              ▼
                                                       blinkit_ads_master (this view)
```

## 2. Source tables

| Table                | Used for                                                                  |
| -------------------- | -------------------------------------------------------------------------- |
| `public.blinkit_ads` | Date, campaign id/name, direct/indirect qty sold, impressions, ad spent, direct/indirect GMV, format |
| `public.ads_master_bs` | Campaign-id → SKU-id mapping (added per month)                          |
| `public.master_sheet`  | SKU-level metadata (SAP name, category, item, item head, per-unit, per-ltr) |

### Excel reference

The user-spec mirrors the **"ADS MASTER RANGE"** sheet of `ADs SPENT (1).xlsx` — the same sheet that drove the Swiggy view. Formulas in row 2 of `ADS MASTER RANGE` were inspected directly during the Swiggy plan and are identical here (the FORMAT column simply contains `'BLINKIT'` instead of `'SWIGGY'`).

| Excel cell (ADS MASTER RANGE) | Formula                                                | Translates to (in our schema)                    |
| ----------------------------- | ------------------------------------------------------ | ------------------------------------------------ |
| `J2` (FORMAT SKU CODE)        | `=XLOOKUP(B2, 'MASTER SHEET'!V:V, 'MASTER SHEET'!W:W)` | `ads_master_bs.sku_id` keyed by `campaign_id`    |
| `K2` (SAP SKU NAME)           | `=XLOOKUP(J2, 'MASTER SHEET'!A:A, F:F)`                | `master_sheet.sku_sap_name`                      |
| `L2` (CATEGORY)               | `=XLOOKUP(J2, A:A, G:G)`                               | `master_sheet.category`                          |
| `M2` (SUB-CATEGORY)           | `=XLOOKUP(J2, A:A, H:H)`                               | `master_sheet.sub_category`                      |
| `N2` (ITEM)                   | `=XLOOKUP(J2, A:A, C:C)`                               | `master_sheet.item`                              |
| `O2` (ITEM HEAD)              | `=XLOOKUP(J2, A:A, K:K)`                               | `master_sheet.item_head`                         |
| `P2` (PER UNIT)               | `=XLOOKUP(J2, A:A, J:J)`                               | `master_sheet.per_unit`                          |
| `Q2` (PER LTR)                | `=XLOOKUP(J2, A:A, N:N)`                               | `master_sheet.per_unit_value`                    |
| `R2` (ADS LTR SOLD)           | `=Q2 * D2`                                             | `per_unit_value × direct_qty_sold`               |
| `S2` (REAL DATE) / T / U / V  | parse + format the DD-MM-YYYY string                   | derived from `blinkit_ads.date` (already typed)  |

## 3. User-specified column aliases (verified against `blinkit_ads` schema)

| User-spec output column | Source — `blinkit_ads` column |
| ----------------------- | ------------------------------ |
| `date`                  | `date`                         |
| `campaign_id`           | `campaign_id`                  |
| `campaign_name`         | `campaign_name`                |
| `direct_qty`            | `direct_qty_sold`              |
| `indirect_qty`          | `indirect_qty_sold`            |
| `impressions`           | `impression`                   |
| `ad_spent`              | `ad_spent` *(stores "Estimated Budget Consumed")* |
| `direct_gmv`            | `direct_gmv`                   |
| `indirect_gmv`          | `indirect_gmv`                 |
| `format`                | `format` *(constant `'BLINKIT'`)* |

All source columns verified to exist in `blinkit_ads` (migration `0014_blinkit_ads_full_dedup`).

> **Note on row granularity:** `blinkit_ads` now contains **one row per (date, campaign_id, campaign_name)** thanks to the keyword-merge logic added to the uploader. The view inherits that granularity — each output row is already a campaign-day, no further pre-aggregation needed.

## 4. Output columns (final view shape — 22 columns)

| #  | Column              | Type    | Source                                                              |
| -- | ------------------- | ------- | ------------------------------------------------------------------- |
| 1  | `date`              | DATE    | `blinkit_ads.date`                                                  |
| 2  | `campaign_id`       | TEXT    | `blinkit_ads.campaign_id`                                           |
| 3  | `campaign_name`     | TEXT    | `blinkit_ads.campaign_name`                                         |
| 4  | `direct_qty_sold`   | NUMERIC | `blinkit_ads.direct_qty_sold`                                       |
| 5  | `indirect_qty_sold` | NUMERIC | `blinkit_ads.indirect_qty_sold`                                     |
| 6  | `impressions`       | NUMERIC | `blinkit_ads.impression` *(note DB col is singular)*                |
| 7  | `ad_spent`          | NUMERIC | `blinkit_ads.ad_spent`                                              |
| 8  | `direct_gmv`        | NUMERIC | `blinkit_ads.direct_gmv`                                            |
| 9  | `indirect_gmv`      | NUMERIC | `blinkit_ads.indirect_gmv`                                          |
| 10 | `format`            | TEXT    | `blinkit_ads.format` *(always `'BLINKIT'`)* — see §5.1               |
| 11 | `format_sku_code`   | TEXT    | `ads_master_bs.sku_id` (joined by `campaign_id`)                    |
| 12 | `sap_sku_name`      | TEXT    | `master_sheet.sku_sap_name`                                         |
| 13 | `category`          | TEXT    | `master_sheet.category`                                             |
| 14 | `sub_category`      | TEXT    | `master_sheet.sub_category`                                         |
| 15 | `item`              | TEXT    | `master_sheet.item`                                                 |
| 16 | `item_head`         | TEXT    | `master_sheet.item_head`                                            |
| 17 | `per_unit`          | TEXT    | `master_sheet.per_unit`                                             |
| 18 | `per_ltr`           | NUMERIC | `master_sheet.per_unit_value`                                       |
| 19 | `ads_ltr_sold`      | NUMERIC | `per_unit_value × direct_qty_sold`                                  |
| 20 | `month`             | TEXT    | `UPPER(TO_CHAR(date, 'FMMonth'))` → e.g. `'MAY'`                    |
| 21 | `year`              | INT     | `EXTRACT(YEAR FROM date)::int`                                      |
| 22 | `month_day`         | TEXT    | `LPAD(DAY,2,'0') || '-' || month` → e.g. `'15-MAY'`                 |

## 5. SQL skeleton (DRAFT — for review, not for execution)

Same formulas as `swiggy_ads_master`, only the source table and the format-filter literals differ.

```sql
CREATE OR REPLACE VIEW public.blinkit_ads_master AS
SELECT
    -- ── Source columns from blinkit_ads ──
    b.date                                          AS date,
    b.campaign_id                                   AS campaign_id,
    b.campaign_name                                 AS campaign_name,
    b.direct_qty_sold                               AS direct_qty_sold,
    b.indirect_qty_sold                             AS indirect_qty_sold,
    b.impression                                    AS impressions,
    b.ad_spent                                      AS ad_spent,
    b.direct_gmv                                    AS direct_gmv,
    b.indirect_gmv                                  AS indirect_gmv,
    b.format                                        AS format,

    -- ── Joined from ads_master_bs → master_sheet ──
    amb.sku_id                                      AS format_sku_code,
    ms.sku_sap_name                                 AS sap_sku_name,
    ms.category                                     AS category,
    ms.sub_category                                 AS sub_category,
    ms.item                                         AS item,
    ms.item_head                                    AS item_head,
    ms.per_unit                                     AS per_unit,
    ms.per_unit_value                               AS per_ltr,

    -- ── Derived (identical formulas to swiggy_ads_master) ──
    (COALESCE(ms.per_unit_value, 0)
       * COALESCE(b.direct_qty_sold, 0))            AS ads_ltr_sold,
    UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
    EXTRACT(YEAR FROM b.date)::integer              AS year,
    (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
       || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

FROM public.blinkit_ads b

LEFT JOIN public.ads_master_bs amb
       ON amb.campaign_id = b.campaign_id
      AND amb.month       = UPPER(TO_CHAR(b.date, 'FMMonth'))
      AND REPLACE(UPPER(TRIM(amb.format::text)), ' ', '') = 'BLINKIT'

LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text))
        = UPPER(TRIM(amb.sku_id))
      AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BLINKIT';
```

### Reverse SQL
```sql
DROP VIEW IF EXISTS public.blinkit_ads_master;
```

> **Defensive normalization:** the JOIN filters use `REPLACE(UPPER(TRIM(format)), ' ', '')` instead of a literal `= 'BLINKIT'`. Pre-flight checks confirm `master_sheet` and `ads_master_bs` both currently use `'BLINKIT'` exactly, but the normalized comparison is what saved us during the BigBasket implementation (where `master_sheet.format = 'BIG BASKET'` with a space silently broke the literal-match filter). Costs nothing and prevents the same class of bug.

## 6. Edge cases & open questions

### 6.1 — `format` source: `blinkit_ads.format` or `master_sheet.format`?

Unlike Zepto / BigBasket where I recommended Choice A (pull from `master_sheet`), the Blinkit data model differs slightly:

| Choice | Behavior | Trade-off |
| ------ | -------- | --------- |
| **A. `blinkit_ads.format`** *(used in §5 draft — always `'BLINKIT'`)* | Constant. Never NULL even for unmapped campaigns. | Loses the master-sheet validation signal. |
| **B. `master_sheet.format`** *(matches Excel formula H)*               | NULL for unmapped campaigns; surfaces mapping gaps. | Many rows will be NULL until `ads_master_bs` is populated for every campaign. |

**Recommendation:** Choice A — `blinkit_ads.format` is reliable (set by the uploader, NOT NULL DEFAULT 'BLINKIT' in the schema), gives a clean filterable `format` column for downstream dashboards, and the master-sheet validation signal is already visible via the NULL state of the other 8 metadata columns. **User to confirm.**

### 6.2 — `ads_master_bs` join key: month or no month?

`ads_master_bs` has composite unique key `(month, campaign_id, sku_id)` and a per-row `format` column. We join on **`(campaign_id, month_of_date, format='BLINKIT')`** — the same Design B used by `swiggy_ads_master`. This lets the SKU mapping evolve over time (a campaign's May-2026 row looks up the May-2026 mapping, not whichever row sorts first).

Alternative (Design A from Swiggy plan): join on `(campaign_id, format='BLINKIT')` only, dropping the month filter. Simpler but cannot represent campaign-to-SKU remapping across months. Not recommended.

### 6.3 — Most blinkit_ads campaigns are CURRENTLY UNMAPPED in ads_master_bs

Pre-flight: `blinkit_ads` has 252 rows (14 campaigns × 18 days) but `ads_master_bs` has only **1** BLINKIT mapping row. So immediately after the migration runs, ~95%+ of view rows will have NULL `format_sku_code` and NULL metadata columns. The view is correct — `ads_master_bs` just needs more data.

**Action required AFTER migration:** open **Master → ADS Master BS** in the UI and add rows for each Blinkit `(month, campaign_id, sku_id)` mapping. Without these, the BigBasket-style by-item dashboard breakdown will fall entirely into the `(Unmapped)` bucket.

A reasonable starter set is the 14 distinct campaign IDs currently in `blinkit_ads`:
```
45138, 45139, 84482, 121611, 153799, 153800, 385807, 385850,
492584, 492585, …  (run: SELECT DISTINCT campaign_id, campaign_name FROM blinkit_ads)
```

### 6.4 — Multiple `master_sheet` rows per `(format='BLINKIT', format_sku_code)`

`master_sheet_format_sku_unique_idx` (migration `0018`) is UNIQUE on `(format, format_sku_code)`, so the platform filter alone guarantees at most one BLINKIT row per SKU. No `DISTINCT ON` wrapper needed.

### 6.5 — Multiple `blinkit_ads` rows per (date, campaign_id)

After the keyword-merge change in the uploader, `blinkit_ads` has at most one row per (date, campaign_id, campaign_name). The view inherits that. If a campaign_name ever changes mid-month, the same `campaign_id` could appear under two different `campaign_name` values for the same date → two rows would pass through. That's deliberate — preserves the rename history.

### 6.6 — Missing / unmapped SKUs

- Campaign not in `ads_master_bs` → `format_sku_code` is NULL, all 7 `master_sheet` metadata columns are NULL, `ads_ltr_sold` is 0.
- Campaign mapped to a `sku_id` not present in `master_sheet` (or under a different `format`) → same as above for the metadata columns; `ads_ltr_sold` is 0.

`COALESCE(ms.per_unit_value, 0)` keeps `ads_ltr_sold` non-NULL so downstream `SUM` aggregations don't break.

### 6.7 — Performance & indexes

| Index | Status | Purpose |
| ----- | ------ | ------- |
| `blinkit_ads` unique index on `(date, campaign_id, …)` | ✅ exists (migration 0014) | Speeds up date / campaign lookups |
| `ads_master_bs_campaign_idx` on `ads_master_bs.campaign_id` | ✅ exists (migration 0020) | Speeds up the bridge join |
| `ads_master_bs_dedup_idx` UNIQUE on `(month, campaign_id, sku_id)` | ✅ exists | Backs Design B without extra work |
| `master_sheet_format_sku_unique_idx` on `(format, format_sku_code)` | ✅ exists (migration 0018) | Speeds up + de-dups the master_sheet side |

No new indexes needed.

## 7. File / migration structure

Following the established pattern (`0017_amazon_ads_master_view`, `0021_swiggy_ads_master_view`, `0023_zepto_ads_master_view`, `0025_bigbasket_ads_master_view`):

| File                                                                  | Contents                                                       |
| --------------------------------------------------------------------- | -------------------------------------------------------------- |
| `backend/uploads/migrations/0027_blinkit_ads_master_view.py`          | `RunSQL` with `CREATE OR REPLACE VIEW` + `DROP VIEW` reverse.  |

Dependencies: `('uploads', '0026_bigbasket_ads_master_format_fix')`.

No model class — raw SQL via the existing `_dict_rows` helper.

## 8. Wiring (next-phase, out of scope for this plan)

After the view is created, the dashboard wiring follows the existing Swiggy / Zepto / BigBasket pattern verbatim — only names change:

1. `blinkit_ads_dashboard` endpoint in `platforms/views.py` querying `public.blinkit_ads_master` — KPIs (Ad Spent, Direct Qty Sold, Ads Ltr Sold) plus by-item breakdown.
2. Route in `platforms/urls.py` as `<slug:slug>/blinkit-ads-dashboard`.
3. Frontend: `PlatformBlinkitAdsDashboard.jsx` + new ADS NavGroup entry under Blinkit.

Explicitly **out of scope** for this document — this plan covers only the view.

## 9. Testing

Once the migration is applied:

| Check | SQL / expected |
| ----- | -------------- |
| View exists | `SELECT relname FROM pg_class WHERE relname='blinkit_ads_master'` returns 1 row |
| Column count | **22** columns |
| Row count matches base | `COUNT(*)` from `blinkit_ads_master` equals `COUNT(*)` from `blinkit_ads` (LEFT JOIN — no inflation if §6.4 holds; currently expect **252**) |
| Total Ad Spent matches | `SELECT SUM(ad_spent) FROM blinkit_ads_master` → `₹906,814.20` (matches the raw `blinkit_ads` total verified earlier) |
| Spot-check 1 mapped campaign | Whichever campaign is in `ads_master_bs` → expect `format_sku_code`, `sap_sku_name`, `item`, `per_ltr` populated; `ads_ltr_sold = per_ltr × direct_qty_sold`. |
| Spot-check unmapped campaign | The other ~13 campaign IDs → metadata cols all NULL, `ads_ltr_sold = 0`. |
| Date derivations | A `date='2026-05-15'` row → `month='MAY', year=2026, month_day='15-MAY'`. |

## 10. Decision summary — final

1. ✅ `format` ← `blinkit_ads.format` (Choice A in §6.1 — always `'BLINKIT'`, never NULL).
2. ✅ `ads_master_bs` join uses `(campaign_id, month_of_date)` — Design B (§6.2), same as Swiggy.
3. ✅ View column names lowercase snake_case (§6.3).
4. ✅ Populating `ads_master_bs` for unmapped Blinkit campaigns is a follow-up — view rows go live with NULL metadata where unmapped and enrich automatically as mappings are added. **At implementation time: 36 of 252 rows already enriched.**

Implemented as migration `0028_blinkit_ads_master_view.py`. Verified live: 22 columns, 252/252 row parity, `SUM(ad_spent) = ₹906,814.20` matches raw table total.

---

## Reference — all five ad master views side-by-side

| Output column     | Amazon source                | Swiggy source                       | Zepto source           | BigBasket source        | **Blinkit source**          |
| ----------------- | ---------------------------- | ----------------------------------- | ---------------------- | ----------------------- | --------------------------- |
| `date`            | `date`                       | `date`                              | `date`                 | `date`                  | **`date`**                  |
| `sku_id` / SKU    | `advertised_product_id`      | `ads_master_bs.sku_id` (via map)    | `product_id`           | `product_id`            | **`ads_master_bs.sku_id` (via map)** |
| `campaign_id`     | `campaign_id` *(extra)*      | `campaign_id`                       | `campaign_id` *(extra)*| `campaign_id` *(extra)* | **`campaign_id`**           |
| `direct_qty_sold` | `units_sold`                 | `total_conversions`                 | `same_skus`            | `orders_sku`            | **`direct_qty_sold`**       |
| `indirect_qty_sold`| n/a                         | n/a                                 | `other_skus`           | `other_sku_orders`      | **`indirect_qty_sold`**     |
| `impressions`     | `impressions`                | `total_impressions`                 | `impressions`          | `ad_impressions`        | **`impression`**            |
| `ad_spent`        | `total_cost`                 | `total_budget_burnt`                | `spend`                | `ad_spend`              | **`ad_spent`**              |
| `gmv` / sales     | `sales`                      | `total_gmv`                         | `revenue`              | `ad_revenue`            | **`direct_gmv` + `indirect_gmv` (kept separate)** |
| `ads_ltr_sold`    | `per_unit_value × units_sold`| `per_unit_value × total_conversions`| `per_unit_value × same_skus` | `per_unit_value × orders_sku` | **`per_unit_value × direct_qty_sold`** |

**Unique to Blinkit:** keeps `direct_gmv` and `indirect_gmv` as separate columns (other views collapse to a single `gmv`), and uses the `ads_master_bs` bridge (like Swiggy) rather than a direct master_sheet join.
