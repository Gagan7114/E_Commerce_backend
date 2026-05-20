# `zepto_ads_master` View — Implementation Plan

**Status:** ✅ **IMPLEMENTED** as `backend/uploads/migrations/0023_zepto_ads_master_view.py`.

Final decisions taken before implementation:
- `format` ← `master_sheet.format` filtered by `master_sheet.format = 'ZEPTO'` (Choice A, §5.2)
- `impressions` included as column #6 (§5.1)
- View column names lowercase snake_case (§5.4)
- No `DISTINCT ON` wrapper — `master_sheet_format_sku_unique_idx` is UNIQUE on `(format, format_sku_code)`, so the platform filter alone guarantees at most one ZEPTO row per SKU.

## 1. Goal

Reproduce the **"ZEPTO ADS RANGE"** sheet of `ADs SPENT (1).xlsx` as a Postgres view named `zepto_ads_master`. The view renames Zepto's raw ad columns to a consistent reporting vocabulary, attaches SKU metadata from `master_sheet`, and adds derived date and per-litre columns.

```
zepto_ads (raw upload, per upload date)
    ▼ product_id  ─────────►  master_sheet.format_sku_code (SKU metadata)
                                                              │
                                                              ▼
                                                       zepto_ads_master (this view)
```

> **Key difference from `swiggy_ads_master`:** Zepto's ads export already carries `product_id` natively, so there's **no `ads_master_bs` bridge required**. The join goes directly `zepto_ads.product_id → master_sheet.format_sku_code`.

## 2. Source tables

| Table                | Used for                                                                        |
| -------------------- | -------------------------------------------------------------------------------- |
| `public.zepto_ads`    | DATE, SKU ID, SKU NAME, direct/indirect qty sold, impressions, ad spent, GMV     |
| `public.master_sheet` | FORMAT, CATEGORY, SUB-CATEGORY, ITEM, ITEM HEAD, PER UNIT, PER LTR — by SKU code |

### Excel → DB column mapping (verified against the sheet's formulas)

Formulas in row 2 of `ZEPTO ADS RANGE` were inspected directly. Note that every XLOOKUP target column in `MASTER SHEET!A:A` is the **SKU code** column — there is NO campaign-id intermediate step for Zepto.

| Excel cell (ZEPTO ADS RANGE) | Formula                                                | Translates to (in our schema)                    |
| ---------------------------- | ------------------------------------------------------ | ------------------------------------------------ |
| `A2` (DATE)                  | raw text `'DD-MM-YYYY'`                                | `zepto_ads.date` (already a `DATE`)              |
| `B2` (SKU ID)                | raw value                                              | `zepto_ads.product_id`                           |
| `C2` (SKU NAME)              | raw value                                              | `zepto_ads.product_name`                         |
| `D2` (DIRECT QTY SOLD)       | raw value                                              | `zepto_ads.same_skus`                            |
| `E2` (INDIRECT QTY SOLD)     | raw value                                              | `zepto_ads.other_skus`                           |
| `F2` (AD SPENT)              | raw value                                              | `zepto_ads.spend`                                |
| `G2` (GMV)                   | raw value                                              | `zepto_ads.revenue`                              |
| `H2` (FORMAT)                | `=XLOOKUP(B2, 'MASTER SHEET'!A:A, 'MASTER SHEET'!D:D)` | `master_sheet.format`                            |
| `I2` (CATEGORY)              | `=XLOOKUP(B2, A:A, G:G)`                               | `master_sheet.category`                          |
| `J2` (SUB-CATEGORY)          | `=XLOOKUP(B2, A:A, H:H)`                               | `master_sheet.sub_category`                      |
| `K2` (ITEM)                  | `=XLOOKUP(B2, A:A, C:C)`                               | `master_sheet.item`                              |
| `L2` (ITEM HEAD)             | `=XLOOKUP(B2, A:A, K:K)`                               | `master_sheet.item_head`                         |
| `M2` (PER UNIT)              | `=XLOOKUP(B2, A:A, J:J)`                               | `master_sheet.per_unit` (text, e.g. `1 LTR`)     |
| `N2` (PER LTR)               | `=XLOOKUP(B2, A:A, N:N)`                               | `master_sheet.per_unit_value` (numeric, e.g. `1`)|
| `O2` (ADS LTR SOLD)          | `=N2 * D2`                                             | `per_unit_value × same_skus`                     |
| `P2` (REAL DATE)             | `=DATE(RIGHT(A2,4), MID(A2,4,2), LEFT(A2,2))`          | `zepto_ads.date` (already typed)                 |
| `Q2` (MONTH)                 | `=UPPER(TEXT(P2,"MMMM"))`                              | `UPPER(TO_CHAR(date, 'FMMonth'))`                |
| `R2` (YEAR)                  | `=RIGHT(P2,4)`                                         | `EXTRACT(YEAR FROM date)`                        |
| `S2` (MONTH-DAY)             | `=LEFT(A2,2) & "-" & Q2`                               | `LPAD(DAY,2,'0') || '-' || MONTH`                |

Spot-check row 2: `PER LTR = 1`, `DIRECT QTY SOLD = 480` → `ADS LTR SOLD = 1 × 480 = 480` ✓ (matches Excel).

## 3. Output columns (final view shape)

The Excel sheet does **not** include an IMPRESSIONS column, but the user has explicitly asked for it. We add it after INDIRECT QTY SOLD (its natural home), sourced from `zepto_ads.impressions`.

| #  | Column            | Type    | Source                                                              |
| -- | ----------------- | ------- | ------------------------------------------------------------------- |
| 1  | `date`            | DATE    | `zepto_ads.date`                                                    |
| 2  | `sku_id`          | TEXT    | `zepto_ads.product_id`                                              |
| 3  | `sku_name`        | TEXT    | `zepto_ads.product_name`                                            |
| 4  | `direct_qty_sold` | NUMERIC | `zepto_ads.same_skus`                                               |
| 5  | `indirect_qty_sold`| NUMERIC| `zepto_ads.other_skus`                                              |
| 6  | `impressions`     | NUMERIC | `zepto_ads.impressions` *(added — not in Excel sheet, see §5.1)*    |
| 7  | `ad_spent`        | NUMERIC | `zepto_ads.spend`                                                   |
| 8  | `gmv`             | NUMERIC | `zepto_ads.revenue`                                                 |
| 9  | `format`          | TEXT    | `master_sheet.format` *(see §5.2 — NOT `zepto_ads.format`)*         |
| 10 | `category`        | TEXT    | `master_sheet.category`                                             |
| 11 | `sub_category`    | TEXT    | `master_sheet.sub_category`                                         |
| 12 | `item`            | TEXT    | `master_sheet.item`                                                 |
| 13 | `item_head`       | TEXT    | `master_sheet.item_head`                                            |
| 14 | `per_unit`        | TEXT    | `master_sheet.per_unit`                                             |
| 15 | `per_ltr`         | NUMERIC | `master_sheet.per_unit_value`                                       |
| 16 | `ads_ltr_sold`    | NUMERIC | `per_unit_value × same_skus`                                        |
| 17 | `month`           | TEXT    | `UPPER(TO_CHAR(date, 'FMMonth'))` → `'FEBRUARY'`                    |
| 18 | `year`            | INT     | `EXTRACT(YEAR FROM date)::int`                                      |
| 19 | `month_day`       | TEXT    | `LPAD(DAY,2,'0') || '-' || month` → `'07-FEBRUARY'`                 |

Column order follows the user's spec (Excel grouping + `impressions` inserted at position 6).

## 4. SQL skeleton (DRAFT — for review, not for execution)

```sql
CREATE OR REPLACE VIEW public.zepto_ads_master AS
SELECT
    -- ── Source columns from zepto_ads (renamed to reporting vocabulary) ──
    z.date                                          AS date,
    z.product_id                                    AS sku_id,
    z.product_name                                  AS sku_name,
    z.same_skus                                     AS direct_qty_sold,
    z.other_skus                                    AS indirect_qty_sold,
    z.impressions                                   AS impressions,
    z.spend                                         AS ad_spent,
    z.revenue                                       AS gmv,

    -- ── Joined from master_sheet ──
    ms.format                                       AS format,
    ms.category                                     AS category,
    ms.sub_category                                 AS sub_category,
    ms.item                                         AS item,
    ms.item_head                                    AS item_head,
    ms.per_unit                                     AS per_unit,
    ms.per_unit_value                               AS per_ltr,

    -- ── Derived ──
    (COALESCE(ms.per_unit_value, 0)
       * COALESCE(z.same_skus, 0))                  AS ads_ltr_sold,
    UPPER(TO_CHAR(z.date, 'FMMonth'))               AS month,
    EXTRACT(YEAR FROM z.date)::integer              AS year,
    (LPAD(EXTRACT(DAY FROM z.date)::text, 2, '0')
       || '-' || UPPER(TO_CHAR(z.date, 'FMMonth'))) AS month_day

FROM public.zepto_ads z

LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text))
        = UPPER(TRIM(z.product_id))
      AND UPPER(TRIM(ms.format::text)) = 'ZEPTO';
```

### Reverse SQL
```sql
DROP VIEW IF EXISTS public.zepto_ads_master;
```

## 5. Edge cases & open questions

### 5.1 — `impressions` is an addition, not from the Excel sheet ⚠️

The Excel ZEPTO ADS RANGE sheet has no IMPRESSIONS column — it jumps from `INDIRECT QTY SOLD` (column E) directly to `AD SPENT` (column F). The user has explicitly asked for `impressions` to be added, so we pull it from `zepto_ads.impressions` and place it between INDIRECT QTY SOLD and AD SPENT. If you want the view to mirror the Excel exactly (no impressions), drop column #6.

### 5.2 — `format` source: `master_sheet.format` or `zepto_ads.format`?

The Excel sheet looks up FORMAT from MASTER SHEET (column H formula `=XLOOKUP(B2, A:A, D:D)`), even though every Zepto row is obviously `'ZEPTO'`. Two reasonable choices:

| Choice | Behavior | Trade-off |
| ------ | -------- | --------- |
| **A. `master_sheet.format`** *(used in §4 draft — matches Excel)* | Returns whatever the SKU is registered as in `master_sheet`. If a SKU is registered under `'BLINKIT'` instead of `'ZEPTO'`, the view shows `'BLINKIT'`. | Mirrors Excel exactly. Lets you catch SKU-mapping mistakes. May surface inconsistencies. |
| **B. `zepto_ads.format`** *(constant `'ZEPTO'`)* | Always returns `'ZEPTO'` — never NULL. | Cleaner downstream filtering; loses the master-sheet validation signal. |

Choice **A** also filters the join by `master_sheet.format = 'ZEPTO'` — this is important because a single `format_sku_code` value can exist in `master_sheet` under multiple format rows (different platforms map the same SKU). Without the platform filter, you'd risk picking the wrong row.

**Need user sign-off:** A or B?

### 5.3 — Multiple `master_sheet` rows per `format_sku_code`

If `master_sheet` has more than one row for the same `format_sku_code` under `format = 'ZEPTO'`, the `LEFT JOIN` will multiply rows. This is the same risk that exists for the Swiggy view. Mitigations:

- Rely on the unique index `master_sheet_format_sku_code_unique` (added in migration `0018`), which already prevents this for the master sheet.
- If the unique index is per-format rather than global, add `DISTINCT ON (z.id)` to the SELECT, or wrap the master_sheet join in a `DISTINCT ON (format_sku_code) ... ORDER BY format_sku_code, ctid` subquery.

**Action item:** verify behaviour of `master_sheet_format_sku_code_unique` — is it globally unique or per-(format, format_sku_code)?

### 5.4 — Multiple `zepto_ads` rows per (product_id, date)

`zepto_ads`'s unique key is `(date, product_id, campaign_id)`. A single product on a single date can appear in multiple rows — one per campaign. The view will emit ALL of them (correct — each row is a separate campaign). Any dashboard wanting "one row per product per date" must aggregate (`SUM(...) GROUP BY date, sku_id, sku_name`) on top of the view; the view does NOT pre-aggregate.

### 5.5 — Missing / unmapped SKUs

- If a Zepto `product_id` is not in `master_sheet` → all 7 metadata columns (format..per_ltr) come back NULL, and `ads_ltr_sold` is 0 (because of the COALESCE).
- If `master_sheet.per_unit_value` is NULL for a mapped SKU → `ads_ltr_sold` is 0.

`COALESCE(ms.per_unit_value, 0)` keeps `ads_ltr_sold` non-NULL so downstream `SUM` aggregations don't break.

### 5.6 — Performance & indexes

| Index | Status | Purpose |
| ----- | ------ | ------- |
| `zepto_ads_product_id_idx` on `zepto_ads.product_id` | ✅ exists (migration `0022`) | Speeds up the SKU-side of the join |
| `master_sheet_format_sku_code_unique` on `master_sheet.format_sku_code` | ✅ exists (migration `0018`) | Speeds up the master_sheet side; also prevents §5.3 duplication if globally unique |

No new indexes needed.

## 6. File / migration structure

Following the established pattern (`0017_amazon_ads_master_view.py`, `0021_swiggy_ads_master_view.py`):

| File                                                              | Contents                                                       |
| ----------------------------------------------------------------- | -------------------------------------------------------------- |
| `backend/uploads/migrations/0023_zepto_ads_master_view.py`        | `RunSQL` with `CREATE OR REPLACE VIEW` + `DROP VIEW` reverse.  |

Dependencies: `('uploads', '0022_zepto_ads')`.

No model class — the view is reached via raw SQL (the same `_dict_rows` helper pattern used for `amazon_ads_master` / `swiggy_ads_master`).

## 7. Wiring (next-phase, out of scope for this plan)

After the view is created, the standard wiring for the dashboard side would be:

1. Add `zepto_ads_dashboard` endpoint in `platforms/views.py` querying `public.zepto_ads_master` — KPIs (Ad Spent, Direct Qty Sold, Ads Ltr Sold, GMV) plus by-item / by-category breakdowns.
2. Add the route to `platforms/urls.py` as `<slug:slug>/zepto-ads-dashboard`.
3. Frontend: `PlatformZeptoAdsDashboard.jsx` + new ADS NavGroup entry under Zepto.

These are explicitly **out of scope** for this document — this plan covers only the view.

## 8. Testing

Once the migration is applied:

| Check | SQL / expected |
| ----- | -------------- |
| View exists | `SELECT relname FROM pg_class WHERE relname='zepto_ads_master'` returns 1 row |
| Column count | `\d+ public.zepto_ads_master` shows 19 columns |
| Row count matches base | `SELECT COUNT(*) FROM zepto_ads_master = SELECT COUNT(*) FROM zepto_ads` (LEFT JOIN — no inflation if §5.3 is satisfied) |
| Spot-check Excel parity | For `product_id = '41367ef4-80d9-4819-bbae-d1806c184f5c'` on 2026-02-07: expect `sku_name = 'Jivo Groundnut...'`, `direct_qty_sold = 480`, `per_ltr = 1`, `ads_ltr_sold = 480`, `month = 'FEBRUARY'`, `month_day = '07-FEBRUARY'`. |
| Unmapped SKU fallback | A product not in master_sheet → `format / category / item ...` all NULL, `ads_ltr_sold = 0`. |
| `format` filter | Every row's `format` is either `'ZEPTO'` or NULL (no cross-platform leakage). |

## 9. Decision summary — final

1. ✅ `format` ← `master_sheet.format`, filtered by `master_sheet.format = 'ZEPTO'` (Choice A in §5.2).
2. ✅ `impressions` included as column #6 (§5.1).
3. ✅ View column names lowercase snake_case (§5.4).
4. ✅ `master_sheet_format_sku_unique_idx` is UNIQUE on `(format, format_sku_code)` — the platform filter alone is sufficient, no `DISTINCT ON` wrapper needed (§5.3).

Implemented as migration `0023_zepto_ads_master_view.py`. Verified live: 19 columns, row count parity with `zepto_ads` (no JOIN inflation), sample rows show correct master_sheet metadata enrichment and `ads_ltr_sold = per_ltr × direct_qty_sold` math.
