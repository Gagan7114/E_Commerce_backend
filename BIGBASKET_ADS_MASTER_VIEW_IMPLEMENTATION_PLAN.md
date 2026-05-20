# `bigbasket_ads_master` View — Implementation Plan

**Status:** ✅ **IMPLEMENTED** as `backend/uploads/migrations/0025_bigbasket_ads_master_view.py`.

Final decisions taken before implementation:
- `format` ← `master_sheet.format` filtered by `master_sheet.format = 'BIGBASKET'` (Choice A, §5.1)
- `impressions` ← `bigbasket_ads.ad_impressions` (§5.5 — unambiguous mapping)
- View column names lowercase snake_case
- No `DISTINCT ON` wrapper — `master_sheet_format_sku_unique_idx` (migration 0018) is UNIQUE on `(format, format_sku_code)`, so the platform filter alone guarantees at most one BIGBASKET row per SKU.

> **Note on metadata enrichment:** verified live with the 10-row `bb.xlsx` upload — all metadata columns (`format`, `category`, `item_head`, `per_unit`, `per_ltr`) come back NULL because no BigBasket product IDs are registered in `master_sheet` under `format='BIGBASKET'` yet. Add the mapping rows in **Master → Master Sheet** (or directly into `master_sheet` with `format='BIGBASKET'` and `format_sku_code = <BigBasket product_id>`) for the enrichment to flow through. This mirrors how the Zepto rows had to be added before its view enriched.

## 1. Goal

Build a reporting view named `bigbasket_ads_master` over the `bigbasket_ads` raw upload table. The view renames BigBasket's raw ad columns to the **same reporting vocabulary** used by `zepto_ads_master` (per the user's spec), attaches SKU metadata from `master_sheet`, and adds derived date and per-litre columns.

This is the **fourth view in the family** — same shape and same formulas as the existing platforms:

| Platform   | View                  | Bridge needed?    | Join key                                    |
| ---------- | --------------------- | ----------------- | ------------------------------------------- |
| Amazon     | `amazon_ads_master`   | No                | `advertised_product_id` → `format_sku_code` |
| Swiggy     | `swiggy_ads_master`   | Yes (`ads_master_bs`) | `campaign_id → ads_master_bs.sku_id → format_sku_code` |
| Zepto      | `zepto_ads_master`    | No                | `product_id` → `format_sku_code`            |
| **BigBasket** | **`bigbasket_ads_master`** *(this plan)* | **No** | **`product_id` → `format_sku_code`** |

```
bigbasket_ads (raw upload, per upload date)
    ▼ product_id  ─────────►  master_sheet.format_sku_code (SKU metadata)
                                                              │
                                                              ▼
                                                       bigbasket_ads_master
```

> **Key parity with Zepto:** BigBasket's ads export already carries `product_id` natively (BigBasket's SKU-level identifier), so the join goes **directly** to `master_sheet` with no `ads_master_bs` bridge required.

## 2. Source tables

| Table                | Used for                                                                        |
| -------------------- | -------------------------------------------------------------------------------- |
| `public.bigbasket_ads` | DATE, SKU ID, SKU NAME, direct/indirect qty sold, impressions, ad spent, GMV     |
| `public.master_sheet`  | FORMAT, CATEGORY, SUB-CATEGORY, ITEM, ITEM HEAD, PER UNIT, PER LTR — by SKU code |

### User-specified column aliases (verified against `bigbasket_ads` schema)

| User-spec output column | Source — `bigbasket_ads` column |
| ----------------------- | -------------------------------- |
| `date`                  | `date`                           |
| `sku_id`                | `product_id`                     |
| `sku_name`              | `product_name`                   |
| `direct_qty_sold`       | `orders_sku`                     |
| `indirect_qty_sold`     | `other_sku_orders`               |
| `impressions`           | `ad_impressions`                 |
| `ad_spent`              | `ad_spend`                       |
| `gmv`                   | `ad_revenue`                     |

These columns are guaranteed to exist (verified during migration `0024_bigbasket_ads`).

### Master-sheet join semantics (same as Zepto)

```
LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(z.product_id))
      AND UPPER(TRIM(ms.format::text)) = 'BIGBASKET'
```

The format filter prevents picking up the same SKU's row registered under another platform — exactly the safeguard we used for Zepto.

## 3. Output columns (final view shape)

| #  | Column            | Type    | Source                                                              |
| -- | ----------------- | ------- | ------------------------------------------------------------------- |
| 1  | `date`            | DATE    | `bigbasket_ads.date`                                                |
| 2  | `sku_id`          | TEXT    | `bigbasket_ads.product_id`                                          |
| 3  | `sku_name`        | TEXT    | `bigbasket_ads.product_name`                                        |
| 4  | `direct_qty_sold` | NUMERIC | `bigbasket_ads.orders_sku`                                          |
| 5  | `indirect_qty_sold`| NUMERIC| `bigbasket_ads.other_sku_orders`                                    |
| 6  | `impressions`     | NUMERIC | `bigbasket_ads.ad_impressions`                                      |
| 7  | `ad_spent`        | NUMERIC | `bigbasket_ads.ad_spend`                                            |
| 8  | `gmv`             | NUMERIC | `bigbasket_ads.ad_revenue`                                          |
| 9  | `format`          | TEXT    | `master_sheet.format` (filtered to `'BIGBASKET'` — see §5.1)        |
| 10 | `category`        | TEXT    | `master_sheet.category`                                             |
| 11 | `sub_category`    | TEXT    | `master_sheet.sub_category`                                         |
| 12 | `item`            | TEXT    | `master_sheet.item`                                                 |
| 13 | `item_head`       | TEXT    | `master_sheet.item_head`                                            |
| 14 | `per_unit`        | TEXT    | `master_sheet.per_unit`                                             |
| 15 | `per_ltr`         | NUMERIC | `master_sheet.per_unit_value`                                       |
| 16 | `ads_ltr_sold`    | NUMERIC | `per_unit_value × orders_sku` (= PER LTR × DIRECT QTY SOLD)         |
| 17 | `month`           | TEXT    | `UPPER(TO_CHAR(date, 'FMMonth'))` → e.g. `'MAY'`                    |
| 18 | `year`            | INT     | `EXTRACT(YEAR FROM date)::int`                                      |
| 19 | `month_day`       | TEXT    | `LPAD(DAY,2,'0') || '-' || month` → e.g. `'19-MAY'`                 |

**19 columns total — identical shape to `zepto_ads_master`.**

## 4. SQL skeleton (DRAFT — for review, not for execution)

Same formulas as `zepto_ads_master`, only the source table and the `format` filter differ.

```sql
CREATE OR REPLACE VIEW public.bigbasket_ads_master AS
SELECT
    -- ── Source columns from bigbasket_ads (renamed to reporting vocabulary) ──
    b.date                                          AS date,
    b.product_id                                    AS sku_id,
    b.product_name                                  AS sku_name,
    b.orders_sku                                    AS direct_qty_sold,
    b.other_sku_orders                              AS indirect_qty_sold,
    b.ad_impressions                                AS impressions,
    b.ad_spend                                      AS ad_spent,
    b.ad_revenue                                    AS gmv,

    -- ── Joined from master_sheet (BIGBASKET rows only) ──
    ms.format                                       AS format,
    ms.category                                     AS category,
    ms.sub_category                                 AS sub_category,
    ms.item                                         AS item,
    ms.item_head                                    AS item_head,
    ms.per_unit                                     AS per_unit,
    ms.per_unit_value                               AS per_ltr,

    -- ── Derived (identical formulas to zepto_ads_master) ──
    (COALESCE(ms.per_unit_value, 0)
       * COALESCE(b.orders_sku, 0))                 AS ads_ltr_sold,
    UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
    EXTRACT(YEAR FROM b.date)::integer              AS year,
    (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
       || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

FROM public.bigbasket_ads b

LEFT JOIN public.master_sheet ms
       ON UPPER(TRIM(ms.format_sku_code::text))
        = UPPER(TRIM(b.product_id))
      AND UPPER(TRIM(ms.format::text)) = 'BIGBASKET';
```

### Reverse SQL
```sql
DROP VIEW IF EXISTS public.bigbasket_ads_master;
```

## 5. Edge cases & open questions

### 5.1 — `format` source: `master_sheet.format` or `bigbasket_ads.format`?

Same decision as Zepto. Two options:

| Choice | Behavior | Trade-off |
| ------ | -------- | --------- |
| **A. `master_sheet.format`** *(used in §4 draft — matches Zepto choice)* | Returns whatever the SKU is registered as in `master_sheet`. NULL if the SKU isn't mapped to `'BIGBASKET'`. | Catches SKU-mapping gaps. Lets the dashboard show "(Unmapped)" for unknown SKUs. |
| **B. `bigbasket_ads.format`** *(constant `'BIGBASKET'`)* | Always `'BIGBASKET'`, never NULL. | Cleaner downstream filtering; loses the master-sheet validation signal. |

**Recommendation:** Choice A — matches Zepto convention and lets the dashboard surface unmapped SKUs.

### 5.2 — Multiple `master_sheet` rows per `format_sku_code`

Already resolved during Zepto implementation: `master_sheet_format_sku_unique_idx` is UNIQUE on `(format, format_sku_code)` — so the `master_sheet.format = 'BIGBASKET'` filter guarantees at most one row per BigBasket SKU. No `DISTINCT ON` wrapper needed.

### 5.3 — Multiple `bigbasket_ads` rows per (product_id, date)

`bigbasket_ads`'s unique key is `(date, product_id, campaign_id)`. The same product on the same date can appear under multiple campaigns (e.g. SPA + Sponsored Brands when BigBasket adds those reports). The view emits ALL such rows (correct — each row is a separate campaign × product slice). Any dashboard wanting "one row per product per date" must `SUM(...) GROUP BY date, sku_id` on top of the view; the view does NOT pre-aggregate. Today's file is SPA-only so this is moot, but the view is future-proof.

### 5.4 — Missing / unmapped SKUs

- If a `product_id` is not in `master_sheet` (or not under `'BIGBASKET'`) → all 7 metadata columns (format..per_ltr) come back NULL, and `ads_ltr_sold` is 0 (because of the COALESCE).
- If `master_sheet.per_unit_value` is NULL for a mapped SKU → `ads_ltr_sold` is 0.

`COALESCE(ms.per_unit_value, 0)` keeps `ads_ltr_sold` non-NULL so downstream `SUM` aggregations don't break.

### 5.5 — `impressions` column

The user's spec includes `impressions` (sourced from `bigbasket_ads.ad_impressions`). Unlike the Zepto plan §5.1 disclaimer (Excel sheet had no impressions column), BigBasket's source export **does** include `Ad Impressions`, so this is a 1:1 mapping with no judgement call required.

### 5.6 — Performance & indexes

| Index | Status | Purpose |
| ----- | ------ | ------- |
| `bigbasket_ads_product_id_idx` on `bigbasket_ads.product_id` | ✅ exists (migration `0024`) | Speeds up the SKU-side of the join |
| `master_sheet_format_sku_unique_idx` on `master_sheet.(format, format_sku_code)` | ✅ exists (migration `0018`) | Speeds up the master_sheet side; also enforces §5.2 uniqueness |

No new indexes needed.

## 6. File / migration structure

Following the established pattern (`0017_amazon_ads_master_view`, `0021_swiggy_ads_master_view`, `0023_zepto_ads_master_view`):

| File                                                                  | Contents                                                       |
| --------------------------------------------------------------------- | -------------------------------------------------------------- |
| `backend/uploads/migrations/0025_bigbasket_ads_master_view.py`        | `RunSQL` with `CREATE OR REPLACE VIEW` + `DROP VIEW` reverse.  |

Dependencies: `('uploads', '0024_bigbasket_ads')`.

No model class — the view is reached via raw SQL (the same `_dict_rows` helper pattern used for the existing `*_ads_master` views).

## 7. Wiring (next-phase, out of scope for this plan)

After the view is created, the standard dashboard wiring would mirror the existing Zepto / Swiggy dashboard endpoints:

1. Add `bigbasket_ads_dashboard` endpoint in `platforms/views.py` querying `public.bigbasket_ads_master` — KPIs (Ad Spent, Direct Qty Sold, Ads Ltr Sold) plus by-item / by-category breakdowns.
2. Add the route to `platforms/urls.py` as `<slug:slug>/bigbasket-ads-dashboard`.
3. Frontend: `PlatformBigBasketAdsDashboard.jsx` + new ADS NavGroup entry under BigBasket.

Explicitly **out of scope** for this document — this plan covers only the view.

## 8. Testing

Once the migration is applied:

| Check | SQL / expected |
| ----- | -------------- |
| View exists | `SELECT relname FROM pg_class WHERE relname='bigbasket_ads_master'` returns 1 row |
| Column count | 19 columns |
| Row count matches base | `COUNT(*)` from `bigbasket_ads_master` equals `COUNT(*)` from `bigbasket_ads` (LEFT JOIN — no inflation if §5.2 holds) |
| Spot-check `bb.xlsx` row 2 | `product_id='40166397'` (Jivo Extra Light Olive Oil Tin 5L) → expect `sku_name='Jivo Extra Light Olive Oil Tin 5 L'`, `direct_qty_sold=0`, `impressions=447`, `ad_spent=1236.26`, `gmv=0`. If `40166397` is mapped in master_sheet under BIGBASKET, also expect `per_ltr=5`, `ads_ltr_sold=0` (0 orders × 5). |
| Spot-check `bb.xlsx` row 3 | `product_id='40250808'` (Extra Virgin 1L) → `direct_qty_sold=38`, `impressions=3143`, `ad_spent=8928.54`, `gmv=30759`. If mapped, `per_ltr=1`, `ads_ltr_sold=38`. |
| Unmapped SKU fallback | A product not in master_sheet → `format / category / item ...` all NULL, `ads_ltr_sold = 0`. |
| `format` filter | Every populated `format` is `'BIGBASKET'` (no cross-platform leakage). |
| Date derivations | A `date='2026-05-19'` row → `month='MAY', year=2026, month_day='19-MAY'`. |

## 9. Decision summary — final

1. ✅ `format` ← `master_sheet.format`, filtered by `master_sheet.format = 'BIGBASKET'` (Choice A in §5.1).
2. ✅ View column names lowercase snake_case.
3. ✅ `impressions` ← `ad_impressions` (§5.5).

Implemented as migration `0025_bigbasket_ads_master_view.py`. Verified live: 19 columns, row count parity with `bigbasket_ads` (10/10, no JOIN inflation), sample rows show correct aliases and date derivations (`month='APRIL', year=2026, month_day='30-APRIL'`).

---

## Reference — same column family across all four ad master views

| Output column     | Amazon source                | Swiggy source                       | Zepto source           | **BigBasket source**          |
| ----------------- | ---------------------------- | ----------------------------------- | ---------------------- | ----------------------------- |
| `date`            | `date`                       | `date`                              | `date`                 | `date`                        |
| `sku_id` / SKU    | `advertised_product_id`      | `ads_master_bs.sku_id` (via map)    | `product_id`           | **`product_id`**              |
| `sku_name`        | `advertised_product_sku` *(approx)* | `master_sheet.sku_sap_name`  | `product_name`         | **`product_name`**            |
| `direct_qty_sold` | `units_sold`                 | `total_conversions`                 | `same_skus`            | **`orders_sku`**              |
| `indirect_qty_sold` | n/a                        | n/a                                 | `other_skus`           | **`other_sku_orders`**        |
| `impressions`     | `impressions`                | `total_impressions`                 | `impressions`          | **`ad_impressions`**          |
| `ad_spent`        | `total_cost`                 | `total_budget_burnt`                | `spend`                | **`ad_spend`**                |
| `gmv` / sales     | `sales`                      | `total_gmv`                         | `revenue`              | **`ad_revenue`**              |
| `ads_ltr_sold`    | `per_unit_value × units_sold`| `per_unit_value × total_conversions`| `per_unit_value × same_skus` | **`per_unit_value × orders_sku`** |
| `month`/`year`/`month_day` | from `date` via TO_CHAR / EXTRACT — identical SQL across all four |

The downstream BigBasket Ads dashboard can reuse the same Zepto / Swiggy frontend page structure verbatim — KPIs are `ad_spent` / `direct_qty_sold` / `ads_ltr_sold` and the table is by `item`.
