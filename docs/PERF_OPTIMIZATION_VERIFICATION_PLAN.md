# Performance Optimization — Verification Plan (run BEFORE any code change)

Goal: prove every remaining query optimization returns **exactly the same results** as the
current code, then implement only the proven-safe ones. **No SQL or business logic has been
changed yet.** This document is the gate.

Every statement here is read-only (SELECT / EXPLAIN). `EXPLAIN (ANALYZE, BUFFERS)` *executes*
the query (read-only) — run the heavy ones during low traffic. Replace every `-- ADJUST`
literal with a real value (defaults assume today = 2026-06-26: month `JUNE`/`6`, year `2026`,
normalized format `blinkit`, June window `2026-06-01`..`2026-06-30`).

---

## 0. Workflow per item

1. Run the **global prechecks** (§1) once — they decide which risk tier each fix falls in.
2. Run the item's **current-query EXPLAIN** → capture the baseline plan + timing.
3. Run the item's **optimized EXPLAIN** → confirm the plan switches to an Index/Bitmap scan and is faster.
4. Run the item's **equivalence proof** (the bidirectional `EXCEPT`) → it **MUST return ZERO rows**.
5. Honor the **decision gate** — if a gate query returns rows, that change does **not** ship.
6. Only after 3–5 pass: implement, re-run the EXCEPT on the implemented query, deploy.

## 1. Global prechecks (run once, copy the outputs back)

```sql
-- (P1) Column types that decide several fixes:
\d secmaster_mv      -- confirm: is "year" integer/numeric or text?  is "date" date or text?
\d master_po_mv      -- confirm: is "po_date" a real DATE column? is "days_to_expiry" a stored int?
                     -- (master_po is a pass-through VIEW over master_po_mv)

-- (P2) Status domain for every  status != 'rejected' -> status IN (...)  rewrite (items S1, S2):
SELECT DISTINCT status FROM sp_shipments ORDER BY 1;
-- MUST be a subset of: draft, pending_approval, approved, rejected, dispatched, in_transit, delivered
-- If any NULL or unexpected value appears, the IN-list rewrite is NOT equivalent — stop.

-- (P3) Multi-PO gate for the appointment_summary unnest removal (item U2):
SELECT COUNT(*) AS multi_po_rows FROM reporting."appointment" WHERE pos ~ '[,;]';
-- MUST be 0. If > 0, the unnest still does real work — do NOT remove it.
```

### Reusable equivalence-proof template

For a query whose result we are changing, paste the **original** statement body into `old` and
the **optimized** body into `new`, then:

```sql
WITH old AS ( /* ORIGINAL query body */ ),
     new AS ( /* OPTIMIZED query body */ )
SELECT 'old-not-in-new' AS tag, * FROM (SELECT * FROM old EXCEPT SELECT * FROM new) a
UNION ALL
SELECT 'new-not-in-old' AS tag, * FROM (SELECT * FROM new EXCEPT SELECT * FROM old) b;
-- PASS = ZERO rows. Wrap float SUM/aggregate columns in ROUND(x, 4) if float noise appears.
```

For **pure index-adds** (no query text change) there is nothing to diff — equivalence is *by
construction*; verification = "confirm the EXPLAIN plan switches to the new index and timing drops".

---

## 2. Priority (implement in this order — gain ÷ risk)

| # | Item | Area | Fix type | Gain | Risk | Gate |
|---|------|------|----------|------|------|------|
| **D1** | `platform_expiry_alerts` | dashboard | **pure index-add** (partial) | High | **Low** | none (by construction) |
| **D2** | `state_sales_detail` probe | dashboard | retarget live view → matview | High | **Low** | EXCEPT zero |
| **S1** | `AppointmentListView` correlated `NOT EXISTS` | shipment | anti-join (already-used shape) | High | **Low** | P2 + EXCEPT zero |
| **P1** | `"year"::numeric` → `"year"` | platforms/dashboard secmaster | sargability rewrite | High | **Low** | P1 + EXCEPT zero |
| **U2** | `appointment_summary` unnest | uploads | drop redundant unnest → index join | Med | **Low** | **P3** + EXCEPT zero |
| **S2** | `AppointmentItemsView` `status != 'rejected'` | shipment | IN-list sargability | Med | **Low** | P2 + EXCEPT zero |
| **U1** | `amazon_po_summary` 10 scans | uploads | GROUPING SETS, 8→1 scan | Med-High | **Low** | EXCEPT zero per dim |
| **D4** | `fulfilment_health` `_pm_parse_date` | dashboard | index stored `po_date`, compare column | High | Low¹ | P1 + EXCEPT zero |
| **D3** | `category_sku_breakdown` per-month loop | dashboard | single `(month,year) IN (...)` | High | **Med** | EXCEPT zero + Python bucketing |
| **P2** | DRR `item_rows` / `_build_sec_keyed_trend` | platforms | add month/year bound | High | **Med** | EXCEPT zero (data-consistency) |
| **X1** | SAP `inventory_overview` | sap (HANA) | 8→4 round-trips | High | **Med** | value-equality |
| **X2** | SAP `report_sales_analysis` | sap (HANA) | cache on `(source,from,to)` only | Med-High | **Low** | by construction |
| **T1** | **Monthly Targets repoint** | platforms | raw LATERAL → `secmaster_mv` | High | **HIGH** | **hard gate** — see T1 |

¹ D4 is Low risk **only if** P1 shows `master_po_mv.po_date` is a real `DATE` column; otherwise it needs a matview column (Med-High).

> Recommended first batch (all Low risk): **D1, D2, S1, P1, U2, S2, U1, X2.** These are pure
> index-adds, single-predicate sargability rewrites proven set-identical, or caching — each with a
> zero-row EXCEPT or "by construction" guarantee. Tackle D3/P2/D4(PathB)/X1 next. Treat **T1 last**
> and only if its hard gate passes.

---

## 3. Dashboard (`dashboard/views.py`)

### D1 — `platform_expiry_alerts` (PURE INDEX-ADD, do first)
- **Location:** `platform_expiry_alerts`, `dashboard/views.py:3259-3273`. Endpoint `GET /dashboard/platform-expiry-alerts`.
- **Why slow / unused index:** seq scan of ~41k `master_po_mv` rows; the highly selective
  `days_to_expiry BETWEEN 1 AND 5` has **no index** (`idx_mpmv_pendency` doesn't include
  `days_to_expiry` or `po_status`). Classification: **missing index**.
- **Current EXPLAIN (query unchanged):**
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT UPPER(TRIM(format::text)) AS format, COUNT(DISTINCT po_number) AS po_count,
       COALESCE(SUM(total_order_liters),0), COALESCE(SUM(total_order_amt_exclusive),0),
       COALESCE(SUM(order_qty),0)
FROM public.master_po
WHERE days_to_expiry IS NOT NULL AND days_to_expiry >= 1 AND days_to_expiry <= 5
  AND UPPER(TRIM(po_status::text)) IN ('PENDING','APPOINTMENT DONE')
GROUP BY 1 ORDER BY 4 DESC;
```
- **Safe fix — partial index, no query change (`days_to_expiry` is a stored int column):**
```sql
CREATE INDEX CONCURRENTLY idx_mpmv_days_to_expiry_1_5
  ON master_po_mv (UPPER(TRIM(po_status::text)))
  WHERE days_to_expiry >= 1 AND days_to_expiry <= 5;
```
  Re-run the same EXPLAIN → expect a Bitmap Index Scan on the partial index.
- **Equivalence:** by construction (query text unchanged). Refresh-note: `days_to_expiry` is frozen
  at matview refresh (embeds `CURRENT_DATE` of the last refresh) — the index reads those same stored
  values, so semantics are identical; this is a pre-existing freshness property, unchanged by the index.
- **Gain High / Risk Low.**

### D2 — `state_sales_detail` DISTINCT probe hits the live un-materialized view
- **Location:** `state_sales_detail`, `dashboard/views.py:1226-1383`; probe at `:1231-1240`. Endpoint `GET /dashboard/state-sales/detail`.
- **Why slow:** the state-spelling probe runs `SELECT DISTINCT state FROM "SecMaster"` — the
  **non-materialized** view (full recompute, the cost migrations 0040/0042 materialized away), then
  the union data query re-scans `secmaster_mv` for the same period. Classification: **querying the view instead of the matview** + repeated scans.
- **Current vs optimized + EXCEPT (must be ZERO rows):**
```sql
WITH old AS (
  SELECT DISTINCT COALESCE(state::text,'') s FROM "SecMaster"
  WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON','FLIPKART')
    AND (UPPER(TRIM(month::text))='JUNE' AND year::numeric=2026)),      -- ADJUST
new AS (
  SELECT DISTINCT COALESCE(state::text,'') s FROM secmaster_mv          -- was "SecMaster"
  WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON','FLIPKART')
    AND UPPER(TRIM(month::text))='JUNE' AND year=2026)                  -- ADJUST; bare year
SELECT 'old-not-in-new' tag,* FROM (SELECT * FROM old EXCEPT SELECT * FROM new) a
UNION ALL SELECT 'new-not-in-old',* FROM (SELECT * FROM new EXCEPT SELECT * FROM old) b;
```
  Non-zero ⇒ matview stale vs view → refresh then re-run. **Gain High / Risk Low** (matview is row-identical to the view by construction).

### D3 — `category_sku_breakdown` per-month N+1 loop
- **Location:** `category_sku_breakdown` `dashboard/views.py:1879-1981`; loop `:1941-1962` → `_category_sku_rows` `:1763-1872` (once per trailing month, up to 6). Endpoint `GET /dashboard/category-sku-breakdown`.
- **Why slow:** N identical-shaped queries (≤6 round-trips), each re-plans/re-scans. The
  `idx_mpmv_delivmonth_year_head` index *is* used per call; the waste is structural (N+1). Classification: **per-month N+1 loop**.
- **Optimized (single query, returns the month key as a column so Python buckets by it):**
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT UPPER(TRIM(delivery_month::text)) AS dmonth, delivered_year AS dyear,
       COALESCE(NULLIF(TRIM(sku_code::text),''),'—') AS code,
       COALESCE(NULLIF(TRIM(sku_name::text),''),'')  AS sku_name,
       UPPER(TRIM(COALESCE(brand::text,'')))         AS brand,
       COALESCE(SUM(delivered_qty),0) AS units, COALESCE(SUM(total_delivered_liters),0) AS ltrs
FROM public.master_po
WHERE (UPPER(TRIM(delivery_month::text)), delivered_year)
        IN (('JUNE',2026),('MAY',2026),('APRIL',2026))   -- ADJUST trailing set
  AND UPPER(TRIM(item_head::text))='PREMIUM' AND UPPER(TRIM(format::text))='BLINKIT'  -- ADJUST
  AND UPPER(TRIM(sub_category::text))='OLIVE OIL'                                      -- ADJUST
GROUP BY 1,2,3,4,5;
```
- **Equivalence:** `old` = `UNION ALL` of the per-month originals (each `GROUP BY 1,2,3,4,5` with
  `ROUND(...,4)` on the two SUMs), `new` = the single query above; EXCEPT must be ZERO. **Gain High
  (when months>1) / Risk Med** — the Python bucketing at `:1966-1981` must switch from loop-key to
  row `(dmonth,dyear)`. Also drop `year::numeric` on the secondary branch (see P1).

### D4 — `fulfilment_health` / `top_skus` non-IMMUTABLE `_pm_parse_date(po_date::text)`
- **Location:** `fulfilment_health` `dashboard/views.py:2759-2776`; same pattern `top_skus` `:2984,3004,3044`.
- **Why slow / unused index:** `_pm_parse_date` is `IMMUTABLE`, but the `po_date::text` cast
  (date→text) is `STABLE`, so the whole expression is **non-indexable** — 41k per-row parses + COUNT(DISTINCT) sort. Classification: **non-sargable (non-IMMUTABLE) predicate**.
- **Safe fix depends on P1:**
  - **Path A (Low risk) — only if `po_date` is a real DATE column:** the cast round-trips, so compare the column directly and index it:
```sql
CREATE INDEX CONCURRENTLY idx_mpmv_po_date ON master_po_mv (po_date);
-- predicate becomes:  WHERE po_date >= DATE '2026-06-01' AND po_date <= DATE '2026-06-30'
```
  - **Path B (Med-High risk) — if `po_date` is text at the matview surface:** add a materialized
    `po_date_d date = _pm_parse_date(po_date)` column at refresh, index it, query `WHERE po_date_d BETWEEN ...`. This is a **matview-definition change** (rebuild + re-verify).
- **Equivalence (run regardless — catches NULL/unparseable rows):** `old` uses
  `_pm_parse_date(po_date::text) BETWEEN ...`, `new` uses `po_date BETWEEN ...`, both `GROUP BY 1`
  with `ROUND(...,4)` on the float SUMs; EXCEPT must be ZERO. Non-zero ⇒ Path A unsafe → use Path B.
  **Gain High.**

### (also) `state_sales` — see P1 (it shares the `year::numeric` + `UPPER(TRIM(format))` issue).

---

## 4. Platforms secmaster_mv (`platforms/views.py`)

**Shared index fact:** the only useful index on `secmaster_mv` is
`idx_secmaster_mv_fmt_month_year` on
`(regexp_replace(lower(trim(format::text)),'[^a-z0-9]+','','g'), UPPER(TRIM(month::text)), year)`.
Leading col = regexp-normalized format; 2nd = `UPPER(TRIM(month))`; 3rd = **bare** `year`.

### P1 — `"year"::numeric = %s` defeats the index 3rd column (HIGH value, LOW risk)
- **Location:** `_top_ltr_items_from_secmaster` `platforms/views.py:5849-5868`; all 4-5 scans in
  `sku_analysis_dashboard` `:9065,9101,9130,9149`; also `state_sales` `dashboard/views.py:970-997`.
- **Why slow / unused index:** index seeks on format+month but **stops at `year`** because
  `"year"::numeric` is an expression over the column, not the bare `year` key → year applied as a
  post-filter / wider scan. `sku_analysis_dashboard` repeats it 4-5× per request. Classification:
  **non-sargable predicate**.
- **Current EXPLAIN (top-ltr example):**
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COALESCE(NULLIF(TRIM("item"::text),''),'-') AS item,
       COALESCE(NULLIF(UPPER(TRIM("item_head"::text)),''),'OTHER') AS item_head,
       COALESCE(SUM("quantity"),0) AS shipped_units, COALESCE(SUM("ltr_sold"),0) AS shipped_ltr,
       COALESCE(SUM("sales_amt_exc"),0) AS shipped_value
FROM secmaster_mv
WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)),'[^a-z0-9]+','','g')='blinkit'   -- ADJUST
  AND UPPER(TRIM("month"::text))='JUNE' AND "year"::numeric=2026                  -- ADJUST
  AND NULLIF(TRIM("item"::text),'') IS NOT NULL
GROUP BY 1,2 ORDER BY 4 DESC LIMIT 8;
```
- **Safe fix:** drop the cast — `"year"::numeric=2026` → `"year"=2026` (semantically identical *if*
  `year` is integer/numeric per P1; if text, keep `"year"='2026'` and index the bare column). Apply
  to all 5 sku-analysis scans + `state_sales`.
- **Equivalence proof (ZERO rows):**
```sql
WITH old AS (
  SELECT COALESCE(NULLIF(TRIM("item"::text),''),'-') item,
         COALESCE(NULLIF(UPPER(TRIM("item_head"::text)),''),'OTHER') item_head,
         COALESCE(SUM("quantity"),0) u, COALESCE(SUM("ltr_sold"),0) l, COALESCE(SUM("sales_amt_exc"),0) v
  FROM secmaster_mv
  WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)),'[^a-z0-9]+','','g')='blinkit'
    AND UPPER(TRIM("month"::text))='JUNE' AND "year"::numeric=2026
    AND NULLIF(TRIM("item"::text),'') IS NOT NULL GROUP BY 1,2),
new AS (
  SELECT COALESCE(NULLIF(TRIM("item"::text),''),'-') item,
         COALESCE(NULLIF(UPPER(TRIM("item_head"::text)),''),'OTHER') item_head,
         COALESCE(SUM("quantity"),0) u, COALESCE(SUM("ltr_sold"),0) l, COALESCE(SUM("sales_amt_exc"),0) v
  FROM secmaster_mv
  WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)),'[^a-z0-9]+','','g')='blinkit'
    AND UPPER(TRIM("month"::text))='JUNE' AND "year"=2026
    AND NULLIF(TRIM("item"::text),'') IS NOT NULL GROUP BY 1,2)
SELECT 'old-not-in-new' tag,* FROM (SELECT * FROM old EXCEPT SELECT * FROM new) a
UNION ALL SELECT 'new-not-in-old',* FROM (SELECT * FROM new EXCEPT SELECT * FROM old) b;
```
  (Compare the full grouped set, not the `LIMIT 8` — identical sets ⇒ identical top-8.) **Gain High /
  Risk Low.** A non-zero result would itself reveal dirty `year` values (e.g. `' 2026'`).
- **Fallback if you must keep the cast:** expression index, equivalent by construction:
```sql
CREATE INDEX idx_secmaster_mv_fmt_month_yearnum ON public.secmaster_mv
  (REGEXP_REPLACE(LOWER(TRIM("format"::text)),'[^a-z0-9]+','','g'), UPPER(TRIM("month"::text)), (("year")::numeric));
```

### P2 — missing month/year bound: `_build_sec_keyed_trend` & DRR `item_rows`
- **Location:** `_build_sec_keyed_trend` `platforms/views.py:5975-5983` (sec dashboards); DRR `item_rows`
  `:9795-9846` (blinkit) and `:10052-10103` (zepto/swiggy/bigbasket).
- **Why slow / unused index:** these filter `secmaster_mv` by normalized format + a `"date"::date`
  window but carry **no month/year predicate**, so only the index's leading `format` column applies →
  full per-format history scan; `"date"::date` is not in the index at all. Classification: **missing
  month/year bound + non-sargable `"date"::date`**.
- **Safe fix:** add the index-aligned `AND UPPER(TRIM("month"::text))=%s AND "year"=%s` bounds while
  **keeping** the exact `"date"::date` window for correctness. This narrows via the index without
  changing results — **provided** every row dated in the window carries the matching `month`/`year`
  label.
- **Equivalence (the EXCEPT proves both the speedup AND the data assumption):** compare the
  `sales`/trend CTE old (date window only) vs new (date window + month/year bound). EXCEPT must be
  ZERO. **A non-zero `old-not-in-new` row means a June-dated row has a different `month`/`year`
  label** → the bound is unsafe → fall back to the no-query-change expression index:
```sql
CREATE INDEX idx_secmaster_mv_fmt_datecast ON public.secmaster_mv
  (REGEXP_REPLACE(LOWER(TRIM("format"::text)),'[^a-z0-9]+','','g'), (("date")::date));
```
  **Gain High / Risk Med** for the predicate rewrite (data-consistency gated); **Low** for the index-only option.
- **Note on `_build_sec_keyed_trend` specifically:** its *year* series legitimately needs all years,
  so a blanket `year=2026` bound is **not** equivalent for that series — bound only the day/month
  series, prove each per-view. This is why it sits at Med risk, not Low.

### P3 — Ads/Brandfund 3 DISTINCT filter scans (`_ads_dashboard_payload` `:3797-3823`, `_brandfund_dashboard_payload` `:4480-4506`)
- These scan the per-platform `*_ads_master` / `*_brandfund_master` base tables (NOT secmaster), 3
  DISTINCT seq scans each. Classification: **missing index**. Safe fix = add `(year)`, `(date)`,
  `(month,date)` indexes per source table (equivalent by construction) — confirm each table's current
  indexes first. **Gain Low-Med / Risk Low.** Lower priority (already 60s-cached + small tables).

---

## 5. Uploads (`uploads/amazon_uploads.py`)

### U1 — `amazon_po_summary` 8 full-table scans → 1 (GROUPING SETS)
- **Location:** `amazon_po_summary` `:4047-4237` (KPI + 7 GROUP-BY dimensions, each a full seq scan of `reporting."Amazon PO"`). Classification: **repeated/duplicate scans**.
- **Optimized — one pass for the breakdowns** (keep KPI scalars + the `expiry_urgent LIMIT 20` detail as their own statements):
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT po_status, category, fulfillment_center, item_head, state, sub_category,
       COUNT(*) AS count, COALESCE(SUM(total_requested_cost),0) AS order_value,
       SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
FROM reporting."Amazon PO"
GROUP BY GROUPING SETS ((po_status),(category),(fulfillment_center),(item_head),(state),(sub_category));
```
  `fill_rate_pct`, the `TRIM()<>''` filters, per-dimension `ORDER BY count DESC LIMIT N`, and the
  `NULLIF(...,'')→'Unknown'` relabel move to Python (the code already post-processes rows).
- **Equivalence:** per dimension, `old` = the original single-dimension `GROUP BY`, `new` = the
  matching `GROUPING SETS ((dim))` slice; EXCEPT must be ZERO (ORDER BY/LIMIT are presentation-only,
  applied in Python — compare the full grouped set). **Gain Med-High / Risk Low.**

### U2 — `appointment_summary` redundant `unnest` defeats `idx_amazon_po_po_number_norm`
- **Location:** `appointment_summary` `:4240-4437`; SKU join `:4333-4351`, item_head join `:4376-4398`.
- **Why slow / unused index:** migration 0007 already split appointments to one PO per row, but both
  joins still `CROSS JOIN LATERAL unnest(regexp_split_to_array(a.pos,'\s*[,;]\s*'))`. The join key is
  the per-row unnest output, so the new `idx_amazon_po_po_number_norm (UPPER(TRIM(po_number)))` **can't
  be used**. Classification: **redundant per-row string op + non-sargable join key**.
- **Optimized — drop the unnest, plain equality join (lets the index drive a nested loop):**
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.asin, COALESCE(p.sku_name,'') AS sku_name, MIN(NULLIF(TRIM(p.item::text),'')) AS item,
       COUNT(DISTINCT a.appointment_id) AS appointment_count, COALESCE(SUM(p.accepted_qty),0)::bigint AS total_qty
FROM reporting."appointment" a
JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = UPPER(TRIM(a.pos))
WHERE NULLIF(TRIM(a.pos),'') IS NOT NULL AND p.asin IS NOT NULL AND TRIM(p.asin) <> ''
GROUP BY p.asin, p.sku_name;
```
- **Gate (P3) + Equivalence:** only safe if `SELECT COUNT(*) FROM reporting."appointment" WHERE pos ~ '[,;]'` is **0**. Then `old` (unnest) vs `new` (plain join) EXCEPT must be ZERO. **Gain Med / Risk Low** (Low-Med if any multi-PO rows exist — then do NOT ship).

---

## 6. Shipment (`shipment/views.py`)

**Precondition for S1 & S2:** run P2 (`SELECT DISTINCT status FROM sp_shipments`). `status` is a
NOT-NULL CharField with fixed choices, so `status != 'rejected'` ⇔
`status IN ('draft','pending_approval','approved','dispatched','in_transit','delivered')` — but only
if P2 returns no NULL/unexpected values. `sp_shipments_status_idx (status)` becomes seekable only
after the `IN`-list rewrite (`!=` is non-sargable).

### S1 — `AppointmentListView.get` correlated `NOT EXISTS` → materialized anti-join
- **Location:** `AppointmentListView.get` `:1304-1413`; correlated `NOT EXISTS (sp_items ⋈ sp_shipments)` at `:1352-1360`, evaluated per (appointment, PO) pair. Endpoint `GET /…/appointments?date=`.
- **Why slow / unused index:** the `NOT EXISTS` is correlated on `po_upper` + `UPPER(TRIM(p.asin))`,
  re-probing `sp_items ⋈ sp_shipments` per pair; the `UPPER(TRIM(...))` keys can't use
  `sp_items_loaded_asin_po` (raw-column partial index). Classification: **correlated subquery re-scan + non-sargable**.
- **Safe fix:** replace the per-pair `NOT EXISTS` with a single materialized `locked_lookup` CTE +
  `LEFT JOIN ... WHERE ll.po_upper IS NULL` — **the exact shape Query 2 in the same view already uses
  (`:1453-1462`)**, so it's a proven pattern. Standard anti-join rewrite; `locked_lookup` grouped to
  one row per `(po_upper, asin_upper)` can't fan out; NULL-asin behavior matches the original.
```sql
locked_lookup AS (
    SELECT UPPER(TRIM(si.po_number)) AS po_upper, UPPER(TRIM(si.asin)) AS asin_upper
    FROM sp_items si JOIN sp_shipments s ON s.id = si.shipment_id
    WHERE si.not_loaded = FALSE
      AND s.status IN ('draft','pending_approval','approved','dispatched','in_transit','delivered')
    GROUP BY UPPER(TRIM(si.po_number)), UPPER(TRIM(si.asin))
)
-- ... and in po_status: replace the NOT EXISTS(...) with  ll.po_upper IS NULL
--     via  LEFT JOIN locked_lookup ll ON ll.po_upper = app.po_upper AND ll.asin_upper = UPPER(TRIM(p.asin))
```
- **Equivalence:** wrap the **full** original final SELECT (`old`) and the **full** optimized final
  SELECT (`new`), same literal date, in the EXCEPT harness; ZERO rows on several dates (incl. one with
  known locked POs). **Gain High / Risk Low.**

### S2 — `AppointmentItemsView.get` repeated scans + `status != 'rejected'`
- **Location:** `AppointmentItemsView.get` `:1629-2006`; `committed` CTE `:1810-1834`, `_reserved_stock_by_asin` `:1040-1058` (called `:1963`), `_fetch_doh_filler_pool` `locked_pairs` `:1108-1115` (called `:2006`).
- **Safe fix (phase 1, Low risk):** make the `committed` CTE and `locked_pairs` sargable —
  `s.status != 'rejected'` → the active-status `IN`-list (so `sp_shipments_status_idx` can seek).
  **Do NOT change `_reserved_stock_by_asin`** — its `IN ('draft','pending_approval','approved')` is a
  deliberately narrower business rule, not the `!=` guard.
- **Equivalence (committed CTE):** `old` with `s.status != 'rejected'`, `new` with the 6-status
  `IN`-list, both grouped identically; EXCEPT must be ZERO (given P2). **Gain Med / Risk Low.**
- **Phase 2 (optional, Med risk):** compute `committed` (full active set) and `reserved` (3-status
  subset) in ONE scan via `SUM(...) FILTER (WHERE status IN ('draft','pending_approval','approved'))`
  — 3 scans → 1, but touches Python call sites; prove `committed_qty` vs original committed and
  `reserved_qty` vs `_reserved_stock_by_asin` separately. **Gain High / Risk Med.**
- **Index note (proposal only):** `CREATE INDEX ON sp_items (UPPER(TRIM(asin)), UPPER(TRIM(po_number))) WHERE not_loaded=false;` would make the `UPPER(TRIM(...))` group keys in S1/S2 sargable.

---

## 7. SAP HANA (`sap/views.py`, `sap/service.py`) — measurement, not EXPLAIN

These hit remote SAP HANA via hdbcli; there's no Postgres EXPLAIN. Measure with `time.perf_counter()`
around each `_run`/`_count_of`, and on the **cache-miss** path (the endpoints are `@cached_get`-wrapped now).

### X1 — `inventory_overview` 8 HANA round-trips → 4
- **Location:** `inventory_overview` `sap/views.py:643-865`. 8 sequential round-trips; #1 rows, #2
  count, #3 summary KPIs, #4 items-zero, #5 items-below-min all scan the **same** `OITM⨝OITW(⨝OWHS)` set.
- **Safe collapse:** (g-1) fold #2 into #1 with `COUNT(*) OVER ()` (same OITM⨝OITW grain → equals #2's
  `total`); (g-2) fold #3+#4+#5 into one aggregate over a per-item rollup subquery (`SUM`/`CASE` reproduce
  the KPIs + the two `HAVING` counts). Keep #6 (global MinStock flag) and #7/#8 (cascading option lists
  with *different* WHERE) separate. **8 → 4 round-trips.**
- **Equivalence (value-equality, not DB EXCEPT):** for the same `where_sql`/params across representative
  filter combos (none / one warehouse / one group / each stock_state), assert
  `new.total_skus==old#3`, `new.total_units_on_hand==old#3`, `new.total_stock_value==old#3`,
  `new.items_zero_stock==old#4`, `new.items_below_min==old#5`, `windowed total_count==old#2`. **Gain
  High (remote/VPN link) / Risk Med** (grain caveats — gate on value-equality before rollout).

### X2 — `report_sales_analysis` cache on proc inputs only
- **Location:** `report_sales_analysis` `sap/service.py:120-164`; `sales_analysis` view `sap/views.py:231-344`.
- **Issue:** the proc `CALL REPORT_SALES_ANALYSIS(from,to)` returns the full date-range set; Python
  then filters/paginates/summarizes. Because `@cached_get` keys on **all** query params (incl.
  search/filters that don't change the proc inputs), each filter permutation re-CALLs the proc for the
  same `(source,from,to)`. Classification: **fetch-all-then-filter + redundant re-CALL**.
- **Safe fix (no proc rewrite):** memoize the proc result on `(source, from_date, to_date)` only, so all
  filter/search/page/aggregate permutations of one date range reuse a single CALL; keep all Python
  filtering as-is. Also ensure callers pass the tightest date range.
- **Equivalence:** by construction — same proc, same `(source,from,to)` → identical resultset →
  identical Python-derived `filtered/count/filters/summary/aggregate`. Verify: for fixed
  `(source,from,to)`, several requests with different filters return byte-identical responses AND the
  `report_sales_analysis -> fetchall=%d` log fires once per `(source,from,to)` within the TTL. **Gain
  Med-High / Risk Low.**

---

## 8. Monthly Targets repoint (`platforms/monthly_targets.py`) — HIGHEST value, HIGHEST risk, HARD GATE

### T1 — `_read_secmaster_dashboard_many` raw LATERAL → `secmaster_mv`
- **Location:** `_read_secmaster_dashboard_many` `:441-619` (blinkit branch `:474-499`); 5 platform
  branches `UNION ALL`'d. Each scans the raw `"<platform>Sec"` table with a correlated
  `LEFT JOIN LATERAL (SELECT ... FROM master_sheet ... LIMIT 1)` **per row** — the exact per-row
  master_sheet join the `secmaster_mv` matview already bakes in. Classification: **wrong source (raw
  view vs matview)** + per-row string ops + repeated scans.
- **Proposed rewrite:** repoint to `secmaster_mv` and `SUM("ltr_sold")` (the matview's pre-baked
  litres), exactly as `_top_ltr_items_from_secmaster` does — hits `idx_secmaster_mv_fmt_month_year`.
- **⚠ HARD GATE — a real semantic divergence exists in the matview source:** the raw query joins
  master_sheet **deduplicated, case-insensitive, format-filtered** (`UPPER(TRIM(format_sku_code))=UPPER(TRIM(item_id))
  AND regexp_replace(lower(trim(format)))='blinkit' ORDER BY product_name,item,per_unit LIMIT 1`),
  but `secmaster_mv` (= `SELECT * FROM "SecMaster"`, migration 0045 lines 127-128) joins master_sheet
  **plainly**: `m.format_sku_code::text = b.item_id::text` — case-SENSITIVE, not TRIM-ed, NO format
  filter, NO LIMIT 1 dedup (and Flipkart orders differently again, migration 0015). If
  `master_sheet.format_sku_code` has duplicates, the matview can **row-fan-out (double-count litres)**
  or pick a different `per_unit_value`/`item_head` than the lateral.
- **Mandatory equivalence proof — per (item_head, format), `ROUND(...,2)` tolerance, run per platform:**
```sql
WITH old AS (
  SELECT UPPER(TRIM(m.item_head::text)) AS item_head,
         ROUND(COALESCE(SUM(CASE WHEN m.is_litre='Y'
             THEN COALESCE(b.qty_sold,0)::numeric*COALESCE(m.per_unit_value,0)::numeric ELSE 0 END),0),2) AS done_ltrs
  FROM "blinkitSec" b
  LEFT JOIN LATERAL (
        SELECT ms.item_head, ms.per_unit_value, ms.is_litre FROM master_sheet ms
        WHERE UPPER(TRIM(ms.format_sku_code::text))=UPPER(TRIM(b.item_id::text))
          AND regexp_replace(lower(TRIM(ms.format::text)),'[^a-z0-9]+','','g')='blinkit'
        ORDER BY ms.product_name, ms.item, ms.per_unit LIMIT 1) m ON true
  WHERE b.date >= DATE '2026-06-01' AND b.date < DATE '2026-07-01'         -- ADJUST
    AND UPPER(TRIM(m.item_head::text)) IN ('PREMIUM','COMMODITY')          -- ADJUST
  GROUP BY UPPER(TRIM(m.item_head::text))),
new AS (
  SELECT UPPER(TRIM("item_head"::text)) AS item_head,
         ROUND(COALESCE(SUM("ltr_sold"),0)::numeric,2) AS done_ltrs
  FROM secmaster_mv
  WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)),'[^a-z0-9]+','','g')='blinkit'
    AND "date"::date >= DATE '2026-06-01' AND "date"::date < DATE '2026-07-01'   -- ADJUST
    AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM','COMMODITY')                -- ADJUST
  GROUP BY UPPER(TRIM("item_head"::text)))
SELECT 'old-not-in-new' tag,* FROM (SELECT * FROM old EXCEPT SELECT * FROM new) a
UNION ALL SELECT 'new-not-in-old',* FROM (SELECT * FROM new EXCEPT SELECT * FROM old) b;
```
- **If this returns ANY row for ANY platform, the repoint MUST NOT ship** — first reconcile the
  `SecMaster` per-platform master_sheet joins to match the lateral (UPPER/TRIM + format filter + dedup)
  and refresh the matview, then re-run. **Gain High / Risk HIGH.**

---

## 9. Implementation note (when proven)

All Postgres index DDL above uses `CREATE INDEX CONCURRENTLY` and belongs in a non-atomic Django
migration (`atomic = False`) guarded with `IF NOT EXISTS` (and `to_regclass(...)` for the `reporting.*`
tables), matching `uploads/migrations/0058_amazon_po_report_perf_indexes.py`. Query rewrites are pure
code changes in the named view functions — implement only the ones whose EXCEPT returned zero rows and
whose decision gate passed, and re-run the EXCEPT against the implemented query before deploying.
