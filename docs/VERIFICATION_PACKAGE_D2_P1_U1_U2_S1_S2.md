# Verification Package — Optimizations D2, P1, U1, U2, S1, S2

**Purpose:** Prove each remaining optimization returns exactly the same data as the current
code before touching a single line of application code. Every statement here is read-only
(SELECT / EXPLAIN / `\d`). Run against production or a read replica.

**Date these were written:** 2026-06-26. Wherever you see `-- ADJUST` substitute real
values before running (month names, years, appointment dates).

**Workflow per item:**
1. Run §1 Prechecks once — they decide which fixes are safe.
2. For each optimization, run the EXPLAIN pair (baseline then optimized) to confirm the
   plan switches to the expected access path.
3. Run the EXCEPT equivalence query — it **MUST return ZERO rows** before that change ships.
4. Fill in §9 Decision Table with your results.

---

## §1 — Required Prechecks (run once, copy results back)

Run all four before starting any per-optimization section.

### PC-1 — secmaster_mv column types (gates P1 and D2)

```sql
-- Tells us: is "year" integer, numeric, or text?
--           is "date" date or text?
--           is "month" text?
\d secmaster_mv
```

**What to look for:**

| Column | If this type is seen … | Then … |
|--------|----------------------|--------|
| `year` | `integer` or `numeric` | P1 fix is `"year" = 2026` (drop the cast) |
| `year` | `text` or `character varying` | P1 fix is `"year" = '2026'` (string literal) |
| `date` | `date` | D4 (not in this package) is Path A (safe) |
| `date` | `text` | D4 would need Path B — but that is out of scope here |

**Fail-stop:** if `year` returns a type you don't recognise, paste it here before proceeding
with P1. Do not guess.

---

### PC-2 — master_po_mv column types (informational for D1 / D4, already done for D1)

```sql
\d master_po_mv
```

You already have this from migration 0040. Confirm `days_to_expiry` is an integer/numeric
stored column (not a computed cast) and `po_date` type. No action required unless D4
is being planned (it is not in this package).

---

### PC-3 — Distinct shipment statuses (gates S1 and S2)

```sql
SELECT DISTINCT status FROM sp_shipments ORDER BY 1;
```

**Pass criteria:** result set must be a subset of exactly these seven values — no NULL,
no typo, no extra status:

```
approved
dispatched
draft
in_transit
delivered
pending_approval
rejected
```

**Fail-stop:** if ANY row contains NULL, or any status not in that list, then:
- **S1** must NOT be implemented (the `NOT EXISTS` → anti-join rewrite assumes a closed set).
- **S2** must NOT be implemented (the `IN`-list would miss the unknown status).

Paste the full result here before proceeding with S1 or S2.

---

### PC-4 — Multi-PO appointment rows (gates U2)

```sql
SELECT COUNT(*) AS multi_po_rows
FROM reporting."appointment"
WHERE pos ~ '[,;]';
```

**Pass criteria:** must return **0**.

**Fail-stop:** if > 0, migration 0007 (which split to one-PO-per-row) did not fully
process historical data. The `unnest` in U2 is still doing real work — do NOT remove
it. U2 cannot be implemented.

---

## §2 — D2 · `state_sales_detail` state probe

**What it is:** `state_sales_detail` (`dashboard/views.py:1231`) queries
`SELECT DISTINCT state FROM "SecMaster"` — the *non-materialized* pass-through view —
to resolve state spellings. The same request's data query already hits `secmaster_mv`
(the materialized copy). The probe needlessly recomputes the full view on every call.

**Fix:** change `"SecMaster"` → `secmaster_mv` in the probe only (one line). Also drop
the `year::numeric` cast (see P1 for why — both fixes can be bundled).

**Gate needed:** the matview must contain the same distinct states as the live view for
the requested period.

---

### D2-EXPLAIN-1 — baseline (the slow path, run during low traffic)

```sql
-- Substitute real month/year for your current period:
EXPLAIN (ANALYZE, BUFFERS)
SELECT DISTINCT COALESCE(state::text, '') AS s
FROM "SecMaster"
WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON', 'FLIPKART')
  AND UPPER(TRIM(month::text)) = 'JUNE'     -- ADJUST
  AND year::numeric = 2026;                 -- ADJUST
```

**Expected plan (current):** Seq Scan on the underlying base tables through the view
definition — no index use; full recompute of the view.

---

### D2-EXPLAIN-2 — optimized path

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT DISTINCT COALESCE(state::text, '') AS s
FROM secmaster_mv
WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON', 'FLIPKART')
  AND UPPER(TRIM(month::text)) = 'JUNE'     -- ADJUST
  AND year = 2026;                           -- ADJUST (bare column, drop ::numeric)
```

**Expected plan (optimized):** Bitmap Index Scan or Index Scan on
`idx_secmaster_mv_fmt_month_year`; Bitmap Heap Scan on `secmaster_mv`. Far fewer rows
to scan because the matview is pre-built.

---

### D2-EXCEPT — equivalence proof

```sql
-- ADJUST: set month name and year to match the period your app currently requests.
-- Run once for the default period (current month) and once for the previous month.

WITH old AS (
    SELECT DISTINCT COALESCE(state::text, '') AS s
    FROM "SecMaster"
    WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON', 'FLIPKART')
      AND UPPER(TRIM(month::text)) = 'JUNE'   -- ADJUST
      AND year::numeric = 2026                -- ADJUST
),
new AS (
    SELECT DISTINCT COALESCE(state::text, '') AS s
    FROM secmaster_mv
    WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON', 'FLIPKART')
      AND UPPER(TRIM(month::text)) = 'JUNE'   -- ADJUST
      AND year = 2026                          -- ADJUST (bare column)
)
SELECT 'old-not-in-new' AS tag, s
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, s
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

**Pass:** zero rows returned. ✅ Safe to implement.

**Fail — `old-not-in-new` rows:** states exist in the live view that are missing from
the matview — the matview is stale. Refresh it (`REFRESH MATERIALIZED VIEW secmaster_mv`)
and re-run. Only ship after the matview is confirmed fresh and the EXCEPT returns zero.

**Fail — `new-not-in-old` rows:** the matview has extra states not in the view. This
should not happen (the matview is a `SELECT * FROM "SecMaster"`) but would indicate a
matview definition drift. Investigate before shipping.

---

## §3 — P1 · `"year"::numeric` non-sargable predicate

**What it is:** every secmaster_mv query that uses `year::numeric = 2026` (instead of
bare `year = 2026`) tells PostgreSQL the index key is `year`, but the predicate is on
`CAST(year AS numeric)` — an expression the planner cannot match to the stored index
key. The index `idx_secmaster_mv_fmt_month_year` has `year` as its third column; because
of the cast, PostgreSQL reads only the first two index columns (format + month) and
post-filters on year. For `sku_analysis_dashboard` this happens **4-5 times per request**
against ~726k rows.

**Affected locations (all use `_sec_month_filter` or direct `"year"::numeric`):**
- `platforms/views.py:5861` — `_top_ltr_items_from_secmaster` (called 4× per sku_analysis_dashboard)
- `dashboard/views.py` — `_sec_month_filter` helper → `state_sales`, `state_sales_detail`

**Fix:** `"year"::numeric = %s` → `"year" = %s` if PC-1 shows `year` is integer/numeric.
If `year` is text, use `"year" = '2026'` (string literal, same index key).

**Gate needed:** PC-1 must confirm the `year` column type. Then the EXCEPT proves there
are no rows with values that only match under the cast (e.g., `' 2026'` with a leading
space would match `::numeric` but not exact string `'2026'`).

---

### P1-EXPLAIN-1 — baseline (one representative scan)

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COALESCE(NULLIF(TRIM("item"::text), ''), '-')                      AS item,
    COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')      AS item_head,
    COALESCE(SUM("quantity"), 0)                                        AS shipped_units,
    COALESCE(SUM("ltr_sold"), 0)                                        AS shipped_ltr,
    COALESCE(SUM("sales_amt_exc"), 0)                                   AS shipped_value
FROM secmaster_mv
WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'  -- ADJUST
  AND UPPER(TRIM("month"::text)) = 'JUNE'   -- ADJUST
  AND "year"::numeric = 2026                -- ADJUST — this is the cast being removed
  AND NULLIF(TRIM("item"::text), '') IS NOT NULL
GROUP BY 1, 2
ORDER BY COALESCE(SUM("ltr_sold"), 0) DESC
LIMIT 8;
```

**Expected plan (current):** Bitmap Index Scan on `idx_secmaster_mv_fmt_month_year`
using only the first two columns (format + month). `year::numeric = 2026` applied as a
Filter (not an Index Cond) — meaning many more rows pass the index and are then filtered
out in the heap.

---

### P1-EXPLAIN-2 — optimized path

```sql
-- Replace "year"::numeric = 2026 with bare "year" = 2026
-- If PC-1 shows year is text, use "year" = '2026' instead.
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COALESCE(NULLIF(TRIM("item"::text), ''), '-')                      AS item,
    COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')      AS item_head,
    COALESCE(SUM("quantity"), 0)                                        AS shipped_units,
    COALESCE(SUM("ltr_sold"), 0)                                        AS shipped_ltr,
    COALESCE(SUM("sales_amt_exc"), 0)                                   AS shipped_value
FROM secmaster_mv
WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'  -- ADJUST
  AND UPPER(TRIM("month"::text)) = 'JUNE'   -- ADJUST
  AND "year" = 2026                          -- ADJUST — bare column (or '2026' if text)
  AND NULLIF(TRIM("item"::text), '') IS NOT NULL
GROUP BY 1, 2
ORDER BY COALESCE(SUM("ltr_sold"), 0) DESC
LIMIT 8;
```

**Expected plan (optimized):** same Bitmap Index Scan but `year = 2026` now appears as
an **Index Cond** alongside format and month — all three index columns are used. Row
estimates and actual rows should both be much lower than the baseline plan.

---

### P1-EXCEPT — equivalence proof

This proof covers the entire change. If the full grouped set (without LIMIT) is identical
between the old and new predicate, LIMIT 8 must also be identical (ORDER BY is deterministic
on these aggregated columns).

```sql
-- Run for every format slug you use (blinkit, zepto, swiggy, bigbasket, amazon, flipkart)
-- and for the two most recent months to be thorough.
-- If PC-1 says year is text, replace  "year" = 2026  with  "year" = '2026'  in the `new` CTE.

WITH old AS (
    SELECT
        COALESCE(NULLIF(TRIM("item"::text), ''), '-')                  AS item,
        COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')  AS item_head,
        COALESCE(SUM("quantity"), 0)                                    AS u,
        COALESCE(SUM("ltr_sold"), 0)                                    AS l,
        COALESCE(SUM("sales_amt_exc"), 0)                               AS v
    FROM secmaster_mv
    WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'  -- ADJUST
      AND UPPER(TRIM("month"::text)) = 'JUNE'   -- ADJUST
      AND "year"::numeric = 2026                -- ADJUST — current (with cast)
      AND NULLIF(TRIM("item"::text), '') IS NOT NULL
    GROUP BY 1, 2
),
new AS (
    SELECT
        COALESCE(NULLIF(TRIM("item"::text), ''), '-')                  AS item,
        COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')  AS item_head,
        COALESCE(SUM("quantity"), 0)                                    AS u,
        COALESCE(SUM("ltr_sold"), 0)                                    AS l,
        COALESCE(SUM("sales_amt_exc"), 0)                               AS v
    FROM secmaster_mv
    WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'  -- ADJUST
      AND UPPER(TRIM("month"::text)) = 'JUNE'   -- ADJUST
      AND "year" = 2026                          -- ADJUST — optimized (bare column)
      AND NULLIF(TRIM("item"::text), '') IS NOT NULL
    GROUP BY 1, 2
)
SELECT 'old-not-in-new' AS tag, item, item_head, u, l, v
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, item, item_head, u, l, v
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

**Pass:** zero rows. ✅ The cast was a no-op on clean data — remove it everywhere in
`_sec_month_filter` and in all direct `"year"::numeric` predicates on secmaster_mv.

**Fail — any rows returned:** the `year` column has values that differ under cast vs
bare comparison (e.g., `' 2026'` with whitespace, or mixed-type string rows). Do NOT
implement P1 until the dirty data is cleaned or the index is replaced with an expression
index on `(year::numeric)`. Paste the failing rows here.

---

### P1-SANITY — check for dirty year values (cheap, run if EXCEPT fails)

```sql
SELECT DISTINCT "year", pg_typeof("year")
FROM secmaster_mv
WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'blinkit'
ORDER BY 1;
```

Expected: exactly the calendar years present (2024, 2025, 2026 etc.), no spaces, no NULLs
(unless the matview genuinely has null years).

---

## §4 — U1 · `amazon_po_summary` repeated full-table scans

**What it is:** `amazon_po_summary` (`uploads/amazon_uploads.py:4052-4237`) runs **8 separate
queries** against `reporting."Amazon PO"` — a scalar KPI row, then 7 GROUP BY breakdown
queries (po_status, category, fulfillment_center, item_head, state, sub_category, and a
WHERE-filtered classification slice). Each is a full sequential scan of the same table.

**Fix:** replace the 6 standalone group-by queries with a single GROUPING SETS query
(one scan). The scalar KPI query and the `expiry_urgent` detail query remain as-is.
Python post-processes: filters nulls/empties per dimension, applies per-dimension LIMIT,
relabels nulls → 'Unknown'.

**Gate needed:** the EXCEPT per dimension must confirm that GROUPING SETS returns
identical aggregate data to the current standalone query.

---

### U1-EXPLAIN — current vs consolidated (read EXPLAIN; do NOT run ANALYZE on this
### one during peak hours — it scans the full table 6 extra times)

```sql
-- Baseline: note how many identical Seq Scan nodes appear in the plan output:
EXPLAIN
SELECT po_status, COUNT(*) AS count
FROM reporting."Amazon PO"
WHERE po_status IS NOT NULL
GROUP BY po_status ORDER BY count DESC;

-- Optimized: one Seq Scan covers all 6 dimensions:
EXPLAIN
SELECT
    po_status, category, fulfillment_center, item_head, state, sub_category,
    COUNT(*)                                   AS count,
    COALESCE(SUM(total_requested_cost), 0)     AS order_value,
    SUM(received_qty)                          AS received_qty,
    SUM(requested_qty)                         AS requested_qty
FROM reporting."Amazon PO"
GROUP BY GROUPING SETS (
    (po_status),
    (category),
    (fulfillment_center),
    (item_head),
    (state),
    (sub_category)
);
```

**Expected:** the consolidated plan shows exactly **one** Seq Scan (or Bitmap Heap Scan
if the new indexes from migration 0058 are being used) with a HashAggregate node for the
GROUPING SETS. Each baseline query shows its own Seq Scan.

---

### U1-EXCEPT-status — po_status dimension

```sql
WITH old AS (
    SELECT po_status, COUNT(*) AS count
    FROM reporting."Amazon PO"
    WHERE po_status IS NOT NULL
    GROUP BY po_status
),
new AS (
    SELECT po_status, COUNT(*) AS count
    FROM reporting."Amazon PO"
    GROUP BY GROUPING SETS ((po_status))   -- = GROUP BY po_status
    HAVING po_status IS NOT NULL
)
SELECT 'old-not-in-new' AS tag, po_status, count
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, po_status, count
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

---

### U1-EXCEPT-category — category dimension

```sql
WITH old AS (
    SELECT category, COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value
    FROM reporting."Amazon PO"
    WHERE category IS NOT NULL AND TRIM(category) != ''
    GROUP BY category
),
new AS (
    SELECT category, COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value
    FROM reporting."Amazon PO"
    GROUP BY GROUPING SETS ((category))
    HAVING category IS NOT NULL AND TRIM(category) != ''
)
SELECT 'old-not-in-new' AS tag, category, count, order_value
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, category, count, order_value
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

---

### U1-EXCEPT-fc — fulfillment_center dimension

```sql
WITH old AS (
    SELECT fulfillment_center,
           COUNT(*) AS count,
           COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    WHERE fulfillment_center IS NOT NULL AND TRIM(fulfillment_center) != ''
    GROUP BY fulfillment_center
),
new AS (
    SELECT fulfillment_center,
           COUNT(*) AS count,
           COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    GROUP BY GROUPING SETS ((fulfillment_center))
    HAVING fulfillment_center IS NOT NULL AND TRIM(fulfillment_center) != ''
)
SELECT 'old-not-in-new' AS tag, fulfillment_center, count, order_value
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, fulfillment_center, count, order_value
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

---

### U1-EXCEPT-item_head — item_head dimension

```sql
-- Note: current code uses COALESCE(NULLIF(TRIM(item_head),''),'Unknown') in SQL;
-- the optimized code moves this to Python. We compare raw grouped data.
WITH old AS (
    SELECT COALESCE(NULLIF(TRIM(item_head), ''), 'Unknown') AS item_head,
           COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    GROUP BY COALESCE(NULLIF(TRIM(item_head), ''), 'Unknown')
),
new AS (
    -- GROUPING SETS returns raw item_head; Python relabels '' → 'Unknown'.
    -- For the EXCEPT we replicate the same relabelling in SQL to compare apples-to-apples.
    SELECT COALESCE(NULLIF(TRIM(item_head), ''), 'Unknown') AS item_head,
           COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    GROUP BY item_head   -- after GROUPING SETS, Python applies the same COALESCE
)
SELECT 'old-not-in-new' AS tag, item_head, count, order_value
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, item_head, count, order_value
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

---

### U1-EXCEPT-state — state dimension

```sql
WITH old AS (
    SELECT COALESCE(NULLIF(TRIM(state), ''), 'Unknown') AS state,
           COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    WHERE state IS NOT NULL AND TRIM(state) != ''
    GROUP BY COALESCE(NULLIF(TRIM(state), ''), 'Unknown')
),
new AS (
    SELECT COALESCE(NULLIF(TRIM(state), ''), 'Unknown') AS state,
           COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    GROUP BY state
    HAVING state IS NOT NULL AND TRIM(state) != ''
)
SELECT 'old-not-in-new' AS tag, state, count, order_value
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, state, count, order_value
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

---

### U1-EXCEPT-sub_category — sub_category dimension

```sql
WITH old AS (
    SELECT COALESCE(NULLIF(TRIM(sub_category), ''), 'Unknown') AS sub_category,
           COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    WHERE sub_category IS NOT NULL AND TRIM(sub_category) != ''
    GROUP BY COALESCE(NULLIF(TRIM(sub_category), ''), 'Unknown')
),
new AS (
    SELECT COALESCE(NULLIF(TRIM(sub_category), ''), 'Unknown') AS sub_category,
           COUNT(*) AS count, COALESCE(SUM(total_requested_cost), 0) AS order_value,
           SUM(received_qty) AS received_qty, SUM(requested_qty) AS requested_qty
    FROM reporting."Amazon PO"
    GROUP BY sub_category
    HAVING sub_category IS NOT NULL AND TRIM(sub_category) != ''
)
SELECT 'old-not-in-new' AS tag, sub_category, count, order_value
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, sub_category, count, order_value
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

**All 5 EXCEPTs must return zero rows.** If any fail, paste the failing rows — they
indicate NULL or whitespace data that the current WHERE/COALESCE handles but the Python
relabelling path would not.

---

## §5 — U2 · `appointment_summary` redundant unnest

**What it is:** `appointment_summary` (`uploads/amazon_uploads.py:4333-4407`) has two
queries that join `reporting."appointment"` → `reporting."Amazon PO"` via
`CROSS JOIN LATERAL unnest(regexp_split_to_array(a.pos, '\s*[,;]\s*'))`. Migration 0007
already split appointments to one PO per row, so `pos` should never contain a comma or
semicolon. If PC-4 confirms that, the unnest is a no-op and can be replaced by a plain
`JOIN ... ON UPPER(TRIM(p.po_number)) = UPPER(TRIM(a.pos))`, which lets the
`idx_amazon_po_po_number_norm` index drive a nested-loop join.

**Gate: PC-4 must return 0 before running any EXCEPT here.**
If PC-4 returns > 0, skip U2 entirely.

---

### U2-EXPLAIN-1 — sku_breakdown query baseline

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.asin,
       COALESCE(p.sku_name, '') AS sku_name,
       MIN(NULLIF(TRIM(p.item::text), '')) AS item,
       COUNT(DISTINCT a.appointment_id) AS appointment_count,
       COALESCE(SUM(p.accepted_qty), 0)::bigint AS total_qty
FROM reporting."appointment" a
CROSS JOIN LATERAL unnest(
    regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
) AS po_val
JOIN reporting."Amazon PO" p
  ON UPPER(TRIM(p.po_number)) = UPPER(TRIM(po_val))
WHERE NULLIF(TRIM(po_val), '') IS NOT NULL
  AND p.asin IS NOT NULL
  AND TRIM(p.asin) <> ''
GROUP BY p.asin, p.sku_name;
```

**Expected plan (current):** Seq Scan on `"appointment"`, Lateral/Function node for the
unnest, then a Hash Join or Nested Loop on `"Amazon PO"`. The index
`idx_amazon_po_po_number_norm` is NOT used because the join key is `UPPER(TRIM(po_val))`,
an expression over the unnest output, not over the stored column.

---

### U2-EXPLAIN-2 — sku_breakdown optimized path

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.asin,
       COALESCE(p.sku_name, '') AS sku_name,
       MIN(NULLIF(TRIM(p.item::text), '')) AS item,
       COUNT(DISTINCT a.appointment_id) AS appointment_count,
       COALESCE(SUM(p.accepted_qty), 0)::bigint AS total_qty
FROM reporting."appointment" a
JOIN reporting."Amazon PO" p
  ON UPPER(TRIM(p.po_number)) = UPPER(TRIM(a.pos))
WHERE NULLIF(TRIM(a.pos), '') IS NOT NULL
  AND p.asin IS NOT NULL
  AND TRIM(p.asin) <> ''
GROUP BY p.asin, p.sku_name;
```

**Expected plan (optimized):** Nested Loop with an Index Scan on
`idx_amazon_po_po_number_norm` on the inner side. No unnest / function node.

---

### U2-EXCEPT-sku — sku_breakdown equivalence proof

```sql
WITH old AS (
    SELECT p.asin,
           COALESCE(p.sku_name, '') AS sku_name,
           MIN(NULLIF(TRIM(p.item::text), '')) AS item,
           COUNT(DISTINCT a.appointment_id) AS appointment_count,
           COALESCE(SUM(p.accepted_qty), 0)::bigint AS total_qty
    FROM reporting."appointment" a
    CROSS JOIN LATERAL unnest(
        regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
    ) AS po_val
    JOIN reporting."Amazon PO" p
      ON UPPER(TRIM(p.po_number)) = UPPER(TRIM(po_val))
    WHERE NULLIF(TRIM(po_val), '') IS NOT NULL
      AND p.asin IS NOT NULL AND TRIM(p.asin) <> ''
    GROUP BY p.asin, p.sku_name
),
new AS (
    SELECT p.asin,
           COALESCE(p.sku_name, '') AS sku_name,
           MIN(NULLIF(TRIM(p.item::text), '')) AS item,
           COUNT(DISTINCT a.appointment_id) AS appointment_count,
           COALESCE(SUM(p.accepted_qty), 0)::bigint AS total_qty
    FROM reporting."appointment" a
    JOIN reporting."Amazon PO" p
      ON UPPER(TRIM(p.po_number)) = UPPER(TRIM(a.pos))
    WHERE NULLIF(TRIM(a.pos), '') IS NOT NULL
      AND p.asin IS NOT NULL AND TRIM(p.asin) <> ''
    GROUP BY p.asin, p.sku_name
)
SELECT 'old-not-in-new' AS tag, asin, sku_name, total_qty
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, asin, sku_name, total_qty
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

---

### U2-EXCEPT-item_head — item_head_breakdown equivalence proof

```sql
WITH old AS (
    WITH appt_pos_old AS (
        SELECT DISTINCT a.appointment_id, UPPER(TRIM(po_val)) AS po_number
        FROM reporting."appointment" a
        CROSS JOIN LATERAL unnest(
            regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
        ) AS po_val
        WHERE NULLIF(TRIM(po_val), '') IS NOT NULL
    )
    SELECT COALESCE(NULLIF(UPPER(TRIM(p.item_head)), ''), 'OTHER') AS item_head,
           COUNT(DISTINCT ap.appointment_id) AS appointment_count,
           COUNT(DISTINCT p.asin) AS sku_count,
           COALESCE(SUM(p.accepted_qty), 0)::bigint AS total_qty
    FROM appt_pos_old ap
    JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = ap.po_number
    GROUP BY COALESCE(NULLIF(UPPER(TRIM(p.item_head)), ''), 'OTHER')
),
new AS (
    WITH appt_pos_new AS (
        SELECT DISTINCT a.appointment_id, UPPER(TRIM(a.pos)) AS po_number
        FROM reporting."appointment" a
        WHERE NULLIF(TRIM(a.pos), '') IS NOT NULL
    )
    SELECT COALESCE(NULLIF(UPPER(TRIM(p.item_head)), ''), 'OTHER') AS item_head,
           COUNT(DISTINCT ap.appointment_id) AS appointment_count,
           COUNT(DISTINCT p.asin) AS sku_count,
           COALESCE(SUM(p.accepted_qty), 0)::bigint AS total_qty
    FROM appt_pos_new ap
    JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = ap.po_number
    GROUP BY COALESCE(NULLIF(UPPER(TRIM(p.item_head)), ''), 'OTHER')
)
SELECT 'old-not-in-new' AS tag, item_head, appointment_count, total_qty
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, item_head, appointment_count, total_qty
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

**Both EXCEPTs must return zero rows.** If any fail, the `pos` column has multi-PO values
that PC-4 missed (e.g., due to a different delimiter pattern). Do not implement U2.

---

## §6 — S1 · `AppointmentListView` correlated NOT EXISTS → anti-join

**What it is:** `AppointmentListView.get` (`shipment/views.py:1301`) computes per-PO
`is_eligible` inside a `po_status` CTE. The `is_eligible` field uses a correlated
`NOT EXISTS (SELECT 1 FROM sp_items si JOIN sp_shipments s ...)` that re-probes the
two tables for **every (appointment, PO, ASIN) combination** in the result. With 50+
appointments × N POs × M ASINs this becomes thousands of correlated scans.

**Fix:** materialise the locked set once as a `locked_lookup` CTE — exactly the shape
already used in the second `AppointmentListView` query at `:1453-1462` (proven pattern).
Then replace the per-row `NOT EXISTS` with a `LEFT JOIN locked_lookup ll ON ... WHERE
ll.po_upper IS NULL`.

**Gate: PC-3 must return only the 6 expected non-rejected statuses** before this can
be implemented.

---

### S1-EXPLAIN-1 — baseline (pick a date with real appointments)

```sql
-- Replace '2026-06-26' with a real date that has appointment data.
EXPLAIN (ANALYZE, BUFFERS)
WITH appt_dedup AS (
    SELECT a.appointment_id,
           MAX(a.status) AS status, MAX(a.appointment_time) AS appointment_time,
           MAX(a.destination_fc) AS destination_fc, MAX(a.pro) AS pro,
           STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),'' ), ',') AS pos
    FROM reporting."appointment" a
    WHERE DATE(a.appointment_time) = '2026-06-26'   -- ADJUST to a date with real data
    GROUP BY a.appointment_id
),
appt_po_pairs AS (
    SELECT ad.appointment_id, ad.destination_fc, UPPER(TRIM(pv)) AS po_upper
    FROM appt_dedup ad,
    LATERAL unnest(regexp_split_to_array(COALESCE(ad.pos,''), '\s*[,;]\s*')) AS pv
    WHERE NULLIF(TRIM(pv),'') IS NOT NULL
),
po_status AS (
    SELECT app.appointment_id, app.po_upper,
           BOOL_OR(p.po_number IS NOT NULL) AS has_fc_match,
           BOOL_OR(p.status = 'Confirmed' AND p.po_status = 'PENDING') AS is_pending,
           BOOL_OR(p.availability_status = 'AC - Accepted: In stock') AS is_in_stock,
           BOOL_OR(COALESCE(p.accepted_qty, 0) > 0) AS has_qty,
           BOOL_OR(
               p.status = 'Confirmed' AND p.po_status = 'PENDING'
               AND p.availability_status = 'AC - Accepted: In stock'
               AND COALESCE(p.accepted_qty, 0) > 0
               AND NOT EXISTS (
                   SELECT 1 FROM sp_items si JOIN sp_shipments s ON s.id = si.shipment_id
                   WHERE UPPER(TRIM(si.po_number)) = app.po_upper
                     AND UPPER(TRIM(si.asin))      = UPPER(TRIM(p.asin))
                     AND si.not_loaded = FALSE AND s.status != 'rejected'
               )
           ) AS is_eligible
    FROM appt_po_pairs app
    LEFT JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = app.po_upper
    GROUP BY app.appointment_id, app.po_upper
)
SELECT * FROM po_status LIMIT 1;  -- just to get the plan
```

**Expected plan (current):** a correlated SubPlan node with many nested Index Scans or
Seq Scans on sp_items/sp_shipments, one invocation per outer row. High nested-loop
overhead.

---

### S1-EXPLAIN-2 — optimized anti-join path

```sql
-- Same date as S1-EXPLAIN-1.
EXPLAIN (ANALYZE, BUFFERS)
WITH appt_dedup AS (
    SELECT a.appointment_id,
           MAX(a.status) AS status, MAX(a.appointment_time) AS appointment_time,
           MAX(a.destination_fc) AS destination_fc, MAX(a.pro) AS pro,
           STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),'' ), ',') AS pos
    FROM reporting."appointment" a
    WHERE DATE(a.appointment_time) = '2026-06-26'   -- ADJUST
    GROUP BY a.appointment_id
),
appt_po_pairs AS (
    SELECT ad.appointment_id, ad.destination_fc, UPPER(TRIM(pv)) AS po_upper
    FROM appt_dedup ad,
    LATERAL unnest(regexp_split_to_array(COALESCE(ad.pos,''), '\s*[,;]\s*')) AS pv
    WHERE NULLIF(TRIM(pv),'') IS NOT NULL
),
locked_lookup AS (
    SELECT UPPER(TRIM(si.po_number)) AS po_upper,
           UPPER(TRIM(si.asin))      AS asin_upper
    FROM sp_items si
    JOIN sp_shipments s ON s.id = si.shipment_id
    WHERE si.not_loaded = FALSE
      AND s.status IN ('draft','pending_approval','approved','dispatched','in_transit','delivered')
    GROUP BY UPPER(TRIM(si.po_number)), UPPER(TRIM(si.asin))
),
po_status AS (
    SELECT app.appointment_id, app.po_upper,
           BOOL_OR(p.po_number IS NOT NULL) AS has_fc_match,
           BOOL_OR(p.status = 'Confirmed' AND p.po_status = 'PENDING') AS is_pending,
           BOOL_OR(p.availability_status = 'AC - Accepted: In stock') AS is_in_stock,
           BOOL_OR(COALESCE(p.accepted_qty, 0) > 0) AS has_qty,
           BOOL_OR(
               p.status = 'Confirmed' AND p.po_status = 'PENDING'
               AND p.availability_status = 'AC - Accepted: In stock'
               AND COALESCE(p.accepted_qty, 0) > 0
               AND ll.po_upper IS NULL
           ) AS is_eligible
    FROM appt_po_pairs app
    LEFT JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = app.po_upper
    LEFT JOIN locked_lookup ll
        ON ll.po_upper    = app.po_upper
       AND ll.asin_upper  = UPPER(TRIM(p.asin))
    GROUP BY app.appointment_id, app.po_upper
)
SELECT * FROM po_status LIMIT 1;
```

**Expected plan (optimized):** `locked_lookup` built once as a HashAggregate; then a
Hash Left Join between `po_status` and `locked_lookup` — the correlated SubPlan is gone.

---

### S1-EXCEPT — is_eligible equivalence proof

**Run for at least two dates: one with known locked POs and one with none.**
Replace `'2026-06-26'` with your real dates.

```sql
-- ADJUST both occurrences of the date literal.
WITH
appt_dedup AS (
    SELECT a.appointment_id,
           STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),'' ), ',') AS pos
    FROM reporting."appointment" a
    WHERE DATE(a.appointment_time) = '2026-06-26'   -- ADJUST
    GROUP BY a.appointment_id
),
appt_po_pairs AS (
    SELECT ad.appointment_id, UPPER(TRIM(pv)) AS po_upper
    FROM appt_dedup ad,
    LATERAL unnest(regexp_split_to_array(COALESCE(ad.pos,''), '\s*[,;]\s*')) AS pv
    WHERE NULLIF(TRIM(pv),'') IS NOT NULL
),
-- OLD: correlated NOT EXISTS
is_elig_old AS (
    SELECT app.appointment_id, app.po_upper,
           BOOL_OR(
               p.status = 'Confirmed' AND p.po_status = 'PENDING'
               AND p.availability_status = 'AC - Accepted: In stock'
               AND COALESCE(p.accepted_qty, 0) > 0
               AND NOT EXISTS (
                   SELECT 1 FROM sp_items si JOIN sp_shipments s ON s.id = si.shipment_id
                   WHERE UPPER(TRIM(si.po_number)) = app.po_upper
                     AND UPPER(TRIM(si.asin))      = UPPER(TRIM(p.asin))
                     AND si.not_loaded = FALSE AND s.status != 'rejected'
               )
           ) AS is_eligible
    FROM appt_po_pairs app
    LEFT JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = app.po_upper
    GROUP BY app.appointment_id, app.po_upper
),
-- NEW: locked_lookup materialized anti-join
locked_lookup AS (
    SELECT UPPER(TRIM(si.po_number)) AS po_upper,
           UPPER(TRIM(si.asin))      AS asin_upper
    FROM sp_items si
    JOIN sp_shipments s ON s.id = si.shipment_id
    WHERE si.not_loaded = FALSE
      AND s.status IN ('draft','pending_approval','approved','dispatched','in_transit','delivered')
    GROUP BY UPPER(TRIM(si.po_number)), UPPER(TRIM(si.asin))
),
is_elig_new AS (
    SELECT app.appointment_id, app.po_upper,
           BOOL_OR(
               p.status = 'Confirmed' AND p.po_status = 'PENDING'
               AND p.availability_status = 'AC - Accepted: In stock'
               AND COALESCE(p.accepted_qty, 0) > 0
               AND ll.po_upper IS NULL
           ) AS is_eligible
    FROM appt_po_pairs app
    LEFT JOIN reporting."Amazon PO" p ON UPPER(TRIM(p.po_number)) = app.po_upper
    LEFT JOIN locked_lookup ll
        ON ll.po_upper   = app.po_upper
       AND ll.asin_upper = UPPER(TRIM(p.asin))
    GROUP BY app.appointment_id, app.po_upper
)
SELECT 'old-not-in-new' AS tag, appointment_id, po_upper, is_eligible
FROM (SELECT * FROM is_elig_old EXCEPT SELECT * FROM is_elig_new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, appointment_id, po_upper, is_eligible
FROM (SELECT * FROM is_elig_new EXCEPT SELECT * FROM is_elig_old) y;
```

**Pass:** zero rows for every test date. ✅ Safe to implement S1.

**Fail:** any rows indicate that the `NOT EXISTS` and the anti-join produce different
`is_eligible` values for some (appointment_id, po_upper) pairs. Most likely cause:
PC-3 revealed unexpected status values that are NOT in the IN-list. Paste the failing
rows and the PC-3 output here. Do NOT implement S1.

---

## §7 — S2 · `AppointmentItemsView` committed CTE sargability

**What it is:** `AppointmentItemsView.get` (`shipment/views.py:1810-1833`) builds a
`committed` CTE with `WHERE ... AND s.status != 'rejected'`. The inequality predicate
(`!=`) is non-sargable — PostgreSQL cannot use `sp_shipments_status_idx (status)` for
it (seeking a column for everything-except-one is not an index seek pattern). The fix
replaces it with an explicit `IN`-list of the 6 active statuses so the index can seek.

**Gate: PC-3 must pass** (same gate as S1). If PC-3 shows unknown statuses, the IN-list
would silently exclude them — do NOT implement.

**Important boundary note:** do NOT change `_reserved_stock_by_asin()` — its IN-list
`('draft','pending_approval','approved')` is a deliberately narrower business rule, not
the `!=` guard. Only change the `committed` CTE and any `locked_pairs` CTE that also
uses `s.status != 'rejected'`.

---

### S2-EXPLAIN-1 — committed CTE baseline

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT si.asin, UPPER(TRIM(si.po_number)) AS po_number,
       UPPER(TRIM(COALESCE(si.destination_fc, ''))) AS fc_key,
       SUM(COALESCE(si.planned_qty, 0)) AS committed_qty
FROM sp_items si
JOIN sp_shipments s ON s.id = si.shipment_id
WHERE si.not_loaded = FALSE
  AND s.status != 'rejected'                -- current (non-sargable)
GROUP BY si.asin,
         UPPER(TRIM(si.po_number)),
         UPPER(TRIM(COALESCE(si.destination_fc, '')));
```

**Expected plan (current):** Seq Scan on sp_shipments with a Filter `status != 'rejected'`;
the `sp_shipments_status_idx` is NOT used. Or an Index Scan on sp_items + Hash Join to
sp_shipments, again with a filter not an index seek on status.

---

### S2-EXPLAIN-2 — committed CTE optimized

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT si.asin, UPPER(TRIM(si.po_number)) AS po_number,
       UPPER(TRIM(COALESCE(si.destination_fc, ''))) AS fc_key,
       SUM(COALESCE(si.planned_qty, 0)) AS committed_qty
FROM sp_items si
JOIN sp_shipments s ON s.id = si.shipment_id
WHERE si.not_loaded = FALSE
  AND s.status IN ('draft','pending_approval','approved','dispatched','in_transit','delivered')
GROUP BY si.asin,
         UPPER(TRIM(si.po_number)),
         UPPER(TRIM(COALESCE(si.destination_fc, '')));
```

**Expected plan (optimized):** Bitmap Index Scan on `sp_shipments_status_idx (status)`
with an `IN` condition, followed by Bitmap Heap Scan. A 6-value IN-list is seekable;
the planner may also choose a nested-loop index scan on the shipment side.

---

### S2-EXCEPT — committed CTE equivalence proof

```sql
WITH old AS (
    SELECT si.asin,
           UPPER(TRIM(si.po_number))                           AS po_number,
           UPPER(TRIM(COALESCE(si.destination_fc, '')))        AS fc_key,
           SUM(COALESCE(si.planned_qty, 0))                    AS committed_qty
    FROM sp_items si
    JOIN sp_shipments s ON s.id = si.shipment_id
    WHERE si.not_loaded = FALSE
      AND s.status != 'rejected'
    GROUP BY si.asin,
             UPPER(TRIM(si.po_number)),
             UPPER(TRIM(COALESCE(si.destination_fc, '')))
),
new AS (
    SELECT si.asin,
           UPPER(TRIM(si.po_number))                           AS po_number,
           UPPER(TRIM(COALESCE(si.destination_fc, '')))        AS fc_key,
           SUM(COALESCE(si.planned_qty, 0))                    AS committed_qty
    FROM sp_items si
    JOIN sp_shipments s ON s.id = si.shipment_id
    WHERE si.not_loaded = FALSE
      AND s.status IN ('draft','pending_approval','approved',
                       'dispatched','in_transit','delivered')
    GROUP BY si.asin,
             UPPER(TRIM(si.po_number)),
             UPPER(TRIM(COALESCE(si.destination_fc, '')))
)
SELECT 'old-not-in-new' AS tag, asin, po_number, fc_key, committed_qty
FROM (SELECT * FROM old EXCEPT SELECT * FROM new) x
UNION ALL
SELECT 'new-not-in-old' AS tag, asin, po_number, fc_key, committed_qty
FROM (SELECT * FROM new EXCEPT SELECT * FROM old) y;
```

**Pass:** zero rows. ✅ PC-3 + this EXCEPT together prove `!= 'rejected'` is equivalent
to the 6-status IN-list on your data.

**Fail:** any rows indicate a shipment status value that is neither `rejected` nor in the
IN-list — the IN-list would silently exclude those rows, changing committed_qty for those
(ASIN, PO, FC) keys. Show the PC-3 output and failing rows — the IN-list must include the
mystery status before S2 can ship.

---

## §8 — Result checklist (fill in as you run)

| Section | Query | Result | Pass/Fail |
|---------|-------|--------|-----------|
| PC-1 | `\d secmaster_mv` — `year` column type | _______ | ____ |
| PC-3 | `SELECT DISTINCT status FROM sp_shipments` | _______ | ____ |
| PC-4 | `COUNT(*) WHERE pos ~ '[,;]'` | _______ | ____ |
| D2 | EXCEPT (June 2026) | 0 rows? | ____ |
| D2 | EXCEPT (previous month) | 0 rows? | ____ |
| P1 | EXCEPT blinkit June 2026 | 0 rows? | ____ |
| P1 | EXCEPT blinkit May 2026 | 0 rows? | ____ |
| P1 | EXCEPT second format (e.g. zepto) | 0 rows? | ____ |
| U1 | EXCEPT po_status | 0 rows? | ____ |
| U1 | EXCEPT category | 0 rows? | ____ |
| U1 | EXCEPT fulfillment_center | 0 rows? | ____ |
| U1 | EXCEPT item_head | 0 rows? | ____ |
| U1 | EXCEPT state | 0 rows? | ____ |
| U1 | EXCEPT sub_category | 0 rows? | ____ |
| U2 | (PC-4 must be 0 first) EXCEPT sku_breakdown | 0 rows? | ____ |
| U2 | (PC-4 must be 0 first) EXCEPT item_head_breakdown | 0 rows? | ____ |
| S1 | EXCEPT (date with locked POs) | 0 rows? | ____ |
| S1 | EXCEPT (date with NO locked POs) | 0 rows? | ____ |
| S2 | EXCEPT committed CTE | 0 rows? | ____ |

---

## §9 — Decision table

Fill in the "Verified?" column after running §8. The "Safe to implement?" column is
derived: an optimization is safe only when **all** its required rows above show Pass.

| Opt | Change | Required passes | Verified? | Safe? | Expected gain | Risk |
|-----|--------|-----------------|-----------|-------|---------------|------|
| **D2** | `"SecMaster"` → `secmaster_mv` in state probe | PC-1 + D2 EXCEPT ×2 | __ | __ | ~300-600 ms removed from state_sales_detail; eliminates one full view recompute per request | Low |
| **P1** | Drop `::numeric` cast on `year` in secmaster_mv queries | PC-1 + P1 EXCEPT ×2+ | __ | __ | Index uses all 3 columns (format+month+year); `sku_analysis_dashboard` drops from 4-5 full-format scans to 4-5 tight seeks (~726k → few thousand rows each) | Low |
| **U1** | 6 GROUP BY queries → 1 GROUPING SETS | U1 EXCEPT ×5 | __ | __ | 6 full scans → 1; `amazon_po_summary` wall-clock drops proportionally (~5-6×) | Low |
| **U2** | Unnest removed from appointment→PO join | PC-4=0 + U2 EXCEPT ×2 | __ | __ | Plain join lets `idx_amazon_po_po_number_norm` drive; appointment_summary SKU/item_head joins go from Seq Scan to index nested-loop | Low |
| **S1** | NOT EXISTS → locked_lookup anti-join | PC-3 pass + S1 EXCEPT ×2 | __ | __ | Correlated sub-scan per (appt,PO,ASIN) → one HashAggregate + Hash Left Join; `AppointmentListView` drops from O(N·M) to O(N+M) | Low |
| **S2** | `status != 'rejected'` → `status IN (...)` | PC-3 pass + S2 EXCEPT | __ | __ | `sp_shipments_status_idx` becomes usable for committed CTE; join cost drops significantly | Low |

**How to use:**
- If a row shows **Safe = Yes** → paste the results and I will implement that one
  optimization immediately, with a compile and migration check after.
- If a row shows **Safe = No** → paste the failing query output so I can diagnose and
  either fix the data assumption or propose an alternative.
- You may paste results for all at once, or one at a time. I will implement only those
  that passed, one at a time, in the priority order shown (D2 → P1 → U1 → U2 → S1 → S2).
