# Make Amazon Primary re-sync with `master_sheet` (plan)

> **Status: PLAN ONLY — not implemented.** Goal: when `master_sheet` is edited,
> the Amazon primary PO data (`reporting."Amazon PO"`) should pick up the change
> the same way every other platform already does.

## 1. Why it works everywhere except Amazon (root cause)

The non-Amazon "master_po" and the Amazon PO are **two different kinds of object**:

| | "master_po" (non-Amazon) | Amazon primary |
| --- | --- | --- |
| Real object | **VIEW** `public.prim_master_po` | **TABLE** `reporting."Amazon PO"` |
| `category` / `sub_category` / `item_head` / litres | **re-joined from `master_sheet` on every read** | **frozen at upload time** |
| Effect of a `master_sheet` edit | reflected instantly, everywhere | not reflected until that PO line is re-uploaded |

- `prim_master_po` is defined in
  [platforms/migrations/0027_prim_master_po_view.py](../platforms/migrations/0027_prim_master_po_view.py):
  a `master_lookup` CTE `FROM public.master_sheet` is `LEFT JOIN`ed to the 7
  `<slug>_prim` upload tables on `UPPER(TRIM(sku_code)) = UPPER(TRIM(format_sku_code))`,
  and category/litres/amounts are computed **inline in the view**. No stored copy →
  nothing to refresh.
- `reporting."Amazon PO"` is a physical table. Its master_sheet-derived columns are
  written **once**, by the upsert inside `_transform_amazon_po()`
  ([uploads/amazon_uploads.py](../uploads/amazon_uploads.py) ~L1143–1627):
  `INSERT … ON CONFLICT (source_line_key) DO UPDATE SET category = EXCLUDED.category, …`.
  `EXCLUDED.*` is only recomputed when a **new upload** re-runs the join, so a later
  `master_sheet` edit never reaches existing rows (the `SEASAME OIL → SESAME OIL`
  symptom).
- The `master_sheet` save endpoints (`master_sheet_update`,
  `master_sheet_bulk_upsert` in [uploads/views.py](../uploads/views.py)) only write
  `master_sheet`. They trigger **no** propagation — and don't need to for the
  non-Amazon side because it's a view.

**So: yes, this is fixable. The non-Amazon side needs nothing; we only have to give
Amazon a way to re-derive from `master_sheet` after the upload.**

## 2. Columns that must be re-derived (the full chain)

A faithful re-sync cannot just overwrite `category`. The Amazon transform pulls a
set of attributes from `master_sheet` and then **recomputes everything downstream**:

- **Pulled directly from `master_sheet`:** `category`, `sub_category`, `item_head`,
  `per_liter` (from `per_unit_value`, with the ML/LTR parsing fallback), `brand`,
  `item`, `category_head`, `tax_rate`, `uom`, `case_pack` (and any other
  `MASTER_SHEET_COLUMNS` field surfaced on the row).
- **Recomputed from the above + quantities/rates:** `total_order_liters`
  (`requested_qty * per_liter`), `total_delivered_liters` (`received_qty * per_liter`),
  `order_ltrs_cl`, the amount columns, and any fill-rate columns.

Operational columns NOT from master_sheet — `po_status`, `days_to_expiry`,
appointment/shipment fields, dates — **must be left untouched** by any refresh.

## 3. Matching key (Amazon ↔ master_sheet)

The upload join uses a 4-rank fallback (asin → external_id → merchant_sku →
product_name), tie-broken toward `master_sheet.format = 'AMAZON'`. In the stored
table, `sku_code = asin`, so the natural refresh join is:

```
reporting."Amazon PO".sku_code  =  master_sheet.format_sku_code   (UPPER/TRIM both)
```

This is exactly the join the existing `zero_ltr_no_per_unit` management command
already uses. **Open decision:** replicate the full 4-rank fallback for fidelity, or
accept the primary `asin`/`sku_code` match only (simpler; misses rows that originally
matched via fallback ranks 2–4).

## 4. Options

### Option A — Convert Amazon to a live VIEW (true architectural parity)
Persist the raw, un-enriched Amazon rows in a base table and replace
`reporting."Amazon PO"` with a VIEW that joins `master_sheet` live (mirroring
`0027_prim_master_po_view`). Then edits propagate with zero refresh, forever.
- **Pros:** real parity, no drift ever, no trigger to maintain.
- **Cons / blockers:** Amazon PO carries many *operational, post-upload-editable*
  columns (appointments, shipments, statuses, dates) that are **not** derivable from
  master_sheet; a view requires all of those to live in a stable base table. Must
  confirm a persistent raw table exists (today the transform reads transient staging).
  Also recomputes on every read (volume/perf). **Bigger, riskier change.**

### Option B — Full-table refresh function, run on every `master_sheet` save
A `refresh_amazon_po_from_master_sheet()` doing one `UPDATE reporting."Amazon PO" a
SET … FROM (master_sheet join) WHERE a.sku_code = ml.format_sku_code`, re-deriving the
section-2 columns, called from `master_sheet_update` / `master_sheet_bulk_upsert`.
- **Pros:** no schema change; reuses the existing derivation SQL.
- **Cons:** a full-table re-derive on every bulk-upsert is heavy — same class of risk
  as the target-sheet refresh that already times out (~30s → 500) on live data.

### Option C — Incremental refresh on save + one-off backfill (recommended)
Same UPDATE as B, **but scoped to only the SKUs that changed**:
- On `master_sheet_update` (1 row) → update only Amazon rows whose
  `sku_code` matches that row's `format_sku_code`.
- On `master_sheet_bulk_upsert` → update rows matching the changed `format_sku_code`
  set.
- Plus a **management command** (model it on `zero_ltr_no_per_unit.py`) to backfill
  all existing rows once (fixes the current 41 `SEASAME OIL` rows and anything else
  already stale).
- **Pros:** fast (touches only affected rows), avoids the timeout problem, no schema
  migration, achieves "edit master_sheet → Amazon updates" behaviour the user wants.
- **Cons:** still a materialized table (a brand-new Amazon upload of an
  un-mapped SKU could reintroduce drift until the next save), so the backfill command
  is the safety net.

**Recommendation:** Option C now (low risk, directly solves the reported problem);
revisit Option A later if full architectural parity is wanted.

## 5. Rollout for Option C (once approved)

1. Add `refresh_amazon_po_from_master_sheet(sku_codes=None)` in the uploads app:
   one `UPDATE … FROM` re-deriving the section-2 columns; `sku_codes=None` = whole
   table, otherwise scoped to the given `format_sku_code`s.
2. Call it (scoped) at the end of `master_sheet_update` and
   `master_sheet_bulk_upsert`, inside the same transaction, after the master_sheet
   write succeeds.
3. Add a `refresh_amazon_po_master_sheet` management command (full-table) for backfill
   + manual re-sync.
4. Decide the matching fidelity (full 4-rank vs asin-only — §3).
5. Verify: edit a master_sheet category → confirm matching Amazon PO rows + the
   dashboard "Litres by platform" category list update; confirm litres/amounts
   recompute, and operational columns are untouched.

## 6. Note on the immediate `SEASAME OIL` issue

Independent of this plan, the 41 stale `reporting."Amazon PO"` rows can be corrected
right now with a one-off `UPDATE … SET category='SESAME OIL' WHERE category='SEASAME OIL'`.
The backfill command in step 3 generalises that fix.
