# Ads "daily" copy tables + master views — Implementation Plan

## Goal

Create **empty structural copies** of the three platform ads tables and their master
views — **same format / columns, new names, no data copied**:

| Original table | → Copy table | Original master view | → Copy master view |
|---|---|---|---|
| `swiggy_ads` | `swiggyads_daily` | `swiggy_ads_master` | `swiggyads_daily_master` |
| `zepto_ads` | `zeptoads_daily` | `zepto_ads_master` | `zeptoads_daily_master` |
| `bigbasket_ads` | `bigbasketads_daily` | `bigbasket_ads_master` | `bigbasketads_daily_master` |

Only the **structure** (columns, types, defaults, NOT NULLs, indexes) and the **view
logic** are replicated. The copy tables start empty.

---

## Source — what exists today (read from the live `ecms` DB)

All objects live in Postgres schema `public`. Created by `uploads` migrations
`0019` (swiggy_ads), `0021` (swiggy_ads_master), `0022/0023` (zepto), `0024/0025/0026`
(bigbasket), and patched by `0048_ads_master_mapping_fallback`.

### Base tables (column counts)

- **`swiggy_ads`** — 30 cols: `id, date, campaign_id, keyword_count, campaign_name,
  campaign_status, bidding_type, budget_type, brand_name, ad_property_count, city_count,
  product_count, ecpm, ecpc, total_impressions, total_budget, total_budget_burnt,
  total_clicks, total_ctr, total_a2c, a2c_rate, total_gmv, total_conversions, total_roi,
  total_direct_gmv_7_days, total_direct_roi_7_days, total_direct_gmv_14_days,
  total_direct_roi_14_days, format, uploaded_at`
  - `id BIGSERIAL PK`; `format` default `'SWIGGY'`; `uploaded_at` default `NOW()`
  - Indexes: `swiggy_ads_dedup_idx` UNIQUE `(date, campaign_id, keyword_count)`,
    `swiggy_ads_campaign_id_idx`, `swiggy_ads_date_idx`
- **`zepto_ads`** — 24 cols: `id, date, product_id, campaign_id, product_name, brand_id,
  brand_name, campaign_name, category, atc, clicks, cpc, cpm, ctr, impressions, orders,
  other_skus, revenue, roas, robas, same_skus, spend, format, uploaded_at`
  - `format` default `'ZEPTO'`; indexes: `zepto_ads_dedup_idx` UNIQUE, `..._date_idx`,
    `..._campaign_id_idx`, `..._product_id_idx`
- **`bigbasket_ads`** — 20 cols: `id, date, product_id, campaign_id, product_name,
  campaign_name, brand_name, category, ad_spend, ad_impressions, cpm, add_to_cart,
  orders_sku, ad_revenue, roas, other_sku_orders, same_category_orders,
  other_sku_ad_revenue, format, uploaded_at`
  - `format` default `'BIGBASKET'`; indexes: `bigbasket_ads_dedup_idx` UNIQUE, `..._date_idx`,
    `..._campaign_id_idx`, `..._product_id_idx`

### Master views (current live definitions)

All three normalize each platform's ads row into a common reporting shape
(`direct_qty_sold, impressions, ad_spent, gmv/direct_gmv, format_sku_code/sku_id,
sap_sku_name, category, sub_category, item, item_head, per_unit, per_ltr, ads_ltr_sold,
month, year, month_day`) by joining to **`master_sheet`** (SKU metadata). They differ in
how each maps an ad row to a SKU:

- **`swiggy_ads_master`** — Swiggy has no SKU id on the ad row, so it maps **campaign →
  SKU** through **`ads_master_bs`** via a `LEFT JOIN LATERAL` (campaign_id match, then a
  campaign-name fallback; most-recent mapping wins). This fallback was added by
  `0048_ads_master_mapping_fallback` — the copy replicates the **current** form.
- **`zepto_ads_master`** — direct `zepto_ads.product_id → master_sheet.format_sku_code`
  (`format = 'ZEPTO'`).
- **`bigbasket_ads_master`** — direct `bigbasket_ads.product_id → master_sheet.format_sku_code`
  (`REPLACE(UPPER(TRIM(format)),' ','') = 'BIGBASKET'`).

### Shared dependencies (NOT copied — referenced as-is)

- **`ads_master_bs`** — campaign→SKU mapping (used by the Swiggy view).
- **`master_sheet`** — SKU metadata (used by all three views).

The copy views point at the **copy tables** for the ads rows but reuse these shared
mapping tables unchanged.

---

## The copy — design decisions

1. **Tables via `CREATE TABLE … (LIKE … INCLUDING ALL)`.** This is the exact "same
   format, no data" idiom — it replicates every column, type, default, NOT NULL, and
   index (indexes get fresh auto-generated names) from the current table, reflecting any
   later `ALTER`s. No rows are copied.
2. **Independent id sequence.** `INCLUDING ALL` copies the `id` default, which still
   points at the *original's* `*_id_seq`. Each copy gets its **own** sequence so the two
   tables don't share ids.
3. **Views = current definitions with the table name swapped.** Only the ads source
   table name changes (`swiggy_ads → swiggyads_daily`, etc.). For Swiggy, the swap applies
   to **both** the outer `FROM` and the inner `s2` join in the LATERAL. `ads_master_bs`
   and `master_sheet` references are unchanged.
4. **Delivered as a Django migration** (`uploads/migrations/0050_…`) — consistent with how
   the originals were created, so the schema stays tracked and reversible.

---

## The migration

Create `uploads/migrations/0050_ads_daily_copy_tables.py`:

```python
from django.db import migrations


class Migration(migrations.Migration):
    """Empty structural copies of the platform ads tables + their master views.

    swiggy_ads/zepto_ads/bigbasket_ads  ->  *ads_daily
    *_ads_master                        ->  *ads_daily_master
    Same format/columns/indexes, new names, no data. Copy views read the copy
    tables but reuse the shared ads_master_bs + master_sheet mapping tables.
    """

    dependencies = [
        ("uploads", "0049_master_po_mv_perf_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            -- ── Copy tables (structure only, no data) ──────────────────────
            CREATE TABLE IF NOT EXISTS public.swiggyads_daily
                (LIKE public.swiggy_ads INCLUDING ALL);
            CREATE SEQUENCE IF NOT EXISTS public.swiggyads_daily_id_seq
                OWNED BY public.swiggyads_daily.id;
            ALTER TABLE public.swiggyads_daily
                ALTER COLUMN id SET DEFAULT nextval('public.swiggyads_daily_id_seq');

            CREATE TABLE IF NOT EXISTS public.zeptoads_daily
                (LIKE public.zepto_ads INCLUDING ALL);
            CREATE SEQUENCE IF NOT EXISTS public.zeptoads_daily_id_seq
                OWNED BY public.zeptoads_daily.id;
            ALTER TABLE public.zeptoads_daily
                ALTER COLUMN id SET DEFAULT nextval('public.zeptoads_daily_id_seq');

            CREATE TABLE IF NOT EXISTS public.bigbasketads_daily
                (LIKE public.bigbasket_ads INCLUDING ALL);
            CREATE SEQUENCE IF NOT EXISTS public.bigbasketads_daily_id_seq
                OWNED BY public.bigbasketads_daily.id;
            ALTER TABLE public.bigbasketads_daily
                ALTER COLUMN id SET DEFAULT nextval('public.bigbasketads_daily_id_seq');

            -- ── Copy master views (same logic, swapped source table) ───────
            CREATE OR REPLACE VIEW public.swiggyads_daily_master AS
            SELECT
                s.date,
                s.campaign_id,
                s.campaign_name,
                s.total_conversions   AS direct_qty_sold,
                s.total_impressions   AS impressions,
                s.total_budget_burnt  AS ad_spent,
                s.total_gmv           AS direct_gmv,
                s.format,
                amb.sku_id            AS format_sku_code,
                ms.sku_sap_name       AS sap_sku_name,
                ms.category,
                ms.sub_category,
                ms.item,
                ms.item_head,
                ms.per_unit,
                ms.per_unit_value     AS per_ltr,
                (COALESCE(ms.per_unit_value, 0) * COALESCE(s.total_conversions, 0)) AS ads_ltr_sold,
                UPPER(TO_CHAR(s.date, 'FMMonth'))  AS month,
                EXTRACT(YEAR FROM s.date)::integer AS year,
                (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0') || '-'
                   || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day
            FROM public.swiggyads_daily s
            LEFT JOIN LATERAL (
                SELECT amb_1.sku_id
                FROM public.ads_master_bs amb_1
                JOIN public.swiggyads_daily s2 ON s2.campaign_id = amb_1.campaign_id
                WHERE UPPER(TRIM(amb_1.format)) = 'SWIGGY'
                  AND (amb_1.campaign_id = s.campaign_id
                       OR UPPER(TRIM(s2.campaign_name)) = UPPER(TRIM(s.campaign_name)))
                ORDER BY (amb_1.campaign_id = s.campaign_id) DESC,
                         (UPPER(TRIM(amb_1.month)) = UPPER(TO_CHAR(s.date, 'FMMonth'))) DESC,
                         amb_1.updated_at DESC NULLS LAST,
                         amb_1.created_at DESC NULLS LAST
                LIMIT 1
            ) amb ON true
            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(amb.sku_id))
                  AND UPPER(TRIM(ms.format::text)) = 'SWIGGY';

            CREATE OR REPLACE VIEW public.zeptoads_daily_master AS
            SELECT
                z.date,
                z.product_id    AS sku_id,
                z.product_name  AS sku_name,
                z.same_skus     AS direct_qty_sold,
                z.other_skus    AS indirect_qty_sold,
                z.impressions,
                z.spend         AS ad_spent,
                z.revenue       AS gmv,
                ms.format,
                ms.category,
                ms.sub_category,
                ms.item,
                ms.item_head,
                ms.per_unit,
                ms.per_unit_value AS per_ltr,
                (COALESCE(ms.per_unit_value, 0) * COALESCE(z.same_skus, 0)) AS ads_ltr_sold,
                UPPER(TO_CHAR(z.date, 'FMMonth'))  AS month,
                EXTRACT(YEAR FROM z.date)::integer AS year,
                (LPAD(EXTRACT(DAY FROM z.date)::text, 2, '0') || '-'
                   || UPPER(TO_CHAR(z.date, 'FMMonth'))) AS month_day
            FROM public.zeptoads_daily z
            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(z.product_id))
                  AND UPPER(TRIM(ms.format::text)) = 'ZEPTO';

            CREATE OR REPLACE VIEW public.bigbasketads_daily_master AS
            SELECT
                b.date,
                b.product_id       AS sku_id,
                b.product_name     AS sku_name,
                b.orders_sku       AS direct_qty_sold,
                b.other_sku_orders AS indirect_qty_sold,
                b.ad_impressions   AS impressions,
                b.ad_spend         AS ad_spent,
                b.ad_revenue       AS gmv,
                ms.format,
                ms.category,
                ms.sub_category,
                ms.item,
                ms.item_head,
                ms.per_unit,
                ms.per_unit_value  AS per_ltr,
                (COALESCE(ms.per_unit_value, 0) * COALESCE(b.orders_sku, 0)) AS ads_ltr_sold,
                UPPER(TO_CHAR(b.date, 'FMMonth'))  AS month,
                EXTRACT(YEAR FROM b.date)::integer AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0') || '-'
                   || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day
            FROM public.bigbasketads_daily b
            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text)) = UPPER(TRIM(b.product_id))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BIGBASKET';
            """,
            reverse_sql=r"""
            DROP VIEW IF EXISTS public.swiggyads_daily_master;
            DROP VIEW IF EXISTS public.zeptoads_daily_master;
            DROP VIEW IF EXISTS public.bigbasketads_daily_master;
            DROP TABLE IF EXISTS public.swiggyads_daily;
            DROP TABLE IF EXISTS public.zeptoads_daily;
            DROP TABLE IF EXISTS public.bigbasketads_daily;
            """,
        ),
    ]
```

> Dropping each table also drops its `OWNED BY` sequence automatically, so the reverse
> needs no explicit `DROP SEQUENCE`.

---

## Apply & verify

```bash
# from E_Commerce_backend, with the venv active
python manage.py migrate uploads
```

Verify structure matches and tables are empty:

```sql
-- same columns as the originals
SELECT column_name FROM information_schema.columns
WHERE table_name = 'swiggyads_daily' ORDER BY ordinal_position;

-- empty
SELECT count(*) FROM swiggyads_daily;          -- 0
SELECT count(*) FROM zeptoads_daily;           -- 0
SELECT count(*) FROM bigbasketads_daily;       -- 0

-- views resolve (0 rows until the copy tables are populated)
SELECT count(*) FROM swiggyads_daily_master;
SELECT count(*) FROM zeptoads_daily_master;
SELECT count(*) FROM bigbasketads_daily_master;
```

A quick column-parity check (copy vs original) per pair:

```sql
SELECT a.column_name
FROM information_schema.columns a
FULL JOIN information_schema.columns b
  ON a.column_name = b.column_name
WHERE a.table_name = 'swiggy_ads' AND b.table_name = 'swiggyads_daily'
  AND (a.column_name IS NULL OR b.column_name IS NULL);   -- expect 0 rows
```

---

## Notes & out of scope

- **No data is copied** — the copy tables are empty; the copy views return 0 rows until
  the copy tables are populated.
- **Shared mapping tables** (`ads_master_bs`, `master_sheet`) are referenced, not copied,
  so the copy Swiggy view's campaign→SKU fallback works the moment the copy table has rows.
- **Ingestion / uploaders are out of scope.** This plan only creates the structures. If
  these copies are meant to receive their own daily uploads, the upload pipeline that
  targets `swiggy_ads`/`zepto_ads`/`bigbasket_ads` would separately need a path that
  writes to the `*ads_daily` tables — say the word and that can be planned next.
- **No Blinkit copy** — only Swiggy/Zepto/BigBasket were requested. The same recipe
  applies to `blinkit_ads`/`blinkit_ads_master` if needed later.
```
