"""Blinkit per-product litre targets — the only stored data behind the new
Marketing → Blinkit Sale & Target section.

WHY
---
`month_targets` holds ONE target per (format, item_head, month, year) — a single
PREMIUM number and a single COMMODITY number per platform. The Blinkit
"Litre Vise Target" sheet is a level finer: a target per PRODUCT
(Canola 1L, Canola 5L, Pomace 1L, …) inside each item head. There is no source
feed for those numbers — they are typed by a person — so they need their own
store. Nothing here reads or writes `month_targets`; the existing Sec Targets
page is untouched.

Every other column the section shows (done litres, projection, month closes,
growth, achieved %) is derived live from `secmaster_mv`, so this table stays
exactly one number per (product, month, year).

SAFETY / IDEMPOTENT
-------------------
CREATE TABLE / INDEX IF NOT EXISTS, reverse DROP TABLE. Raw SQL (no Django
model) to match the other tables in this app (0043 call_center_targets, the
matview migrations).

The unique index is on the UPPER-CASED product name so "Canola 1L" and
"CANOLA 1L" can never both hold a target for the same month — the API upserts
on that expression.
"""

from django.db import migrations


CREATE_SQL = r"""
CREATE TABLE IF NOT EXISTS public.blinkit_product_targets (
    id          BIGSERIAL PRIMARY KEY,
    item        TEXT NOT NULL,
    item_head   VARCHAR(32) NOT NULL,
    category    TEXT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    target_ltrs NUMERIC NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Upsert key: one target per (product, month, year). Matched case-insensitively
-- so the API can INSERT ... ON CONFLICT ((UPPER(TRIM(item))), month, year).
CREATE UNIQUE INDEX IF NOT EXISTS uq_blinkit_product_targets_item_month_year
    ON public.blinkit_product_targets ((UPPER(TRIM(item))), month, year);
"""

DROP_SQL = "DROP TABLE IF EXISTS public.blinkit_product_targets;"


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0070_meta_data_dedupe_and_date_repair"),
    ]

    operations = [
        migrations.RunSQL(CREATE_SQL, reverse_sql=DROP_SQL),
    ]
