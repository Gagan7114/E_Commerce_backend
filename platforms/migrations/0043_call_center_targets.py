"""Call Center monthly targets — a small, isolated store for a single new target.

This is intentionally self-contained: it does NOT touch any existing target
table (`month_targets`, `primary_month_targets`) or platform logic. It creates a
dedicated `call_center_targets` table holding one numeric target per
(month, year, item_head) where item_head is PREMIUM or COMMODITY.

Raw SQL is used (no Django model) to stay consistent with the matview / index
migrations 0040-0042 in this app. Idempotent: CREATE TABLE / INDEX IF NOT
EXISTS, with a reverse DROP TABLE.
"""

from django.db import migrations


CREATE_SQL = r"""
CREATE TABLE IF NOT EXISTS public.call_center_targets (
    id         BIGSERIAL PRIMARY KEY,
    month      INTEGER NOT NULL,
    year       INTEGER NOT NULL,
    item_head  VARCHAR(32) NOT NULL,
    targets    NUMERIC NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Upsert key: one row per (month, year, item_head). Required for
-- INSERT ... ON CONFLICT (month, year, item_head) DO UPDATE.
CREATE UNIQUE INDEX IF NOT EXISTS uq_call_center_targets_month_year_head
    ON public.call_center_targets (month, year, item_head);
"""

DROP_SQL = "DROP TABLE IF EXISTS public.call_center_targets;"


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0042_secmaster_materialized"),
    ]

    operations = [
        migrations.RunSQL(CREATE_SQL, reverse_sql=DROP_SQL),
    ]
