from django.db import migrations, transaction

# BigBasket secondary rows come in two business types: b2c and bbdaily. The
# source export carries both for the same SKU + city + date, differing only in
# quantity/sales. The bigbasketSec table has no business_type column, so the old
# UNIQUE(source_sku_id, source_city_name, date_range) treated the two channels as
# one row — the second insert overwrote the first (last-wins), silently dropping
# the larger b2c sale.
#
# Widen the dedup key to include total_quantity + total_sales so the two channels
# no longer collide and both survive. The new key is a superset of the old one,
# so existing rows (already unique on the 3-tuple) stay unique — no dedup needed
# before creating the index.
#
# Downstream reports (platforms.monthly_targets, the SecMaster views) SUM
# total_quantity and JOIN on source_sku_id only; they do not rely on row
# uniqueness, so splitting a collided pair back into two rows makes them count
# both channels correctly rather than breaking them.

_TABLE = 'public."bigbasketSec"'
_INDEX = 'bigbasket_unique'

DROP_OLD = f"""
ALTER TABLE IF EXISTS {_TABLE} DROP CONSTRAINT IF EXISTS {_INDEX};
DROP INDEX IF EXISTS public.{_INDEX};
"""

CREATE_NEW = f"""
CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX}
    ON {_TABLE} (source_sku_id, source_city_name, date_range, total_quantity, total_sales);
"""

CREATE_OLD = f"""
CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX}
    ON {_TABLE} (source_sku_id, source_city_name, date_range);
"""


def _table_exists(cur) -> bool:
    cur.execute("SELECT to_regclass('public.\"bigbasketSec\"')")
    return cur.fetchone()[0] is not None


def widen_key(apps, schema_editor):
    conn = schema_editor.connection
    with conn.cursor() as cur:
        if not _table_exists(cur):
            return
    with transaction.atomic():
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '30s'")
            cur.execute(DROP_OLD)
            cur.execute(CREATE_NEW)


def narrow_key(apps, schema_editor):
    # Reverse: restore the original 3-column key. This can fail if b2c/bbdaily
    # rows have since been inserted (the 3-tuple would no longer be unique); that
    # is expected — a manual dedup would be required before reverting.
    conn = schema_editor.connection
    with conn.cursor() as cur:
        if not _table_exists(cur):
            return
    with transaction.atomic():
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '30s'")
            cur.execute(DROP_OLD)
            cur.execute(CREATE_OLD)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("uploads", "0076_primary_po_appointment_date"),
    ]

    operations = [
        migrations.RunPython(widen_key, narrow_key),
    ]
