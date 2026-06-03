"""Functional index matching the Primary Dashboard CTE filter on master_po.

The primary-dashboard CTE filters with:

    REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = '<slug>'

Without a functional index on this exact expression, Postgres must seq-scan
the entire master_po table for every Swiggy / Zepto / Blinkit / Flipkart /
JioMart / CityMall / Zomato primary-dashboard request. This migration adds
the matching expression index so those scans become index lookups.

Safe / reversible:
  * Index only — no rows read, written, or modified.
  * Created CONCURRENTLY (no table lock).
  * Reverse migration drops the index.
"""

from django.db import migrations


def _master_po_is_indexable(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = 'master_po'
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    return bool(row and row[0] in ("r", "p"))


def create_index(apps, schema_editor):
    if not _master_po_is_indexable(schema_editor.connection):
        return
    schema_editor.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "idx_master_po_format_normalized "
        'ON "master_po" '
        "((REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), "
        "'[^a-z0-9]+', '', 'g')));"
    )


def drop_index(apps, schema_editor):
    schema_editor.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS "
        "idx_master_po_format_normalized;"
    )


class Migration(migrations.Migration):

    atomic = False

    dependencies = [
        ("platforms", "0036_master_po_pendency_indexes"),
    ]

    operations = [
        migrations.RunPython(create_index, drop_index),
    ]
