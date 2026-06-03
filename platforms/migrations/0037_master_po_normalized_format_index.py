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


class Migration(migrations.Migration):

    atomic = False

    dependencies = [
        ("platforms", "0036_master_po_pendency_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "idx_master_po_format_normalized "
                'ON "master_po" '
                "((REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), "
                "'[^a-z0-9]+', '', 'g')));"
            ),
            reverse_sql=(
                "DROP INDEX CONCURRENTLY IF EXISTS "
                "idx_master_po_format_normalized;"
            ),
        ),
    ]
