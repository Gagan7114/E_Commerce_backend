"""Pendency / Primary dashboard read-path indexes on master_po.

Adds three indexes used by views that filter master_po by
(format, open_close, po_year, po_month) and parse po_date from text.
Created CONCURRENTLY so existing reads/writes are not blocked.

Safe / reversible:
  * No data is read, written, or modified.
  * Original rows untouched.
  * Reverse migration drops the indexes (CONCURRENTLY).
"""

from django.db import migrations


class Migration(migrations.Migration):

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction.
    atomic = False

    dependencies = [
        ("platforms", "0035_amazon_mp_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
                'idx_master_po_format_open_year_month '
                'ON "master_po" '
                '(UPPER(TRIM("format"::text)), '
                ' UPPER(TRIM("open_close"::text)), '
                ' "po_year", '
                ' UPPER(TRIM("po_month"::text)));'
            ),
            reverse_sql=(
                'DROP INDEX CONCURRENTLY IF EXISTS '
                'idx_master_po_format_open_year_month;'
            ),
        ),
        migrations.RunSQL(
            sql=(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
                'idx_master_po_po_date_ddmmyyyy '
                'ON "master_po" '
                '((TO_DATE(TRIM("po_date"::text), \'DD-MM-YYYY\'))) '
                "WHERE TRIM(\"po_date\"::text) ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$';"
            ),
            reverse_sql=(
                'DROP INDEX CONCURRENTLY IF EXISTS '
                'idx_master_po_po_date_ddmmyyyy;'
            ),
        ),
        migrations.RunSQL(
            sql=(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
                'idx_master_po_po_date_iso '
                'ON "master_po" '
                '((TRIM("po_date"::text)::date)) '
                "WHERE TRIM(\"po_date\"::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';"
            ),
            reverse_sql=(
                'DROP INDEX CONCURRENTLY IF EXISTS '
                'idx_master_po_po_date_iso;'
            ),
        ),
    ]
