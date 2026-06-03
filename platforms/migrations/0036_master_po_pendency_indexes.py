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


def create_indexes(apps, schema_editor):
    if not _master_po_is_indexable(schema_editor.connection):
        return
    schema_editor.execute(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'idx_master_po_format_open_year_month '
        'ON "master_po" '
        '(UPPER(TRIM("format"::text)), '
        ' UPPER(TRIM("open_close"::text)), '
        ' "po_year", '
        ' UPPER(TRIM("po_month"::text)));'
    )
    schema_editor.execute(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'idx_master_po_po_date_ddmmyyyy '
        'ON "master_po" '
        '((TO_DATE(TRIM("po_date"::text), \'DD-MM-YYYY\'))) '
        "WHERE TRIM(\"po_date\"::text) ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$';"
    )
    schema_editor.execute(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'idx_master_po_po_date_iso '
        'ON "master_po" '
        '((TRIM("po_date"::text)::date)) '
        "WHERE TRIM(\"po_date\"::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';"
    )


def drop_indexes(apps, schema_editor):
    schema_editor.execute(
        'DROP INDEX CONCURRENTLY IF EXISTS '
        'idx_master_po_format_open_year_month;'
    )
    schema_editor.execute(
        'DROP INDEX CONCURRENTLY IF EXISTS '
        'idx_master_po_po_date_ddmmyyyy;'
    )
    schema_editor.execute(
        'DROP INDEX CONCURRENTLY IF EXISTS '
        'idx_master_po_po_date_iso;'
    )


class Migration(migrations.Migration):

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction.
    atomic = False

    dependencies = [
        ("platforms", "0035_amazon_mp_master_view"),
    ]

    operations = [
        migrations.RunPython(create_indexes, drop_indexes),
    ]
