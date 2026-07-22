"""Read-path performance indexes for the new slow sections.

Targets three cold spots the recent features exposed:

  * Penetration report (dashboard/penetration.py) — the sec x inventory join
    scans `all_platform_inventory` (a UNION view) filtered by
    `inventory_date BETWEEN` plus a `MAX(inventory_date) GROUP BY format`
    sub-query. Each UNION branch is a physical `<platform>_inventory` table
    with NO index on inventory_date -> six seq scans per load. Also feeds the
    Inventory DOH alerts.
  * Ads Sale carry-forward (platforms/views.py) — a per-row
    `LEFT JOIN LATERAL (... FROM monthly_landing_rate ... LIMIT 1)` over
    blinkit/zepto/bigbasket ads masters. `monthly_landing_rate` has zero
    indexes, so every outer row re-scans the whole table.
  * Penetration SecMaster branch — filters secmaster_mv by
    `UPPER(TRIM(month)) = %s AND year::numeric = %s`; the existing
    idx_secmaster_mv_fmt_month_year leads with the regexp-normalized *format*
    expression, which this query does not constrain, so it cannot be used ->
    ~726k-row seq scan. A month-leading index fixes that.

Safe / reversible:
  * No data read, written, or modified.
  * All indexes CREATE ... CONCURRENTLY IF NOT EXISTS (no table lock, no dup).
  * Each target is guarded by a relkind check, so absent tables are skipped.
  * Reverse drops every index CONCURRENTLY.
"""

from django.db import migrations


# Plain-column inventory_date index, one per physical inventory base table.
_INVENTORY_TABLES = (
    "blinkit_inventory",
    "zepto_inventory",
    "swiggy_inventory",
    "bigbasket_inventory",
    "jiomart_inventory",
    "amazon_inventory",
)

# (index_name, relation, create_sql, drop_sql) for the expression indexes.
_EXPR_INDEXES = (
    (
        "idx_mlr_fmt_sku_month_created",
        "monthly_landing_rate",
        # NOTE: `month` is a TEXT column, so `month::date` is only STABLE
        # (DateStyle-dependent) and Postgres rejects it in an index expression
        # ("functions in index expression must be marked IMMUTABLE"). We index
        # the raw text `month` instead — every key here is immutable. The win
        # of this index is the equality seek on (format, sku_code) that the
        # Ads-Sale LATERAL join performs; the trailing month/created_at columns
        # only tie-break within the small matched group.
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mlr_fmt_sku_month_created "
        "ON public.monthly_landing_rate ("
        "  REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g'),"
        "  UPPER(TRIM(\"sku_code\"::text)),"
        "  \"month\" DESC,"
        "  \"created_at\" DESC"
        ") WHERE \"month\" IS NOT NULL;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_mlr_fmt_sku_month_created;",
    ),
    (
        "idx_secmaster_mv_month_year",
        "secmaster_mv",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_secmaster_mv_month_year "
        "ON public.secmaster_mv ("
        "  UPPER(TRIM(\"month\"::text)),"
        "  \"year\""
        ");",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_secmaster_mv_month_year;",
    ),
)


def _relkind(connection, relname):
    """'r'/'p' table/partitioned, 'm' matview, None if the relation is absent."""
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s
            LIMIT 1
            """,
            [relname],
        )
        row = cur.fetchone()
    return row[0] if row else None


def create_indexes(apps, schema_editor):
    conn = schema_editor.connection

    # 1) inventory_date on each physical inventory base table (skip absentees).
    for table in _INVENTORY_TABLES:
        if _relkind(conn, table) in ("r", "p"):
            schema_editor.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"idx_{table}_inventory_date "
                f'ON public."{table}" ("inventory_date");'
            )

    # 2) expression indexes (monthly_landing_rate table + secmaster_mv matview).
    for _name, relation, create_sql, _drop_sql in _EXPR_INDEXES:
        if _relkind(conn, relation) in ("r", "p", "m"):
            schema_editor.execute(create_sql)


def drop_indexes(apps, schema_editor):
    for table in _INVENTORY_TABLES:
        schema_editor.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            f"idx_{table}_inventory_date;"
        )
    for _name, _relation, _create_sql, drop_sql in _EXPR_INDEXES:
        schema_editor.execute(drop_sql)


class Migration(migrations.Migration):

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    atomic = False

    dependencies = [
        ("platforms", "0058_bigbasket_sec_range_master_view"),
    ]

    operations = [
        migrations.RunPython(create_indexes, drop_indexes),
    ]
