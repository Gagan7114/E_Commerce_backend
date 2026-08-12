"""Index `blinkitSec` by date.

The table's only useful index is `blinkit_unique_key (item_id, city_id, date)`.
Date is the *trailing* column there, so a `WHERE date BETWEEN %s AND %s` cannot
seek on it and Postgres falls back to a sequential scan of the whole table.

Measured on the Campaigns Optimization brand-fund query for a single month
(`SELECT date, city_name, item_id, SUM(qty_sold) ... GROUP BY date, item_id,
city_name`): a Seq Scan over 145,068 rows to return 3,200. That is the slowest
statement behind the page, and it gets slower every month as the table grows,
while the queries beside it already seek on their own date indexes.

Same shape as migration 0059's inventory_date indexes: plain column, CONCURRENTLY,
guarded so an environment without the table is skipped rather than failing.

CREATE INDEX requires table OWNERSHIP, not merely write access, and `blinkitSec`
is one of the relations owned by `postgres` rather than by the app role — the
same wall the FKG master upload hits. Where that is the case this migration logs
and moves on rather than breaking the deploy; a superuser can then run:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blinkitsec_date
        ON public."blinkitSec" ("date");

and the page gets the speed-up with no code change.
"""

import logging

from django.db import migrations
from django.db.utils import ProgrammingError

logger = logging.getLogger(__name__)

_TABLE = "blinkitSec"
_INDEX = "idx_blinkitsec_date"


def _is_table(connection, relname):
    """'r'/'p' for a table, None when the relation is absent."""
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
    return bool(row) and row[0] in ("r", "p")


def create_index(apps, schema_editor):
    conn = schema_editor.connection
    if not _is_table(conn, _TABLE):
        return
    try:
        schema_editor.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} "
            f'ON public."{_TABLE}" ("date");'
        )
    except ProgrammingError as exc:
        # "must be owner of table" — see the module docstring. The index is a
        # pure optimisation, so a deploy that cannot create it should still
        # succeed; the page stays correct, just slower.
        logger.warning(
            "Skipped %s on %s: %s. A superuser can create it by hand.",
            _INDEX,
            _TABLE,
            exc,
        )


def drop_index(apps, schema_editor):
    try:
        schema_editor.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX};")
    except ProgrammingError as exc:
        logger.warning("Skipped dropping %s: %s", _INDEX, exc)


class Migration(migrations.Migration):
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    atomic = False

    dependencies = [
        ("platforms", "0071_blinkit_product_targets"),
    ]

    operations = [
        migrations.RunPython(create_index, drop_index),
    ]
