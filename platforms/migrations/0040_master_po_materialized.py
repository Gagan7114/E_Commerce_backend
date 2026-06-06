"""Materialize master_po for instant dashboard reads.

WHY
---
`master_po` is a 3-layer VIEW (master_po -> master_po_raw -> master_po_base)
recomputed on every dashboard query. EXPLAIN ANALYZE measured ~1.2-2.4s per
call, almost all of it in the view's joins/dedupe/sort (the underlying table
scans are only ~30ms). Indexes cannot speed up a view's recomputation.

WHAT
----
Build a MATERIALIZED VIEW `master_po_mv` from master_po's exact current
definition (so its columns and rows are byte-for-byte identical), then redefine
the `master_po` view as a thin pass-through: ``SELECT * FROM master_po_mv``.

Result: every existing ``FROM master_po`` query is UNCHANGED and returns the
SAME data, but now reads pre-computed rows (~10-30ms) instead of recomputing the
view (~1.4s). Freshness is maintained by refreshing master_po_mv after uploads
(platforms/master_po_refresh.refresh_master_po_mv, wired into the upload
cache-clear hook, plus the `refresh_master_po` management command).

SAFETY / REVERSIBLE
-------------------
  * Output identical: master_po_mv is created from master_po's own definition.
  * Nothing else in the DB depends on master_po (verified), so swapping its
    definition to a pass-through is safe.
  * Guarded: if master_po is not a plain view, or master_po_mv already exists,
    this no-ops instead of failing.
  * Reverse restores master_po to its original live-view definition (recovered
    from master_po_mv's own definition) and drops master_po_mv.

ROLL BACK
---------
  python manage.py migrate platforms 0039
"""

from django.db import migrations

MV = "public.master_po_mv"


def _relkind(cur, schema, name):
    cur.execute(
        """
        SELECT c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        LIMIT 1
        """,
        [schema, name],
    )
    row = cur.fetchone()
    return row[0] if row else None


def forwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cur:
        # Only act when master_po is a plain view and the matview is absent.
        if _relkind(cur, "public", "master_po") != "v":
            return
        if _relkind(cur, "public", "master_po_mv") is not None:
            return
        cur.execute("SELECT pg_get_viewdef('public.master_po'::regclass, true)")
        view_def = cur.fetchone()[0].rstrip().rstrip(";")

    # 1) Materialized copy, built from the view's exact definition + populated.
    schema_editor.execute(f"CREATE MATERIALIZED VIEW {MV} AS {view_def} WITH DATA")
    # 2) Redefine master_po as a pass-through. Identical columns/order, so
    #    CREATE OR REPLACE VIEW is accepted and all `FROM master_po` queries
    #    keep working unchanged.
    schema_editor.execute(
        "CREATE OR REPLACE VIEW public.master_po AS SELECT * FROM public.master_po_mv"
    )
    # 3) Indexes on the stored copy (these DO help now, unlike on a view).
    schema_editor.execute(
        "CREATE INDEX IF NOT EXISTS idx_mpmv_delivmonth_year_head ON "
        f"{MV} (UPPER(TRIM(delivery_month)), delivered_year, UPPER(TRIM(item_head)))"
    )
    schema_editor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_mpmv_format ON {MV} (UPPER(TRIM(format)))"
    )
    # NB: we intentionally do NOT index public._pm_parse_date(po_date::text):
    # casting a date column to text is not IMMUTABLE, so Postgres rejects it as
    # an index expression. It isn't needed anyway — at ~41k rows the matview
    # scans in ~30ms, and the date-window dashboard queries are already fast
    # against the stored copy.


def backwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cur:
        if _relkind(cur, "public", "master_po_mv") is None:
            return
        # Recover the original view body from the materialized copy.
        cur.execute("SELECT pg_get_viewdef('public.master_po_mv'::regclass, true)")
        mv_def = cur.fetchone()[0].rstrip().rstrip(";")

    # Restore master_po to its original live-view definition, then drop the copy.
    schema_editor.execute(f"CREATE OR REPLACE VIEW public.master_po AS {mv_def}")
    schema_editor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {MV} CASCADE")


class Migration(migrations.Migration):

    atomic = True

    dependencies = [
        ("platforms", "0039_merge_primary_targets_amazon_mp"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
