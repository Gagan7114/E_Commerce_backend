"""Materialize blinkit_ads_master and swiggy_ads_master for instant reads.

WHY
---
Migration 0048 (ads_master_mapping_fallback) gave both views a per-row
``LEFT JOIN LATERAL`` that, for every row of blinkit_ads/swiggy_ads, re-scans
ads_master_bs AND self-joins the ads table again, comparing campaign_id OR
campaign_name with UPPER/TRIM/REPLACE expressions (no index can serve those).
The ADS dashboard's payload builder then runs that view ~7 times per request
(summary, breakdown, trend, years, months, dates), so a single dashboard load
re-evaluates the whole lateral join seven times — measured at 40s+, which the
browser surfaces as a timeout (net::ERR / pending) and React Query retries,
compounding DB load. No index can speed up a view's recomputation.

WHAT
----
Same proven pattern as master_po_mv (platforms migration 0040): build a
MATERIALIZED VIEW from each view's exact current definition (byte-for-byte
identical columns/rows), then redefine the plain view as a thin pass-through
``SELECT * FROM <name>_mv``. Every existing ``FROM blinkit_ads_master`` /
``FROM swiggy_ads_master`` query is UNCHANGED and returns the SAME data, but now
reads pre-computed rows (~20ms) instead of re-running the lateral join.

Freshness is maintained by refreshing the matviews after uploads that touch
their sources (blinkit_ads / swiggy_ads / ads_master_bs / master_sheet) — see
platforms/master_po_refresh.refresh_ads_master_mvs, wired into
uploads.views._refresh_matviews_async.

SAFETY / REVERSIBLE
-------------------
  * Output identical: each matview is created from the view's own definition.
  * Guarded: if the view is not a plain view, or the matview already exists,
    that view is skipped instead of failing.
  * Reverse restores each view to its original lateral-join definition
    (recovered from the matview's own definition) and drops the matview.

ROLL BACK
---------
  python manage.py migrate uploads 0059
"""

from django.db import migrations

VIEWS = ("blinkit_ads_master", "swiggy_ads_master")


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
    for view in VIEWS:
        mv = f"public.{view}_mv"
        with schema_editor.connection.cursor() as cur:
            # Only act when the view is a plain view and the matview is absent.
            if _relkind(cur, "public", view) != "v":
                continue
            if _relkind(cur, "public", f"{view}_mv") is not None:
                continue
            cur.execute(f"SELECT pg_get_viewdef('public.{view}'::regclass, true)")
            view_def = cur.fetchone()[0].rstrip().rstrip(";")

        # 1) Materialized copy, built from the view's exact definition + populated.
        schema_editor.execute(f"CREATE MATERIALIZED VIEW {mv} AS {view_def} WITH DATA")
        # 2) Redefine the view as a pass-through. Identical columns/order/types, so
        #    CREATE OR REPLACE VIEW is accepted and all `FROM <view>` queries keep
        #    working unchanged.
        schema_editor.execute(
            f"CREATE OR REPLACE VIEW public.{view} AS SELECT * FROM {mv}"
        )
        # 3) Plain-column indexes on the stored copy (these DO help now, unlike on
        #    a view). The dashboard filters by date/year/month and lists DISTINCT
        #    year/month/date for the filter dropdowns.
        schema_editor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{view}_mv_date ON {mv} (date)"
        )
        schema_editor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{view}_mv_year ON {mv} (year)"
        )
        schema_editor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{view}_mv_month ON {mv} (month)"
        )


def backwards(apps, schema_editor):
    for view in VIEWS:
        mv = f"public.{view}_mv"
        with schema_editor.connection.cursor() as cur:
            if _relkind(cur, "public", f"{view}_mv") is None:
                continue
            # Recover the original view body from the materialized copy.
            cur.execute(f"SELECT pg_get_viewdef('public.{view}_mv'::regclass, true)")
            mv_def = cur.fetchone()[0].rstrip().rstrip(";")

        # Restore the view to its original lateral-join definition, then drop copy.
        schema_editor.execute(f"CREATE OR REPLACE VIEW public.{view} AS {mv_def}")
        schema_editor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv} CASCADE")


class Migration(migrations.Migration):

    atomic = True

    dependencies = [
        ("uploads", "0059_mpmv_expiry_partial_index"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
