"""Re-materialize the per-day ads master views (Swiggy / Zepto / BigBasket
Daily Ads dashboards).

Why
---
Migration 0055 materialized these three views into `*_daily_master_mv` and
re-pointed the plain view at the matview (a cheap table scan instead of a 5-10s
per-row LATERAL recompute). A later uploads migration
(0079_ads_master_basic_rate_carry_forward) re-issued
`CREATE OR REPLACE VIEW <name> AS <heavy body>` to ADD the carry-forward
`total_sale_basic_rate` / `basic_rate` columns — which silently pointed the view
BACK at the raw tables and orphaned the matview. Every dashboard read then
recomputed the heavy view again (measured ~14.7s cold for Swiggy Daily), and
`_ads_dashboard_payload` reads the source 6× per request.

This migration restores the intended matview backing WITHOUT losing the newer
carry-forward columns: it materializes each view's CURRENT definition (the 0079
body, captured live via `pg_get_viewdef`) into its matview, then re-points the
plain view at that matview. Column set and semantics are therefore identical to
today's live view — only the read cost changes (as-of-refresh instead of live).

Freshness: `refresh_ads_master_mvs()` already refreshes these three matviews,
and `_ADS_MASTER_SOURCE_TABLES` already includes their raw source tables
(swiggyads_daily / zeptoads_daily / bigbasketads_daily), so every relevant
upload rebuilds them — the dashboards stay current, exactly as designed by 0055.

Safe / reversible:
  * No row data read, written, or modified.
  * Idempotent: a view already reading its matview is skipped.
  * Reverse restores the plain (heavy) view from the matview's own definition,
    then drops the matview — so the newer 0079 columns survive a rollback too.
"""

from django.db import migrations


# (plain view name, its materialized-view name). The matviews already exist
# (created by 0055) but are stale/orphaned; we rebuild them from the live body.
_DAILY = (
    ("swiggyads_daily_master", "swiggyads_daily_master_mv"),
    ("zeptoads_daily_master", "zeptoads_daily_master_mv"),
    ("bigbasketads_daily_master", "bigbasketads_daily_master_mv"),
)


def _viewdef(cur, relname):
    cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", [relname])
    row = cur.fetchone()
    return (row[0] or "").strip().rstrip(";") if row else ""


def forwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cur:
        for view, mv in _DAILY:
            body = _viewdef(cur, view)
            if not body or (mv.lower() in body.lower()):
                # View already reads its matview -> nothing to do (idempotent).
                continue
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS public.{mv} CASCADE")
            cur.execute(f"CREATE MATERIALIZED VIEW public.{mv} AS {body}")
            cur.execute(
                f"CREATE OR REPLACE VIEW public.{view} AS "
                f"SELECT * FROM public.{mv}"
            )
            # Cheap filter/sort indexes for the dashboard's year/month/date scans.
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{mv}_year_month '
                f'ON public.{mv} ("year", "month")'
            )
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{mv}_date '
                f'ON public.{mv} ("date")'
            )


def backwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cur:
        for view, mv in _DAILY:
            # The matview's own definition IS the heavy body (we materialized it
            # from the live view above), so restore the plain view from it.
            body = _viewdef(cur, mv)
            if not body:
                continue
            cur.execute(f"CREATE OR REPLACE VIEW public.{view} AS {body}")
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS public.{mv} CASCADE")


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0061_master_po_fact_aware_status"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
