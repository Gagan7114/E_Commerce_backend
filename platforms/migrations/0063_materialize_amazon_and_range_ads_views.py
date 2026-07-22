"""Materialize the remaining heavy read-path views behind three slow dashboards.

Targets (all plain, un-materialized VIEWs today, each recomputing a DISTINCT ON
over master_sheet + per-row LATERAL/scalar subqueries on EVERY read):

  Amazon SOH/DOH dashboard (`_amazon_soh_doh_dashboard`) reads:
    * amazon_master_inventory        (amazon_inventory + master_sheet)
    * amazon_sec_range_master_view   (amazon_sec_range + margins + master_sheet)
  The endpoint references each of these across 4-6 sub-queries per request, so
  every reference re-runs the whole view -> ~6.3s cold.

  Marketing "Ads Summary" (`marketing_ads_summary`) unions in:
    * zepto_ads_master               (zepto_ads + master_sheet + landing rate)
    * bigbasket_ads_master           (bigbasket_ads + master_sheet + landing rate)
    * amazon_sec_daily_master_view   (amazon_sec_daily + margins + master_sheet)
  (swiggy_ads_master / blinkit_ads_master / secmaster_mv are already matview-
  backed; these three were the remaining un-materialized union branches -> ~2.8s.)

Approach (identical to 0062): materialize each view's CURRENT definition (captured
live via `pg_get_viewdef`, so column set + semantics are byte-identical to the
live view) into `<view>_mv`, then re-point the plain view at that matview. Reads
become a cheap indexed table scan; data is as-of-last-refresh instead of live.

Freshness: refresh is wired in platforms.master_po_refresh
(`refresh_ads_master_mvs` gains the two range-ads matviews;
`refresh_amazon_view_mvs` is new for the three Amazon matviews) and dispatched
from uploads.views._clear_upload_dependent_cache whenever a source table is
uploaded — the same mechanism that already keeps secmaster_mv / amazon_mp_master
/ the ads matviews current.

Note on amazon_master_inventory: its body picks, per SKU, the inventory row
closest to CURRENT_DATE. Materializing freezes CURRENT_DATE at refresh time;
because it refreshes on every amazon_inventory upload and snapshots are historical
(never future-dated), "closest to today" == "latest uploaded snapshot", which is
exactly what the dashboard shows today — so the result is unchanged in practice.

Safe / reversible:
  * No row data read, written, or modified.
  * Idempotent: a view already reading its `<view>_mv` is skipped.
  * Reverse restores each plain view from its matview's own definition, then
    drops the matview.
"""

from django.db import migrations


# view -> columns to index on the resulting matview (only those that exist are
# created). These match the dashboards' equality/range filters (year/month,
# date) and the roll-up grain (asin), giving indexed scans on the small matview.
_VIEWS = (
    ("amazon_master_inventory", ("year", "inventory_date", "asin")),
    ("amazon_sec_range_master_view", ("year", "from_date", "asin")),
    ("amazon_sec_daily_master_view", ("year", "from_date", "asin")),
    ("zepto_ads_master", ("year", "date")),
    ("bigbasket_ads_master", ("year", "date")),
)


def _viewdef(cur, relname):
    cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", [relname])
    row = cur.fetchone()
    return (row[0] or "").strip().rstrip(";") if row else ""


def _mv_columns(cur, mv):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s",
        [mv],
    )
    return {r[0] for r in cur.fetchall()}


def forwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cur:
        for view, index_cols in _VIEWS:
            mv = f"{view}_mv"
            body = _viewdef(cur, view)
            if not body or (mv.lower() in body.lower()):
                # Already materialized/re-pointed -> idempotent skip.
                continue
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS public.{mv} CASCADE")
            cur.execute(f"CREATE MATERIALIZED VIEW public.{mv} AS {body}")
            cur.execute(
                f"CREATE OR REPLACE VIEW public.{view} AS "
                f"SELECT * FROM public.{mv}"
            )
            present = _mv_columns(cur, mv)
            for col in index_cols:
                if col in present:
                    cur.execute(
                        f'CREATE INDEX IF NOT EXISTS idx_{mv}_{col} '
                        f'ON public.{mv} ("{col}")'
                    )


def backwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cur:
        for view, _cols in _VIEWS:
            mv = f"{view}_mv"
            body = _viewdef(cur, mv)  # matview def == the original heavy body
            if not body:
                continue
            cur.execute(f"CREATE OR REPLACE VIEW public.{view} AS {body}")
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS public.{mv} CASCADE")


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0062_rematerialize_daily_ads_masters"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
