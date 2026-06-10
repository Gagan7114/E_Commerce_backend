"""Refresh helper for the master_po materialized view (see migration 0040).

`master_po_mv` is a materialized copy of the master_po view. Its source tables
(total_po, total_po_zbs, master_sheet) only change on upload, so we refresh it
right after uploads to keep the dashboard's data current without recomputing the
view on every read.
"""

import logging

from django.db import connection

logger = logging.getLogger(__name__)


def refresh_master_po_mv() -> bool:
    """Refresh public.master_po_mv if it exists. Best-effort; never raises.

    Returns True if a refresh ran, False if the matview is absent (e.g. the
    materialization migration 0040 has not been applied yet) or the refresh
    failed. Callers can safely ignore the result.
    """
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('public.master_po_mv')")
            if cur.fetchone()[0] is None:
                # Migration not applied -> nothing to refresh. Cheap no-op.
                return False
            cur.execute("REFRESH MATERIALIZED VIEW public.master_po_mv")
        return True
    except Exception:  # noqa: BLE001 - a refresh failure must not break callers
        logger.exception("Failed to refresh master_po_mv")
        return False


def refresh_amazon_mp_master() -> bool:
    """Refresh public.amazon_mp_master if it's a materialized view (migration
    0041). Best-effort; never raises.

    amazon_mp_master is fed by amazon_mp + master_sheet, which only change on
    upload, so refreshing after uploads keeps it current while making every read
    a cheap table scan instead of re-parsing shipment_date per row. Returns True
    if a refresh ran, False if the object is absent or still a plain view (0041
    not applied) or the refresh failed.
    """
    try:
        with connection.cursor() as cur:
            # 'm' = materialized view; a plain view ('v') has nothing to refresh.
            cur.execute(
                "SELECT relkind FROM pg_class WHERE relname = 'amazon_mp_master'"
            )
            row = cur.fetchone()
            if not row or row[0] != "m":
                return False
            cur.execute("REFRESH MATERIALIZED VIEW public.amazon_mp_master")
        return True
    except Exception:  # noqa: BLE001 - a refresh failure must not break callers
        logger.exception("Failed to refresh amazon_mp_master")
        return False
