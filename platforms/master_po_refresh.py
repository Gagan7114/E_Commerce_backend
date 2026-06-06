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
