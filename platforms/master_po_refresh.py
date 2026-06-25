"""Refresh helper for the master_po materialized view (see migration 0040).

`master_po_mv` is a materialized copy of the master_po view. Its source tables
(total_po, total_po_zbs, master_sheet) only change on upload, so we refresh it
right after uploads to keep the dashboard's data current without recomputing the
view on every read.
"""

import logging
import threading

from django.db import connection

logger = logging.getLogger(__name__)

# Serializes background refreshes so concurrent triggers (sheet-edit saves,
# uploads firing close together) don't spawn parallel rebuilds of the same
# matview. Postgres would queue them at the DB level anyway, but coalescing in
# the app avoids redundant multi-second REFRESH runs.
_ASYNC_REFRESH_LOCK = threading.Lock()


def refresh_master_po_mv_async() -> None:
    """Refresh master_po_mv in a background daemon thread; returns immediately.

    Lets a request handler (e.g. a Sheet Preview save) respond in the time the
    UPDATE takes instead of blocking on the multi-second REFRESH MATERIALIZED
    VIEW. The refresh runs after the request's transaction has committed, so the
    rebuilt matview includes the just-saved rows. Best-effort; never raises.
    """

    def _run():
        with _ASYNC_REFRESH_LOCK:
            try:
                refresh_master_po_mv()
            finally:
                # Background threads get their own DB connection — close it so it
                # isn't leaked back into the pool.
                connection.close()

    threading.Thread(
        target=_run, name="refresh-master-po-mv-async", daemon=True
    ).start()


# Platforms whose delivery month follows the "PO completed if any SKU delivered"
# rule: once ANY SKU on a PO has a GRN/delivery date, that PO's still-undelivered
# SKUs are counted in the same delivery month (they inherit the PO's delivery
# date). A fully-undelivered PO carries no delivery date. This mirrors how the
# source sheet classifies these platforms. (Swiggy + Blinkit only — other
# platforms are untouched.)
PO_COMPLETION_FORMATS = ("swiggy", "blinkit")


def apply_po_completion_delivery_dates(formats=PO_COMPLETION_FORMATS) -> None:
    """Apply the PO-completion delivery-date rule to total_po_zbs source rows.

    Idempotent and best-effort (never raises). Run before each master_po_mv
    refresh so the rule is applied permanently — undelivered SKUs of a completed
    PO get the PO's delivery date; fully-undelivered POs get none. This is what
    keeps Swiggy/Blinkit DEL-month classification matching the sheet without a
    manual re-propagation after every upload.
    """
    fmts = [f.lower() for f in formats]
    fp = "REGEXP_REPLACE(LOWER(TRIM(format::text)),'[^a-z0-9]+','','g') = ANY(%s)"
    try:
        with connection.cursor() as cur:
            # 1) a fully-undelivered PO must carry NO delivery date (no phantom).
            cur.execute(
                f"""UPDATE total_po_zbs SET grn_date = NULL
                    WHERE {fp} AND COALESCE(delivered_qty, 0) = 0 AND grn_date IS NOT NULL
                      AND po_number NOT IN (
                        SELECT po_number FROM total_po_zbs
                        WHERE {fp} AND COALESCE(delivered_qty, 0) > 0)""",
                [fmts, fmts],
            )
            # 2) undelivered SKUs of a completed PO inherit the PO's delivery date.
            #    MATERIALIZED forces the per-PO delivery-date aggregate to be
            #    computed ONCE; without it Postgres inlined the CTE and re-ran the
            #    full aggregate per candidate row (nested loop) — ~40s vs ~7s.
            cur.execute(
                f"""WITH pd AS MATERIALIZED (
                        SELECT po_number, MAX(grn_date) AS d FROM total_po_zbs
                        WHERE {fp} AND COALESCE(delivered_qty, 0) > 0 AND grn_date IS NOT NULL
                        GROUP BY po_number)
                    UPDATE total_po_zbs t SET grn_date = pd.d
                    FROM pd
                    WHERE t.po_number = pd.po_number AND {fp}
                      AND COALESCE(t.delivered_qty, 0) = 0 AND t.grn_date IS NULL""",
                [fmts, fmts],
            )
    except Exception:  # noqa: BLE001 - must never break the upload/refresh path
        logger.exception("apply_po_completion_delivery_dates failed")


def refresh_master_po_mv() -> bool:
    """Refresh public.master_po_mv if it exists. Best-effort; never raises.

    Returns True if a refresh ran, False if the matview is absent (e.g. the
    materialization migration 0040 has not been applied yet) or the refresh
    failed. Callers can safely ignore the result.
    """
    # Apply the PO-completion delivery-date rule to the source rows first so the
    # rebuilt matview reflects it. Best-effort; never raises.
    apply_po_completion_delivery_dates()
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


# Separate lock for secmaster_mv (a different matview than master_po_mv, so the
# two can refresh independently without blocking each other).
_SECMASTER_REFRESH_LOCK = threading.Lock()


def refresh_secmaster_mv() -> bool:
    """Refresh public.secmaster_mv if it exists. Best-effort; never raises.

    Plain (non-CONCURRENTLY) refresh: a full rebuild takes ~10s and locks reads
    of secmaster_mv for that window — the same trade-off as master_po_mv. (We
    measured CONCURRENTLY at ~46s here because the matview has no stable key, so
    its row-by-row diff is useless; the plain rebuild is ~4x faster.) Returns
    False if the matview is absent or the refresh failed.
    """
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('public.secmaster_mv')")
            if cur.fetchone()[0] is None:
                # Migration 0042 not applied yet -> nothing to refresh.
                return False
            cur.execute("REFRESH MATERIALIZED VIEW public.secmaster_mv")
        return True
    except Exception:  # noqa: BLE001 - a refresh failure must not break callers
        logger.exception("Failed to refresh secmaster_mv")
        return False


def refresh_secmaster_mv_async() -> None:
    """Refresh secmaster_mv in a background daemon thread; returns immediately.

    Lets an upload request respond without blocking on the multi-second rebuild.
    Serialized by its own lock so bursts of secondary uploads coalesce.
    """

    def _run():
        with _SECMASTER_REFRESH_LOCK:
            try:
                refresh_secmaster_mv()
            finally:
                connection.close()

    threading.Thread(
        target=_run, name="refresh-secmaster-mv-async", daemon=True
    ).start()


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
