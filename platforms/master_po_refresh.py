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

# Cluster-wide (cross-process) advisory-lock key for the secmaster_mv refresh.
# The threading.Lock above only serializes refreshes WITHIN one Python process;
# with several gunicorn workers (and an OS-cron `refresh_secmaster`) each has its
# own lock, so triggers from different processes still stacked full REFRESHes at
# the DB. A plain REFRESH takes an AccessExclusiveLock, so a pile-up locks every
# reader of secmaster_mv out continuously -> the whole dashboard reads 0. This
# Postgres advisory lock is shared by every process, so at most one refresh runs
# cluster-wide and any overlapping trigger is dropped instead of queued.
_SECMASTER_ADVISORY_KEY = 728301

# Skip a refresh if secmaster_mv was already refreshed within this many seconds.
# Collapses a burst of triggers (uploads + schedule + multiple workers) into a
# single rebuild so the exclusive-lock window can't dominate the dashboard.
# Tunable via SECMASTER_REFRESH_MIN_INTERVAL_S (env read by callers if needed).
_SECMASTER_MIN_INTERVAL_S = 120


def refresh_secmaster_mv(min_interval_s: int = _SECMASTER_MIN_INTERVAL_S) -> bool:
    """Refresh public.secmaster_mv, coalescing concurrent/near-simultaneous
    triggers cluster-wide. Best-effort; never raises.

    Plain (non-CONCURRENTLY) refresh: a full rebuild takes ~10s and locks reads
    of secmaster_mv for that window. Because that lock blocks every reader, we
    must never let refreshes stack:

    * A non-blocking ``pg_try_advisory_lock`` guard means only ONE refresh runs
      across all workers/processes at a time — a trigger that arrives while a
      refresh is in flight is dropped (returns False), not queued behind an
      exclusive lock.
    * A ``min_interval_s`` debounce (tracked in ``matview_refresh_state``) skips
      a refresh that ran within the window, so a burst of uploads/schedule ticks
      rebuilds the view once instead of back-to-back.

    Returns True only if a refresh actually ran; False if it was skipped
    (coalesced/debounced), the matview is absent, or the refresh failed.
    """
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('public.secmaster_mv')")
            if cur.fetchone()[0] is None:
                # Migration 0042 not applied yet -> nothing to refresh.
                return False

            # Cross-process guard: if another process holds it, a refresh is
            # already running/queued there — coalesce (skip) instead of stacking.
            cur.execute("SELECT pg_try_advisory_lock(%s)", [_SECMASTER_ADVISORY_KEY])
            if not cur.fetchone()[0]:
                return False
            try:
                # Debounce is best-effort: if the state table can't be used we
                # still refresh (correctness over optimisation), we just don't
                # get the "skip if recent" saving.
                try:
                    cur.execute(
                        "CREATE TABLE IF NOT EXISTS matview_refresh_state ("
                        "name text PRIMARY KEY, "
                        "last_refreshed timestamptz NOT NULL DEFAULT now())"
                    )
                    cur.execute(
                        "SELECT now() - last_refreshed < make_interval(secs => %s) "
                        "FROM matview_refresh_state WHERE name = 'secmaster_mv'",
                        [min_interval_s],
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        return False  # refreshed within the window -> skip
                except Exception:  # noqa: BLE001 - debounce is optional
                    logger.exception(
                        "secmaster_mv debounce check failed; refreshing anyway"
                    )

                cur.execute("REFRESH MATERIALIZED VIEW public.secmaster_mv")

                try:
                    cur.execute(
                        "INSERT INTO matview_refresh_state (name, last_refreshed) "
                        "VALUES ('secmaster_mv', now()) "
                        "ON CONFLICT (name) DO UPDATE SET last_refreshed = now()"
                    )
                except Exception:  # noqa: BLE001 - stamp is optional
                    logger.exception("secmaster_mv debounce stamp failed")
                return True
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s)", [_SECMASTER_ADVISORY_KEY])
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


# Separate lock for the ads-master matviews (blinkit_ads_master_mv /
# swiggy_ads_master_mv from uploads migration 0060), so ads uploads coalesce
# without blocking the master_po / secmaster refreshes.
_ADS_MASTER_REFRESH_LOCK = threading.Lock()


def refresh_ads_master_mvs() -> bool:
    """Refresh blinkit_ads_master_mv / swiggy_ads_master_mv if present.

    Both are materialized copies (uploads migration 0060) of views whose per-row
    LATERAL join is too expensive to recompute on every dashboard read. Their
    sources (blinkit_ads / swiggy_ads / ads_master_bs / master_sheet) only change
    on upload, so refreshing after uploads keeps the dashboards current while
    making every read a cheap table scan. Best-effort; never raises. Returns True
    if at least one matview was refreshed."""
    refreshed = False
    for mv in (
        "public.blinkit_ads_master_mv",
        "public.swiggy_ads_master_mv",
        # Per-day ads masters (Daily Ads dashboards) — migration 0055.
        "public.swiggyads_daily_master_mv",
        "public.zeptoads_daily_master_mv",
        "public.bigbasketads_daily_master_mv",
    ):
        try:
            with connection.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", [mv])
                if cur.fetchone()[0] is None:
                    # Migration 0060 not applied yet -> nothing to refresh.
                    continue
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
            refreshed = True
        except Exception:  # noqa: BLE001 - a refresh failure must not break callers
            logger.exception("Failed to refresh %s", mv)
    return refreshed


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
            # Stamp the refresh time so the dashboard's cheap /version endpoint can
            # tell open clients that fresh data is ready — they refetch within one
            # poll interval (~1.5s) instead of waiting out the 60s cache. Best
            # effort: a stamp failure must not fail the refresh itself.
            try:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS matview_refresh_state ("
                    "name text PRIMARY KEY, "
                    "last_refreshed timestamptz NOT NULL DEFAULT now())"
                )
                cur.execute(
                    "INSERT INTO matview_refresh_state (name, last_refreshed) "
                    "VALUES ('amazon_mp_master', now()) "
                    "ON CONFLICT (name) DO UPDATE SET last_refreshed = now()"
                )
            except Exception:  # noqa: BLE001 - stamp is optional
                logger.exception("amazon_mp_master refresh stamp failed")
        return True
    except Exception:  # noqa: BLE001 - a refresh failure must not break callers
        logger.exception("Failed to refresh amazon_mp_master")
        return False
