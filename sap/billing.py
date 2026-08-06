"""
SAP billing sync + freshness for the shipment planner.

Pulls RK-World (``CardCode = CUSTA000048``) Sales / Sales-Return rows from the
HANA ``REPORT_SALES_ANALYSIS`` proc, aggregates **net billed quantity** (Sales −
Sales Return, in eaches) per ``(U_MART_DOC_NO, ItemCode)``, and rebuilds the
``sap_billing`` table. The planner then joins that table on
``(po_number, sap_item_code)`` to keep already-billed units off the truck.

"Live + auto-updating": ``ensure_billing_fresh()`` kicks a background resync
whenever the table is older than ``SYNC_STALE_SECONDS`` (single-flight via a cache
lock), so the data self-refreshes as the views/planner are used even without an
external scheduler. A ``sync_sap_billing`` management command is provided for
cron/Celery if you prefer a fixed cadence.
"""
from __future__ import annotations

import datetime
import logging
import threading
from decimal import Decimal, InvalidOperation

from django.core.cache import cache
from django.db import connection, transaction
from django.utils import timezone

from sap.models import SapBilling
from sap.service import HANA_SCHEMAS, report_sales_analysis, select

logger = logging.getLogger(__name__)

# Customer = R K Worldinfocom (the Amazon seller-of-record billed in SAP).
RK_CARDCODE = "CUSTA000048"
RK_CARDNAME = "R K WORLDINFOCOM PVT LTD"

BILLING_WINDOW_MONTHS = 6          # how far back to pull billing
SYNC_STALE_SECONDS = 15 * 60       # auto-resync when older than this
_LAST_SYNC_KEY = "sap_billing:last_sync"
_SYNC_LOCK_KEY = "sap_billing:syncing"
# Postgres session advisory-lock key — serializes the rebuild ACROSS gunicorn
# workers (the LocMem cache lock only de-dupes threads within one worker).
_PG_LOCK_KEY = 8274_10031


def _norm(s) -> str:
    return str(s or "").strip().upper()


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v if v is not None else 0))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


# ── Dispatch ────────────────────────────────────────────────────────────────
# An invoice counts as DISPATCHED once the warehouse stamps any of these OINV
# header fields — the load has physically left, so those units can never go on
# another truck. Verified against 60 days of RK invoices (289 invoices, 254
# dispatched): U_Dipatch_Date / U_BiltyDate / U_VehicleNoM are always written
# together, U_BilltyNumber is missing on 7 of them, and U_DriverName is never
# used at all — so the rule has to be ANY-of, not all-of, and must not lean on
# any single field.
DISPATCH_FIELDS = (
    "U_Dipatch_Date",
    "U_BiltyDate",
    "U_BilltyNumber",
    "U_VehicleNoM",
    "U_DriverName",
)


# Reading the split back out. `dispatched_qty` is computed on read instead of
# living in its own column: a new column means a migration, and migrations here
# are applied by hand against prod. The invoices JSONB already carries the per
# invoice stamp, so the split costs nothing but this expression.
#
# Two rules are baked in:
#  * Credit notes free up DISPATCHED units first — a return means the goods came
#    back and can ship again — so Sales Return rows (always negative) land in the
#    same sum. GREATEST clamps the case where returns exceed what went out.
#  * A row whose invoices predate this feature has no `dispatched` key at all.
#    Those fall back to billed_qty, i.e. the old "invoiced is not plannable"
#    behaviour, so the window between deploying and the first sync can never
#    hand an already-dispatched load back to the planner. It self-heals on the
#    next sync.
def dispatched_qty_sql(alias: str = "b0") -> str:
    """The dispatched-units expression for one sap_billing row, aliased `alias`.

    Exposed on its own so a caller that only needs the number can correlate
    straight against sap_billing instead of joining SAP_BILLING_SPLIT_SQL —
    which, evaluated per outer row, re-derives the whole table (notes included)
    and cost ~2s on the pendency page.
    """
    return f"""CASE
            WHEN EXISTS (
                SELECT 1 FROM jsonb_array_elements(COALESCE({alias}.invoices, '[]'::jsonb)) e
                WHERE NOT jsonb_exists(e, 'dispatched')
            ) THEN {alias}.billed_qty
            ELSE GREATEST(COALESCE((
                SELECT SUM((e->>'qty')::numeric)
                FROM jsonb_array_elements(COALESCE({alias}.invoices, '[]'::jsonb)) e
                WHERE (e->>'dispatched')::boolean IS TRUE
                   OR e->>'type' = 'Sales Return'
            ), 0), 0)
        END"""


# SAP bills a handful of Amazon SKUs under a second item code — the same product
# in a round bottle — and the Amazon PO only ever carries the canonical one. Left
# unfolded, the planner's join finds no billing row at all and offers units SAP
# has already invoiced and DISPATCHED, which is how a load gets shipped twice.
#
# Deliberately a fixed list, not a name-matching rule: the other unmatched codes
# billed on Amazon POs are genuinely different products (olive, 1L+1L combos,
# beverages, water), and a fuzzy rule would silently merge those. Confirm both
# codes in SAP *Mart* before adding a pair — item codes are per-company, and
# FG0000384 is a completely different product (Sano Extra Virgin) in Oil.
#
#   FG0000384  MUSTARD KACHI GHANI 1 LTR 20 PCS ROUND BOTTLE  -> FG0000030
#   FG0000386  JIVO GOLD 1 LTR 20 PCS ROUND BOTTLE            -> FG0000149
#   FG0000395  SOYABEAN OIL 1 LTR 20 PCS ROUND BOTTLE         -> FG0000193
#
# Lives here rather than in uploads because it describes sap_billing itself, and
# uploads already imports this module (the reverse would be a cycle).
SAP_ITEM_ALIASES = {
    "FG0000384": "FG0000030",
    "FG0000386": "FG0000149",
    "FG0000395": "FG0000193",
}


def sap_item_code_sql(alias: str = "b0") -> str:
    """`alias`.sap_item_code folded onto the code the Amazon PO actually uses."""
    if not SAP_ITEM_ALIASES:
        return f"{alias}.sap_item_code"
    whens = " ".join(
        "WHEN '{}' THEN '{}'".format(variant, canonical)
        for variant, canonical in sorted(SAP_ITEM_ALIASES.items())
    )
    return f"(CASE {alias}.sap_item_code {whens} ELSE {alias}.sap_item_code END)"


# Folding is an AGGREGATION, not a rename: 29 live POs are billed under both a
# variant and its canonical code, so two rows collapse onto one key. Emitting
# them unaggregated would silently DOUBLE every joined PO line rather than fail
# loudly — worse than the scalar-subquery error the same fold caused on the
# pendency page. Hence the GROUP BY, and SUM() on both quantities.
SAP_BILLING_SPLIT_SQL = f"""(
    SELECT
        po_number,
        sap_item_code,
        SUM(billed_qty)     AS billed_qty,
        SUM(dispatched_qty) AS dispatched_qty,
        -- Most recent dispatch across the folded codes; NULL only when none of
        -- them went out, so this still doubles as the "is gated" flag.
        (array_agg(dispatch_note ORDER BY last_dispatch DESC NULLS LAST)
            FILTER (WHERE dispatch_note IS NOT NULL))[1] AS dispatch_note
    FROM (
        SELECT
            b0.po_number,
            {sap_item_code_sql("b0")} AS sap_item_code,
            b0.billed_qty,
            {dispatched_qty_sql("b0")} AS dispatched_qty,
            -- Why a line is gated, in the planner's words. Non-null even when
            -- SAP stamped the dispatch without any of the details.
            (
                SELECT COALESCE(NULLIF(concat_ws(' · ',
                           'Dispatched ' || NULLIF(e->'dispatch'->>'dispatch_date', ''),
                           'vehicle '    || NULLIF(e->'dispatch'->>'vehicle', ''),
                           'bilty '      || NULLIF(e->'dispatch'->>'bilty_no', '')
                       ), ''), 'Dispatched')
                FROM jsonb_array_elements(COALESCE(b0.invoices, '[]'::jsonb)) e
                WHERE (e->>'dispatched')::boolean IS TRUE
                ORDER BY e->'dispatch'->>'dispatch_date' DESC NULLS LAST
                LIMIT 1
            ) AS dispatch_note,
            (
                SELECT MAX(e->'dispatch'->>'dispatch_date')
                FROM jsonb_array_elements(COALESCE(b0.invoices, '[]'::jsonb)) e
                WHERE (e->>'dispatched')::boolean IS TRUE
            ) AS last_dispatch
        FROM sap_billing b0
    ) folded
    GROUP BY 1, 2
)"""


def _window(months: int = BILLING_WINDOW_MONTHS) -> tuple[str, str]:
    today = timezone.now().date()
    frm = (today - datetime.timedelta(days=months * 31)).replace(day=1)
    return frm.isoformat(), today.isoformat()


def last_sync_at() -> str | None:
    """Last successful sync time — cache fast-path, else the table's own timestamp
    (robust across per-worker LocMem caches, since a wholesale rebuild stamps every
    row's updated_at with the sync time)."""
    cached = cache.get(_LAST_SYNC_KEY)
    if cached:
        return cached
    from django.db.models import Max

    latest = SapBilling.objects.aggregate(m=Max("updated_at")).get("m")
    return latest.isoformat() if latest else None


def _fetch_dispatch_map(frm: str, to: str) -> dict[str, dict]:
    """``DocNum -> {dispatched, dispatch_date, bilty_no, vehicle, transporter}``
    for every RK A/R invoice in the window.

    The dispatch stamp lives on the OINV *header*, not on the sales-analysis
    procedure, so it takes its own read. One truck covers many invoices across
    many POs (one bilty/vehicle repeats over several DocNums), which is why this
    is keyed per invoice document and never per PO.

    Raises rather than returning empty on failure — see the caller: an empty map
    would silently mark every dispatched load as available to plan again.
    """
    rows = select(
        """
        SELECT "DocNum",
               "U_Dipatch_Date"    AS "DISPATCH_DATE",
               "U_BiltyDate"       AS "BILTY_DATE",
               "U_BilltyNumber"    AS "BILTY_NO",
               "U_VehicleNoM"      AS "VEHICLE",
               TO_VARCHAR("U_DriverName") AS "DRIVER",
               "U_TransporterName" AS "TRANSPORTER"
        FROM OINV
        WHERE "CardCode" = ?
          AND "CANCELED" = 'N'
          AND "DocDate" >= ?
        """,
        [RK_CARDCODE, frm],
        schema=HANA_SCHEMAS["mart"],
    )

    out: dict[str, dict] = {}
    for r in rows:
        doc = str(r.get("DocNum") or "").strip()
        if not doc:
            continue
        values = {
            "dispatch_date": _date_str(r.get("DISPATCH_DATE")),
            "bilty_date": _date_str(r.get("BILTY_DATE")),
            "bilty_no": str(r.get("BILTY_NO") or "").strip(),
            "vehicle": str(r.get("VEHICLE") or "").strip(),
            "driver": str(r.get("DRIVER") or "").strip(),
            "transporter": str(r.get("TRANSPORTER") or "").strip(),
        }
        # ANY of the five stamps means the load went out. `transporter` is
        # deliberately NOT one of them — it is sometimes pre-filled at booking.
        dispatched = any(
            values[k]
            for k in ("dispatch_date", "bilty_date", "bilty_no", "vehicle", "driver")
        )
        out[doc] = {"dispatched": dispatched, **values}
    return out


def _date_str(v) -> str:
    if not v:
        return ""
    if hasattr(v, "date"):
        return v.date().isoformat()
    return str(v)[:10]


def sync_rk_billing(months: int = BILLING_WINDOW_MONTHS, force: bool = True) -> dict:
    """Pull RK-World Sales/Sales-Return from SAP and rebuild ``sap_billing``.

    Returns a summary dict. Raises on an unreachable SAP source (caller decides
    whether to swallow — the background path does).

    A Postgres session advisory lock makes the whole pull+rebuild single-flight
    across workers: if another worker (or a cron run) already holds it, this call
    returns ``{"skipped": True}`` without touching SAP or the table."""
    with connection.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", [_PG_LOCK_KEY])
        if not cur.fetchone()[0]:
            logger.info("sap_billing sync skipped — another worker holds the advisory lock")
            return {"skipped": True}
    try:
        return _do_sync(months, force)
    finally:
        with connection.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", [_PG_LOCK_KEY])


def _do_sync(months: int, force: bool) -> dict:
    frm, to = _window(months)
    rows = report_sales_analysis(frm, to, source="mart", force=force)
    # Deliberately NOT wrapped in try/except: if the dispatch read fails we must
    # abort the whole rebuild. Writing the table without it would mark every
    # already-dispatched load as plannable again, and the planner would happily
    # put goods that are on a truck onto a second one. Aborting leaves the
    # previous (correct) rows in place — the rebuild is one transaction.
    dispatch = _fetch_dispatch_map(frm, to)

    # (po, item) -> {"qty": Decimal(net), "invoices": {doc_num: {...}}}
    agg: dict[tuple[str, str], dict] = {}
    considered = 0
    for r in rows:
        if _norm(r.get("CardCode")) != RK_CARDCODE:
            continue
        typ = str(r.get("Type") or "").strip().lower()
        if typ not in ("sales", "sales return"):
            continue
        po = _norm(r.get("U_MART_DOC_NO"))
        if not po or po == "-":            # unstamped / manual junk PO numbers
            continue
        item = _norm(r.get("ItemCode"))
        if not item:
            continue
        considered += 1
        # The proc already returns Sales-Return rows with NEGATIVE Quantity/LineTotal
        # (see sap/distributor_inventory.py:244-248), so summing the raw signed values
        # nets returns out. Do NOT flip the sign — that would ADD returns to billed.
        is_return = typ == "sales return"
        net = _dec(r.get("Quantity"))
        entry = agg.setdefault((po, item), {"qty": Decimal(0), "invoices": {}})
        entry["qty"] += net
        doc = str(r.get("DocNum") or "")
        # Key invoices by (DocNum, kind): an A/R invoice (Sales) and a credit memo
        # (Sales Return) can share a numeric DocNum but are distinct SAP documents.
        ikey = (doc, "return" if is_return else "sales")
        dd = r.get("DocDate")
        dd_iso = dd.date().isoformat() if hasattr(dd, "date") else (str(dd)[:10] if dd else None)
        inv = entry["invoices"].setdefault(
            ikey,
            {
                "doc_num": doc,
                "doc_date": dd_iso,
                "qty": Decimal(0),
                "amount": Decimal(0),
                "type": "Sales Return" if is_return else "Sales",
            },
        )
        inv["qty"] += net
        inv["amount"] += _dec(r.get("LineTotal"))

    now = timezone.now()
    objs = []
    dispatched_lines = 0
    for (po, item), entry in agg.items():
        net_qty = entry["qty"]
        if net_qty < 0:               # more returns than sales — clamp (unusual)
            net_qty = Decimal(0)
        invoices = []
        line_dispatched = False
        for (doc, kind), iv in entry["invoices"].items():
            row = {**iv, "qty": float(iv["qty"]), "amount": round(float(iv["amount"]), 2)}
            # Only A/R invoices carry a dispatch stamp. Credit notes (Sales
            # Return) are ORIN documents with their own numbering, so a lookup
            # by DocNum would be meaningless — and a return is the opposite of
            # a dispatch anyway.
            info = dispatch.get(doc) if kind == "sales" else None
            row["dispatched"] = bool(info and info["dispatched"])
            if row["dispatched"]:
                line_dispatched = True
                row["dispatch"] = {
                    k: info[k]
                    for k in ("dispatch_date", "bilty_no", "vehicle", "transporter")
                    if info.get(k)
                }
            invoices.append(row)
        dispatched_lines += 1 if line_dispatched else 0
        objs.append(SapBilling(po_number=po, sap_item_code=item, billed_qty=net_qty, invoices=invoices))

    # Wholesale rebuild inside one transaction — MVCC keeps readers on the old
    # snapshot until commit, so there is no empty window.
    with transaction.atomic():
        SapBilling.objects.all().delete()
        if objs:
            SapBilling.objects.bulk_create(objs, batch_size=1000)

    cache.set(_LAST_SYNC_KEY, now.isoformat(), timeout=None)
    summary = {
        "sap_rows": len(rows),
        "rk_po_lines_considered": considered,
        "keys_written": len(objs),
        "invoices_seen": len(dispatch),
        "invoices_dispatched": sum(1 for d in dispatch.values() if d["dispatched"]),
        "lines_with_dispatch": dispatched_lines,
        "synced_at": now.isoformat(),
        "window": [frm, to],
    }
    logger.info("sap_billing sync: %s", summary)
    return summary


def ensure_billing_fresh() -> None:
    """Fire a background resync if the table is stale (> SYNC_STALE_SECONDS) or
    never synced. Single-flight via a cache lock; serves current data immediately.
    Safe to call on every planner/billing read."""
    last = last_sync_at()  # table-backed, so all workers agree on freshness
    stale = True
    if last:
        try:
            age = (timezone.now() - datetime.datetime.fromisoformat(last)).total_seconds()
            stale = age > SYNC_STALE_SECONDS
        except (ValueError, TypeError):
            stale = True
    if not stale:
        return
    if not cache.add(_SYNC_LOCK_KEY, "1", timeout=300):  # another worker is already syncing
        return

    def _run():
        try:
            sync_rk_billing()
        except Exception as e:  # never let a background refresh surface to a request
            logger.warning("background sap_billing sync failed: %s", e)
        finally:
            cache.delete(_SYNC_LOCK_KEY)

    threading.Thread(target=_run, daemon=True, name="sap-billing-sync").start()
