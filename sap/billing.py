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
from django.db import transaction
from django.utils import timezone

from sap.models import SapBilling
from sap.service import report_sales_analysis

logger = logging.getLogger(__name__)

# Customer = R K Worldinfocom (the Amazon seller-of-record billed in SAP).
RK_CARDCODE = "CUSTA000048"
RK_CARDNAME = "R K WORLDINFOCOM PVT LTD"

BILLING_WINDOW_MONTHS = 6          # how far back to pull billing
SYNC_STALE_SECONDS = 15 * 60       # auto-resync when older than this
_LAST_SYNC_KEY = "sap_billing:last_sync"
_SYNC_LOCK_KEY = "sap_billing:syncing"


def _norm(s) -> str:
    return str(s or "").strip().upper()


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v if v is not None else 0))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def _window(months: int = BILLING_WINDOW_MONTHS) -> tuple[str, str]:
    today = timezone.now().date()
    frm = (today - datetime.timedelta(days=months * 31)).replace(day=1)
    return frm.isoformat(), today.isoformat()


def last_sync_at() -> str | None:
    return cache.get(_LAST_SYNC_KEY)


def sync_rk_billing(months: int = BILLING_WINDOW_MONTHS, force: bool = True) -> dict:
    """Pull RK-World Sales/Sales-Return from SAP and rebuild ``sap_billing``.

    Returns a summary dict. Raises on an unreachable SAP source (caller decides
    whether to swallow — the background path does)."""
    frm, to = _window(months)
    rows = report_sales_analysis(frm, to, source="mart", force=force)

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
        sign = Decimal(-1) if typ == "sales return" else Decimal(1)
        net = _dec(r.get("Quantity")) * sign
        entry = agg.setdefault((po, item), {"qty": Decimal(0), "invoices": {}})
        entry["qty"] += net
        doc = str(r.get("DocNum") or "")
        dd = r.get("DocDate")
        dd_iso = dd.date().isoformat() if hasattr(dd, "date") else (str(dd)[:10] if dd else None)
        inv = entry["invoices"].setdefault(
            doc, {"doc_num": doc, "doc_date": dd_iso, "qty": Decimal(0), "amount": Decimal(0), "type": "Sales"}
        )
        inv["qty"] += net
        inv["amount"] += _dec(r.get("LineTotal")) * sign
        if typ == "sales return":
            inv["type"] = "Sales Return"

    now = timezone.now()
    objs = []
    for (po, item), entry in agg.items():
        net_qty = entry["qty"]
        if net_qty < 0:               # more returns than sales — clamp (unusual)
            net_qty = Decimal(0)
        invoices = [
            {**iv, "qty": float(iv["qty"]), "amount": round(float(iv["amount"]), 2)}
            for iv in entry["invoices"].values()
        ]
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
        "synced_at": now.isoformat(),
        "window": [frm, to],
    }
    logger.info("sap_billing sync: %s", summary)
    return summary


def ensure_billing_fresh() -> None:
    """Fire a background resync if the table is stale (> SYNC_STALE_SECONDS) or
    never synced. Single-flight via a cache lock; serves current data immediately.
    Safe to call on every planner/billing read."""
    last = cache.get(_LAST_SYNC_KEY)
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
