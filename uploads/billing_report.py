"""
Amazon PO billing-status report.

Reconciles each PENDING Amazon PO line against SAP billing (``sap_billing``, synced
from RK-World Sales Analysis) and the planner's shipped tally, so you can see, per
PO and per SKU: accepted / billed / shipped / still-shippable, plus the SAP invoice
detail behind each billed line. Powers the standalone "Billing" view and the
shipment-planner appointment panel.

Billing rule (per product decision): SAP billing is the authority for "done" —
``shippable = accepted − billed`` (the planner's own shipped tally is surfaced for
context but does NOT reduce shippable here).
"""
from __future__ import annotations

import json

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require
from sap.billing import ensure_billing_fresh, last_sync_at


def _page(request):
    def _int(name, default):
        try:
            return int(request.query_params.get(name, default))
        except (TypeError, ValueError):
            return default

    page = max(0, _int("page", 0))
    page_size = min(max(1, _int("page_size", 25)), 200)
    return page, page_size


def _po_status(g) -> str:
    if g["billed"] <= 0:
        return "unbilled"
    return "billed" if g["shippable"] <= 0 else "partial"


@api_view(["GET"])
@permission_classes([require("platform.po.view")])
def amazon_po_billing(request):
    """Per-PO billing status (grouped, PO-paginated). Filters: fulfillment_center,
    channel, search, billing_status (unbilled|partial|billed)."""
    ensure_billing_fresh()  # background self-refresh if the billing table is stale

    q = request.query_params
    where = ["p.status = 'Confirmed'", "p.po_status = 'PENDING'", "p.accepted_qty > 0"]
    params: list = []
    fc = (q.get("fulfillment_center") or "").strip()
    if fc:
        where.append("UPPER(TRIM(p.fulfillment_center)) = UPPER(TRIM(%s))")
        params.append(fc)
    channel = (q.get("channel") or "").strip()
    if channel:
        where.append("UPPER(TRIM(p.core_fresh_now)) = UPPER(TRIM(%s))")
        params.append(channel)
    search = (q.get("search") or "").strip()
    if search:
        where.append(
            "(p.po_number ILIKE %s OR p.asin ILIKE %s OR p.item ILIKE %s OR p.sap_sku_code ILIKE %s)"
        )
        params += [f"%{search}%"] * 4
    # Restrict to a specific PO list (used by the appointment panel).
    po_list = [x.strip().upper() for x in (q.get("po") or "").replace(";", ",").split(",") if x.strip()]
    if po_list:
        where.append("UPPER(TRIM(p.po_number)) = ANY(%s)")
        params.append(po_list)

    sql = f"""
      WITH billed AS (
          SELECT po_number, sap_item_code, billed_qty, invoices FROM sap_billing
      ),
      committed AS (
          SELECT si.asin, UPPER(TRIM(si.po_number)) AS po_number,
                 SUM(COALESCE(si.planned_qty, 0)) AS shipped_qty
          FROM sp_items si JOIN sp_shipments s ON s.id = si.shipment_id
          WHERE si.not_loaded = FALSE AND s.status <> 'rejected'
          GROUP BY si.asin, UPPER(TRIM(si.po_number))
      )
      SELECT p.po_number, p.order_date, p.fulfillment_center, p.core_fresh_now,
             p.asin, p.item, p.sap_sku_code, p.accepted_qty,
             COALESCE(b.billed_qty, 0)  AS billed_qty,
             COALESCE(c.shipped_qty, 0) AS shipped_qty,
             GREATEST(p.accepted_qty - COALESCE(b.billed_qty, 0), 0) AS shippable_qty,
             (COALESCE(b.billed_qty, 0) >= p.accepted_qty) AS fully_billed,
             b.invoices
      FROM reporting."Amazon PO" p
      LEFT JOIN billed b
        ON b.po_number = UPPER(TRIM(p.po_number))
       AND b.sap_item_code = UPPER(TRIM(p.sap_sku_code))
      LEFT JOIN committed c
        ON c.asin = p.asin AND c.po_number = UPPER(TRIM(p.po_number))
      WHERE {' AND '.join(where)}
      ORDER BY p.po_number, p.item
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    pos: dict = {}
    for r in rows:
        po = r["po_number"]
        g = pos.setdefault(
            po,
            {
                "po_number": po, "order_date": r["order_date"],
                "fulfillment_center": r["fulfillment_center"], "channel": r["core_fresh_now"],
                "lines": [], "accepted": 0.0, "billed": 0.0, "shipped": 0.0, "shippable": 0.0,
            },
        )
        inv = r["invoices"]
        if isinstance(inv, str):
            try:
                inv = json.loads(inv)
            except (ValueError, TypeError):
                inv = []
        acc = float(r["accepted_qty"] or 0)
        sh = float(r["shipped_qty"] or 0)
        g["lines"].append({
            "asin": r["asin"], "item": r["item"], "sap_item_code": r["sap_sku_code"],
            "accepted": acc, "billed": float(r["billed_qty"] or 0), "shipped": sh,
            "shippable": 0.0, "fully_billed": False, "invoices": inv or [],
            "_sku_key": str(r["sap_sku_code"] or "").strip().upper(),
        })
        g["shipped"] += sh

    # sap_billing has ONE net-billed row per (po, item); the join repeats it on every
    # Amazon line that shares a sap_sku_code (two ASINs can map to one SAP code).
    # Allocate that single billed total across the sibling lines greedily (by ASIN,
    # consumed once) so billed is never double-counted and per-line billed <= accepted
    # (hence a PO's billed can never exceed its accepted). Then roll up to the PO.
    for g in pos.values():
        by_sku: dict = {}
        for ln in g["lines"]:
            by_sku.setdefault(ln["_sku_key"], []).append(ln)
        for sku_lines in by_sku.values():
            remaining = max((ln["billed"] for ln in sku_lines), default=0.0)  # the sku's net billed
            for ln in sorted(sku_lines, key=lambda x: str(x["asin"] or "")):
                alloc = min(ln["accepted"], remaining) if remaining > 0 else 0.0
                remaining -= alloc
                ln["billed"] = alloc
                ln["shippable"] = max(ln["accepted"] - alloc, 0.0)
                ln["fully_billed"] = ln["accepted"] > 0 and alloc >= ln["accepted"]
        for ln in g["lines"]:
            ln.pop("_sku_key", None)
        g["accepted"] = sum(ln["accepted"] for ln in g["lines"])
        g["billed"] = sum(ln["billed"] for ln in g["lines"])
        g["shippable"] = sum(ln["shippable"] for ln in g["lines"])

    grouped = []
    for g in pos.values():
        g["status"] = _po_status(g)
        grouped.append(g)

    status_f = (q.get("billing_status") or "").strip().lower()
    if status_f in ("unbilled", "partial", "billed"):
        grouped = [g for g in grouped if g["status"] == status_f]

    # Partially-billed first (they need attention), then unbilled, then fully billed.
    _order = {"partial": 0, "unbilled": 1, "billed": 2}
    grouped.sort(key=lambda g: (_order.get(g["status"], 3), str(g["po_number"])))

    total = len(grouped)
    page, page_size = _page(request)
    start = page * page_size
    return Response({
        "results": grouped[start : start + page_size],
        "count": total,
        "page": page,
        "page_size": page_size,
        "last_sync": last_sync_at(),
    })
