"""Distributor inventory by purchase price (FIFO lots).

Implements docs/distributor-inventory-fifo-master-view-plan.md. Because the
sources live in two different databases — opening + deliveries in Postgres
(`sustain_dist`, `master_po`/`master_sheet`) and primary sales/returns in SAP
HANA (`REPORT_SALES_ANALYSIS`) — the FIFO cannot be a single SQL view; it is
assembled and consumed here in Python.

Per distributor + SAP item code:
    on-hand layers = FIFO( opening lots ⊕ purchase lots , consume = deliveries + returns )

* Opening lots   : each `sustain_dist` row (oldest layer, month-start baseline).
* Purchase lots  : HANA REPORT_SALES_ANALYSIS lines with Quantity > 0
                   (rate = LineTotal / Quantity), newer layers.
* Outflow        : SUM(master_po.delivered_qty) (platform→SAP via master_sheet)
                   + SUM(ABS(HANA Quantity < 0))  (sales returns).
Outflow consumes the oldest lots first; remaining lots are the layered on-hand,
each still showing its actual purchase price. Short stock (sold beyond
available) is floored to 0 and surfaced as a flag, never a negative qty.
"""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date, datetime

from django.db import connection

from .service import report_sales_analysis

logger = logging.getLogger(__name__)

# v1: the opening table `sustain_dist` holds only the Sustain card's opening and
# carries no card_code column. Map the launch card → its master_po `vendor_new`
# (== SAP CardName) so deliveries can be attributed without requiring HANA OCRD.
# Generalising to other distributors = add their opening rows (keyed by
# card_code) and their CardName here (or resolve from OCRD).
SUSTAIN_OPENING_CARD = "CUSTA000907"
# Distributor cards exposed in the UI selector → their master_po `vendor_new`
# (== SAP CardName). These are the CardType='C' customer codes that
# report_sales_analysis and sustain_dist key on. Only Sustain has an opening
# snapshot loaded today; the others compute from billing flows until their
# opening is loaded.
KNOWN_CARD_NAMES = {
    "CUSTA000907": "SUSTAINQUEST PRIVATE LIMITED",
    "CUSTA000927": "ANTIZE FOODS PRIVATE LIMITED",
    "CUSTA000900": "BABA LOKENATH TRADERS",
    "CUSTA000354": "CHIRAG ENTERPRISES MUMBAI",
    "CUSTA000906": "EVARA ENTERPRISES",
    "CUSTA000592": "KNOWTABLE ONLINE SERVICES PRIVATE LIMITED",
    "CUSTA000048": "R K WORLDINFOCOM PVT LTD",
}


def _f(value) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _iso(value) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (date, datetime)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    text = str(value).strip()
    # HANA dates often come back as 'YYYY-MM-DD' or 'YYYYMMDD'.
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except ValueError:
            continue
    return text[:10] or None


def resolve_card_name(card_code: str, override: str | None = None) -> str | None:
    """master_po.vendor_new (== SAP CardName) for the given card_code."""
    if override:
        return override.strip()
    return KNOWN_CARD_NAMES.get(card_code)


def anchor_month(card_code: str) -> tuple[date, date, date]:
    """(month_start, month_last_day, month_end_exclusive).

    The opening snapshot's date IS the month-start baseline, so the anchor month
    is taken from `sustain_dist`. Falls back to the current calendar month.
    """
    start = None
    if card_code == SUSTAIN_OPENING_CARD:
        with connection.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM sustain_dist")
            row = cur.fetchone()
        if row and row[0]:
            start = row[0].replace(day=1)
    if start is None:
        start = date.today().replace(day=1)
    last_day = monthrange(start.year, start.month)[1]
    month_last = date(start.year, start.month, last_day)
    if start.month == 12:
        month_end = date(start.year + 1, 1, 1)
    else:
        month_end = date(start.year, start.month + 1, 1)
    return start, month_last, month_end


def _opening_lots(card_code: str) -> list[dict]:
    """Opening lots — each sustain_dist row is its own price layer (oldest)."""
    if card_code != SUSTAIN_OPENING_CARD:
        return []
    with connection.cursor() as cur:
        cur.execute(
            "SELECT sap_code, quantity, rate, date "
            "FROM sustain_dist WHERE quantity IS NOT NULL ORDER BY sap_code, id"
        )
        rows = cur.fetchall()
    lots = []
    for sap_code, qty, rate, d in rows:
        q = _f(qty)
        if q <= 0:
            continue
        lots.append({
            "sap_code": (sap_code or "").strip(),
            "qty": q,
            "rate": _f(rate),
            "lot_source": "Opening",
            "lot_date": _iso(d),
            "seq": 0,
        })
    return lots


def _delivery_out(card_name: str, month_start: date, month_end: date):
    """Total pieces delivered to formats per SAP code (platform→SAP via master_sheet).

    Returns (deliveries_by_sap, unmapped_qty, unmapped_skus, ambiguous_codes) so
    the caller can warn about silent miscounts:
      * unmapped — master_po.sku_code with no master_sheet row → outflow dropped
        (would overstate on-hand); 0 today but no schema guarantee.
      * ambiguous — a format_sku_code mapping to >1 distinct SAP code (uniqueness
        is only on (format, format_sku_code)); MIN() picks one deterministically.
    """
    if not card_name:
        return {}, 0.0, 0, 0
    sql = """
        WITH sku_map AS (
            SELECT format_sku_code, MIN(TRIM(sku_sap_code)) AS sap_code
            FROM master_sheet
            WHERE format_sku_code IS NOT NULL AND TRIM(COALESCE(sku_sap_code, '')) <> ''
            GROUP BY format_sku_code
        )
        SELECT m.sap_code, SUM(mp.delivered_qty) AS out_qty
        FROM master_po mp
        JOIN sku_map m ON m.format_sku_code = mp.sku_code
        WHERE mp.vendor_new = %s
          AND mp.delivery_date >= %s AND mp.delivery_date < %s
          AND mp.delivered_qty > 0
        GROUP BY m.sap_code
    """
    unmapped_sql = """
        SELECT COUNT(DISTINCT mp.sku_code), COALESCE(SUM(mp.delivered_qty), 0)
        FROM master_po mp
        LEFT JOIN (
            SELECT DISTINCT format_sku_code FROM master_sheet
            WHERE TRIM(COALESCE(sku_sap_code, '')) <> ''
        ) m ON m.format_sku_code = mp.sku_code
        WHERE mp.vendor_new = %s
          AND mp.delivery_date >= %s AND mp.delivery_date < %s
          AND mp.delivered_qty > 0 AND m.format_sku_code IS NULL
    """
    ambiguous_sql = """
        SELECT COUNT(*) FROM (
            SELECT format_sku_code
            FROM master_sheet
            WHERE format_sku_code IS NOT NULL AND TRIM(COALESCE(sku_sap_code, '')) <> ''
            GROUP BY format_sku_code
            HAVING COUNT(DISTINCT TRIM(sku_sap_code)) > 1
        ) x
    """
    with connection.cursor() as cur:
        cur.execute(sql, [card_name, month_start, month_end])
        deliveries = {(r[0] or "").strip(): _f(r[1]) for r in cur.fetchall() if r[0]}
        cur.execute(unmapped_sql, [card_name, month_start, month_end])
        u = cur.fetchone()
        unmapped_skus, unmapped_qty = (int(u[0]), _f(u[1])) if u else (0, 0.0)
        cur.execute(ambiguous_sql)
        a = cur.fetchone()
        ambiguous = int(a[0]) if a else 0
    return deliveries, unmapped_qty, unmapped_skus, ambiguous


def _enrichment() -> dict[str, dict]:
    """sap_code → display fields from master_sheet."""
    sql = """
        SELECT sku_sap_code,
               MAX(sku_sap_name) AS item_name,
               MAX(item_head)    AS item_head,
               MAX(sub_category) AS variety,
               MAX(per_unit)     AS pack_size
        FROM master_sheet
        WHERE sku_sap_code IS NOT NULL
        GROUP BY sku_sap_code
    """
    out: dict[str, dict] = {}
    with connection.cursor() as cur:
        cur.execute(sql)
        for sap_code, name, head, variety, pack in cur.fetchall():
            out[(sap_code or "").strip()] = {
                "item_name": (name or "").strip() or None,
                "item_head": (head or "").strip() or None,
                "variety": (variety or "").strip() or None,
                "pack_size": (str(pack).strip() if pack not in (None, "") else None),
            }
    return out


def _hana_primary(card_code: str, month_start: date, month_last: date):
    """(purchase_lots, returns_by_sap) from HANA REPORT_SALES_ANALYSIS.

    Quantity > 0 → purchase lot (rate = LineTotal / Quantity).
    Quantity < 0 → sales return (outflow, summed ABS by SAP code).
    Returns ([], {}, error_message) when HANA is unreachable so the endpoint can
    degrade to opening − deliveries instead of failing outright.
    """
    try:
        rows = report_sales_analysis(month_start.isoformat(), month_last.isoformat())
    except Exception as exc:  # noqa: BLE001 - surfaced as a warning, not a 500
        logger.warning("[dist-inv] HANA primary unavailable: %s", exc)
        return [], {}, f"Primary purchases unavailable (HANA): {exc}"

    purchase_lots: list[dict] = []
    returns_by_sap: dict[str, float] = {}
    for row in rows:
        if str(row.get("CardCode") or "").strip() != card_code:
            continue
        sap_code = str(row.get("ItemCode") or "").strip()
        if not sap_code:
            continue
        qty = _f(row.get("Quantity"))
        # Verified June 2026 (CUSTA000907): the only negative-Quantity rows are
        # Type='Sales Return' (17 lines, -15,443); positives are Type='Sales'.
        # No credit-notes / reversals go negative, so ABS-summing Quantity<0 as
        # physical outflow is correct. If a non-physical negative Type ever
        # appears for another distributor, gate this branch on row['Type'].
        if qty > 0:
            line_total = _f(row.get("LineTotal"))
            purchase_lots.append({
                "sap_code": sap_code,
                "qty": qty,
                "rate": (line_total / qty) if qty else 0.0,
                "lot_source": f"Bought {_iso(row.get('DocDate')) or ''}".strip(),
                "lot_date": _iso(row.get("DocDate")),
                "seq": 1,
            })
        elif qty < 0:
            returns_by_sap[sap_code] = returns_by_sap.get(sap_code, 0.0) + abs(qty)
    return purchase_lots, returns_by_sap, None


def _consume_fifo(lots: list[dict], total_out: float):
    """FIFO consume oldest-first. Returns (surviving_layers, total_in, short_qty)."""
    cum = 0.0
    survivors = []
    for lot in lots:
        qty = _f(lot["qty"])
        cum_end = cum + qty
        remaining = max(0.0, min(qty, cum_end - total_out))
        if remaining > 1e-9:
            survivors.append({**lot, "remaining_qty": remaining})
        cum = cum_end
    short_qty = max(0.0, total_out - cum)
    return survivors, cum, short_qty


def build_distributor_inventory(card_code: str, card_name: str | None = None) -> dict:
    """Assemble the layered FIFO on-hand position for one distributor card."""
    card_code = (card_code or "").strip()
    card_name = resolve_card_name(card_code, card_name)
    month_start, month_last, month_end = anchor_month(card_code)
    warnings: list[str] = []

    opening = _opening_lots(card_code)
    if not opening:
        warnings.append(
            "No opening-stock snapshot is loaded for this distributor — on-hand "
            "is derived from billing flows only (purchases − deliveries − "
            "returns) and may be unreliable until an opening is loaded."
        )
    purchases, returns_by_sap, hana_err = _hana_primary(card_code, month_start, month_last)
    if hana_err:
        warnings.append(hana_err)
    deliveries, unmapped_qty, unmapped_skus, ambiguous = _delivery_out(
        card_name, month_start, month_end
    )
    if not card_name:
        warnings.append(
            "No CardName mapping for this distributor — deliveries (outflow) "
            "could not be applied; on-hand may be overstated."
        )
    if unmapped_qty > 0:
        warnings.append(
            f"{int(round(unmapped_qty))} pcs of deliveries across {unmapped_skus} "
            "platform SKU(s) had no SAP mapping in master_sheet and were not "
            "applied — on-hand may be overstated for those items."
        )
    if ambiguous > 0:
        warnings.append(
            f"{ambiguous} platform SKU code(s) map to more than one SAP code in "
            "master_sheet; one was chosen deterministically. Deliveries for those "
            "may be attributed to the wrong SAP item."
        )
    enrich = _enrichment()

    # Group lots by SAP code; union opening ∪ purchases.
    by_sap: dict[str, list[dict]] = {}
    for lot in opening + purchases:
        by_sap.setdefault(lot["sap_code"], []).append(lot)

    # Every SAP code that has any inflow OR any outflow (so short-only SKUs show).
    sap_codes = set(by_sap) | set(deliveries) | set(returns_by_sap)

    rows: list[dict] = []
    movements: dict[str, dict] = {}
    on_hand_qty = 0.0
    fifo_value = 0.0
    layer_count = 0
    short_flags = 0

    for sap_code in sorted(sap_codes):
        lots = sorted(
            by_sap.get(sap_code, []),
            key=lambda l: (l["seq"], l["lot_date"] or "9999-99-99"),
        )
        delivered_qty = deliveries.get(sap_code, 0.0)
        returned_qty = returns_by_sap.get(sap_code, 0.0)
        total_out = delivered_qty + returned_qty
        survivors, total_in, short_qty = _consume_fifo(lots, total_out)

        # Per-SKU movement so the UI can show what drove the position (and make
        # short rows self-explanatory instead of a bare 0).
        opening_qty = sum(_f(l["qty"]) for l in lots if l["seq"] == 0)
        purchased_qty = sum(_f(l["qty"]) for l in lots if l["seq"] != 0)
        movements[sap_code] = {
            "opening_qty": round(opening_qty, 2),
            "purchased_qty": round(purchased_qty, 2),
            "delivered_qty": round(delivered_qty, 2),
            "returned_qty": round(returned_qty, 2),
            "net_qty": round(total_in - total_out, 2),
            "on_hand_qty": round(max(0.0, total_in - total_out), 2),
            "short_qty": round(short_qty, 2),
        }

        meta = enrich.get(sap_code, {})
        base = {
            "sap_code": sap_code,
            "item_name": meta.get("item_name") or sap_code,
            "item_head": meta.get("item_head"),
            "variety": meta.get("variety"),
            "pack_size": meta.get("pack_size"),
        }

        for lot in survivors:
            qty = round(lot["remaining_qty"], 3)
            value = round(qty * lot["rate"], 2)
            on_hand_qty += qty
            fifo_value += value
            layer_count += 1
            rows.append({
                **base,
                "lot_source": lot["lot_source"],
                "lot_date": lot["lot_date"],
                "rate": round(lot["rate"], 2),
                "remaining_qty": qty,
                "lot_value": value,
                "is_short": 0,
                "short_qty": 0,
                "flag": None,
            })

        if short_qty > 1e-9:
            short_flags += 1
            short = int(round(short_qty))
            rows.append({
                **base,
                "lot_source": "SHORT",
                "lot_date": None,
                "rate": None,
                "remaining_qty": 0,
                "lot_value": 0,
                "is_short": 1,
                "short_qty": short,
                "flag": f"{short} pcs sold beyond available — check opening/data window",
            })

    live_skus = {r["sap_code"] for r in rows if r["remaining_qty"] and r["remaining_qty"] > 0}

    return {
        "card_code": card_code,
        "card_name": card_name or card_code,
        "as_of_month": month_start.isoformat(),
        "rows": rows,
        "movements": movements,
        "totals": {
            "on_hand_qty": round(on_hand_qty, 2),
            "fifo_value": round(fifo_value, 2),
            "skus": len(live_skus),
            "layers": layer_count,
            "short_flags": short_flags,
        },
        "warnings": warnings,
    }
