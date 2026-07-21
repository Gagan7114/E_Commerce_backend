"""Per-unit litres for SAP finished goods (JM Inventory).

SAP `OITW.OnHand` is a piece count and the ERP has **no populated litres column**
(`OITW.U_UNE_LTR` exists but is empty). To express stock in litres we need a
litres-per-piece factor. Two sources, merged and gated to litre SKUs:

1. **JM Primary sales proc** — `SUM(Liter)/SUM(Quantity)` per ItemCode over a
   trailing window. This is SAP's *own* computed Liter, so it matches the ERP
   and covers ~96% of on-hand litre units. Primary wins on conflicts.
2. **Postgres `master_sheet.per_unit_value`** (`is_litre='Y'`) — fills SKUs not
   sold within the window.

The merged map is cached per source so the heavy sales proc runs at most once
per TTL, not once per grid request. Callers still gate on SAP `U_IsLitre='Y'`
before applying a factor, so a non-litre SKU never gets litres even if a stray
mapping exists.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from django.core.cache import cache
from django.db import connection
from django.utils import timezone

from .service import report_sales_analysis

logger = logging.getLogger(__name__)

_CACHE_TTL = 3600          # 1h — refresh factors hourly, not per request
_PRIMARY_WINDOW_DAYS = 120  # trailing sales window used to derive per-unit litres

_TRUE = {"Y", "YES", "1", "TRUE"}


def is_litre_flag(value) -> bool:
    """True when a SAP U_IsLitre / master_sheet is_litre value marks a litre SKU."""
    return str(value or "").strip().upper() in _TRUE


def _primary_per_unit(source: str) -> dict[str, float]:
    """{ItemCode: litres/piece} from JM Primary Liter÷Quantity (litre SKUs only)."""
    today = timezone.localdate()
    frm = (today - timedelta(days=_PRIMARY_WINDOW_DAYS)).isoformat()
    to = today.isoformat()
    try:
        rows = report_sales_analysis(frm, to, source=source)
    except Exception as exc:  # noqa: BLE001 - degrade to master_sheet only
        logger.warning("[litres] JM Primary unavailable for %s: %s", source, exc)
        return {}
    agg: dict[str, list[float]] = {}
    for r in rows:
        code = str(r.get("ItemCode") or "").strip().upper()
        if not code:
            continue
        try:
            qty = float(r.get("Quantity") or 0)
            ltr = float(r.get("Liter") or 0)
        except (TypeError, ValueError):
            continue
        a = agg.setdefault(code, [0.0, 0.0])
        a[0] += ltr
        a[1] += qty
    # Only keep items that actually moved litres (litre SKUs); non-litre items
    # have Liter=0 so they're naturally excluded.
    return {c: (sl / sq) for c, (sl, sq) in agg.items() if sq and sl > 0}


def _master_sheet_per_unit() -> dict[str, float]:
    """{SAP code: litres/piece} from Postgres master_sheet (is_litre='Y' only)."""
    out: dict[str, float] = {}
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(TRIM(sku_sap_code)), MAX(per_unit_value)
                FROM master_sheet
                WHERE TRIM(COALESCE(sku_sap_code, '')) <> ''
                  AND UPPER(COALESCE(is_litre, '')) IN ('Y', 'YES', '1', 'TRUE')
                  AND per_unit_value IS NOT NULL AND per_unit_value > 0
                GROUP BY UPPER(TRIM(sku_sap_code))
                """
            )
            for code, puv in cur.fetchall():
                try:
                    out[code] = float(puv)
                except (TypeError, ValueError):
                    continue
    except Exception as exc:  # noqa: BLE001
        logger.warning("[litres] master_sheet per-unit lookup failed: %s", exc)
    return out


def per_unit_litre_map(source: str = "mart") -> dict[str, float]:
    """Merged, cached litres-per-piece map for litre SKUs (Primary ∪ master_sheet).

    Keyed by UPPER(ItemCode). master_sheet provides the base; JM Primary overrides
    it because it's SAP's own computed figure for the same company DB."""
    key = f"sap:litres:perunit:{source}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    merged = dict(_master_sheet_per_unit())      # fallback layer
    merged.update(_primary_per_unit(source))     # Primary wins
    try:
        cache.set(key, merged, _CACHE_TTL)
    except Exception:
        pass
    return merged


def row_litres(item_code, on_hand, is_litre, litre_map: dict[str, float]):
    """Litres on hand for one row, or None when it's a litre SKU we can't convert.

    * non-litre SKU  → 0        (definitively no litres)
    * litre SKU + factor → OnHand × factor
    * litre SKU, no factor → None (uncovered; shown blank, excluded from totals)
    """
    if not is_litre_flag(is_litre):
        return 0
    factor = litre_map.get(str(item_code or "").strip().upper())
    if factor is None:
        return None
    try:
        return round(float(on_hand or 0) * factor, 2)
    except (TypeError, ValueError):
        return None
