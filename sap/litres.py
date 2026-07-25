"""Per-unit litres for SAP finished goods (JM Inventory).

SAP `OITW.OnHand` is a piece count and the ERP has **no populated litres column**
(`OITW.U_UNE_LTR` exists but is empty). The litres-per-unit factor SAP itself uses
lives on the item master: `OITM.SalPackUn`. The JM Primary procedure computes its
`Liter` column as `Quantity × SalPackUn` gated on `U_IsLitre='Y'`, so reading
`SalPackUn` directly reproduces exactly what SAP would report — no sales history,
no averaging, no cache, no coverage gap.

This used to back-derive the factor as `SUM(Liter)/SUM(Quantity)` from a trailing
window of the JM Primary procedure. That undercounted: the procedure's Delivery and
Return levels emit `0 "Liter"` alongside a real `Quantity`, so those units inflated
the denominator (oil totals came out 57.8% low). `SalPackUn` has no such problem.

Callers still gate on SAP `U_IsLitre='Y'`, so a non-litre SKU never gets litres even
if a stray `SalPackUn` exists on it.
"""

from __future__ import annotations

_TRUE = {"Y", "YES", "1", "TRUE"}


def is_litre_flag(value) -> bool:
    """True when a SAP U_IsLitre value marks a litre SKU."""
    return str(value or "").strip().upper() in _TRUE


def row_litres(on_hand, is_litre, sal_pack_un):
    """Litres on hand for one row, or None when it's a litre SKU we can't convert.

    * non-litre SKU            → 0     (definitively no litres)
    * litre SKU + SalPackUn    → OnHand × SalPackUn
    * litre SKU, no SalPackUn  → None  (unconvertible; shown blank, excluded from totals)

    Every stocked litre SKU carries a SalPackUn today (verified 135/135 on mart,
    211/211 on oil), but it is master data anyone can edit in SAP — so the None
    branch stays rather than silently reporting 0 litres for a real oil SKU.
    """
    if not is_litre_flag(is_litre):
        return 0
    try:
        factor = float(sal_pack_un or 0)
        if factor <= 0:
            return None
        return round(float(on_hand or 0) * factor, 2)
    except (TypeError, ValueError):
        return None
