"""SAP-flavoured branches of the Reports page (JM Primary, JM Inventory).

The generic /api/reports/columns and /api/reports/raw endpoints in
`platforms.reports` detect view names prefixed `sap:` and route here. View-name
shape: `sap:<kind>:<source>` — kind ∈ {jm_primary, jm_inventory},
source ∈ {mart, oil}.

JM Primary  → wraps `sap.service.report_sales_analysis(from, to, source)`.
JM Inventory → wraps the same SAP HANA item × warehouse query the JM Inventory
               dashboard already uses (snapshot — date filter is ignored).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sap.service import (
    SALES_ANALYSIS_DEFAULT_SOURCE,
    HANA_SCHEMAS,
    report_sales_analysis,
    resolve_schema,
    select,
)

logger = logging.getLogger(__name__)

SAP_KINDS = {"jm_primary", "jm_inventory"}
SAP_SOURCES = set(HANA_SCHEMAS)  # {"mart", "oil"}


def is_sap_view(name: str) -> bool:
    return isinstance(name, str) and name.startswith("sap:")


def parse_sap_view(name: str) -> tuple[str, str]:
    """`sap:<kind>:<source>` → (kind, source). Raises ValueError on bad input."""
    parts = (name or "").split(":")
    if len(parts) != 3 or parts[0] != "sap":
        raise ValueError(f"Bad SAP view name: {name!r}")
    _, kind, source = parts
    if kind not in SAP_KINDS:
        raise ValueError(f"Unknown SAP report kind: {kind!r}")
    source = (source or "").lower() or SALES_ANALYSIS_DEFAULT_SOURCE
    if source not in SAP_SOURCES:
        raise ValueError(f"Unknown SAP source: {source!r}")
    return kind, source


# ─── JM Inventory: item × warehouse snapshot (static columns) ───
# Mirrors the SELECT in sap.views.inventory_overview so the Reports table reads
# the same row shape the JM Inventory dashboard already shows.
_JM_INVENTORY_COLUMNS: list[tuple[str, str]] = [
    ("ItemCode", "text"),
    ("ItemName", "text"),
    ("GroupCode", "integer"),
    ("GroupName", "text"),
    ("UOM", "text"),
    ("Active", "text"),
    ("LastPurchasePrice", "numeric"),
    ("Currency", "text"),
    ("WhsCode", "text"),
    ("WhsName", "text"),
    ("Location", "text"),
    ("City", "text"),
    ("OnHand", "integer"),
    ("Committed", "integer"),
    ("Available", "integer"),
    ("OnOrder", "integer"),
    ("MinStock", "integer"),
    ("MaxStock", "integer"),
    ("StockValue", "numeric"),
]

_JM_INVENTORY_SELECT = """
    SELECT
        T0."ItemCode",
        T0."ItemName",
        T0."ItmsGrpCod"      AS "GroupCode",
        T3."ItmsGrpNam"      AS "GroupName",
        T0."SalUnitMsr"      AS "UOM",
        T0."validFor"        AS "Active",
        T0."LastPurPrc"      AS "LastPurchasePrice",
        T0."LastPurCur"      AS "Currency",
        T1."WhsCode",
        T2."WhsName",
        T2."Location",
        T2."City",
        T1."OnHand",
        T1."IsCommited"      AS "Committed",
        T1."OnHand" - T1."IsCommited" AS "Available",
        T1."OnOrder",
        T1."MinStock",
        T1."MaxStock",
        T1."OnHand" * T0."LastPurPrc" AS "StockValue"
    FROM OITM T0
    INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
    LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
    LEFT  JOIN OITB T3 ON T3."ItmsGrpCod" = T0."ItmsGrpCod"
    ORDER BY T0."ItemName", T1."WhsCode"
    LIMIT ? OFFSET ?
"""


def fetch_jm_inventory(source: str, page: int, page_size: int) -> tuple[list[dict], int]:
    """Snapshot rows + total count for JM Inventory under one schema."""
    _, schema = resolve_schema(source)
    offset = page * page_size
    rows = select(_JM_INVENTORY_SELECT, [page_size, offset], schema=schema)
    count_rows = select(
        """
        SELECT COUNT(*) AS "total"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        """,
        schema=schema,
    )
    total = int(count_rows[0].get("total") or 0) if count_rows else 0
    return rows, total


# ─── JM Primary: REPORT_SALES_ANALYSIS procedure ───
# Column shape is discovered from the procedure itself (one cheap call per
# source, then cached) so the Reports column toggles match the actual rows.
_JM_PRIMARY_COLUMN_CACHE: dict[str, list[tuple[str, str]]] = {}

# Per-key type hints used when the discovery sample is empty (or when the
# Python value alone isn't enough to tell text from numeric). Keys are matched
# case-insensitively. Anything not hinted falls back to value-based inference.
_JM_PRIMARY_HINT_NUMERIC = {
    "liter", "litre", "litres", "quantity", "qty",
    "linetotal", "amount", "value", "rate", "cost", "price",
}
_JM_PRIMARY_HINT_INTEGER = {"docnum", "docentry", "linenum"}
_JM_PRIMARY_HINT_DATE = {"docdate", "duedate", "taxdate"}


def _infer_type(key: str, value: Any) -> str:
    k = (key or "").lower()
    if k in _JM_PRIMARY_HINT_INTEGER:
        return "integer"
    if k in _JM_PRIMARY_HINT_DATE:
        return "date"
    if k in _JM_PRIMARY_HINT_NUMERIC:
        return "numeric"
    if isinstance(value, bool):
        return "text"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, (float, Decimal)):
        return "numeric"
    if isinstance(value, date):
        return "date"
    return "text"


def _jm_primary_fallback_columns() -> list[tuple[str, str]]:
    """Used only if the discovery call returns no rows (cold DB / off-hours)."""
    return [
        ("DocNum", "integer"),
        ("DocDate", "date"),
        ("CardCode", "text"),
        ("CardName", "text"),
        ("ItemCode", "text"),
        ("ItemName", "text"),
        ("Quantity", "numeric"),
        ("Liter", "numeric"),
        ("LineTotal", "numeric"),
        ("U_TYPE", "text"),
    ]


def jm_primary_columns(source: str) -> list[tuple[str, str]]:
    """Discover JM Primary columns by running REPORT_SALES_ANALYSIS once
    against a tiny date range. Cached per source for the lifetime of the
    process (procedure column shape doesn't change at runtime)."""
    if source in _JM_PRIMARY_COLUMN_CACHE:
        return _JM_PRIMARY_COLUMN_CACHE[source]
    today = date.today()
    week_ago = today - timedelta(days=7)
    sample: list[dict] = []
    try:
        sample = report_sales_analysis(week_ago.isoformat(), today.isoformat(), source=source)
    except Exception as exc:  # noqa: BLE001
        logger.warning("jm_primary_columns discovery failed (%s); using fallback", exc)
    if sample:
        first = sample[0]
        cols = [(k, _infer_type(k, v)) for k, v in first.items()]
    else:
        cols = _jm_primary_fallback_columns()
    _JM_PRIMARY_COLUMN_CACHE[source] = cols
    return cols


def fetch_jm_primary(
    source: str,
    from_date: str,
    to_date: str,
    page: int,
    page_size: int,
) -> tuple[list[dict], int]:
    """All-rows + Python-side pagination. The procedure already filters by
    [from_date, to_date], so the full result set fits in memory."""
    if not from_date or not to_date:
        return [], 0
    rows = report_sales_analysis(from_date, to_date, source=source)
    total = len(rows)
    offset = page * page_size
    return rows[offset : offset + page_size], total


# ─── dispatcher used by platforms.reports ───
def columns_for(view: str) -> list[tuple[str, str]]:
    kind, source = parse_sap_view(view)
    if kind == "jm_primary":
        return jm_primary_columns(source)
    if kind == "jm_inventory":
        return _JM_INVENTORY_COLUMNS
    raise ValueError(f"Unknown SAP kind: {kind!r}")


def fetch_for(
    view: str,
    *,
    from_date: str,
    to_date: str,
    page: int,
    page_size: int,
) -> tuple[list[dict], int]:
    kind, source = parse_sap_view(view)
    if kind == "jm_primary":
        return fetch_jm_primary(source, from_date, to_date, page, page_size)
    if kind == "jm_inventory":
        # snapshot — date range is intentionally ignored
        return fetch_jm_inventory(source, page, page_size)
    raise ValueError(f"Unknown SAP kind: {kind!r}")
