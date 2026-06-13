"""SAP B1 (HANA) read endpoints. Mirrors FastAPI routes/sap.py."""

import logging
import re
from calendar import monthrange
from datetime import date
from decimal import InvalidOperation

from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import NotFound, APIException, ValidationError
from rest_framework.response import Response

from accounts.permissions import require

from .service import (
    HANA_SCHEMAS,
    SALES_ANALYSIS_DEFAULT_SOURCE,
    SALES_ANALYSIS_PROCEDURES,
    report_sales_analysis,
    resolve_schema,
    select,
)

logger = logging.getLogger(__name__)


# Platform slug → SAP U_Chain values + CardName patterns
PLATFORM_CHAIN_MAP = {
    "blinkit":   {"chains": [],            "names": ["%blinkit%", "%blink commerce%", "%fashnear%", "%grofer%"]},
    "zepto":     {"chains": [],            "names": ["%zepto%", "%kiranakart%"]},
    "jiomart":   {"chains": ["JIOMART"],   "names": ["%jiomart%", "%reliance retail%"]},
    "amazon":    {"chains": ["AMAZON"],    "names": ["%amazon%"]},
    "bigbasket": {"chains": ["BIG BASKET"], "names": ["%bigbasket%", "%big basket%", "%innovative retail%"]},
    "swiggy":    {"chains": ["SWIGGY"],    "names": ["%swiggy%", "%scootsy%"]},
    "flipkart":  {"chains": ["FLIPKART"],  "names": ["%flipkart%"]},
}


class SAPError(APIException):
    status_code = 500
    default_detail = "SAP HANA error"
    default_code = "sap_hana_error"


def _page(request) -> tuple[int, int]:
    # Page size cap raised from 200 -> 100_000 so the JM Primary Dashboard
    # (PlatformSapDashboard.jsx) can pull a full month's rows in a single
    # request and compute correct per-vendor totals and latest-sale dates.
    # Tables still default to small page sizes; this cap is a safety ceiling.
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(100_000, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 0, 50
    return page, page_size


def _run(
    sql: str, params: list | tuple | None = None, schema: str | None = None
) -> list[dict]:
    try:
        return select(sql, params, schema)
    except Exception as e:
        raise SAPError(f"SAP HANA error: {e}")


def _count_of(
    sql: str, params: list | tuple | None = None, schema: str | None = None
) -> int:
    rows = _run(sql, params, schema)
    if not rows:
        return 0
    val = next(iter(rows[0].values()))
    try:
        return int(val or 0)
    except (TypeError, ValueError):
        return 0


def _month_end_from_params(request) -> str | None:
    raw_month = str(request.query_params.get("month") or "").strip()
    raw_year = str(request.query_params.get("year") or "").strip()
    if not raw_month or not raw_year:
        return None
    try:
        month = int(raw_month)
        year = int(raw_year)
        if month < 1 or month > 12 or year < 1900 or year > 2200:
            return None
    except ValueError:
        return None
    last_day = monthrange(year, month)[1]
    return date(year, month, last_day).isoformat()


# ─── /distributors ───
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SALES_ANALYSIS_FILTERS = {
    "main_group": "U_Main_Group",
    "chain": "U_Chain",
    "state": "State",
    "type": "Type",
    "brand": "Brand",
    "location": "Location",
    "item_head": "U_TYPE",
    "sub_group": "U_Sub_Group",
    "sales_person": "U_SALES_PERSON",
    "cardname": ("CardName", "CARDNAME"),
}
# Filters that accept multiple values (sent as repeated `?key=A&key=B…`).
# The view reads these via getlist; _row_matches treats them as set-membership.
_SALES_ANALYSIS_MULTI_FILTERS = {"cardname", "item_head"}
_SALES_ANALYSIS_SEARCH_COLS = (
    "CardCode",
    "CardName",
    "ItemCode",
    "ItemName",
    "SKU",
    "U_Main_Group",
    "U_Chain",
    "State",
    "Brand",
    "Location",
    "U_SALES_PERSON",
)


def _date_param(request, key: str) -> str:
    raw = str(request.query_params.get(key) or "").strip()
    if not _DATE_RE.match(raw):
        raise ValidationError(f"`{key}` must be YYYY-MM-DD.")
    try:
        date.fromisoformat(raw)
    except ValueError:
        raise ValidationError(f"`{key}` must be a valid calendar date.")
    return raw


def _num(value) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError, InvalidOperation):
        return 0.0


def _row_matches(row: dict, query: str, filters: dict) -> bool:
    """`filters` values are pre-lowercased: a `str` for single-value filters
    or a `set[str]` for multi-value filters. Empty/None means 'no filter'."""
    for param, column in _SALES_ANALYSIS_FILTERS.items():
        selected = filters.get(param)
        if not selected:
            continue
        row_val = str(_row_value(row, column) or "").strip().lower()
        if isinstance(selected, set):
            if row_val not in selected:
                return False
        elif row_val != selected:
            return False

    if not query:
        return True
    haystack = " ".join(str(_row_value(row, col) or "") for col in _SALES_ANALYSIS_SEARCH_COLS)
    return query.lower() in haystack.lower()


def _row_value(row: dict, column):
    columns = column if isinstance(column, (tuple, list)) else (column,)
    for key in columns:
        if key in row:
            return row.get(key)
    lower_map = {str(key).lower(): key for key in row.keys()}
    for key in columns:
        actual = lower_map.get(str(key).lower())
        if actual is not None:
            return row.get(actual)
    return None


def _filter_options(rows: list[dict], filters: dict, query: str) -> dict:
    """Filter option lists.

    Single-value filters (Main Group, Chain, State, Type, Item Head, Sub
    Group, Brand, Location, Sales Person) always show every distinct value
    in the date range, so the user can pivot freely between selections.

    Multi-value filters (Card Name) cascade: their options are computed from
    rows that pass **all other** active filters and the search query. That
    way picking Main Group + Chain narrows Card Name to only the customers
    that exist in those rows.

    Each option list is sorted and capped at 300 to keep dropdowns light."""
    result = {}
    for param, column in _SALES_ANALYSIS_FILTERS.items():
        if param in _SALES_ANALYSIS_MULTI_FILTERS:
            sub_filters = {k: v for k, v in filters.items() if k != param}
            source_rows = (
                r for r in rows if _row_matches(r, query, sub_filters)
            )
        else:
            source_rows = rows
        values: set[str] = set()
        for row in source_rows:
            raw = _row_value(row, column)
            if raw is None:
                continue
            s = str(raw).strip()
            if s:
                values.add(s)
        result[param] = sorted(values)[:300]
    return result


def _sales_analysis_summary(rows: list[dict]) -> dict:
    customers = {row.get("CardCode") for row in rows if row.get("CardCode")}
    items = {row.get("ItemCode") for row in rows if row.get("ItemCode")}
    return {
        "rows": len(rows),
        "customers": len(customers),
        "items": len(items),
        "quantity": sum(_num(row.get("Quantity")) for row in rows),
        "liter": sum(_num(row.get("Liter")) for row in rows),
        "line_total": sum(_num(row.get("LineTotal")) for row in rows),
        "scheme_sale_amt": sum(_num(row.get("SchemeSaleAmt")) for row in rows),
        "scheme_amt": sum(_num(row.get("SchemeAmt")) for row in rows),
        "cogs": sum(_num(row.get("COGS")) for row in rows),
    }


@api_view(["GET"])
@permission_classes([require("sap.view")])
def sales_analysis(request):
    from_date = _date_param(request, "from_date")
    to_date = _date_param(request, "to_date")
    if date.fromisoformat(from_date) > date.fromisoformat(to_date):
        raise ValidationError("`from_date` cannot be after `to_date`.")

    page, page_size = _page(request)
    query = str(request.query_params.get("search") or "").strip()
    # Pre-normalize filters once so per-row matching is just a hash lookup or
    # string compare — no repeated lower()/strip() inside the inner loop.
    filters: dict = {}
    for param in _SALES_ANALYSIS_FILTERS:
        if param in _SALES_ANALYSIS_MULTI_FILTERS:
            values = [
                v.strip()
                for v in request.query_params.getlist(param)
                if v and v.strip()
            ]
            filters[param] = {v.lower() for v in values} if values else None
        else:
            raw = str(request.query_params.get(param) or "").strip()
            filters[param] = raw.lower() if raw else None

    raw_source = str(request.query_params.get("source") or "").strip().lower()
    source = raw_source or SALES_ANALYSIS_DEFAULT_SOURCE
    if source not in SALES_ANALYSIS_PROCEDURES:
        raise ValidationError(
            f"`source` must be one of {sorted(SALES_ANALYSIS_PROCEDURES)}."
        )
    # Surface the resolved HANA procedure in the response so the UI can show
    # exactly which one served the data.
    procedure_label = (
        SALES_ANALYSIS_PROCEDURES[source].replace('"', '')
    )

    try:
        rows = report_sales_analysis(from_date, to_date, source=source)
    except Exception as e:
        # Surface infrastructure failures with a human-readable hint instead
        # of dumping the raw hdbcli stack trace. rc=10060 / RTE:[89006] are
        # TCP connect timeouts — almost always a VPN/firewall/host-down issue,
        # not a backend bug, so the message points the user at the right fix.
        text = str(e)
        if "rc=10060" in text or "RTE:[89006]" in text or "Connection failed" in text:
            raise SAPError(
                "Cannot reach SAP HANA database — connection timed out. "
                "Check VPN / network access to the HANA host and that the "
                "SAP HANA server is running."
            )
        raise SAPError(f"SAP HANA procedure error: {e}")

    columns = list(rows[0].keys()) if rows else []
    filtered = [row for row in rows if _row_matches(row, query, filters)]
    offset = page * page_size
    active = {k: v for k, v in filters.items() if v}
    logger.warning(
        "[SAP] sales_analysis source=%s %s..%s | procedure=%d | filtered=%d | page=%d/%d | active=%s | search=%r",
        source,
        from_date,
        to_date,
        len(rows),
        len(filtered),
        page,
        page_size,
        active,
        query,
    )
    return Response({
        "data": filtered[offset:offset + page_size],
        "count": len(filtered),
        "page": page,
        "page_size": page_size,
        "columns": columns,
        "filters": _filter_options(rows, filters, query),
        "summary": _sales_analysis_summary(filtered),
        "procedure": procedure_label,
        "source": source,
        "sources": sorted(SALES_ANALYSIS_PROCEDURES),
        "from_date": from_date,
        "to_date": to_date,
    })


@api_view(["GET"])
@permission_classes([require("sap.view")])
def distributors(request):
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    where = 'WHERE T0."CardType" = \'S\''
    params: list = []
    if search:
        where += (
            ' AND (T0."CardCode" LIKE ? OR T0."CardName" LIKE ?'
            ' OR T0."Phone1" LIKE ? OR T0."City" LIKE ?)'
        )
        s = f"%{search}%"
        params.extend([s, s, s, s])

    sql = f"""
        SELECT
            T0."CardCode", T0."CardName", T0."CardType", T0."GroupCode",
            T0."Phone1", T0."Phone2", T0."Cellular", T0."Fax",
            T0."E_Mail" AS "Email",
            T0."Address", T0."City", T0."ZipCode",
            T0."State1" AS "State", T0."Country", T0."Currency",
            T0."Balance", T0."CreditLine",
            T0."LicTradNum" AS "GSTIN",
            T0."validFor" AS "Active",
            T0."CreateDate", T0."UpdateDate"
        FROM OCRD T0
        {where}
        ORDER BY T0."CardName"
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, params or None)
    total = _count_of(f'SELECT COUNT(*) AS "total" FROM OCRD T0 {where}', params or None)
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /distributors/{card_code} ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def distributor_detail(request, card_code: str):
    bp_sql = """
        SELECT
            T0."CardCode", T0."CardName", T0."CardType", T0."GroupCode",
            T0."Phone1", T0."Phone2", T0."Cellular", T0."Fax",
            T0."E_Mail" AS "Email",
            T0."Address", T0."City", T0."ZipCode",
            T0."State1" AS "State", T0."Country", T0."Currency",
            T0."Balance", T0."CreditLine",
            T0."LicTradNum" AS "GSTIN", T0."validFor" AS "Active",
            T0."CreateDate", T0."UpdateDate"
        FROM OCRD T0
        WHERE T0."CardCode" = ?
    """
    addr_sql = """
        SELECT
            T0."Address", T0."AdresType", T0."Street", T0."Block",
            T0."City", T0."ZipCode", T0."State", T0."Country"
        FROM CRD1 T0
        WHERE T0."CardCode" = ?
    """
    contact_sql = """
        SELECT
            T0."Name", T0."FirstName", T0."LastName",
            T0."Tel1", T0."Tel2", T0."E_MailL" AS "Email",
            T0."Position", T0."Active"
        FROM OCPR T0
        WHERE T0."CardCode" = ?
    """
    bp = _run(bp_sql, (card_code,))
    if not bp:
        raise NotFound("Distributor not found")
    addresses = _run(addr_sql, (card_code,))
    contacts = _run(contact_sql, (card_code,))
    return Response({"distributor": bp[0], "addresses": addresses, "contacts": contacts})


# ─── /distributor-orders/{card_code} ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def distributor_orders(request, card_code: str):
    page, page_size = _page(request)
    offset = page * page_size
    sql = f"""
        SELECT
            T0."DocEntry", T0."DocNum", T0."DocDate", T0."DocDueDate",
            T0."CardCode", T0."CardName", T0."DocTotal", T0."DocCur",
            T0."DocStatus", T0."NumAtCard" AS "VendorRef",
            T0."Comments"
        FROM OPOR T0
        WHERE T0."CardCode" = ?
        ORDER BY T0."DocDate" DESC
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, (card_code,))
    total = _count_of(
        'SELECT COUNT(*) AS "total" FROM OPOR T0 WHERE T0."CardCode" = ?',
        (card_code,),
    )
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /distributor-invoices/{card_code} ───
@api_view(["GET"])
@permission_classes([require("sap.invoice.view")])
def distributor_invoices(request, card_code: str):
    page, page_size = _page(request)
    offset = page * page_size
    sql = f"""
        SELECT
            T0."DocEntry", T0."DocNum", T0."DocDate", T0."DocDueDate",
            T0."CardCode", T0."CardName", T0."DocTotal", T0."DocCur",
            T0."DocStatus", T0."NumAtCard" AS "VendorRef",
            T0."Comments"
        FROM OPCH T0
        WHERE T0."CardCode" = ?
        ORDER BY T0."DocDate" DESC
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, (card_code,))
    total = _count_of(
        'SELECT COUNT(*) AS "total" FROM OPCH T0 WHERE T0."CardCode" = ?',
        (card_code,),
    )
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /items ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def items(request):
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    where = "WHERE 1=1"
    params: list = []
    if search:
        where += (
            ' AND (T0."ItemCode" LIKE ? OR T0."ItemName" LIKE ?'
            ' OR T0."CodeBars" LIKE ?)'
        )
        s = f"%{search}%"
        params.extend([s, s, s])

    sql = f"""
        SELECT
            T0."ItemCode", T0."ItemName", T0."CodeBars" AS "Barcode",
            T0."ItmsGrpCod" AS "GroupCode",
            T0."OnHand" AS "InStock", T0."IsCommited" AS "Committed",
            T0."OnOrder", T0."OnHand" - T0."IsCommited" AS "Available",
            T0."BuyUnitMsr" AS "PurchaseUOM", T0."SalUnitMsr" AS "SalesUOM",
            T0."LastPurPrc" AS "LastPurchasePrice",
            T0."LastPurCur" AS "Currency",
            T0."validFor" AS "Active"
        FROM OITM T0
        {where}
        ORDER BY T0."ItemName"
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, params or None)
    total = _count_of(f'SELECT COUNT(*) AS "total" FROM OITM T0 {where}', params or None)
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /warehouses ───
def _warehouse_columns() -> str:
    return """
            T0."WhsCode", T0."WhsName", T0."Inactive",
            T0."Location", T0."DropShip", T0."BinActivat", T0."Locked",
            T0."Street", T0."Block", T0."StreetNo",
            T0."City", T0."County", T0."State", T0."Country", T0."ZipCode",
            T0."Address2", T0."Address3",
            T0."GlblLocNum", T0."BPLid",
            T0."U_PriceList", T0."U_Owner",
            T0."createDate" AS "CreateDate",
            T0."updateDate" AS "UpdateDate"
    """


def _warehouse_inactive_filter(raw: str) -> str:
    value = raw.strip().lower()
    if value in {"y", "yes", "true", "1"}:
        return "Y"
    if value in {"n", "no", "false", "0"}:
        return "N"
    return ""


@api_view(["GET"])
@permission_classes([require("sap.view")])
def warehouses(request):
    search = request.query_params.get("search", "").strip()
    inactive = _warehouse_inactive_filter(request.query_params.get("inactive", ""))
    page, page_size = _page(request)
    offset = page * page_size

    where = "WHERE 1=1"
    params: list = []
    if inactive:
        where += ' AND T0."Inactive" = ?'
        params.append(inactive)
    if search:
        where += (
            ' AND (T0."WhsCode" LIKE ? OR T0."WhsName" LIKE ?'
            ' OR T0."City" LIKE ? OR T0."State" LIKE ? OR T0."Country" LIKE ?)'
        )
        s = f"%{search}%"
        params.extend([s, s, s, s, s])

    sql = f"""
        SELECT
            {_warehouse_columns()}
        FROM OWHS T0
        {where}
        ORDER BY T0."WhsCode"
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, params or None)
    total = _count_of(f'SELECT COUNT(*) AS "total" FROM OWHS T0 {where}', params or None)
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /warehouses/{whs_code} ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def warehouse_detail(request, whs_code: str):
    warehouse_sql = f"""
        SELECT
            {_warehouse_columns()}
        FROM OWHS T0
        WHERE T0."WhsCode" = ?
    """
    warehouse = _run(warehouse_sql, (whs_code,))
    if not warehouse:
        raise NotFound("Warehouse not found")

    stock_summary_sql = """
        SELECT
            COUNT(*) AS "itemWarehouseRows",
            COUNT(DISTINCT CASE
                WHEN T0."OnHand" > 0 THEN T0."ItemCode"
            END) AS "itemsWithOnHand",
            COUNT(DISTINCT CASE
                WHEN T0."OnHand" <> 0 OR T0."IsCommited" <> 0 OR T0."OnOrder" <> 0
                THEN T0."ItemCode"
            END) AS "activeStockItems",
            SUM(T0."OnHand") AS "OnHand",
            SUM(T0."IsCommited") AS "Committed",
            SUM(T0."OnOrder") AS "OnOrder",
            SUM(T0."OnHand" - T0."IsCommited") AS "Available"
        FROM OITW T0
        WHERE T0."WhsCode" = ?
    """
    stock_summary = _run(stock_summary_sql, (whs_code,))
    return Response({
        "warehouse": warehouse[0],
        "stock_summary": stock_summary[0] if stock_summary else None,
    })


# ─── /stock-by-warehouse ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def stock_by_warehouse(request):
    item_code = request.query_params.get("item_code", "").strip()
    where = 'WHERE T0."OnHand" > 0'
    params: list = []
    if item_code:
        where += ' AND T0."ItemCode" = ?'
        params.append(item_code)

    sql = f"""
        SELECT
            T0."ItemCode", T1."ItemName",
            T0."WhsCode", T0."OnHand", T0."IsCommited" AS "Committed",
            T0."OnOrder", T0."OnHand" - T0."IsCommited" AS "Available"
        FROM OITW T0
        INNER JOIN OITM T1 ON T0."ItemCode" = T1."ItemCode"
        {where}
        ORDER BY T0."WhsCode"
    """
    data = _run(sql, params or None)
    return Response({"data": data})


# ─── /inventory-overview ───
# Item × Warehouse drill-down grid + KPI summary + filter option lists.
# Joins OITM ↔ OITW ↔ OWHS ↔ OITB (for item group name). Filters: warehouse,
# item group, validFor status, stock state (in/out/low), free-text search.

def _split_csv(raw: str) -> list[str]:
    return [piece.strip() for piece in (raw or "").split(",") if piece.strip()]


@api_view(["GET"])
@permission_classes([require("sap.view")])
def inventory_overview(request):
    page, page_size = _page(request)
    offset = page * page_size

    # Which company DB to read — mart (default) or oil. Every query below runs
    # against this schema via the `schema=` override on _run/_count_of.
    source, schema = resolve_schema(request.query_params.get("source"))

    search = (request.query_params.get("search") or "").strip()
    status = (request.query_params.get("status") or "").strip().upper()
    stock_state = (request.query_params.get("stock_state") or "").strip().lower()
    warehouse_codes = _split_csv(request.query_params.get("warehouse", ""))
    warehouse_code_codes = _split_csv(request.query_params.get("warehouse_code", ""))
    group_codes_raw = _split_csv(request.query_params.get("group", ""))
    # Item group codes are integers in OITM; drop anything non-numeric to keep
    # the IN-clause safe and to avoid a HANA cast error.
    group_codes: list[int] = []
    for piece in group_codes_raw:
        try:
            group_codes.append(int(piece))
        except ValueError:
            pass

    # Each active filter is a named SQL fragment. The option-list queries below
    # rebuild the WHERE while EXCLUDING one dimension, which is what makes the
    # dropdowns cascade — every filter's options reflect the OTHER active
    # filters. `warehouse` and `warehouse_code` both constrain T1."WhsCode" and
    # form one "warehouse" dimension for the purpose of narrowing options.
    frag: dict[str, tuple[str, list]] = {}

    if search:
        s = f"%{search}%"
        frag["search"] = (
            '(T0."ItemCode" LIKE ? OR T0."ItemName" LIKE ? OR T0."CodeBars" LIKE ?)',
            [s, s, s],
        )
    if status in ("Y", "N"):
        frag["status"] = ('T0."validFor" = ?', [status])
    if warehouse_codes:
        ph = ", ".join(["?"] * len(warehouse_codes))
        frag["warehouse"] = (f'T1."WhsCode" IN ({ph})', list(warehouse_codes))
    if warehouse_code_codes:
        ph = ", ".join(["?"] * len(warehouse_code_codes))
        frag["warehouse_code"] = (f'T1."WhsCode" IN ({ph})', list(warehouse_code_codes))
    if group_codes:
        ph = ", ".join(["?"] * len(group_codes))
        frag["group"] = (f'T0."ItmsGrpCod" IN ({ph})', list(group_codes))
    if stock_state == "in":
        frag["stock_state"] = ('T1."OnHand" > 0', [])
    elif stock_state == "out":
        frag["stock_state"] = ('T1."OnHand" = 0', [])
    elif stock_state == "low":
        # Low-stock requires a populated MinStock; rows without one are excluded.
        frag["stock_state"] = (
            'T1."OnHand" > 0 AND T1."MinStock" > 0 AND T1."OnHand" < T1."MinStock"',
            [],
        )

    def compose(exclude: tuple[str, ...] = ()):
        clauses = ["1=1"]
        ps: list = []
        for name, (clause_sql, clause_params) in frag.items():
            if name in exclude:
                continue
            clauses.append(clause_sql)
            ps.extend(clause_params)
        return "WHERE " + " AND ".join(clauses), ps

    where_sql, params = compose()

    # 1) Paginated item × warehouse rows
    rows_sql = f"""
        SELECT
            T0."ItemCode",
            T0."ItemName",
            T0."ItmsGrpCod" AS "GroupCode",
            T3."ItmsGrpNam" AS "GroupName",
            T0."SalUnitMsr" AS "UOM",
            T0."validFor"   AS "Active",
            T0."LastPurPrc" AS "LastPurchasePrice",
            T0."LastPurCur" AS "Currency",
            T1."WhsCode",
            T2."WhsName",
            T2."Location",
            T2."City",
            T1."OnHand",
            T1."IsCommited" AS "Committed",
            T1."OnHand" - T1."IsCommited" AS "Available",
            T1."OnOrder",
            T1."MinStock",
            T1."MaxStock",
            T1."OnHand" * T0."LastPurPrc" AS "StockValue"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
        LEFT  JOIN OITB T3 ON T3."ItmsGrpCod" = T0."ItmsGrpCod"
        {where_sql}
        ORDER BY T0."ItemName", T1."WhsCode"
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(rows_sql, params or None, schema=schema)

    # 2) Total row count for pagination footer
    count_sql = f"""
        SELECT COUNT(*) AS "total"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
        {where_sql}
    """
    total = _count_of(count_sql, params or None, schema=schema)

    # 3) KPI aggregates over the FULL filtered set (not just the current page)
    summary_sql = f"""
        SELECT
            COUNT(DISTINCT T0."ItemCode") AS "total_skus",
            COALESCE(SUM(T1."OnHand"), 0) AS "total_units_on_hand",
            COALESCE(SUM(T1."OnHand" * T0."LastPurPrc"), 0) AS "total_stock_value"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
        {where_sql}
    """
    summary_rows = _run(summary_sql, params or None, schema=schema)
    summary = dict(summary_rows[0]) if summary_rows else {
        "total_skus": 0, "total_units_on_hand": 0, "total_stock_value": 0,
    }

    # Items where the total OnHand across all (filtered) warehouses is 0.
    items_zero_sql = f"""
        SELECT COUNT(*) AS "n" FROM (
            SELECT T0."ItemCode"
            FROM OITM T0
            INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
            LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
            {where_sql}
            GROUP BY T0."ItemCode"
            HAVING COALESCE(SUM(T1."OnHand"), 0) = 0
        )
    """
    summary["items_zero_stock"] = _count_of(items_zero_sql, params or None, schema=schema)

    # Items where total OnHand < total MinStock (only counts items whose
    # MinStock is actually populated — otherwise this would always be 0 vs 0).
    items_below_min_sql = f"""
        SELECT COUNT(*) AS "n" FROM (
            SELECT T0."ItemCode"
            FROM OITM T0
            INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
            LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
            {where_sql}
            GROUP BY T0."ItemCode"
            HAVING COALESCE(SUM(T1."MinStock"), 0) > 0
               AND COALESCE(SUM(T1."OnHand"), 0) < COALESCE(SUM(T1."MinStock"), 0)
        )
    """
    summary["items_below_min"] = _count_of(items_below_min_sql, params or None, schema=schema)

    # Global flag: does ANY OITW row have a non-zero MinStock? Frontend hides
    # the "Items Below Min" KPI when this is false, since the count would be
    # meaningless (always 0).
    has_min_stock = _count_of(
        'SELECT COUNT(*) FROM OITW WHERE "MinStock" > 0', schema=schema
    )
    summary["min_stock_tracked"] = has_min_stock > 0

    # 4) Cascading filter option lists. Each list is built from the rows that
    # match every OTHER active filter, so selecting one filter narrows the
    # choices in the rest (only values that actually have data are shown).
    # Warehouse + warehouse-code share one dimension, so both are excluded when
    # building the warehouse list and both narrow the group list.
    wh_where, wh_params = compose(exclude=("warehouse", "warehouse_code"))
    warehouses_opts = _run(
        f"""
        SELECT DISTINCT T1."WhsCode", T2."WhsName", T2."Inactive"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
        {wh_where}
        ORDER BY T2."WhsName"
        """,
        wh_params or None,
        schema=schema,
    )

    grp_where, grp_params = compose(exclude=("group",))
    groups_opts = _run(
        f"""
        SELECT DISTINCT T0."ItmsGrpCod", T3."ItmsGrpNam"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OITB T3 ON T3."ItmsGrpCod" = T0."ItmsGrpCod"
        {grp_where}
        ORDER BY T3."ItmsGrpNam"
        """,
        grp_params or None,
        schema=schema,
    )

    return Response({
        "data": data,
        "count": total,
        "page": page,
        "page_size": page_size,
        "summary": summary,
        "source": source,
        "sources": sorted(HANA_SCHEMAS),
        "filters": {
            "warehouses": warehouses_opts,
            "groups": groups_opts,
        },
    })


# ─── /inventory-warehouse-comparison ───
# One aggregate row per warehouse for the JM Inventory Dashboard cards + the
# "Stock Value by Warehouse" chart. Reads the mart or oil schema via ?source=.
@api_view(["GET"])
@permission_classes([require("sap.view")])
def inventory_warehouse_comparison(request):
    source, schema = resolve_schema(request.query_params.get("source"))
    month_end = _month_end_from_params(request)
    movement_join = ""
    on_hand_expr = 'T1."OnHand"'
    params = None

    if month_end:
        movement_join = """
        LEFT JOIN (
            SELECT
                "ItemCode",
                "Warehouse",
                COALESCE(SUM(COALESCE("InQty", 0) - COALESCE("OutQty", 0)), 0)
                    AS "PostPeriodQty"
            FROM OINM
            WHERE "DocDate" > ?
            GROUP BY "ItemCode", "Warehouse"
        ) M ON M."ItemCode" = T1."ItemCode" AND M."Warehouse" = T1."WhsCode"
        """
        on_hand_expr = '(T1."OnHand" - COALESCE(M."PostPeriodQty", 0))'
        params = [month_end]

    rows = _run(
        f"""
        SELECT
            T1."WhsCode",
            T2."WhsName",
            T2."Inactive",
            COUNT(DISTINCT T0."ItemCode") AS "items",
            COALESCE(SUM({on_hand_expr}), 0) AS "on_hand",
            COALESCE(SUM({on_hand_expr} * T0."LastPurPrc"), 0) AS "stock_value",
            COUNT(DISTINCT CASE WHEN {on_hand_expr} = 0 THEN T0."ItemCode" END)
                AS "zero_stock"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OWHS T2 ON T2."WhsCode"  = T1."WhsCode"
        {movement_join}
        GROUP BY T1."WhsCode", T2."WhsName", T2."Inactive"
        ORDER BY T2."WhsName"
        """,
        params,
        schema=schema,
    )

    return Response({
        "warehouses": rows,
        "source": source,
        "sources": sorted(HANA_SCHEMAS),
        "as_of_date": month_end,
    })


# ─── /inventory-finished-goods ───
# Pivot view for the JM Inventory Dashboard: rows are FINISHED-group items
# (sub_group / variety / item code / name) and columns are the fixed warehouse
# code list below. Cell = OnHand for that item × warehouse. Includes a per-row
# Grand Total. Reads mart or oil schema via ?source=.
FG_WAREHOUSE_CODES: tuple[str, ...] = (
    "BH-FGM", "DL-MP", "DL-EC", "DL-GR", "DL-FG", "BH-JM",
    "FBF-HR", "KT-FG", "DL-INT", "KT-FBF", "PB-FG", "BH-GR", "BH-FG",
)
FG_GROUP_NAME = "FINISHED"


@api_view(["GET"])
@permission_classes([require("sap.view")])
def inventory_finished_goods(request):
    source, schema = resolve_schema(request.query_params.get("source"))
    placeholders = ",".join(["?"] * len(FG_WAREHOUSE_CODES))
    params: list = [FG_GROUP_NAME, *FG_WAREHOUSE_CODES]

    rows = _run(
        f"""
        SELECT
            T0."ItemCode",
            T0."ItemName",
            T0."U_Sub_Group" AS "SubGroup",
            T0."U_Variety"   AS "Variety",
            T1."WhsCode",
            COALESCE(SUM(T1."OnHand"), 0) AS "OnHand"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode" = T0."ItemCode"
        LEFT  JOIN OITB T3 ON T3."ItmsGrpCod" = T0."ItmsGrpCod"
        WHERE UPPER(T3."ItmsGrpNam") = ?
          AND T1."WhsCode" IN ({placeholders})
        GROUP BY T0."ItemCode", T0."ItemName", T0."U_Sub_Group",
                 T0."U_Variety", T1."WhsCode"
        """,
        params,
        schema=schema,
    )

    # Pivot to one row per item with a warehouses dict + grand_total.
    pivot: dict[str, dict] = {}
    for r in rows:
        code = r["ItemCode"]
        entry = pivot.get(code)
        if entry is None:
            entry = {
                "ItemCode": code,
                "ItemName": r.get("ItemName") or "",
                "SubGroup": r.get("SubGroup") or "",
                "Variety": r.get("Variety") or "",
                "warehouses": {w: 0 for w in FG_WAREHOUSE_CODES},
                "grand_total": 0,
            }
            pivot[code] = entry
        qty = float(r.get("OnHand") or 0)
        entry["warehouses"][r["WhsCode"]] = qty
        entry["grand_total"] += qty

    items = sorted(
        pivot.values(),
        key=lambda it: (
            (it.get("SubGroup") or "").upper(),
            (it.get("Variety") or "").upper(),
            it["ItemCode"],
        ),
    )

    column_totals = {w: 0.0 for w in FG_WAREHOUSE_CODES}
    grand_total = 0.0
    for it in items:
        for w, v in it["warehouses"].items():
            column_totals[w] += v
        grand_total += it["grand_total"]

    return Response({
        "source": source,
        "sources": sorted(HANA_SCHEMAS),
        "warehouses": list(FG_WAREHOUSE_CODES),
        "group": FG_GROUP_NAME,
        "items": items,
        "column_totals": column_totals,
        "grand_total": grand_total,
    })


# ─── /sales-invoices ───
@api_view(["GET"])
@permission_classes([require("sap.invoice.view")])
def sales_invoices(request):
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    where = "WHERE 1=1"
    params: list = []
    if search:
        where += (
            ' AND (T0."DocNum" LIKE ? OR T0."CardCode" LIKE ?'
            ' OR T0."CardName" LIKE ? OR T0."NumAtCard" LIKE ?)'
        )
        s = f"%{search}%"
        params.extend([s, s, s, s])

    sql = f"""
        SELECT
            T0."DocEntry", T0."DocNum", T0."DocDate", T0."DocDueDate", T0."TaxDate",
            T0."CardCode", T0."CardName",
            T0."NumAtCard" AS "CustomerRef",
            T0."DocTotal", T0."DocTotalFC", T0."VatSum", T0."DiscSum",
            T0."PaidToDate", T0."DocTotal" - T0."PaidToDate" AS "BalanceDue",
            T0."DocCur", T0."DocRate",
            T0."DocStatus", T0."CANCELED",
            T0."Comments", T0."JrnlMemo",
            T0."SlpCode", T0."OwnerCode",
            T0."CreateDate", T0."UpdateDate"
        FROM OINV T0
        {where}
        ORDER BY T0."DocDate" DESC
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, params or None)
    total = _count_of(f'SELECT COUNT(*) AS "total" FROM OINV T0 {where}', params or None)
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /sales-invoices/{card_code} ───
@api_view(["GET"])
@permission_classes([require("sap.invoice.view")])
def customer_sales_invoices(request, card_code: str):
    page, page_size = _page(request)
    offset = page * page_size
    sql = f"""
        SELECT
            T0."DocEntry", T0."DocNum", T0."DocDate", T0."DocDueDate", T0."TaxDate",
            T0."CardCode", T0."CardName",
            T0."NumAtCard" AS "CustomerRef",
            T0."DocTotal", T0."VatSum", T0."DiscSum",
            T0."PaidToDate", T0."DocTotal" - T0."PaidToDate" AS "BalanceDue",
            T0."DocCur", T0."DocRate",
            T0."DocStatus", T0."CANCELED",
            T0."Comments",
            T0."CreateDate", T0."UpdateDate"
        FROM OINV T0
        WHERE T0."CardCode" = ?
        ORDER BY T0."DocDate" DESC
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, (card_code,))
    total = _count_of(
        'SELECT COUNT(*) AS "total" FROM OINV T0 WHERE T0."CardCode" = ?',
        (card_code,),
    )
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /sales-invoice-lines/{doc_entry} ───
@api_view(["GET"])
@permission_classes([require("sap.invoice.view")])
def sales_invoice_lines(request, doc_entry: int):
    header_sql = """
        SELECT
            T0."DocEntry", T0."DocNum", T0."DocDate", T0."DocDueDate",
            T0."CardCode", T0."CardName",
            T0."DocTotal", T0."VatSum", T0."DiscSum",
            T0."PaidToDate", T0."DocTotal" - T0."PaidToDate" AS "BalanceDue",
            T0."DocCur", T0."DocStatus", T0."CANCELED"
        FROM OINV T0
        WHERE T0."DocEntry" = ?
    """
    lines_sql = """
        SELECT
            T1."LineNum", T1."ItemCode", T1."Dscription" AS "ItemName",
            T1."Quantity", T1."UnitMsr" AS "UOM",
            T1."Price", T1."DiscPrcnt" AS "DiscountPercent",
            T1."LineTotal", T1."VatSum" AS "LineTax",
            T1."WhsCode" AS "Warehouse",
            T1."TaxCode", T1."Currency",
            T1."ShipDate"
        FROM INV1 T1
        WHERE T1."DocEntry" = ?
        ORDER BY T1."LineNum"
    """
    header = _run(header_sql, (doc_entry,))
    if not header:
        raise NotFound("Sales invoice not found")
    lines = _run(lines_sql, (doc_entry,))
    return Response({"invoice": header[0], "lines": lines})


def _build_platform_where(slug: str) -> tuple[str, list]:
    mapping = PLATFORM_CHAIN_MAP.get(slug)
    if not mapping:
        raise NotFound(f"Unknown platform: {slug}")
    conditions: list[str] = []
    params: list = []
    for chain in mapping["chains"]:
        conditions.append('T0."U_Chain" = ?')
        params.append(chain)
    for pattern in mapping["names"]:
        conditions.append('LOWER(T0."CardName") LIKE ?')
        params.append(pattern)
    where = f"({' OR '.join(conditions)})" if conditions else "1=0"
    return where, params


# ─── /platform-distributors/{slug} ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def platform_distributors(request, slug: str):
    platform_where, platform_params = _build_platform_where(slug)
    search = request.query_params.get("search", "").strip()
    search_clause = ""
    search_params: list = []
    if search:
        search_clause = (
            ' AND (T0."CardCode" LIKE ? OR T0."CardName" LIKE ?'
            ' OR T0."Phone1" LIKE ? OR T0."City" LIKE ?)'
        )
        s = f"%{search}%"
        search_params.extend([s, s, s, s])

    where = f"WHERE {platform_where}{search_clause}"
    all_params = platform_params + search_params
    page, page_size = _page(request)
    offset = page * page_size

    sql = f"""
        SELECT
            T0."CardCode", T0."CardName", T0."CardType",
            T0."U_Chain" AS "Chain", T0."U_Main_Group" AS "MainGroup",
            T0."Phone1", T0."Cellular",
            T0."E_Mail" AS "Email",
            T0."Address", T0."City", T0."State1" AS "State",
            T0."Country", T0."Currency", T0."Balance", T0."CreditLine",
            T0."LicTradNum" AS "GSTIN",
            T0."validFor" AS "Active",
            T0."CreateDate", T0."UpdateDate"
        FROM OCRD T0
        {where}
        ORDER BY T0."CardName"
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, all_params or None)
    total = _count_of(
        f'SELECT COUNT(*) AS "total" FROM OCRD T0 {where}',
        all_params or None,
    )
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


# ─── /platform-distributors/{slug}/{card_code} ───
@api_view(["GET"])
@permission_classes([require("sap.view")])
def platform_distributor_detail(request, slug: str, card_code: str):
    _build_platform_where(slug)  # validate slug
    bp_sql = """
        SELECT
            T0."CardCode", T0."CardName", T0."CardType",
            T0."U_Chain" AS "Chain", T0."U_Main_Group" AS "MainGroup",
            T0."Phone1", T0."Phone2", T0."Cellular", T0."Fax",
            T0."E_Mail" AS "Email",
            T0."Address", T0."City", T0."ZipCode",
            T0."State1" AS "State", T0."Country", T0."Currency",
            T0."Balance", T0."CreditLine",
            T0."LicTradNum" AS "GSTIN", T0."validFor" AS "Active",
            T0."CreateDate", T0."UpdateDate"
        FROM OCRD T0
        WHERE T0."CardCode" = ?
    """
    addr_sql = """
        SELECT T0."Address", T0."AdresType", T0."Street", T0."Block",
               T0."City", T0."ZipCode", T0."State", T0."Country"
        FROM CRD1 T0 WHERE T0."CardCode" = ?
    """
    contact_sql = """
        SELECT T0."Name", T0."FirstName", T0."LastName",
               T0."Tel1", T0."Tel2", T0."E_MailL" AS "Email",
               T0."Position", T0."Active"
        FROM OCPR T0 WHERE T0."CardCode" = ?
    """
    bp = _run(bp_sql, (card_code,))
    if not bp:
        raise NotFound("Distributor not found")
    addresses = _run(addr_sql, (card_code,))
    contacts = _run(contact_sql, (card_code,))
    return Response({"distributor": bp[0], "addresses": addresses, "contacts": contacts})


# ─── /platform-sales-invoices/{slug} ───
@api_view(["GET"])
@permission_classes([require("sap.invoice.view")])
def platform_sales_invoices(request, slug: str):
    platform_where, platform_params = _build_platform_where(slug)
    search = request.query_params.get("search", "").strip()
    search_clause = ""
    search_params: list = []
    if search:
        search_clause = (
            ' AND (T0."DocNum" LIKE ? OR T0."CardName" LIKE ?'
            ' OR T0."NumAtCard" LIKE ?)'
        )
        s = f"%{search}%"
        search_params.extend([s, s, s])

    # Re-alias the platform_where predicates to T1 so they bind to OCRD inside
    # the subquery (outer query's T0 is OINV).
    sub_where = platform_where.replace('T0.', 'T1.')
    where = (
        f'WHERE T0."CardCode" IN (SELECT T1."CardCode" FROM OCRD T1 WHERE {sub_where})'
        f"{search_clause}"
    )
    all_params = platform_params + search_params
    page, page_size = _page(request)
    offset = page * page_size

    sql = f"""
        SELECT
            T0."DocEntry", T0."DocNum", T0."DocDate", T0."DocDueDate",
            T0."CardCode", T0."CardName",
            T0."NumAtCard" AS "CustomerRef",
            T0."DocTotal", T0."VatSum", T0."DiscSum",
            T0."PaidToDate", T0."DocTotal" - T0."PaidToDate" AS "BalanceDue",
            T0."DocCur", T0."DocStatus", T0."CANCELED",
            T0."Comments",
            T0."CreateDate"
        FROM OINV T0
        {where}
        ORDER BY T0."DocDate" DESC
        LIMIT {page_size} OFFSET {offset}
    """
    data = _run(sql, all_params or None)
    total = _count_of(
        f'SELECT COUNT(*) AS "total" FROM OINV T0 {where}',
        all_params or None,
    )
    return Response({"data": data, "count": total, "page": page, "page_size": page_size})


@api_view(["GET"])
@permission_classes([require("sap.view")])
def distributor_inventory(request):
    """Distributor inventory by purchase price (FIFO lots) for one card.

    GET /api/sap/distributor-inventory?card_code=CUSTA000907[&card_name=...]
    Returns the layered on-hand position for the current (opening) month. See
    sap/distributor_inventory.py and the FIFO master-view plan doc.
    """
    from .distributor_inventory import build_distributor_inventory

    card_code = str(request.query_params.get("card_code") or "CUSTA000907").strip()
    card_name = str(request.query_params.get("card_name") or "").strip() or None
    try:
        payload = build_distributor_inventory(card_code, card_name)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[dist-inv] failed for card_code=%s", card_code)
        raise SAPError(f"Distributor inventory error: {exc}")
    return Response(payload)
