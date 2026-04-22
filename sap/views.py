"""SAP B1 (HANA) read endpoints. Mirrors FastAPI routes/sap.py."""

from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import NotFound, APIException
from rest_framework.response import Response

from accounts.permissions import require

from .service import select


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
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(200, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 0, 50
    return page, page_size


def _run(sql: str, params: list | tuple | None = None) -> list[dict]:
    try:
        return select(sql, params)
    except Exception as e:
        raise SAPError(f"SAP HANA error: {e}")


def _count_of(sql: str, params: list | tuple | None = None) -> int:
    rows = _run(sql, params)
    if not rows:
        return 0
    val = next(iter(rows[0].values()))
    try:
        return int(val or 0)
    except (TypeError, ValueError):
        return 0


# ─── /distributors ───
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
