import re
from calendar import monthrange
from datetime import date

from django.db import connection
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import can_access_platform, require

from .models import PlatformConfig

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str) -> str:
    if not name or not _IDENT.match(name):
        raise ValidationError(f"Invalid table identifier: {name!r}")
    return name


def _safe_col(name: str) -> str | None:
    return name if name and _IDENT.match(name) else None


def _scalar(sql: str, params: list):
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def _dict_rows(sql: str, params: list) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _get_platform(slug: str) -> PlatformConfig:
    return get_object_or_404(PlatformConfig, slug=slug, is_active=True)


def _ensure_scope(user, slug: str) -> None:
    if not can_access_platform(user, slug):
        raise PermissionDenied(f"Your account is not authorized for the '{slug}' platform.")


def _page(request) -> tuple[int, int]:
    try:
        page = max(0, int(request.query_params.get("page", 0)))
        page_size = min(200, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 0, 50
    return page, page_size


# ─── /{slug}/stats ───
@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
def platform_stats(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    inv = _safe_ident(p.inventory_table) if p.inventory_table else None
    sec = _safe_ident(p.secondary_table) if p.secondary_table else None
    master = _safe_ident(p.master_po_table or "master_po")

    filter_col = _safe_col(p.po_filter_column or "platform") or "platform"
    filter_val = p.po_filter_value or p.slug

    inventory_count = 0
    sells_count = 0
    open_pos = 0

    try:
        if inv:
            inventory_count = _scalar(f'SELECT COUNT(*) FROM "{inv}"', []) or 0
    except Exception:
        inventory_count = 0
    try:
        if sec:
            sells_count = _scalar(f'SELECT COUNT(*) FROM "{sec}"', []) or 0
    except Exception:
        sells_count = 0
    try:
        open_pos = _scalar(
            f'SELECT COUNT(*) FROM "{master}" WHERE "{filter_col}" ILIKE %s',
            [f"%{filter_val}%"],
        ) or 0
    except Exception:
        open_pos = 0

    return Response({
        "inventory": int(inventory_count),
        "sells": int(sells_count),
        "openPOs": int(open_pos),
        "activeTrucks": 0,
    })


# ─── /{slug}/pos ───
@api_view(["GET"])
@permission_classes([require("platform.po.view")])
def platform_pos(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    master = _safe_ident(p.master_po_table or "master_po")
    filter_col = _safe_col(p.po_filter_column or "platform") or "platform"
    filter_val = p.po_filter_value or p.slug
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    where = f'WHERE "{filter_col}" ILIKE %s'
    params: list = [f"%{filter_val}%"]
    if search:
        where += (
            ' AND ("po_number" ILIKE %s OR "sku_name" ILIKE %s OR "sku_code" ILIKE %s)'
        )
        s = f"%{search}%"
        params.extend([s, s, s])

    try:
        total = _scalar(f'SELECT COUNT(*) FROM "{master}" {where}', params) or 0
        rows = _dict_rows(
            f'SELECT * FROM "{master}" {where} LIMIT %s OFFSET %s',
            params + [page_size, offset],
        )
    except Exception:
        total = 0
        rows = []

    return Response({
        "data": rows,
        "count": int(total),
        "page": page,
        "page_size": page_size,
    })


# ─── /{slug}/inventory-match?sku= ───
@api_view(["GET"])
@permission_classes([require("platform.inventory.view")])
def inventory_match(request, slug: str):
    _ensure_scope(request.user, slug)
    p = _get_platform(slug)
    sku = request.query_params.get("sku", "").strip()
    if not sku or not p.inventory_table:
        return Response({"match": None})
    inv = _safe_ident(p.inventory_table)
    match_col = _safe_col(p.match_column or "sku") or "sku"
    try:
        rows = _dict_rows(
            f'SELECT * FROM "{inv}" WHERE "{match_col}" = %s LIMIT 1',
            [sku],
        )
    except Exception:
        rows = []
    return Response({"match": rows[0] if rows else None})


# ─── Monthly Landing Rate ───
# Single shared table `monthly_landing_rate` with columns:
#   sku_code, sku_name, landing_rate, basic_rate, format, month
# `format` partitions rows per-platform.
# Only INSERTs are performed — prior rows are kept as history.

_LANDING_PLATFORMS = {
    "blinkit",
    "zepto",
    "swiggy",
    "bigbasket",
    "flipkart_grocery",
}

_LANDING_PLATFORM_LABELS = "blinkit, zepto, swiggy, bigbasket, flipkart_grocery"


def _format_for(p: PlatformConfig) -> str:
    # Store canonical platform formats in uppercase, matching the source
    # sheets/tables convention: BLINKIT, BIG BASKET, FLIPKART GROCERY, etc.
    return (p.po_filter_value or p.slug).strip().upper()


def _format_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _format_match_clause(p: PlatformConfig) -> tuple[str, list]:
    aliases = {
        _format_key(p.po_filter_value),
        _format_key(p.slug),
        _format_key(p.name),
    }
    aliases.discard("")
    placeholders = ", ".join(["%s"] * len(aliases))
    return (
        "REGEXP_REPLACE(LOWER(TRIM(\"format\"::text)), '[^a-z0-9]+', '', 'g') "
        f"IN ({placeholders})",
        sorted(aliases),
    )


def _parse_month(val: str) -> str | None:
    """Accept `YYYY-MM` or `YYYY-MM-DD`, normalize to first-of-month `YYYY-MM-01`."""
    if not val:
        return None
    val = val.strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}", val):
            y, m = val.split("-")
            return f"{y}-{m}-01"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", val):
            y, m, _ = val.split("-")
            return f"{y}-{m}-01"
    except Exception:
        return None
    return None


# --- Flipkart Grocery Sec Dashboard ---

_FK_GROCERY_SEC_ITEM_HEADS = ("PREMIUM", "COMMODITY", "OTHER")

_FK_GROCERY_SEC_DETAIL_ROWS = (
    ("PREMIUM", "CANOLA", "CANOLA", "1 LTR"),
    ("PREMIUM", "OLIVE", "EXTRA LIGHT", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "1 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "2 LTR"),
    ("PREMIUM", "OLIVE", "JIVO POMACE", "5 LTR"),
    ("COMMODITY", "BLENDED", "GOLD", "1 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "1 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "4 LTR"),
    ("COMMODITY", "MUSTARD", "MUSTARD KACCHI GHANI", "5 LTR"),
    ("COMMODITY", "SOYABEAN", "SOYABEAN", "1 LTR"),
    ("COMMODITY", "SUNFLOWER", "SUNFLOWER", "4 LTR"),
    ("OTHER", "DRINKS", "APPLE SF", "200 MLS"),
    ("OTHER", "DRINKS", "BLUEBERRY", "200 MLS"),
    ("OTHER", "DRINKS", "GINGER ALE SF", "200 MLS"),
    ("OTHER", "DRINKS", "JEERA", "160 MLS"),
    ("OTHER", "DRINKS", "JEERA SF", "200 MLS"),
    ("OTHER", "DRINKS", "MANGO", "500 MLS"),
    ("OTHER", "DRINKS", "MINERAL WATER", "1 LTR"),
    ("OTHER", "DRINKS", "MOJITO", "200 MLS"),
)


def _norm_sec_key(value) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().upper())


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _per_liter_shpd(units, litres):
    litres = _num(litres)
    if litres == 0:
        return None
    return _num(units) / litres


def _sec_total(rows: list[dict], *, include_ratio: bool = True) -> dict:
    shipped_units = sum(_num(r.get("shipped_units")) for r in rows)
    shipped_ltr = sum(_num(r.get("shipped_ltr")) for r in rows)
    shipped_value = sum(_num(r.get("shipped_value")) for r in rows)
    total = {
        "shipped_units": shipped_units,
        "shipped_ltr": shipped_ltr,
        "shipped_value": shipped_value,
    }
    if include_ratio:
        total["per_liter_shpd"] = _per_liter_shpd(shipped_units, shipped_ltr)
    return total


def _safe_div(numerator, denominator) -> float:
    denominator = _num(denominator)
    if denominator == 0:
        return 0.0
    return _num(numerator) / denominator


_FK_GROCERY_DRR_ITEM_ORDER = (
    "CANOLA 1L",
    "EXTRA LIGHT 2L",
    "GOLD 1L",
    "JIVO POMACE 1L",
    "JIVO POMACE 2L",
    "JIVO POMACE 5L",
    "MUSTARD 1L",
    "MUSTARD 4L",
    "MUSTARD 5L",
    "PUNJABI JEERA 160ML",
    "SOYABEAN 1L POUCH",
    "SUNFLOWER 4L",
    "WATER 1L",
    "WG APPLE JUICE 200 ML",
    "WG BLUEBERRY JUICE 200ML",
    "WG GINGER ALE 200ML",
    "WG JEERA 200ML",
    "WG MANGO JUICE 500ML",
    "WG MOJITO 200ML",
)

_FK_GROCERY_MOM_TARGETS = {
    "PREMIUM": 2000,
    "COMMODITY": 52000,
}

_FK_GROCERY_MOM_TEMPLATE = (
    ("CANOLA", "CANOLA 1L", "PREMIUM", 1000),
    ("EXTRA LIGHT", "EXTRA LIGHT 2L", "PREMIUM", 200),
    ("GOLD", "GOLD 5L", "COMMODITY", 0),
    ("JIVO POMACE", "JIVO POMACE 1L", "PREMIUM", 400),
    ("JIVO POMACE", "JIVO POMACE 5L", "PREMIUM", 400),
    ("MUSTARD KACHI GHANI", "MUSTARD 1L", "COMMODITY", 45000),
    ("MUSTARD KACHI GHANI", "MUSTARD 4L", "COMMODITY", 4500),
    ("MUSTARD KACHI GHANI", "MUSTARD 5L", "COMMODITY", 1000),
    ("SOYABEAN", "SOYABEAN 1L POUCH", "COMMODITY", 1000),
    ("SUNFLOWER", "SUNFLOWER 4L", "COMMODITY", 500),
)


def _parse_sec_month_year(params) -> tuple[int, int, bool]:
    raw_month = str(params.get("month") or "").strip()
    raw_year = str(params.get("year") or "").strip()

    if re.fullmatch(r"\d{4}-\d{2}", raw_month):
        year, month = raw_month.split("-")
        return int(month), int(year), False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_month):
        year, month, _ = raw_month.split("-")
        return int(month), int(year), False

    if raw_month and raw_year:
        try:
            month = int(raw_month)
            year = int(raw_year)
        except ValueError:
            raise ValidationError("`month` and `year` must be numeric or month must be YYYY-MM.")
        if not 1 <= month <= 12:
            raise ValidationError("`month` must be 1-12.")
        if year < 2000 or year > 2100:
            raise ValidationError("`year` looks out of range.")
        return month, year, False

    latest = _dict_rows(
        """
        SELECT "month", "year"
        FROM "flipkart_grocery_master"
        WHERE "real_date" IS NOT NULL
        ORDER BY "real_date" DESC
        LIMIT 1
        """,
        [],
    )
    if latest:
        return int(latest[0]["month"]), int(latest[0]["year"]), True

    today = date.today()
    return today.month, today.year, True


def _date_span(month: int, year: int, max_date: date | None) -> list[date]:
    if not max_date:
        return []
    end_day = min(max_date.day, monthrange(year, month)[1])
    return [date(year, month, day) for day in range(1, end_day + 1)]


def _shift_month(month: int, year: int, offset: int) -> tuple[int, int]:
    zero_based = (year * 12) + (month - 1) + offset
    shifted_year, shifted_month_zero = divmod(zero_based, 12)
    return shifted_month_zero + 1, shifted_year


def _month_name(month: int) -> str:
    return date(2000, month, 1).strftime("%B").upper()


def _sum_mom_rows(rows: list[dict]) -> dict:
    keys = (
        "target",
        "current_done_ltr",
        "estimated_ltr",
        "previous_1_ltr",
        "previous_2_ltr",
        "previous_3_ltr",
        "previous_4_ltr",
    )
    return {key: sum(_num(row.get(key)) for row in rows) for key in keys}


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
def flipkart_grocery_sec_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "flipkart_grocery":
        raise ValidationError("Sec Dashboard is available only for Flipkart Grocery.")

    month, year, defaulted_to_latest = _parse_sec_month_year(request.query_params)

    max_date = _scalar(
        """
        SELECT MAX("real_date")
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        """,
        [month, year],
    )

    summary_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("item_head"::text)) AS item_head,
            COALESCE(SUM("qty"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr,
            COALESCE(SUM("sale_amt_exclusive"), 0) AS shipped_value
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
          AND UPPER(TRIM("item_head"::text)) IN ('PREMIUM', 'COMMODITY', 'OTHER')
        GROUP BY UPPER(TRIM("item_head"::text))
        """,
        [month, year],
    )
    summary_by_head = {_norm_sec_key(r.get("item_head")): r for r in summary_raw}
    summary = []
    for item_head in _FK_GROCERY_SEC_ITEM_HEADS:
        row = summary_by_head.get(item_head, {})
        shipped_units = _num(row.get("shipped_units"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        summary.append({
            "item_head": item_head,
            "shipped_units": shipped_units,
            "shipped_ltr": shipped_ltr,
            "shipped_value": _num(row.get("shipped_value")),
            "per_liter_shpd": _per_liter_shpd(shipped_units, shipped_ltr),
        })

    detail_raw = _dict_rows(
        """
        SELECT
            UPPER(TRIM("sub_category"::text)) AS sub_category_key,
            UPPER(TRIM("per_ltr_unit"::text)) AS per_ltr_key,
            COALESCE(SUM("sale_amt_exclusive"), 0) AS shipped_value,
            COALESCE(SUM("qty"), 0) AS shipped_units,
            COALESCE(SUM("ltr_sold"), 0) AS shipped_ltr
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        GROUP BY
            UPPER(TRIM("sub_category"::text)),
            UPPER(TRIM("per_ltr_unit"::text))
        """,
        [month, year],
    )
    detail_by_key = {
        (_norm_sec_key(r.get("sub_category_key")), _norm_sec_key(r.get("per_ltr_key"))): r
        for r in detail_raw
    }

    details = []
    for item_head, category, sub_category, per_ltr in _FK_GROCERY_SEC_DETAIL_ROWS:
        row = detail_by_key.get((_norm_sec_key(sub_category), _norm_sec_key(per_ltr)), {})
        shipped_units = _num(row.get("shipped_units"))
        shipped_ltr = _num(row.get("shipped_ltr"))
        details.append({
            "item_head": item_head,
            "category": category,
            "sub_category": sub_category,
            "per_ltr": per_ltr,
            "shipped_value": _num(row.get("shipped_value")),
            "shipped_units": shipped_units,
            "shipped_ltr": shipped_ltr,
            "per_liter_shpd": _per_liter_shpd(shipped_units, shipped_ltr),
        })

    return Response({
        "source": "flipkart_grocery_master",
        "detail_rows_fixed": True,
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "summary": summary,
        "summary_total": _sec_total(summary),
        "details": details,
        "detail_total": _sec_total(details),
    })


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
def flipkart_grocery_drr_dashboard(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "flipkart_grocery":
        raise ValidationError("DRR Dashboard is available only for Flipkart Grocery.")

    month, year, defaulted_to_latest = _parse_sec_month_year(request.query_params)
    sales_of = str(request.query_params.get("sales_of") or "ALL").strip().upper() or "ALL"
    if sales_of != "ALL":
        raise ValidationError("DRR Dashboard currently supports SALES OF = ALL only.")

    max_date = _scalar(
        """
        SELECT MAX("real_date")
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        """,
        [month, year],
    )

    daily_raw = _dict_rows(
        """
        SELECT
            "real_date",
            COALESCE(SUM("sale_amt_exclusive"), 0) AS ops,
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        GROUP BY "real_date"
        ORDER BY "real_date"
        """,
        [month, year],
    )
    daily_by_date = {r["real_date"]: r for r in daily_raw}
    daily = []
    for current_date in _date_span(month, year, max_date):
        row = daily_by_date.get(current_date, {})
        daily.append({
            "date": current_date.isoformat(),
            "display_date": current_date.strftime("%d-%m-%Y"),
            "ops": _num(row.get("ops")),
            "ltr": _num(row.get("ltr")),
        })

    item_raw = _dict_rows(
        """
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED') AS item,
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER') AS item_head,
            COALESCE(SUM("qty"), 0) AS qty,
            COALESCE(SUM("ltr_sold"), 0) AS liters,
            COALESCE(SUM("sale_amt_exclusive"), 0) AS landing_amt
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED'),
            COALESCE(NULLIF(UPPER(TRIM("item_head"::text)), ''), 'OTHER')
        """,
        [month, year],
    )

    elapsed_days = max_date.day if max_date else 0
    days_in_month = monthrange(year, month)[1]
    order = {item: idx for idx, item in enumerate(_FK_GROCERY_DRR_ITEM_ORDER)}
    items = []
    for row in sorted(
        item_raw,
        key=lambda r: (order.get(str(r.get("item") or "").upper(), 999), str(r.get("item") or "")),
    ):
        qty = _num(row.get("qty"))
        liters = _num(row.get("liters"))
        landing_amt = _num(row.get("landing_amt"))
        drr_qty = _safe_div(qty, elapsed_days)
        drr_liters = _safe_div(liters, elapsed_days)
        drr_value = _safe_div(landing_amt, elapsed_days)
        items.append({
            "item": row.get("item"),
            "item_head": row.get("item_head"),
            "qty": qty,
            "liters": liters,
            "landing_amt": landing_amt,
            "drr_qty": drr_qty,
            "drr_liters": drr_liters,
            "drr_value": drr_value,
            "estimated_liters": drr_liters * days_in_month,
        })

    total_qty = sum(_num(r.get("qty")) for r in items)
    total_liters = sum(_num(r.get("liters")) for r in items)
    total_landing_amt = sum(_num(r.get("landing_amt")) for r in items)
    total_drr_qty = _safe_div(total_qty, elapsed_days)
    total_drr_liters = _safe_div(total_liters, elapsed_days)
    total_drr_value = _safe_div(total_landing_amt, elapsed_days)
    totals = {
        "qty": total_qty,
        "liters": total_liters,
        "landing_amt": total_landing_amt,
        "drr_qty": total_drr_qty,
        "drr_liters": total_drr_liters,
        "drr_value": total_drr_value,
        "estimated_liters": total_drr_liters * days_in_month,
    }

    return Response({
        "source": "flipkart_grocery_master",
        "defaulted_to_latest": defaulted_to_latest,
        "sales_of": sales_of,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "daily": daily,
        "daily_groups": [daily[i:i + 9] for i in range(0, len(daily), 9)],
        "items": items,
        "totals": totals,
    })


@api_view(["GET"])
@permission_classes([require("platform.secondary.view")])
def flipkart_grocery_month_on_month_sale(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug != "flipkart_grocery":
        raise ValidationError("Month On Month Sale is available only for Flipkart Grocery.")

    month, year, defaulted_to_latest = _parse_sec_month_year(request.query_params)
    max_date = _scalar(
        """
        SELECT MAX("real_date")
        FROM "flipkart_grocery_master"
        WHERE "month" = %s
          AND "year" = %s
        """,
        [month, year],
    )

    comparison_months = []
    for index, offset in enumerate([0, -1, -2, -3, -4]):
        compare_month, compare_year = _shift_month(month, year, offset)
        comparison_months.append({
            "key": "current" if index == 0 else f"previous_{index}",
            "month": compare_month,
            "year": compare_year,
            "label": _month_name(compare_month),
        })

    params: list = []
    clauses = []
    for item in comparison_months:
        clauses.append('("month" = %s AND "year" = %s)')
        params.extend([item["month"], item["year"]])

    item_month_rows = _dict_rows(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED') AS item,
            "month",
            "year",
            COALESCE(SUM("ltr_sold"), 0) AS ltr
        FROM "flipkart_grocery_master"
        WHERE {" OR ".join(clauses)}
        GROUP BY
            COALESCE(NULLIF(TRIM("item"::text), ''), 'UNMAPPED'),
            "month",
            "year"
        """,
        params,
    )
    ltr_by_key = {
        (_norm_sec_key(row.get("item")), int(row.get("month")), int(row.get("year"))): _num(row.get("ltr"))
        for row in item_month_rows
    }

    elapsed_days = max_date.day if max_date else 0
    days_in_month = monthrange(year, month)[1]
    group_map: dict[str, list[dict]] = {}
    for sub_category, item, item_head, target in _FK_GROCERY_MOM_TEMPLATE:
        current_ltr = ltr_by_key.get((_norm_sec_key(item), month, year), 0.0)
        row = {
            "sub_category": sub_category,
            "item": item,
            "item_head": item_head,
            "target": float(target),
            "current_done_ltr": current_ltr,
            "estimated_ltr": _safe_div(current_ltr, elapsed_days) * days_in_month,
            "previous_1_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[1]["month"], comparison_months[1]["year"]),
                0.0,
            ),
            "previous_2_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[2]["month"], comparison_months[2]["year"]),
                0.0,
            ),
            "previous_3_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[3]["month"], comparison_months[3]["year"]),
                0.0,
            ),
            "previous_4_ltr": ltr_by_key.get(
                (_norm_sec_key(item), comparison_months[4]["month"], comparison_months[4]["year"]),
                0.0,
            ),
        }
        group_map.setdefault(sub_category, []).append(row)

    groups = []
    for sub_category, rows in group_map.items():
        groups.append({
            "sub_category": sub_category,
            "rows": rows,
            "total": _sum_mom_rows(rows),
        })

    group_totals = [group["total"] for group in groups]
    target_summary = [
        {"item_head": item_head, "target": float(target)}
        for item_head, target in _FK_GROCERY_MOM_TARGETS.items()
    ]
    target_summary.append({
        "item_head": "TOTAL",
        "target": float(sum(_FK_GROCERY_MOM_TARGETS.values())),
    })

    return Response({
        "source": "flipkart_grocery_master",
        "defaulted_to_latest": defaulted_to_latest,
        "month": month,
        "year": year,
        "max_date": max_date.isoformat() if max_date else None,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "target_summary": target_summary,
        "comparison_months": comparison_months,
        "groups": groups,
        "grand_total": _sum_mom_rows(group_totals),
    })


# ─── /{slug}/landing-rate  (GET) ───
@api_view(["GET"])
@permission_classes([require("platform.landing_rate.view")])
def landing_rate_list(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)

    mode = (request.query_params.get("mode") or "effective").lower()
    month = _parse_month(request.query_params.get("month") or "") or date.today().replace(day=1).isoformat()
    search = (request.query_params.get("search") or "").strip()
    page, page_size = _page(request)
    offset = page * page_size

    # Match stored format aliases, so old `bigbasket` rows and canonical
    # `big basket` rows are shown together.
    format_clause, format_params = _format_match_clause(p)
    base_where = [format_clause]
    base_params: list = format_params
    if search:
        base_where.append('("sku_code"::text ILIKE %s OR "sku_name" ILIKE %s)')
        s = f"%{search}%"
        base_params.extend([s, s])

    try:
        if mode == "history":
            where_sql = " WHERE " + " AND ".join(base_where)
            total = _scalar(
                f'SELECT COUNT(*) FROM "monthly_landing_rate"{where_sql}',
                base_params,
            ) or 0
            rows = _dict_rows(
                f'SELECT * FROM "monthly_landing_rate"{where_sql} '
                f'ORDER BY "month" DESC, "sku_code" ASC LIMIT %s OFFSET %s',
                base_params + [page_size, offset],
            )
        else:
            # Effective view: show the latest row per SKU inside the selected
            # calendar month only. This keeps May 2026 from showing April 2026
            # or May rows from any other year.
            where = base_where + [
                '"month"::date >= %s::date',
                '"month"::date < (%s::date + INTERVAL \'1 month\')',
            ]
            params = base_params + [month, month]
            where_sql = " WHERE " + " AND ".join(where)
            sub = (
                f'SELECT DISTINCT ON ("sku_code") * FROM "monthly_landing_rate"'
                f'{where_sql} ORDER BY "sku_code", "month" DESC, "created_at" DESC'
            )
            total = _scalar(f"SELECT COUNT(*) FROM ({sub}) t", params) or 0
            rows = _dict_rows(
                f'SELECT * FROM ({sub}) t ORDER BY "sku_code" ASC LIMIT %s OFFSET %s',
                params + [page_size, offset],
            )
    except Exception as e:
        return Response({"data": [], "count": 0, "error": str(e)})

    return Response({
        "data": rows,
        "count": int(total),
        "page": page,
        "page_size": page_size,
        "format": fmt,
        "month": month,
        "mode": mode,
    })


# ─── /{slug}/landing-rate/skus  (GET) ───
# Returns distinct (sku_code, sku_name) pairs already in the table for this
# platform, for autocomplete. Frontend lets the user add new SKUs too.
@api_view(["GET"])
@permission_classes([require("platform.landing_rate.view")])
def landing_rate_skus(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)
    format_clause, format_params = _format_match_clause(p)
    try:
        rows = _dict_rows(
            'SELECT DISTINCT ON ("sku_code") "sku_code", "sku_name" '
            'FROM "monthly_landing_rate" '
            f"WHERE {format_clause} "
            'ORDER BY "sku_code", "month" DESC',
            format_params,
        )
    except Exception:
        rows = []
    return Response({"skus": rows, "format": fmt})


# ─── /{slug}/landing-rate/add  (POST) ───
@api_view(["POST"])
@permission_classes([require("platform.landing_rate.edit")])
def landing_rate_add(request, slug: str):
    _ensure_scope(request.user, slug)
    if slug not in _LANDING_PLATFORMS:
        raise ValidationError(f"Monthly landing rate is only available for {_LANDING_PLATFORM_LABELS}.")
    p = _get_platform(slug)
    fmt = _format_for(p)

    body = request.data or {}
    sku_code = str(body.get("sku_code") or "").strip()
    sku_name = str(body.get("sku_name") or "").strip()
    month = _parse_month(str(body.get("month") or ""))
    if not sku_code or not sku_name or not month:
        raise ValidationError("sku_code, sku_name and month are required.")

    try:
        landing_rate = float(body.get("landing_rate"))
        basic_rate = float(body.get("basic_rate"))
    except (TypeError, ValueError):
        raise ValidationError("landing_rate and basic_rate must be numeric.")

    try:
        with connection.cursor() as cur:
            cur.execute(
                'INSERT INTO "monthly_landing_rate" '
                '("sku_code","sku_name","landing_rate","basic_rate","format","month") '
                'VALUES (%s,%s,%s,%s,%s,%s) '
                'RETURNING "created_at"',
                [sku_code, sku_name, landing_rate, basic_rate, fmt, month],
            )
            created_at = cur.fetchone()[0]
    except Exception as e:
        return Response({"ok": False, "error": str(e)}, status=400)

    return Response({
        "ok": True,
        "row": {
            "sku_code": sku_code,
            "sku_name": sku_name,
            "landing_rate": landing_rate,
            "basic_rate": basic_rate,
            "format": fmt,
            "month": month,
            "created_at": created_at.isoformat() if created_at else None,
        },
    })
