import calendar
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from django.db import connection, transaction
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require
from platforms.primary_po_columns import (
    PRIMARY_MASTER_PO_TABLES,
    order_primary_master_po_columns,
    order_primary_master_po_row,
    primary_master_po_labels,
)

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Tables the dashboard can query. Mirrors FastAPI ALLOWED_TABLES.
ALLOWED_TABLES = {
    "master_po", "prim_master_po", "test_master_po",
    "total_po", "total_po_zbs",
    "amazon_price_data", "amazon_sec_daily", "amazon_sec_daily_master_view", "amazon_sec_range",
    "amazon_sec_range_margins", "amazon_sec_range_master_view",
    "bigbasketSec", "blinkitSec", "flipkart_grocery_master", "fk_grocery", "flipkartSec", "flipkart_secondary_all",
    "jiomartSec", "swiggySec", "zeptoSec",
    "zomatoSec", "citymallSec",
    "amazon_inventory", "bigbasket_inventory",
    "blinkit_inventory", "jiomart_inventory", "swiggy_inventory", "zepto_inventory",
    "zomato_inventory", "citymall_inventory",
    "all_platform_inventory",
    # Ads — destination tables for the unified Upload Hub Ads flow.
    "blinkit_ads", "amazon_ads", "swiggy_ads", "zepto_ads", "bigbasket_ads", "flipkart_ads",
    # Brand Fund
    "zepto_brandfund", "swiggy_brandfund", "blinkit_brandfund",
    # Coupons (Amazon)
    "amazon_coupon",
}

# Mirrors FastAPI INVENTORY_CONFIG for /inventory-charts aggregation.
INVENTORY_CONFIG = {
    "blinkit":   {"table": "blinkit_inventory",   "qty_col": "total_inv_qty",           "name_col": "item_name",       "city_col": None,     "color": "#f5c518"},
    "zepto":     {"table": "zepto_inventory",     "qty_col": "units",                   "name_col": "sku_name",        "city_col": "city",   "color": "#7b2ff7"},
    "swiggy":    {"table": "swiggy_inventory",    "qty_col": "warehouse_qty_available", "name_col": "sku_description", "city_col": "city",   "color": "#fc8019"},
    "bigbasket": {"table": "bigbasket_inventory", "qty_col": "soh",                     "name_col": "sku_name",        "city_col": "city",   "color": "#84c225"},
    "jiomart":   {"table": "jiomart_inventory",   "qty_col": "total_sellable_inv",      "name_col": "title",           "city_col": None,     "color": "#0078ad"},
    "amazon":    {"table": "amazon_inventory",    "qty_col": "sellable_on_hand_units",  "name_col": "product_title",   "id_col": "asin",     "city_col": None, "color": "#ff9900"},
}


def _quoted(table: str) -> str:
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table {table!r} not allowed")
    return f'"{table}"'


def _table_exists(table: str) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = %s LIMIT 1",
            [table],
        )
        return cur.fetchone() is not None


def _count(table: str) -> int:
    try:
        with connection.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {_quoted(table)}")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception:
        return 0


def _sample_row(table: str) -> dict | None:
    try:
        with connection.cursor() as cur:
            cur.execute(f"SELECT * FROM {_quoted(table)} LIMIT 1")
            if cur.description is None:
                return None
            cols = [c[0] for c in cur.description]
            row = cur.fetchone()
            return dict(zip(cols, row)) if row else None
    except Exception:
        return None


def _is_code(name) -> bool:
    if not name or not isinstance(name, str):
        return True
    name = name.strip()
    if len(name) <= 12 and name.isalnum():
        return True
    return False


def _date_expr(col: str) -> str:
    qc = f'"{col}"'
    text_value = f"btrim({qc}::text)"
    return (
        "CASE "
        f"WHEN {qc} IS NULL THEN NULL "
        f"WHEN {text_value} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' "
        f"THEN left({text_value}, 10)::date "
        f"WHEN {text_value} ~ '^\\d{{2}}-\\d{{2}}-\\d{{4}}' "
        f"THEN to_date(left({text_value}, 10), 'DD-MM-YYYY') "
        f"WHEN {text_value} ~ '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}}' "
        f"THEN to_date(split_part({text_value}, ' ', 1), 'DD/MM/YYYY') "
        "ELSE NULL END"
    )


# ─── /table-count/{table} ───
@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def table_count(request, table_name: str):
    if table_name not in ALLOWED_TABLES:
        return Response({"error": "Table not allowed", "count": 0})
    return Response({"table": table_name, "count": _count(table_name)})


# ─── /table-counts ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def table_counts(request):
    requested = request.query_params.get("tables", "")
    if requested:
        tables = [
            table.strip()
            for table in requested.split(",")
            if table.strip() in ALLOWED_TABLES
        ]
    else:
        tables = sorted(ALLOWED_TABLES)
    return Response({t: _count(t) for t in tables})


# ─── /table-columns/{table} ───
@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def table_columns(request, table_name: str):
    if table_name not in ALLOWED_TABLES:
        return Response({"error": "Table not allowed", "columns": [], "sample": None})
    sample = _sample_row(table_name)
    if not sample:
        return Response({"columns": [], "sample": None})
    columns = list(sample.keys())
    if table_name in PRIMARY_MASTER_PO_TABLES:
        columns = order_primary_master_po_columns(columns)
        sample = order_primary_master_po_row(sample)
        return Response({
            "columns": columns,
            "column_labels": primary_master_po_labels(columns),
            "sample": sample,
        })
    return Response({"columns": columns, "sample": sample})


@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def table_distinct_values(request, table_name: str, column_name: str):
    if table_name not in ALLOWED_TABLES or not _IDENT.match(column_name):
        return Response({"error": "Table or column not allowed", "values": []})

    sample = _sample_row(table_name)
    if not sample or column_name not in sample:
        return Response({"error": "Column not found", "values": []})

    qt = _quoted(table_name)
    qc = f'"{column_name}"'
    column_filters_raw = request.query_params.get("column_filters", "")
    where: list[str] = []
    params: list = []
    if column_filters_raw:
        try:
            parsed_filters = json.loads(column_filters_raw)
        except (TypeError, ValueError):
            parsed_filters = []
        for item in parsed_filters if isinstance(parsed_filters, list) else []:
            col = item.get("column") if isinstance(item, dict) else ""
            values = item.get("values") if isinstance(item, dict) else []
            if not col or not _IDENT.match(col) or col not in sample:
                continue
            if not isinstance(values, list):
                continue
            cleaned_values = ["" if v is None else str(v) for v in values[:500]]
            if not cleaned_values:
                where.append("1 = 0")
                continue
            placeholders = ", ".join(["%s"] * len(cleaned_values))
            where.append(f"COALESCE(\"{col}\"::text, '') IN ({placeholders})")
            params.extend(cleaned_values)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    try:
        with connection.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT COALESCE({qc}::text, '') AS value
                FROM {qt}
                {where_sql}
                ORDER BY value ASC
                LIMIT 5000
                """,
                params,
            )
            values = [row[0] for row in cur.fetchall()]
    except Exception:
        return Response({"error": "Query failed", "values": []})

    return Response({"table": table_name, "column": column_name, "values": values})


# ─── /expiry-alerts/{table} ───
DATE_COL_PATTERNS = re.compile(
    r"expir|delivery_date|deliver_by|due_date|valid_until|best_before|shelf_life|end_date|dispatch_date",
    re.I,
)
ALERT_DAYS = 7


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def expiry_alerts(request, table_name: str):
    if table_name not in ALLOWED_TABLES:
        return Response({"alerts": []})
    sample = _sample_row(table_name)
    if not sample:
        return Response({"alerts": []})

    date_cols = []
    for col, val in sample.items():
        if not DATE_COL_PATTERNS.search(col):
            continue
        if isinstance(val, (date, datetime)):
            date_cols.append(col)
        elif isinstance(val, str) and (
            re.match(r"^\d{4}-\d{2}-\d{2}", val)
            or re.match(r"^\d{1,2}-\d{1,2}-\d{4}", val)
            or re.match(r"^\d{1,2}/\d{1,2}/\d{4}", val)
        ):
            date_cols.append(col)
    if not date_cols:
        return Response({"alerts": []})

    today = date.today()
    soon = today + timedelta(days=ALERT_DAYS)
    alerts = []
    qt = _quoted(table_name)
    date_exprs = [_date_expr(col) for col in date_cols]
    expired_condition = " OR ".join(f"{expr} < %s::date" for expr in date_exprs)
    expiring_condition = " OR ".join(
        f"({expr} >= %s::date AND {expr} <= %s::date)" for expr in date_exprs
    )
    expired_params = [today] * len(date_cols)
    expiring_params = expired_params + [v for _ in date_cols for v in (today, soon)]
    columns_label = ", ".join(date_cols)

    with connection.cursor() as cur:
        try:
            cur.execute(
                f"SELECT COUNT(*) FROM {qt} WHERE {expired_condition}",
                expired_params,
            )
            expired_count = int(cur.fetchone()[0] or 0)
            if expired_count:
                cur.execute(
                    f"SELECT * FROM {qt} WHERE {expired_condition} "
                    f"ORDER BY {date_exprs[0]} DESC NULLS LAST LIMIT 5",
                    expired_params,
                )
                cols = [c[0] for c in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                alerts.append({
                    "table": table_name,
                    "column": columns_label,
                    "columns": date_cols,
                    "type": "expired",
                    "count": expired_count,
                    "rows": rows,
                })

            cur.execute(
                f"SELECT COUNT(*) FROM {qt} "
                f"WHERE NOT ({expired_condition}) AND ({expiring_condition})",
                expiring_params,
            )
            soon_count = int(cur.fetchone()[0] or 0)
            if soon_count:
                cur.execute(
                    f"SELECT * FROM {qt} "
                    f"WHERE NOT ({expired_condition}) AND ({expiring_condition}) "
                    f"ORDER BY {date_exprs[0]} ASC NULLS LAST LIMIT 5",
                    expiring_params,
                )
                cols = [c[0] for c in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                alerts.append({
                    "table": table_name,
                    "column": columns_label,
                    "columns": date_cols,
                    "type": "expiring",
                    "count": soon_count,
                    "rows": rows,
                })
        except Exception:
            return Response({"alerts": []})

    return Response({"alerts": alerts})


# ─── /inventory-charts ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def inventory_charts(request):
    platform_totals = []
    city_totals: dict[str, int] = defaultdict(int)
    top_products = []

    for platform, cfg in INVENTORY_CONFIG.items():
        table = cfg["table"]
        qty_col = cfg["qty_col"]
        name_col = cfg["name_col"]
        id_col = cfg.get("id_col")
        city_col = cfg.get("city_col")
        color = cfg["color"]

        total_qty = 0
        sku_count = 0
        rows: list[dict] = []

        try:
            select_cols = [qty_col, name_col]
            if id_col:
                select_cols.append(id_col)
            if city_col:
                select_cols.append(city_col)
            cols_sql = ", ".join(f'"{c}"' for c in select_cols)
            qt = _quoted(table)
            with connection.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {qt}")
                sku_count = int(cur.fetchone()[0] or 0)
                cur.execute(f"SELECT {cols_sql} FROM {qt} LIMIT 5000")
                rows = [dict(zip(select_cols, r)) for r in cur.fetchall()]
        except Exception:
            platform_totals.append({
                "platform": platform, "total_qty": 0, "sku_count": 0, "color": color,
            })
            continue

        name_lookup: dict = {}
        if id_col:
            for r in rows:
                rid = r.get(id_col)
                rname = r.get(name_col)
                if rid and rname and not _is_code(rname):
                    name_lookup[rid] = rname

        for r in rows:
            q = r.get(qty_col)
            try:
                total_qty += int(q or 0)
            except (TypeError, ValueError):
                pass

        platform_totals.append({
            "platform": platform,
            "total_qty": total_qty,
            "sku_count": sku_count,
            "color": color,
        })

        if city_col:
            for r in rows:
                city = r.get(city_col)
                try:
                    qty = int(r.get(qty_col) or 0)
                except (TypeError, ValueError):
                    qty = 0
                if city and qty > 0:
                    city_totals[str(city).upper().strip()] += qty

        product_map: dict[str, int] = defaultdict(int)
        for r in rows:
            name = r.get(name_col)
            try:
                qty = int(r.get(qty_col) or 0)
            except (TypeError, ValueError):
                qty = 0
            if not name or qty <= 0:
                continue
            if _is_code(name) and id_col:
                rid = r.get(id_col) or name
                name = name_lookup.get(rid, name)
            product_map[str(name)] += qty

        for name, qty in sorted(product_map.items(), key=lambda x: -x[1])[:5]:
            top_products.append({
                "product": name[:80],
                "qty": qty,
                "platform": platform,
                "color": color,
            })

    platform_totals.sort(key=lambda x: -x["total_qty"])
    city_list = sorted(
        [{"city": c, "qty": q} for c, q in city_totals.items()],
        key=lambda x: -x["qty"],
    )
    top_products.sort(key=lambda x: -x["qty"])

    return Response({
        "platform_totals": platform_totals,
        "city_distribution": city_list[:15],
        "top_products": top_products[:15],
    })


# ─── /primary-po-litres ───
@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def primary_po_litres(request):
    """SUM(total_delivered_liters) for the current month from master_po + reporting.Amazon PO."""
    today = date.today()
    month_name = calendar.month_name[today.month].upper()  # e.g. 'MAY'
    year = today.year

    results = []
    errors = []
    with connection.cursor() as cur:
        try:
            cur.execute("""
                SELECT UPPER(TRIM(format::text)) AS format,
                       COALESCE(SUM(total_delivered_liters), 0) AS delivered_ltrs
                FROM public.master_po
                WHERE UPPER(TRIM(delivery_month::text)) = %s
                  AND delivered_year = %s
                GROUP BY 1
                ORDER BY delivered_ltrs DESC
            """, [month_name, year])
            for row in cur.fetchall():
                results.append({"format": row[0], "delivered_ltrs": float(row[1] or 0)})
        except Exception as e:
            errors.append({"source": "master_po", "error": str(e)})
        try:
            cur.execute("""
                SELECT COALESCE(SUM(total_delivered_liters), 0)
                FROM reporting."Amazon PO"
                WHERE po_month = %s
                  AND year = %s
            """, [today.month, year])
            row = cur.fetchone()
            if row:
                results.append({"format": "AMAZON", "delivered_ltrs": float(row[0] or 0)})
        except Exception as e:
            errors.append({"source": "amazon_po", "error": str(e)})
    return Response({"platforms": results, "errors": errors, "month": month_name, "year": year})


# ─── /category-litres ───
# Slug → master_po `format` value (Amazon is handled via reporting."Amazon PO").
_CATEGORY_SLUG_TO_FORMAT = {
    'blinkit': 'BLINKIT',
    'zepto': 'ZEPTO',
    'swiggy': 'SWIGGY',
    'bigbasket': 'BIG BASKET',
    'flipkart_grocery': 'FLIPKART GROCERY',
    'zomato': 'ZOMATO',
    'citymall': 'CITY MALL',
}


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def category_litres(request):
    """Delivered litres grouped by oil category for one item head.

    Amazon pulls from reporting."Amazon PO"; every other platform pulls from
    master_po. With no platform filter both sources are merged (Amazon rows are
    excluded from master_po so they aren't double counted)."""
    today = date.today()
    try:
        month_num = int(request.GET.get("month") or today.month)
    except (TypeError, ValueError):
        month_num = today.month
    try:
        year = int(request.GET.get("year") or today.year)
    except (TypeError, ValueError):
        year = today.year
    if not 1 <= month_num <= 12:
        month_num = today.month
    month_name = calendar.month_name[month_num].upper()  # e.g. 'MAY'

    head = (request.GET.get("head") or "premium").strip().lower()
    head_sql = "COMMODITY" if head == "commodity" else "PREMIUM"
    head = "commodity" if head == "commodity" else "premium"

    platform = (request.GET.get("platform") or "").strip().lower() or None

    use_master = platform != "amazon"
    use_amazon = platform is None or platform == "amazon"

    totals = {}
    errors = []

    def add(category, ltrs):
        label = (str(category).strip() if category else "") or "Uncategorized"
        totals[label] = totals.get(label, 0.0) + float(ltrs or 0)

    with connection.cursor() as cur:
        if use_master:
            sql = """
                SELECT COALESCE(NULLIF(TRIM(category::text), ''), 'Uncategorized') AS category,
                       COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                FROM public.master_po
                WHERE UPPER(TRIM(delivery_month::text)) = %s
                  AND delivered_year = %s
                  AND UPPER(TRIM(item_head::text)) = %s
            """
            params = [month_name, year, head_sql]
            if platform:
                fmt = _CATEGORY_SLUG_TO_FORMAT.get(
                    platform, platform.replace("_", " ").upper()
                )
                sql += " AND UPPER(TRIM(format::text)) = %s"
                params.append(fmt)
            else:
                sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
            sql += " GROUP BY 1"
            try:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    add(row[0], row[1])
            except Exception as e:
                errors.append({"source": "master_po", "error": str(e)})

        if use_amazon:
            try:
                cur.execute("""
                    SELECT COALESCE(NULLIF(TRIM(category::text), ''), 'Uncategorized') AS category,
                           COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                    FROM reporting."Amazon PO"
                    WHERE po_month = %s
                      AND year = %s
                      AND UPPER(TRIM(item_head::text)) = %s
                    GROUP BY 1
                """, [month_num, year, head_sql])
                for row in cur.fetchall():
                    add(row[0], row[1])
            except Exception as e:
                errors.append({"source": "amazon_po", "error": str(e)})

    categories = sorted(
        ({"category": name, "ltrs": round(ltrs, 2)} for name, ltrs in totals.items() if ltrs > 0),
        key=lambda c: c["ltrs"],
        reverse=True,
    )
    total_ltrs = round(sum(c["ltrs"] for c in categories), 2)

    return Response({
        "head": head,
        "platform": platform,
        "month": month_num,
        "year": year,
        "total_ltrs": total_ltrs,
        "categories": categories,
        "errors": errors,
    })


# ─── /category-breakdown ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def category_breakdown(request):
    """Litres by category AND sub_category, for BOTH heads, from one source.

    source=primary  → master_po (non-AMZ) + reporting."Amazon PO".
    source=secondary → "SecMaster" (non-AMZ) + amazon_sec_range_master_view (AMZ,
                       latest cumulative month_day snapshot only).
    Powers the home "Category Split" 2x2 grid in a single call."""
    today = date.today()
    try:
        month_num = int(request.GET.get("month") or today.month)
    except (TypeError, ValueError):
        month_num = today.month
    try:
        year = int(request.GET.get("year") or today.year)
    except (TypeError, ValueError):
        year = today.year
    if not 1 <= month_num <= 12:
        month_num = today.month
    month_name = calendar.month_name[month_num].upper()  # e.g. 'MAY'

    source = "secondary" if (request.GET.get("source") or "").strip().lower() == "secondary" else "primary"
    platform = (request.GET.get("platform") or "").strip().lower() or None

    use_amazon = platform is None or platform == "amazon"
    use_other = platform != "amazon"
    fmt = None
    if platform and platform != "amazon":
        fmt = _CATEGORY_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper())

    errors = []
    # cat[HEAD][name] and sub[HEAD][name] accumulate litres. One DB scan per
    # source table (grouped by head + category + sub_category) feeds both — far
    # cheaper than a query per head×dimension.
    cat = {"PREMIUM": {}, "COMMODITY": {}}
    sub = {"PREMIUM": {}, "COMMODITY": {}}

    def absorb(rows):
        for head_val, c, s, ltrs in rows:
            head = (str(head_val).strip().upper() if head_val else "")
            if head not in cat:
                continue
            val = float(ltrs or 0)
            if val == 0:
                continue
            clabel = (str(c).strip() if c else "") or "Uncategorized"
            slabel = (str(s).strip() if s else "") or "Uncategorized"
            cat[head][clabel] = cat[head].get(clabel, 0.0) + val
            sub[head][slabel] = sub[head].get(slabel, 0.0) + val

    def run(label, sql, params):
        try:
            cur.execute(sql, params)
            absorb(cur.fetchall())
        except Exception as e:  # noqa: BLE001
            errors.append({"source": label, "error": str(e)})

    with connection.cursor() as cur:
        if source == "primary":
            if use_other:
                sql = """
                    SELECT UPPER(TRIM(item_head::text)) AS head, category, sub_category,
                           COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                    FROM public.master_po
                    WHERE UPPER(TRIM(delivery_month::text)) = %s
                      AND delivered_year = %s
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                """
                params = [month_name, year]
                if fmt:
                    sql += " AND UPPER(TRIM(format::text)) = %s"
                    params.append(fmt)
                else:
                    sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                sql += " GROUP BY 1, 2, 3"
                run("master_po", sql, params)
            if use_amazon:
                run("amazon_po", """
                    SELECT UPPER(TRIM(item_head::text)) AS head, category, sub_category,
                           COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                    FROM reporting."Amazon PO"
                    WHERE po_month = %s AND year = %s
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    GROUP BY 1, 2, 3
                """, [month_num, year])
        else:  # secondary
            if use_other:
                sql = """
                    SELECT UPPER(TRIM(item_head::text)) AS head, category, sub_category,
                           COALESCE(SUM(ltr_sold), 0) AS ltrs
                    FROM "SecMaster"
                    WHERE UPPER(TRIM(month::text)) = %s
                      AND year::numeric = %s
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                """
                params = [month_name, year]
                if fmt:
                    sql += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                    params.append(fmt)
                else:
                    sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                sql += " GROUP BY 1, 2, 3"
                run("secmaster", sql, params)
            if use_amazon:
                # Mirror amazon_sec_range_master_view against the raw table for
                # speed (the view is ~140s): join master_sheet for category/head,
                # shipped_litres = shipped_units * per_unit_value, and use only the
                # latest cumulative snapshot (max to_date day within the month).
                run("amazon_sec_range", """
                    WITH ml AS (
                        SELECT DISTINCT ON (format_sku_code)
                               format_sku_code, category, sub_category, item_head, per_unit_value
                        FROM master_sheet
                        WHERE format_sku_code IS NOT NULL AND format_sku_code::text <> ''
                        ORDER BY format_sku_code
                    ),
                    base AS (
                        SELECT r.asin,
                               COALESCE(r.shipped_units, 0) AS units,
                               EXTRACT(DAY FROM r.to_date)::int AS to_day
                        FROM amazon_sec_range r
                        WHERE EXTRACT(YEAR FROM r.from_date) = %s
                          AND UPPER(to_char(r.from_date, 'FMMonth')) = %s
                    ),
                    latest AS (SELECT MAX(to_day) AS md FROM base)
                    SELECT UPPER(TRIM(ml.item_head::text)) AS head, ml.category, ml.sub_category,
                           COALESCE(SUM(b.units * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs
                    FROM base b
                    CROSS JOIN latest l
                    JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(b.asin::text))
                    WHERE b.to_day = l.md
                      AND UPPER(TRIM(ml.item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    GROUP BY 1, 2, 3
                """, [year, month_name])

    def to_list(d):
        return sorted(
            ({"name": n, "ltrs": round(v, 2)} for n, v in d.items() if v > 0),
            key=lambda c: c["ltrs"],
            reverse=True,
        )

    out = {"source": source, "platform": platform, "month": month_num, "year": year}
    for head_key, head_sql in (("premium", "PREMIUM"), ("commodity", "COMMODITY")):
        cats = to_list(cat[head_sql])
        out[head_key] = {
            "categories": cats,
            "sub_categories": to_list(sub[head_sql]),
            "total_ltrs": round(sum(c["ltrs"] for c in cats), 2),
        }
    out["errors"] = errors
    return Response(out)


# Month name (UPPER) → month number, for normalising the text month columns
# (master_po.delivery_month, SecMaster.month) back to integers.
_MONTH_NUM = {calendar.month_name[i].upper(): i for i in range(1, 13)}


def _trailing_months(end_month, end_year, n):
    """(month_num, year, MONTH_NAME) for the n months ending at end (oldest→newest)."""
    out = []
    m, y = end_month, end_year
    for _ in range(n):
        out.append((m, y, calendar.month_name[m].upper()))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    out.reverse()
    return out


def _month_token_to_num(tok):
    """Accept a numeric month (int/Decimal/'7'/'7.0') or an (UPPER) month name
    and return its month number, or None."""
    if tok is None:
        return None
    s = str(tok).strip().upper()
    if not s:
        return None
    try:
        return int(float(s))  # '7', '7.0', Decimal('7') → 7
    except ValueError:
        return _MONTH_NUM.get(s)  # 'MAY' → 5


# ─── /category-trend ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def category_trend(request):
    """Premium / Commodity delivered litres over the trailing N months.

    Same source semantics as /category-breakdown (primary = master_po + Amazon
    PO; secondary = SecMaster + amazon_sec_range), but aggregated to a single
    {premium, commodity} pair per month so the home "Category Trend" line chart
    can plot the product mix over time. Honours the platform filter; month/year
    is the END of the window."""
    today = date.today()
    try:
        end_month = int(request.GET.get("month") or today.month)
    except (TypeError, ValueError):
        end_month = today.month
    try:
        end_year = int(request.GET.get("year") or today.year)
    except (TypeError, ValueError):
        end_year = today.year
    if not 1 <= end_month <= 12:
        end_month = today.month
    try:
        n_months = int(request.GET.get("months") or 6)
    except (TypeError, ValueError):
        n_months = 6
    n_months = max(1, min(n_months, 24))

    source = "secondary" if (request.GET.get("source") or "").strip().lower() == "secondary" else "primary"
    platform = (request.GET.get("platform") or "").strip().lower() or None
    use_amazon = platform is None or platform == "amazon"
    use_other = platform != "amazon"
    fmt = None
    if platform and platform != "amazon":
        fmt = _CATEGORY_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper())

    window = _trailing_months(end_month, end_year, n_months)
    # bucket[(year, month_num)] = {"PREMIUM": x, "COMMODITY": y}
    bucket = {(y, m): {"PREMIUM": 0.0, "COMMODITY": 0.0} for (m, y, _) in window}
    errors = []

    def absorb(rows):
        # rows: (year, month_token, head, ltrs)
        for yr, mon_tok, head_val, ltrs in rows:
            mnum = _month_token_to_num(mon_tok)
            key = (int(yr), mnum) if mnum else None
            if key is None or key not in bucket:
                continue
            head = (str(head_val).strip().upper() if head_val else "")
            if head not in bucket[key]:
                continue
            bucket[key][head] += float(ltrs or 0)

    def run(label, sql, params):
        try:
            cur.execute(sql, params)
            absorb(cur.fetchall())
        except Exception as e:  # noqa: BLE001
            errors.append({"source": label, "error": str(e)})

    name_year_pairs = [(mon, y) for (_, y, mon) in window]  # (MONTH_NAME, year)
    num_year_pairs = [(m, y) for (m, y, _) in window]       # (month_num, year)

    with connection.cursor() as cur:
        if source == "primary":
            if use_other:
                ph = ", ".join(["(%s, %s)"] * len(name_year_pairs))
                sql = f"""
                    SELECT delivered_year AS yr,
                           UPPER(TRIM(delivery_month::text)) AS mon,
                           UPPER(TRIM(item_head::text)) AS head,
                           COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                    FROM public.master_po
                    WHERE (UPPER(TRIM(delivery_month::text)), delivered_year) IN ({ph})
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                """
                params = [v for pair in name_year_pairs for v in pair]
                if fmt:
                    sql += " AND UPPER(TRIM(format::text)) = %s"
                    params.append(fmt)
                else:
                    sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                sql += " GROUP BY 1, 2, 3"
                run("master_po", sql, params)
            if use_amazon:
                ph = ", ".join(["(%s, %s)"] * len(num_year_pairs))
                run("amazon_po", f"""
                    SELECT year AS yr, po_month AS mon,
                           UPPER(TRIM(item_head::text)) AS head,
                           COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                    FROM reporting."Amazon PO"
                    WHERE (po_month, year) IN ({ph})
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    GROUP BY 1, 2, 3
                """, [v for pair in num_year_pairs for v in pair])
        else:  # secondary
            if use_other:
                ph = ", ".join(["(%s, %s)"] * len(name_year_pairs))
                sql = f"""
                    SELECT year::int AS yr, UPPER(TRIM(month::text)) AS mon,
                           UPPER(TRIM(item_head::text)) AS head,
                           COALESCE(SUM(ltr_sold), 0) AS ltrs
                    FROM "SecMaster"
                    WHERE (UPPER(TRIM(month::text)), year::numeric) IN ({ph})
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                """
                params = [v for pair in name_year_pairs for v in pair]
                if fmt:
                    sql += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                    params.append(fmt)
                else:
                    sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                sql += " GROUP BY 1, 2, 3"
                run("secmaster", sql, params)
            if use_amazon:
                ph = ", ".join(["(%s, %s)"] * len(name_year_pairs))  # (year, MONTH_NAME)
                run("amazon_sec_range", f"""
                    WITH ml AS (
                        SELECT DISTINCT ON (format_sku_code)
                               format_sku_code, item_head, per_unit_value
                        FROM master_sheet
                        WHERE format_sku_code IS NOT NULL AND format_sku_code::text <> ''
                        ORDER BY format_sku_code
                    ),
                    base AS (
                        SELECT r.asin,
                               COALESCE(r.shipped_units, 0) AS units,
                               EXTRACT(YEAR FROM r.from_date)::int AS yr,
                               UPPER(to_char(r.from_date, 'FMMonth')) AS mon,
                               EXTRACT(DAY FROM r.to_date)::int AS to_day
                        FROM amazon_sec_range r
                        WHERE (EXTRACT(YEAR FROM r.from_date)::int,
                               UPPER(to_char(r.from_date, 'FMMonth'))) IN ({ph})
                    ),
                    latest AS (SELECT yr, mon, MAX(to_day) AS md FROM base GROUP BY yr, mon)
                    SELECT b.yr, b.mon, UPPER(TRIM(ml.item_head::text)) AS head,
                           COALESCE(SUM(b.units * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs
                    FROM base b
                    JOIN latest l ON b.yr = l.yr AND b.mon = l.mon AND b.to_day = l.md
                    JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(b.asin::text))
                    WHERE UPPER(TRIM(ml.item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    GROUP BY 1, 2, 3
                """, [v for (m, y, mon) in window for v in (y, mon)])

    series = []
    for (m, y, _) in window:
        b = bucket[(y, m)]
        prem = round(b["PREMIUM"], 2)
        comm = round(b["COMMODITY"], 2)
        series.append({
            "month": m,
            "year": y,
            "label": f"{calendar.month_abbr[m]} '{str(y)[2:]}",  # "May '26"
            "premium_ltrs": prem,
            "commodity_ltrs": comm,
            "total_ltrs": round(prem + comm, 2),
        })
    return Response({
        "source": source, "platform": platform, "months": n_months,
        "series": series, "errors": errors,
    })


# ─── /fulfilment-health ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def fulfilment_health(request):
    """Fill / miss rate for primary POs over a trailing date window.

    master_po (non-AMZ, by po_date) + reporting."Amazon PO" (AMZ, by order_date).
    The window is a `window_days`-day span ending `lag_days` before today, so
    recent in-flight POs (not yet fulfilled) are excluded:
        end   = today - lag_days        (default 7)
        start = end  - window_days      (default 30)

    Rates use the litre columns the business reports on:
        fill_rate = SUM(filled_ltrs) / SUM(order_ltrs_cl) * 100
        miss_rate = SUM(missed_ltrs) / SUM(order_ltrs_cl) * 100
    Honours the platform filter."""
    today = date.today()
    try:
        lag_days = int(request.GET.get("lag_days") or 7)
    except (TypeError, ValueError):
        lag_days = 7
    try:
        window_days = int(request.GET.get("window_days") or 30)
    except (TypeError, ValueError):
        window_days = 30
    lag_days = max(0, min(lag_days, 366))
    window_days = max(1, min(window_days, 366))
    end_date = today - timedelta(days=lag_days)
    start_date = end_date - timedelta(days=window_days)

    platform = (request.GET.get("platform") or "").strip().lower() or None
    use_amazon = platform is None or platform == "amazon"
    use_other = platform != "amazon"
    fmt = None
    if platform and platform != "amazon":
        fmt = _CATEGORY_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper())

    slug_by_format = {v: k for k, v in _CATEGORY_SLUG_TO_FORMAT.items()}
    rows = []
    errors = []
    with connection.cursor() as cur:
        if use_other:
            # The base master_po table has no `order_ltrs_cl` column (only the
            # prim_master_po view computes it). Reconstruct "ORDER LTRS - CL"
            # the same way the view does — total_order_liters, zeroed for
            # cancelled POs. filled_ltrs / missed_ltrs are already materialised.
            sql = """
                SELECT UPPER(TRIM(format::text)) AS fmt,
                       COALESCE(SUM(CASE WHEN UPPER(TRIM(po_status::text)) = 'CANCELLED'
                                         THEN 0 ELSE COALESCE(total_order_liters, 0) END), 0) AS ordered,
                       COALESCE(SUM(filled_ltrs), 0)   AS filled,
                       COALESCE(SUM(missed_ltrs), 0)   AS missed,
                       COUNT(DISTINCT po_number)       AS po_count
                FROM public.master_po
                WHERE public._pm_parse_date(po_date::text) >= %s
                  AND public._pm_parse_date(po_date::text) <= %s
            """
            params = [start_date, end_date]
            if fmt:
                sql += " AND UPPER(TRIM(format::text)) = %s"
                params.append(fmt)
            else:
                sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
            sql += " GROUP BY 1"
            try:
                cur.execute(sql, params)
                for f, o, fi, mi, pc in cur.fetchall():
                    rows.append({
                        "format": f,
                        "slug": slug_by_format.get(f, (f or "").lower().replace(" ", "_")),
                        "ordered_ltrs": float(o or 0),
                        "filled_ltrs": float(fi or 0),
                        "missed_ltrs": float(mi or 0),
                        "po_count": int(pc or 0),
                    })
            except Exception as e:  # noqa: BLE001
                errors.append({"source": "master_po", "error": str(e)})
        if use_amazon:
            try:
                cur.execute("""
                    SELECT COALESCE(SUM(order_ltrs_cl), 0),
                           COALESCE(SUM(filled_ltrs), 0),
                           COALESCE(SUM(missed_ltrs), 0),
                           COUNT(DISTINCT po_number)
                    FROM reporting."Amazon PO"
                    WHERE public._pm_parse_date(order_date::text) >= %s
                      AND public._pm_parse_date(order_date::text) <= %s
                """, [start_date, end_date])
                o, fi, mi, pc = cur.fetchone()
                if (float(o or 0) + float(fi or 0) + float(mi or 0)) > 0:
                    rows.append({
                        "format": "AMAZON", "slug": "amazon",
                        "ordered_ltrs": float(o or 0),
                        "filled_ltrs": float(fi or 0),
                        "missed_ltrs": float(mi or 0),
                        "po_count": int(pc or 0),
                    })
            except Exception as e:  # noqa: BLE001
                errors.append({"source": "amazon_po", "error": str(e)})

    for r in rows:
        ordered = r["ordered_ltrs"]
        r["fill_rate"] = round((r["filled_ltrs"] / ordered * 100) if ordered > 0 else 0, 1)
        r["miss_rate"] = round((r["missed_ltrs"] / ordered * 100) if ordered > 0 else 0, 1)
        r["ordered_ltrs"] = round(ordered, 2)
        r["filled_ltrs"] = round(r["filled_ltrs"], 2)
        r["missed_ltrs"] = round(r["missed_ltrs"], 2)
    # Worst fill rate first (lowest %), best last — so the platforms that need
    # attention surface at the top of the list.
    rows.sort(key=lambda x: x["fill_rate"])

    tot_ord = round(sum(r["ordered_ltrs"] for r in rows), 2)
    tot_fill = round(sum(r["filled_ltrs"] for r in rows), 2)
    tot_miss = round(sum(r["missed_ltrs"] for r in rows), 2)
    total = {
        "ordered_ltrs": tot_ord,
        "filled_ltrs": tot_fill,
        "missed_ltrs": tot_miss,
        "fill_rate": round((tot_fill / tot_ord * 100) if tot_ord > 0 else 0, 1),
        "miss_rate": round((tot_miss / tot_ord * 100) if tot_ord > 0 else 0, 1),
        "po_count": sum(r["po_count"] for r in rows),
    }
    return Response({
        "platform": platform,
        "window": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "window_days": window_days,
            "lag_days": lag_days,
        },
        "total": total, "by_platform": rows, "errors": errors,
    })


# ─── /top-skus ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def top_skus(request):
    """Top SKUs by delivered litres for a month, with prior-month delta.

    Same source semantics as /category-breakdown. Powers the home "Top Movers"
    leaderboard: current-month top-N SKUs (name + item head + litres) plus each
    SKU's previous-month litres so the UI can show % change and risers/fallers.
    Honours the platform filter."""
    today = date.today()
    try:
        month_num = int(request.GET.get("month") or today.month)
    except (TypeError, ValueError):
        month_num = today.month
    try:
        year = int(request.GET.get("year") or today.year)
    except (TypeError, ValueError):
        year = today.year
    if not 1 <= month_num <= 12:
        month_num = today.month
    try:
        limit = int(request.GET.get("limit") or 10)
    except (TypeError, ValueError):
        limit = 10
    limit = max(1, min(limit, 50))

    source = "secondary" if (request.GET.get("source") or "").strip().lower() == "secondary" else "primary"
    platform = (request.GET.get("platform") or "").strip().lower() or None
    use_amazon = platform is None or platform == "amazon"
    use_other = platform != "amazon"
    fmt = None
    if platform and platform != "amazon":
        fmt = _CATEGORY_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper())

    prev_month = month_num - 1 if month_num > 1 else 12
    prev_year = year if month_num > 1 else year - 1

    errors = []
    # acc[(month_num, year)][upper_name] = {"name": display, "head": h, "ltrs": x}
    acc = {(month_num, year): {}, (prev_month, prev_year): {}}

    def absorb(dest, rows):
        # rows: (name, head, ltrs)
        for name_val, head_val, ltrs in rows:
            val = float(ltrs or 0)
            if val == 0:
                continue
            name = (str(name_val).strip() if name_val else "") or "Unknown"
            key = name.upper()
            head = (str(head_val).strip().upper() if head_val else "")
            if head not in ("PREMIUM", "COMMODITY"):
                head = "OTHER"
            slot = dest.get(key)
            if slot is None:
                dest[key] = {"name": name, "head": head, "ltrs": val}
            else:
                slot["ltrs"] += val

    def run(label, dest, sql, params):
        try:
            cur.execute(sql, params)
            absorb(dest, cur.fetchall())
        except Exception as e:  # noqa: BLE001
            errors.append({"source": label, "error": str(e)})

    with connection.cursor() as cur:
        for (m, y) in ((month_num, year), (prev_month, prev_year)):
            dest = acc[(m, y)]
            mname = calendar.month_name[m].upper()
            if source == "primary":
                if use_other:
                    sql = """
                        SELECT COALESCE(NULLIF(TRIM(item::text), ''),
                                        NULLIF(TRIM(sku_name::text), ''), 'Unknown') AS name,
                               UPPER(TRIM(item_head::text)) AS head,
                               COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                        FROM public.master_po
                        WHERE UPPER(TRIM(delivery_month::text)) = %s AND delivered_year = %s
                          AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    """
                    params = [mname, y]
                    if fmt:
                        sql += " AND UPPER(TRIM(format::text)) = %s"
                        params.append(fmt)
                    else:
                        sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                    sql += " GROUP BY 1, 2"
                    run("master_po", dest, sql, params)
                if use_amazon:
                    run("amazon_po", dest, """
                        SELECT COALESCE(NULLIF(TRIM(item::text), ''),
                                        NULLIF(TRIM(sku_name::text), ''), 'Unknown') AS name,
                               UPPER(TRIM(item_head::text)) AS head,
                               COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                        FROM reporting."Amazon PO"
                        WHERE po_month = %s AND year = %s
                          AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                        GROUP BY 1, 2
                    """, [m, y])
            else:  # secondary
                if use_other:
                    sql = """
                        SELECT COALESCE(NULLIF(TRIM(item::text), ''), 'Unknown') AS name,
                               UPPER(TRIM(item_head::text)) AS head,
                               COALESCE(SUM(ltr_sold), 0) AS ltrs
                        FROM "SecMaster"
                        WHERE UPPER(TRIM(month::text)) = %s AND year::numeric = %s
                          AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    """
                    params = [mname, y]
                    if fmt:
                        sql += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                        params.append(fmt)
                    else:
                        sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                    sql += " GROUP BY 1, 2"
                    run("secmaster", dest, sql, params)
                if use_amazon:
                    run("amazon_sec_range", dest, """
                        WITH ml AS (
                            SELECT DISTINCT ON (format_sku_code)
                                   format_sku_code, item_head, per_unit_value,
                                   COALESCE(NULLIF(TRIM(product_name::text), ''),
                                            NULLIF(TRIM(item::text), '')) AS name
                            FROM master_sheet
                            WHERE format_sku_code IS NOT NULL AND format_sku_code::text <> ''
                            ORDER BY format_sku_code
                        ),
                        base AS (
                            SELECT r.asin,
                                   COALESCE(r.shipped_units, 0) AS units,
                                   EXTRACT(DAY FROM r.to_date)::int AS to_day
                            FROM amazon_sec_range r
                            WHERE EXTRACT(YEAR FROM r.from_date) = %s
                              AND UPPER(to_char(r.from_date, 'FMMonth')) = %s
                        ),
                        latest AS (SELECT MAX(to_day) AS md FROM base)
                        SELECT COALESCE(ml.name, b.asin) AS name,
                               UPPER(TRIM(ml.item_head::text)) AS head,
                               COALESCE(SUM(b.units * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs
                        FROM base b
                        CROSS JOIN latest l
                        JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(b.asin::text))
                        WHERE b.to_day = l.md
                          AND UPPER(TRIM(ml.item_head::text)) IN ('PREMIUM', 'COMMODITY')
                        GROUP BY 1, 2
                    """, [y, mname])

    cur_map = acc[(month_num, year)]
    prev_map = acc[(prev_month, prev_year)]
    ranked = sorted(cur_map.values(), key=lambda s: s["ltrs"], reverse=True)[:limit]

    skus = []
    for s in ranked:
        prev = prev_map.get(s["name"].upper())
        prev_ltrs = round(prev["ltrs"], 2) if prev else 0.0
        ltrs = round(s["ltrs"], 2)
        if prev_ltrs > 0:
            delta_pct = round((ltrs - prev_ltrs) / prev_ltrs * 100, 1)
        else:
            delta_pct = None  # no prior baseline → "NEW"
        skus.append({
            "name": s["name"],
            "head": s["head"],
            "ltrs": ltrs,
            "prev_ltrs": prev_ltrs,
            "delta_pct": delta_pct,
            "is_new": prev is None,
        })

    # A riser must actually have grown (> 0) and a faller must actually have
    # shrunk (< 0). Without the sign guard, an all-rising month would report the
    # slowest riser as the "biggest faller".
    movers = [s for s in skus if s["delta_pct"] is not None]
    risers = [s for s in movers if s["delta_pct"] > 0]
    fallers = [s for s in movers if s["delta_pct"] < 0]
    top_riser = max(risers, key=lambda s: s["delta_pct"], default=None)
    top_faller = min(fallers, key=lambda s: s["delta_pct"], default=None)
    return Response({
        "source": source, "platform": platform,
        "month": month_num, "year": year,
        "prev_month": prev_month, "prev_year": prev_year,
        "skus": skus, "top_riser": top_riser, "top_faller": top_faller,
        "errors": errors,
    })


# ─── /platform-expiry-alerts ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def platform_expiry_alerts(request):
    """Unique POs with 1 <= days_to_expiry <= 5 in the current month, per platform."""
    today = date.today()
    month_name = calendar.month_name[today.month].upper()  # e.g. 'MAY'
    year = today.year

    FORMAT_TO_SLUG = {
        'BLINKIT': 'blinkit',
        'ZEPTO': 'zepto',
        'SWIGGY': 'swiggy',
        'BIG BASKET': 'bigbasket',
        'FLIPKART GROCERY': 'flipkart_grocery',
        'ZOMATO': 'zomato',
        'CITY MALL': 'citymall',
        'AMAZON': 'amazon',
    }
    results = []
    errors = []
    with connection.cursor() as cur:
        try:
            cur.execute("""
                SELECT
                    UPPER(TRIM(format::text))                       AS format,
                    COUNT(DISTINCT po_number)                       AS po_count,
                    COALESCE(SUM(total_order_liters), 0)            AS total_litrs,
                    COALESCE(SUM(total_order_amt_exclusive), 0)     AS total_units
                FROM public.master_po
                WHERE days_to_expiry IS NOT NULL
                  AND days_to_expiry >= 1
                  AND days_to_expiry <= 5
                  AND UPPER(TRIM(po_status::text)) IN ('PENDING', 'APPOINTMENT DONE')
                GROUP BY 1
                ORDER BY total_units DESC
            """, [])
            for row in cur.fetchall():
                fmt = row[0]
                slug = FORMAT_TO_SLUG.get(fmt, (fmt.lower().replace(' ', '_') if fmt else None))
                results.append({
                    "format": fmt,
                    "slug": slug,
                    "po_count": int(row[1] or 0),
                    "total_litrs": float(row[2] or 0),
                    "total_units": float(row[3] or 0),
                })
        except Exception as e:
            errors.append({"source": "master_po", "error": str(e)})
        try:
            cur.execute("""
                SELECT
                    COUNT(DISTINCT po_number)                   AS po_count,
                    COALESCE(SUM(total_order_liters), 0)        AS total_litrs,
                    COALESCE(SUM(requested_qty), 0)             AS total_units
                FROM reporting."Amazon PO"
                WHERE days_to_expiry IS NOT NULL
                  AND days_to_expiry >= 1
                  AND days_to_expiry <= 5
                  AND UPPER(TRIM(po_status::text)) = 'PENDING'
            """, [])
            row = cur.fetchone()
            if row and int(row[0] or 0) > 0:
                results.append({
                    "format": "AMAZON",
                    "slug": "amazon",
                    "po_count": int(row[0] or 0),
                    "total_litrs": float(row[1] or 0),
                    "total_units": float(row[2] or 0),
                })
        except Exception as e:
            errors.append({"source": "amazon_po", "error": str(e)})
    return Response({
        "platforms": results,
        "errors": errors,
        "month": month_name,
        "year": year,
    })


# ─── /platform-expiry-alerts/<slug>/pos ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def platform_expiry_alert_pos(request, slug: str):
    """Distinct POs (1 <= days_to_expiry <= 5) for a platform in the current month."""
    today = date.today()
    month_name = calendar.month_name[today.month].upper()
    year = today.year

    SLUG_TO_FORMAT = {
        'blinkit': 'BLINKIT',
        'zepto': 'ZEPTO',
        'swiggy': 'SWIGGY',
        'bigbasket': 'BIG BASKET',
        'flipkart_grocery': 'FLIPKART GROCERY',
        'zomato': 'ZOMATO',
        'citymall': 'CITY MALL',
    }

    rows = []
    error = None
    with connection.cursor() as cur:
        if slug == 'amazon':
            try:
                cur.execute("""
                    SELECT
                        po_number,
                        MAX(sku_name)                                           AS sku_name,
                        MAX(item)                                               AS item,
                        MAX(days_to_expiry)                                     AS days_to_expiry,
                        MAX(expiry_date)                                        AS expiry_date,
                        MAX(po_status)                                          AS po_status,
                        MAX(fulfillment_center)                                 AS location,
                        COALESCE(SUM(total_order_liters), 0)                    AS total_litrs,
                        COALESCE(SUM(requested_qty), 0)                         AS total_units
                    FROM reporting."Amazon PO"
                    WHERE days_to_expiry IS NOT NULL
                      AND days_to_expiry >= 1
                      AND days_to_expiry <= 5
                      AND UPPER(TRIM(po_status::text)) = 'PENDING'
                    GROUP BY po_number
                    ORDER BY days_to_expiry ASC, po_number
                """, [])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            except Exception as e:
                error = str(e)
        else:
            fmt = SLUG_TO_FORMAT.get(slug)
            if not fmt:
                return Response({"error": f"Unknown platform slug: {slug}"}, status=400)
            try:
                cur.execute("""
                    SELECT
                        po_number,
                        MAX(COALESCE(item, sku_name))               AS item,
                        MAX(sku_name)                               AS sku_name,
                        MAX(days_to_expiry)                         AS days_to_expiry,
                        MAX(po_expiry_date)                         AS expiry_date,
                        MAX(po_status)                              AS po_status,
                        MAX(location)                               AS location,
                        COALESCE(SUM(total_order_liters), 0)            AS total_litrs,
                        COALESCE(SUM(total_order_amt_exclusive), 0)     AS total_units
                    FROM public.master_po
                    WHERE UPPER(TRIM(format::text)) = %s
                      AND days_to_expiry IS NOT NULL
                      AND days_to_expiry >= 1
                      AND days_to_expiry <= 5
                      AND UPPER(TRIM(po_status::text)) IN ('PENDING', 'APPOINTMENT DONE')
                    GROUP BY po_number
                    ORDER BY days_to_expiry ASC, po_number
                """, [fmt])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            except Exception as e:
                error = str(e)

    # Serialise Decimal / date objects
    def _clean(v):
        if hasattr(v, 'isoformat'):
            return v.isoformat()
        if hasattr(v, '__float__'):
            return float(v)
        return v

    rows = [{k: _clean(v) for k, v in row.items()} for row in rows]
    return Response({"pos": rows, "error": error, "month": month_name, "year": year})


# ─── /platform-expiry-alerts/<slug>/pos/<po_number>/items ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
def platform_expiry_alert_po_items(request, slug: str, po_number: str):
    """Individual line items for a single PO within the 1–5 day expiry window."""

    SLUG_TO_FORMAT = {
        'blinkit': 'BLINKIT',
        'zepto': 'ZEPTO',
        'swiggy': 'SWIGGY',
        'bigbasket': 'BIG BASKET',
        'flipkart_grocery': 'FLIPKART GROCERY',
        'zomato': 'ZOMATO',
        'citymall': 'CITY MALL',
    }

    rows = []
    error = None
    with connection.cursor() as cur:
        if slug == 'amazon':
            try:
                cur.execute("""
                    SELECT
                        sku_name,
                        item,
                        merchant_sku                                AS sku_code,
                        requested_qty                               AS qty,
                        COALESCE(total_order_liters, 0)             AS litrs,
                        po_status,
                        fulfillment_center                          AS location,
                        expiry_date,
                        days_to_expiry
                    FROM reporting."Amazon PO"
                    WHERE po_number = %s
                      AND days_to_expiry >= 1
                      AND days_to_expiry <= 5
                      AND UPPER(TRIM(po_status::text)) = 'PENDING'
                    ORDER BY sku_name
                """, [po_number])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            except Exception as e:
                error = str(e)
        else:
            fmt = SLUG_TO_FORMAT.get(slug)
            if not fmt:
                return Response({"error": f"Unknown platform slug: {slug}"}, status=400)
            try:
                cur.execute("""
                    SELECT
                        COALESCE(item, sku_name)                    AS item,
                        sku_name,
                        sku_code,
                        delivered_qty                               AS qty,
                        COALESCE(total_order_liters, 0)             AS litrs,
                        COALESCE(total_order_amt_exclusive, 0)      AS order_value,
                        po_status,
                        location,
                        po_expiry_date                              AS expiry_date,
                        days_to_expiry
                    FROM public.master_po
                    WHERE UPPER(TRIM(format::text)) = %s
                      AND po_number = %s
                      AND days_to_expiry >= 1
                      AND days_to_expiry <= 5
                      AND UPPER(TRIM(po_status::text)) IN ('PENDING', 'APPOINTMENT DONE')
                    ORDER BY sku_name
                """, [fmt, po_number])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            except Exception as e:
                error = str(e)

    def _clean(v):
        if hasattr(v, 'isoformat'):
            return v.isoformat()
        if hasattr(v, '__float__'):
            return float(v)
        return v

    rows = [{k: _clean(v) for k, v in row.items()} for row in rows]
    return Response({"items": rows, "error": error})


# ─── /table-data/{table} ───
@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def table_data(request, table_name: str):
    if table_name not in ALLOWED_TABLES:
        return Response({"error": "Table not allowed", "data": [], "count": 0})

    q = request.query_params
    try:
        page = max(0, int(q.get("page", 0)))
        page_size = min(200, max(1, int(q.get("page_size", 50))))
    except ValueError:
        page, page_size = 0, 50

    search = (q.get("search") or "")[:200]
    search_columns = q.get("search_columns", "")
    date_column = q.get("date_column", "")
    date_from = q.get("date_from", "")
    date_to = q.get("date_to", "")
    year = q.get("year", "")
    month = q.get("month", "")
    single_date = q.get("date", "")
    max_date = q.get("max_date", "")
    sort_by = q.get("sort_by", "")
    sort_dir = (q.get("sort_dir", "desc") or "desc").lower()
    expiry_column = q.get("expiry_column", "")
    expiry_before = q.get("expiry_before", "")
    column_filters_raw = q.get("column_filters", "")

    where: list[str] = []
    params: list = []

    def _validate_col(name: str) -> str | None:
        return name if name and _IDENT.match(name) else None

    query_date_column = date_column
    if table_name == "flipkart_grocery_master" and date_column == "date":
        query_date_column = "real_date"

    dc = _validate_col(query_date_column)
    date_expr = _date_expr(dc) if dc else None
    if dc:
        if date_from:
            where.append(f"{date_expr} >= %s::date")
            params.append(date_from)
        if date_to:
            where.append(f"{date_expr} <= %s::date")
            params.append(date_to)
        if year and not date_from and not date_to:
            where.append(f"{date_expr} >= %s::date")
            params.append(f"{year}-01-01")
            where.append(f"{date_expr} <= %s::date")
            params.append(f"{year}-12-31")
        if month:
            y = year or str(datetime.now().year)
            try:
                m = int(month)
                last_day = calendar.monthrange(int(y), m)[1]
                where.append(f"{date_expr} >= %s::date")
                params.append(f"{y}-{m:02d}-01")
                where.append(f"{date_expr} <= %s::date")
                params.append(f"{y}-{m:02d}-{last_day}")
            except ValueError:
                pass
        if single_date and not date_from and not date_to:
            where.append(f"{date_expr} = %s::date")
            params.append(single_date)

    ec = _validate_col(expiry_column)
    if ec and expiry_before:
        where.append(f'"{ec}" < %s')
        params.append(expiry_before)

    if search and search_columns:
        cols = [c.strip() for c in search_columns.split(",") if c.strip() and _IDENT.match(c.strip())]
        if cols:
            ors = " OR ".join(f'"{c}"::text ILIKE %s' for c in cols)
            where.append(f"({ors})")
            params.extend([f"%{search}%"] * len(cols))

    if column_filters_raw:
        try:
            parsed_filters = json.loads(column_filters_raw)
        except (TypeError, ValueError):
            parsed_filters = []
        sample = _sample_row(table_name) or {}
        for item in parsed_filters if isinstance(parsed_filters, list) else []:
            col = item.get("column") if isinstance(item, dict) else ""
            values = item.get("values") if isinstance(item, dict) else []
            if not col or not _IDENT.match(col) or col not in sample:
                continue
            if not isinstance(values, list):
                continue
            cleaned_values = ["" if v is None else str(v) for v in values[:500]]
            if not cleaned_values:
                where.append("1 = 0")
                continue
            placeholders = ", ".join(["%s"] * len(cleaned_values))
            where.append(f"COALESCE(\"{col}\"::text, '') IN ({placeholders})")
            params.extend(cleaned_values)

    qt = _quoted(table_name)

    # Snapshot of WHERE state before the max-date self-filter is applied.
    # Used both for the max-date subquery and for the "Latest Date" pill so
    # they reflect the user's other filters (year/month/date/search/expiry)
    # instead of the unfiltered global max.
    filter_where = list(where)
    filter_params = list(params)

    if max_date and date_expr:
        base_where_sql = f" WHERE {' AND '.join(filter_where)}" if filter_where else ""
        where.append(
            f"{date_expr} = (SELECT MAX({_date_expr(dc)}) FROM {qt}{base_where_sql})"
        )
        params.extend(filter_params)

    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    order_sql = ""
    order_col = _validate_col(sort_by) or dc
    if order_col:
        direction = "ASC" if sort_dir == "asc" else "DESC"
        order_sql = f" ORDER BY {_date_expr(order_col)} {direction} NULLS LAST"
        if table_name == "flipkart_grocery_master":
            order_sql += ', "sku_id" ASC NULLS LAST'
        elif table_name in {"amazon_sec_daily_master_view", "amazon_sec_range_master_view"}:
            order_sql += ', "to_date" ASC NULLS LAST, "asin" ASC NULLS LAST'
    elif table_name == "flipkart_grocery_master":
        order_sql = ' ORDER BY "real_date" DESC NULLS LAST, "sku_id" ASC NULLS LAST'
    elif table_name in {"amazon_sec_daily_master_view", "amazon_sec_range_master_view"}:
        order_sql = ' ORDER BY "from_date" ASC NULLS LAST, "to_date" ASC NULLS LAST, "asin" ASC NULLS LAST'

    try:
        with connection.cursor() as cur:
            latest_date = None
            if date_expr:
                pill_where_sql = (
                    f" WHERE {' AND '.join(filter_where)}" if filter_where else ""
                )
                cur.execute(
                    f"SELECT MAX({date_expr}) FROM {qt}{pill_where_sql}",
                    filter_params,
                )
                latest_date = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {qt}{where_sql}", params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"SELECT * FROM {qt}{where_sql}{order_sql} LIMIT %s OFFSET %s",
                params + [page_size, page * page_size],
            )
            if cur.description is None:
                return Response({"error": "No data returned", "data": [], "count": 0})
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if table_name in PRIMARY_MASTER_PO_TABLES:
                rows = [order_primary_master_po_row(row) for row in rows]
    except Exception:
        return Response({"error": "Query failed", "data": [], "count": 0})

    return Response({
        "data": rows,
        "count": total,
        "page": page,
        "page_size": page_size,
        "max_date": latest_date,
    })


PRIMARY_REMARK_UPDATE_TABLES = {"total_po", "total_po_zbs"}
PRIMARY_REMARK_UPDATE_COLUMNS = {"remark"}
PRIMARY_MANUAL_FULL_UPDATE_FORMATS = {"CITY MALL", "FLIPKART GROCERY"}
PRIMARY_MANUAL_FULL_UPDATE_COLUMNS = {"grn_date", "status", "delivered_qty"}


def _manual_date_value(value):
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    raise ValueError("GRN Date must be YYYY-MM-DD or DD-MM-YYYY.")


def _manual_decimal_value(value):
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except InvalidOperation as exc:
        raise ValueError("Delivered Qty must be numeric.") from exc


def _clean_primary_manual_updates(updates: dict, expected_format: str = "") -> dict:
    cleaned = {}
    normalized_format = str(expected_format or "").strip().upper()
    allowed_columns = set(PRIMARY_REMARK_UPDATE_COLUMNS)
    if normalized_format in PRIMARY_MANUAL_FULL_UPDATE_FORMATS:
        allowed_columns.update(PRIMARY_MANUAL_FULL_UPDATE_COLUMNS)
    for raw_col, raw_value in updates.items():
        col = "remark" if raw_col == "remarks" else str(raw_col or "").strip()
        if col not in allowed_columns:
            continue
        if col == "remark":
            cleaned[col] = None if raw_value is None else str(raw_value).strip()
        elif col == "grn_date":
            cleaned[col] = _manual_date_value(raw_value)
        elif col == "delivered_qty":
            cleaned[col] = _manual_decimal_value(raw_value)
        elif col == "status":
            cleaned[col] = None if raw_value is None else str(raw_value).strip().upper()
    return cleaned


def _primary_manual_format_guard(expected_format: str) -> tuple[str, list]:
    expected_format = str(expected_format or "").strip().upper()
    if expected_format:
        if expected_format == "AMAZON":
            raise ValueError("Amazon remarks are not editable here.")
        return 'AND UPPER(TRIM("format"::text)) = %s', [expected_format]
    return (
        'AND UPPER(TRIM("format"::text)) <> \'AMAZON\'',
        [],
    )


@api_view(["POST"])
@permission_classes([require("upload.use")])
def update_primary_manual_fields(request, table_name: str):
    if table_name not in PRIMARY_REMARK_UPDATE_TABLES:
        return Response({"detail": "Only Primary PO remark rows can be edited here."}, status=400)

    body = request.data or {}
    row_id = body.get("id") or body.get("row_id")
    try:
        row_id = int(row_id)
    except (TypeError, ValueError):
        return Response({"detail": "Row id is required."}, status=400)

    updates = body.get("updates") or {}
    if not isinstance(updates, dict):
        return Response({"detail": "updates must be an object."}, status=400)

    try:
        format_guard, format_params = _primary_manual_format_guard(body.get("format"))
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    try:
        cleaned = _clean_primary_manual_updates(updates, body.get("format"))
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    if not cleaned:
        return Response({"detail": "No editable fields supplied."}, status=400)

    assignments = ", ".join(f'"{col}" = %s' for col in cleaned)
    params = list(cleaned.values())
    params.append(row_id)
    params.extend(format_params)

    try:
        with connection.cursor() as cur:
            cur.execute(
                f"""
                UPDATE "{table_name}"
                   SET {assignments}
                 WHERE id = %s
                   {format_guard}
             RETURNING *
                """,
                params,
            )
            row = cur.fetchone()
            if not row:
                return Response({"detail": "Matching editable row not found."}, status=404)
            cols = [c[0] for c in cur.description]
    except Exception as exc:
        return Response({"detail": f"Update failed: {exc}"}, status=400)

    return Response({"row": dict(zip(cols, row))})


@api_view(["POST"])
@permission_classes([require("upload.use")])
def bulk_update_primary_manual_fields(request, table_name: str):
    if table_name not in PRIMARY_REMARK_UPDATE_TABLES:
        return Response({"detail": "Only Primary PO remark rows can be edited here."}, status=400)

    body = request.data or {}
    rows = body.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return Response({"detail": "No rows supplied."}, status=400)

    try:
        format_guard, format_params = _primary_manual_format_guard(body.get("format"))
    except ValueError as exc:
        return Response({"detail": str(exc)}, status=400)

    updated = 0
    failed = []
    saved_rows = []

    try:
        with transaction.atomic(), connection.cursor() as cur:
            for index, item in enumerate(rows):
                row_id = item.get("id") or item.get("row_id")
                try:
                    row_id = int(row_id)
                except (TypeError, ValueError):
                    failed.append({"index": index, "detail": "Row id is required."})
                    continue

                updates = item.get("updates") or {}
                if not isinstance(updates, dict):
                    failed.append({"id": row_id, "detail": "updates must be an object."})
                    continue

                try:
                    cleaned = _clean_primary_manual_updates(updates, body.get("format"))
                except ValueError as exc:
                    failed.append({"id": row_id, "detail": str(exc)})
                    continue

                if not cleaned:
                    continue

                assignments = ", ".join(f'"{col}" = %s' for col in cleaned)
                params = [*cleaned.values(), row_id, *format_params]
                cur.execute(
                    f"""
                    UPDATE "{table_name}"
                       SET {assignments}
                     WHERE id = %s
                       {format_guard}
                 RETURNING *
                    """,
                    params,
                )
                row = cur.fetchone()
                if not row:
                    failed.append({"id": row_id, "detail": "Matching editable row not found."})
                    continue
                cols = [c[0] for c in cur.description]
                saved_rows.append(dict(zip(cols, row)))
                updated += 1
            if failed:
                transaction.set_rollback(True)
    except Exception as exc:
        return Response({"detail": f"Bulk update failed: {exc}"}, status=400)

    if failed:
        return Response(
            {
                "detail": "Some rows could not be saved.",
                "updated": updated,
                "failed": failed,
            },
            status=400,
        )

    return Response({"updated": updated, "rows": saved_rows})
