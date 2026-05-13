import calendar
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Tables the dashboard can query. Mirrors FastAPI ALLOWED_TABLES.
ALLOWED_TABLES = {
    "master_po", "test_master_po",
    "bigbasket_prim", "blinkit_prim", "citymall_prim",
    "flipkart_grocery_prim", "swiggy_prim", "zepto_prim", "zomato_prim",
    "blinkit_grn", "swiggy_grn", "zepto_grn",
    "amazon_price_data", "amazon_sec_daily", "amazon_sec_daily_master_view", "amazon_sec_range",
    "amazon_sec_range_margins", "amazon_sec_range_master_view",
    "bigbasketSec", "blinkitSec", "flipkart_grocery_master", "fk_grocery", "flipkartSec", "flipkart_secondary_all",
    "jiomartSec", "swiggySec", "zeptoSec",
    "amazon_inventory", "bigbasket_inventory",
    "blinkit_inventory", "jiomart_inventory", "swiggy_inventory", "zepto_inventory",
    "all_platform_inventory",
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
    return Response({t: _count(t) for t in ALLOWED_TABLES})


# ─── /table-columns/{table} ───
@api_view(["GET"])
@permission_classes([require("dashboard.table.view")])
def table_columns(request, table_name: str):
    if table_name not in ALLOWED_TABLES:
        return Response({"error": "Table not allowed", "columns": [], "sample": None})
    sample = _sample_row(table_name)
    if not sample:
        return Response({"columns": [], "sample": None})
    return Response({"columns": list(sample.keys()), "sample": sample})


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
        elif isinstance(val, str) and re.match(r"^\d{4}-\d{2}-\d{2}", val):
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

    qt = _quoted(table_name)

    if max_date and date_expr:
        base_where = list(where)
        base_params = list(params)
        base_where_sql = f" WHERE {' AND '.join(base_where)}" if base_where else ""
        where.append(
            f"{date_expr} = (SELECT MAX({_date_expr(dc)}) FROM {qt}{base_where_sql})"
        )
        params.extend(base_params)

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
                cur.execute(f"SELECT MAX({date_expr}) FROM {qt}")
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
    except Exception:
        return Response({"error": "Query failed", "data": [], "count": 0})

    return Response({
        "data": rows,
        "count": total,
        "page": page,
        "page_size": page_size,
        "max_date": latest_date,
    })
