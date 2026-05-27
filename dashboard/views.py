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
