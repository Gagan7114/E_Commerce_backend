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
from config.perf_cache import cached_get
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
    """Row count for a stat card.

    Fast path: for ordinary/partitioned tables and materialized views, use the
    `reltuples` estimate Postgres maintains in pg_class via ANALYZE/autovacuum.
    That is O(1) instead of a full COUNT(*) scan over the whole table.

    Safe fallback: for plain VIEWs (reltuples is not maintained for them) or a
    table that has never been analyzed (reltuples < 0, or 0 which is ambiguous),
    fall back to an exact COUNT(*) so the number is never silently wrong. Many
    ALLOWED_TABLES entries are views, so this fallback matters.
    """
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT c.relkind, c.reltuples::bigint
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = %s
                LIMIT 1
                """,
                [table],
            )
            row = cur.fetchone()
            if row is not None:
                relkind, estimate = row[0], row[1]
                # 'r' table, 'p' partitioned table, 'm' materialized view all
                # keep a usable estimate. Only trust it when > 0 (a 0/negative
                # estimate means empty or never-analyzed -> verify exactly).
                if relkind in ("r", "p", "m") and estimate and estimate > 0:
                    return int(estimate)
            # View, missing stats, or never analyzed -> exact (and for empty
            # tables COUNT(*) is itself instant).
            cur.execute(f"SELECT COUNT(*) FROM {_quoted(table)}")
            exact = cur.fetchone()
            return int(exact[0]) if exact else 0
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
@cached_get(timeout=60, prefix="dash.table_counts")
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


def _fetch_latest_reporting_date(sql: str, params=None):
    try:
        with connection.cursor() as cur:
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception:
        return None


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=300, prefix="dash.latest_month")
def latest_month(request):
    """Calendar month used as the default dashboard period."""
    today = date.today()
    return Response({
        "month": today.month,
        "year": today.year,
        "month_label": calendar.month_name[today.month].upper(),
        "source_date": today.replace(day=1).isoformat(),
        "defaulted": False,
        "source": "calendar",
    })


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
def _inv_num_sql(col: str) -> str:
    """SQL that coerces a (possibly text) quantity column to an integer,
    mirroring the old Python ``int(value or 0)``: NULL / non-numeric -> 0,
    floats floored. Works whether the column is numeric or text."""
    txt = f'btrim("{col}"::text)'
    return (
        f"CASE WHEN {txt} ~ '^-?[0-9]+(\\.[0-9]+)?$' "
        f"THEN floor({txt}::numeric) ELSE 0 END"
    )


def _inv_is_code_sql(col: str) -> str:
    """SQL mirror of the old Python ``_is_code``: empty, or <=12 chars and
    purely alphanumeric -> treated as a code rather than a real product name."""
    txt = f'btrim("{col}"::text)'
    return f"({txt} = '' OR ({txt} ~ '^[A-Za-z0-9]+$' AND length({txt}) <= 12))"


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.inventory_charts")
def inventory_charts(request):
    """Inventory totals, city split, and top products per platform.

    All aggregation runs in SQL (COUNT / SUM / GROUP BY), so each platform
    returns only a few summary rows instead of up to 5,000 raw rows pulled into
    Python. This is far faster AND more correct: the previous version summed
    only the first 5,000 rows (``LIMIT 5000``), so totals were silently
    truncated on any table larger than that.
    """
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
        num_sql = _inv_num_sql(qty_col)

        try:
            qt = _quoted(table)
            with connection.cursor() as cur:
                # 1) Whole-table total + SKU count (no row cap).
                cur.execute(f"SELECT COUNT(*), COALESCE(SUM({num_sql}), 0) FROM {qt}")
                row = cur.fetchone()
                sku_count = int(row[0] or 0)
                total_qty = int(row[1] or 0)

                # 2) Top 5 products by quantity.
                if id_col:
                    # Amazon: aggregate by id, then label with a non-code title
                    # for that id (fall back to any title, then the id itself).
                    code_sql = _inv_is_code_sql(name_col)
                    cur.execute(
                        f"""
                        WITH agg AS (
                            SELECT "{id_col}"::text AS id,
                                   COALESCE(SUM({num_sql}), 0) AS qty,
                                   MAX(CASE WHEN NOT {code_sql}
                                            THEN "{name_col}"::text END) AS good_name,
                                   MAX("{name_col}"::text) AS any_name
                            FROM {qt}
                            GROUP BY "{id_col}"::text
                        )
                        SELECT COALESCE(good_name, any_name, id) AS product, qty
                        FROM agg
                        WHERE qty > 0
                        ORDER BY qty DESC
                        LIMIT 5
                        """
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT "{name_col}"::text AS product,
                               COALESCE(SUM({num_sql}), 0) AS qty
                        FROM {qt}
                        WHERE "{name_col}" IS NOT NULL
                          AND btrim("{name_col}"::text) <> ''
                        GROUP BY "{name_col}"::text
                        HAVING COALESCE(SUM({num_sql}), 0) > 0
                        ORDER BY qty DESC
                        LIMIT 5
                        """
                    )
                for prod, qty in cur.fetchall():
                    top_products.append({
                        "product": (str(prod) if prod else "")[:80],
                        "qty": int(qty or 0),
                        "platform": platform,
                        "color": color,
                    })

                # 3) City distribution (only platforms that carry a city column).
                if city_col:
                    cur.execute(
                        f"""
                        SELECT UPPER(btrim("{city_col}"::text)) AS city,
                               COALESCE(SUM({num_sql}), 0) AS qty
                        FROM {qt}
                        WHERE "{city_col}" IS NOT NULL
                          AND btrim("{city_col}"::text) <> ''
                        GROUP BY 1
                        HAVING COALESCE(SUM({num_sql}), 0) > 0
                        """
                    )
                    for city, qty in cur.fetchall():
                        if city:
                            city_totals[str(city)] += int(qty or 0)
        except Exception:
            platform_totals.append({
                "platform": platform, "total_qty": 0, "sku_count": 0, "color": color,
            })
            continue

        platform_totals.append({
            "platform": platform,
            "total_qty": total_qty,
            "sku_count": sku_count,
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
@cached_get(timeout=120, prefix="dash.primary_po_litres")
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
@cached_get(timeout=120, prefix="dash.category_litres")
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


# ─── /state-sales ───
# Canonical India state/UT names + common aliases seen across master_po
# (fulfilment state) and amazon_sec_state (consumer ship-to state). Anything not
# resolvable to a canonical name is counted in the total but left off the map.
_INDIA_STATE_ALIASES = {
    "ORISSA": "ODISHA",
    "PONDICHERRY": "PUDUCHERRY",
    "NCT OF DELHI": "DELHI",
    "NEW DELHI": "DELHI",
    "DELHI (NCT)": "DELHI",
    "UTTARANCHAL": "UTTARAKHAND",
    "UTTRAKHAND": "UTTARAKHAND",
    "UTTER PRADESH": "UTTAR PRADESH",
    "TAMILNADU": "TAMIL NADU",
    "TAMILNADU STATE": "TAMIL NADU",
    "TELENGANA": "TELANGANA",
    "TELANAGANA": "TELANGANA",
    "CHATTISGARH": "CHHATTISGARH",
    "CHHATTIGARH": "CHHATTISGARH",
    "MAHARASTRA": "MAHARASHTRA",
    "MHARASHTRA": "MAHARASHTRA",
    "MUMBAI MAHARASHTRA": "MAHARASHTRA",
    "HARYAN": "HARYANA",
    "KARANATAKA": "KARNATAKA",
    "GUJRAT": "GUJARAT",
    "GAUJRAT": "GUJARAT",
    "ANDRHRA PRADESH": "ANDHRA PRADESH",
    "J AND K": "JAMMU AND KASHMIR",
    "JAMMU AND KASHMIR (UT)": "JAMMU AND KASHMIR",
    "ANDAMAN AND NICOBAR": "ANDAMAN AND NICOBAR ISLANDS",
    "DADRA AND NAGAR HAVELI": "DADRA AND NAGAR HAVELI AND DAMAN AND DIU",
    "DAMAN AND DIU": "DADRA AND NAGAR HAVELI AND DAMAN AND DIU",
}
# ISO 3166-2:IN two-letter codes Amazon sometimes ships instead of full names.
_INDIA_STATE_CODES = {
    "AP": "ANDHRA PRADESH", "AR": "ARUNACHAL PRADESH", "AS": "ASSAM",
    "BR": "BIHAR", "CG": "CHHATTISGARH", "CT": "CHHATTISGARH", "GA": "GOA",
    "GJ": "GUJARAT", "HR": "HARYANA", "HP": "HIMACHAL PRADESH", "JH": "JHARKHAND",
    "JK": "JAMMU AND KASHMIR", "KA": "KARNATAKA", "KL": "KERALA",
    "MP": "MADHYA PRADESH", "MH": "MAHARASHTRA", "MN": "MANIPUR",
    "ML": "MEGHALAYA", "MZ": "MIZORAM", "NL": "NAGALAND", "OD": "ODISHA",
    "OR": "ODISHA", "PB": "PUNJAB", "RJ": "RAJASTHAN", "SK": "SIKKIM",
    "TN": "TAMIL NADU", "TG": "TELANGANA", "TS": "TELANGANA", "TR": "TRIPURA",
    "UP": "UTTAR PRADESH", "UK": "UTTARAKHAND", "UT": "UTTARAKHAND",
    "WB": "WEST BENGAL", "AN": "ANDAMAN AND NICOBAR ISLANDS", "CH": "CHANDIGARH",
    "DL": "DELHI", "DN": "DADRA AND NAGAR HAVELI AND DAMAN AND DIU",
    "DD": "DADRA AND NAGAR HAVELI AND DAMAN AND DIU", "LA": "LADAKH",
    "LD": "LAKSHADWEEP", "PY": "PUDUCHERRY",
}
_INDIA_STATE_BLANKS = {"", "UNKNOWN", "NA", "N/A", "-", "OTHER", "OTHERS", "NULL", "UNK"}
# Canonical 28 states + 8 UTs (UPPERCASE). A value is placed on the map only if
# it resolves to one of these — anything else (cities, junk, foreign) is counted
# in the total but kept off the map so pct_mapped stays honest.
_INDIA_STATES_CANON = {
    "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR", "CHHATTISGARH",
    "GOA", "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JHARKHAND", "KARNATAKA",
    "KERALA", "MADHYA PRADESH", "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM",
    "NAGALAND", "ODISHA", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU",
    "TELANGANA", "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL",
    "ANDAMAN AND NICOBAR ISLANDS", "CHANDIGARH",
    "DADRA AND NAGAR HAVELI AND DAMAN AND DIU", "DELHI", "JAMMU AND KASHMIR",
    "LADAKH", "LAKSHADWEEP", "PUDUCHERRY",
}
# Compact (alpha-only) → canonical, for no-space variants like "ANDHRAPRADESH".
_INDIA_STATES_COMPACT = {re.sub(r"[^A-Z]", "", s): s for s in _INDIA_STATES_CANON}


def _norm_state(raw):
    s = str(raw or "").strip().upper().replace("&", " AND ")
    s = re.sub(r"\s+", " ", s).strip()
    if s in _INDIA_STATE_BLANKS:
        return None
    # Drop trailing junk: "ODISHA,MOBILE-9437…", "PUNJAB (SAS NAGAR)".
    s = re.sub(r"\s+", " ", re.split(r"[,(]", s)[0]).strip()
    s = _INDIA_STATE_ALIASES.get(s, s)
    if s in _INDIA_STATES_CANON:
        return s
    if s in _INDIA_STATE_CODES:
        return _INDIA_STATE_CODES[s]
    compact = re.sub(r"[^A-Z]", "", s)
    if compact in _INDIA_STATE_CODES:        # "U.P." → "UP"
        return _INDIA_STATE_CODES[compact]
    if compact in _INDIA_STATES_COMPACT:     # "ANDHRAPRADESH", "WESTBENGAL"
        return _INDIA_STATES_COMPACT[compact]
    return None


# Frontend platform slug → "SecMaster".format value (secondary source). BigBasket
# and Jio Mart carry a space in the view's format string. Flipkart is intentionally
# absent — it has no usable state in the secondary feed and is excluded from the map.
_SEC_SLUG_TO_FORMAT = {
    "blinkit": "BLINKIT", "zepto": "ZEPTO", "swiggy": "SWIGGY",
    "bigbasket": "BIG BASKET", "jiomart": "JIO MART",
}

# Metric the State-wise Sales map can show. The same toggle drives the map, the
# Top-states list and the drill-down drawer. `label`/`unit` are echoed to the
# client so the UI never has to hard-code them.
_STATE_METRICS = {
    "units": {"label": "Units sold", "unit": "units"},
    "value": {"label": "Sales value", "unit": "₹"},
    "litres": {"label": "Litres sold", "unit": "L"},
}


def _state_metric(request):
    """Normalise the ?metric= param to one of units|value|litres (default units)."""
    m = (request.GET.get("metric") or "units").strip().lower()
    if m in ("ltr", "ltrs", "litre", "litres", "liter", "liters"):
        return "litres"
    if m in ("val", "value", "amount", "revenue", "gmv", "sales"):
        return "value"
    if m in ("unit", "units", "qty", "quantity"):
        return "units"
    return "units"


def _state_periods(request, today):
    """Resolve the month selection into a list of (year, month) periods.

    Single mode (back-compat): one period from ?month/?year. Range mode: every
    month from from_month/from_year to to_month/to_year inclusive — triggered when
    both from_month and to_month are supplied — capped at 36 months. Returns
    (mode, periods, echo) where `echo` is the month fields to mirror back to the
    client (month/year for single; from_*/to_* for range)."""
    def _int(name, default):
        try:
            return int(request.GET.get(name) or default)
        except (TypeError, ValueError):
            return default

    month_num = _int("month", today.month)
    year = _int("year", today.year)
    if not 1 <= month_num <= 12:
        month_num = today.month

    if request.GET.get("from_month") and request.GET.get("to_month"):
        fmn = _int("from_month", month_num)
        tmn = _int("to_month", month_num)
        fy = _int("from_year", year)
        ty = _int("to_year", year)
        if not 1 <= fmn <= 12:
            fmn = month_num
        if not 1 <= tmn <= 12:
            tmn = month_num
        start = fy * 12 + (fmn - 1)
        end = ty * 12 + (tmn - 1)
        if end < start:
            start, end = end, start
        end = min(end, start + 35)  # cap the span at 36 months
        periods = [(k // 12, (k % 12) + 1) for k in range(start, end + 1)]
        echo = {
            "from_month": (start % 12) + 1, "from_year": start // 12,
            "to_month": (end % 12) + 1, "to_year": end // 12,
        }
        return "range", periods, echo
    return "single", [(year, month_num)], {"month": month_num, "year": year}


# Per-source aggregate expressions for each metric. {metric: SQL}. The table
# aliases (s./a./f. or none) match how each query below references its columns.
_SEC_METRIC_SQL = {
    "units": "COALESCE(SUM(quantity), 0)",
    "value": "COALESCE(SUM(amount), 0)",
    "litres": "COALESCE(SUM(ltr_sold), 0)",
}
_AZ_METRIC_SQL = {
    "units": "COALESCE(SUM(a.shipped_units), 0)",
    "value": "COALESCE(SUM(a.shipped_revenue), 0)",
    "litres": (
        "COALESCE(SUM(CASE WHEN UPPER(TRIM(m.is_litre::text)) = 'Y' "
        "THEN a.shipped_units::numeric * m.per_unit_value ELSE 0 END), 0)"
    ),
}
_FK_QTY_SQL = "NULLIF(regexp_replace(item_quantity, '[^0-9.-]', '', 'g'), '')::numeric"
_FK_METRIC_SQL = {
    "units": "COALESCE(SUM(%s), 0)" % _FK_QTY_SQL,
    "value": (
        "COALESCE(SUM(NULLIF(regexp_replace(final_invoice_amount, "
        "'[^0-9.-]', '', 'g'), '')::numeric), 0)"
    ),
    "litres": (
        "COALESCE(SUM(CASE WHEN UPPER(TRIM(is_litre::text)) = 'Y' "
        "THEN %s * per_unit_value ELSE 0 END), 0)" % _FK_QTY_SQL
    ),
}


def _sec_month_filter(periods, alias=""):
    """(sql, params) matching SecMaster month/year against any of `periods`.

    `alias` is an optional column prefix (e.g. "s.") for queries that alias the
    table."""
    m, y = f"{alias}month", f"{alias}year"
    sql = " AND (" + " OR ".join(
        [f"(UPPER(TRIM({m}::text)) = %s AND {y}::numeric = %s)"] * len(periods)
    ) + ")"
    params = []
    for yr, mn in periods:
        params += [calendar.month_name[mn].upper(), yr]
    return sql, params


def _az_month_filter(periods, alias="a."):
    """(sql, params) matching amazon_sec_state.from_date against any of `periods`."""
    col = f"{alias}from_date"
    sql = " AND (" + " OR ".join(
        [f"(EXTRACT(MONTH FROM {col}) = %s AND EXTRACT(YEAR FROM {col}) = %s)"]
        * len(periods)
    ) + ")"
    params = []
    for yr, mn in periods:
        params += [mn, yr]
    return sql, params


def _fk_yms(periods):
    """['YYYY-MM', ...] for matching flipkart order_date prefixes."""
    return ["%04d-%02d" % (yr, mn) for yr, mn in periods]


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.state_sales")
def state_sales(request):
    """State-wise consumer units sold for the India map on Home (secondary data).

    QC platforms come from the "SecMaster" view: SUM(quantity) grouped on the
    view's already-resolved `state` (Jio Mart ships a DELIVERY_STATE directly; the
    QC platforms get state from city_state_mapping inside the view). Amazon →
    amazon_sec_state (shipped_units, joined to master_sheet on ASIN). Flipkart →
    flipkart_state_sales_master (SUM(item_quantity) for Sale events, grouped on
    customer_delivery_state).

    Metric (?metric=units|value|litres, default units) picks the figure summed
    per state: units = quantity, value = sales amount/revenue, litres = litres
    sold. Period is either a single month (?month/?year) or a range
    (?from_month/?from_year .. ?to_month/?to_year), capped at 36 months.

    Filters: platform, brand (Jivo/Sano, multi), category (multi), sub_category
    (multi). Category/sub_category option lists come from master_sheet."""
    today = date.today()
    metric = _state_metric(request)
    mode, periods, month_echo = _state_periods(request, today)

    platform = (request.GET.get("platform") or "").strip().lower() or None
    brands = [b.strip().upper() for b in request.GET.getlist("brand")
              if b.strip() and b.strip().lower() != "all"]
    cats = [c.strip().upper() for c in request.GET.getlist("category") if c.strip()]
    subs = [s.strip().upper() for s in request.GET.getlist("sub_category") if s.strip()]

    use_other = platform not in ("amazon", "flipkart")   # SecMaster QC platforms
    use_amazon = platform is None or platform == "amazon"
    use_flipkart = platform is None or platform == "flipkart"

    by_state = {}        # canonical name -> {value, by_platform}
    total_value = 0.0    # incl. rows whose state can't be mapped
    errors = []

    def add(raw_state, fmt, value):
        nonlocal total_value
        u = float(value or 0)
        total_value += u
        canon = _norm_state(raw_state)
        if canon is None:
            return
        e = by_state.setdefault(canon, {"value": 0.0, "by_platform": {}})
        e["value"] += u
        e["by_platform"][fmt] = e["by_platform"].get(fmt, 0.0) + u

    with connection.cursor() as cur:
        if use_other:
            # Secondary (consumer) sales from the "SecMaster" view. `state` is
            # resolved inside the view (Jio Mart direct + QC via city_state_mapping),
            # so we just group on it. Flipkart is excluded — its only geo is a
            # Location-Id code, which would otherwise inflate the unmapped total.
            sec_month_sql, sec_month_params = _sec_month_filter(periods)
            sql = """
                SELECT COALESCE(state::text, '') AS state,
                       UPPER(TRIM(format::text)) AS fmt,
                       %s AS units
                FROM "SecMaster"
                WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON', 'FLIPKART')
            """ % _SEC_METRIC_SQL[metric]
            sql += sec_month_sql
            params = list(sec_month_params)
            if platform:
                fmt = _SEC_SLUG_TO_FORMAT.get(
                    platform, platform.replace("_", " ").upper()
                )
                sql += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                params.append(fmt)
            if brands:
                sql += " AND UPPER(TRIM(brand::text)) = ANY(%s)"
                params.append(brands)
            if cats:
                sql += " AND UPPER(TRIM(category::text)) = ANY(%s)"
                params.append(cats)
            if subs:
                sql += " AND UPPER(TRIM(sub_category::text)) = ANY(%s)"
                params.append(subs)
            sql += " GROUP BY 1, 2"
            try:
                cur.execute(sql, params)
                for st, fmt, units in cur.fetchall():
                    add(st, fmt, units)
            except Exception as e:
                errors.append({"source": "secmaster", "error": str(e)})

        if use_amazon:
            az_month_sql, az_month_params = _az_month_filter(periods)
            sql_a = """
                SELECT COALESCE(a.state::text, '') AS state,
                       %s AS units
                FROM public.amazon_sec_state a
                LEFT JOIN public.master_sheet m
                  ON UPPER(TRIM(m.format_sku_code::text)) = UPPER(TRIM(a.asin))
                 AND UPPER(TRIM(m.format::text)) = 'AMAZON'
                WHERE 1 = 1
            """ % _AZ_METRIC_SQL[metric]
            sql_a += az_month_sql
            pa = list(az_month_params)
            if brands:
                sql_a += " AND UPPER(TRIM(m.brand::text)) = ANY(%s)"
                pa.append(brands)
            if cats:
                sql_a += " AND UPPER(TRIM(m.category::text)) = ANY(%s)"
                pa.append(cats)
            if subs:
                sql_a += " AND UPPER(TRIM(m.sub_category::text)) = ANY(%s)"
                pa.append(subs)
            sql_a += " GROUP BY 1"
            try:
                cur.execute(sql_a, pa)
                for st, units in cur.fetchall():
                    add(st, "AMAZON", units)
            except Exception as e:
                errors.append({"source": "amazon_sec_state", "error": str(e)})

        if use_flipkart:
            # Flipkart consumer sales by delivery state, from the FSN-enriched
            # master view. Count Sale events only (gross units sold; Returns /
            # Cancellations excluded). item_quantity is TEXT -> strip to numeric;
            # order_date is ISO text ('YYYY-MM-DD …') -> match on the YYYY-MM prefix.
            sql_f = """
                SELECT COALESCE(customer_delivery_state::text, '') AS state,
                       %s AS units
                FROM public.flipkart_state_sales_master
                WHERE left(order_date, 7) = ANY(%%s)
                  AND UPPER(TRIM(event_type::text)) = 'SALE'
            """ % _FK_METRIC_SQL[metric]
            pf = [_fk_yms(periods)]
            if brands:
                sql_f += " AND UPPER(TRIM(brand::text)) = ANY(%s)"
                pf.append(brands)
            if cats:
                sql_f += " AND UPPER(TRIM(category::text)) = ANY(%s)"
                pf.append(cats)
            if subs:
                sql_f += " AND UPPER(TRIM(sub_category::text)) = ANY(%s)"
                pf.append(subs)
            sql_f += " GROUP BY 1"
            try:
                cur.execute(sql_f, pf)
                for st, units in cur.fetchall():
                    add(st, "FLIPKART", units)
            except Exception as e:
                errors.append({"source": "flipkart_state_sales_master", "error": str(e)})

        # Dropdown options — categories + sub_categories from master_sheet (the
        # catalog that feeds SecMaster.category; a cheap superset, vs a full
        # DISTINCT scan of the heavy view).
        categories_all, sub_categories_all = [], []
        try:
            cur.execute(
                "SELECT DISTINCT UPPER(TRIM(category::text)) FROM master_sheet "
                "WHERE NULLIF(TRIM(category::text), '') IS NOT NULL ORDER BY 1"
            )
            categories_all = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT DISTINCT UPPER(TRIM(category::text)), UPPER(TRIM(sub_category::text)) "
                "FROM master_sheet WHERE NULLIF(TRIM(sub_category::text), '') IS NOT NULL "
                "ORDER BY 1, 2"
            )
            sub_categories_all = [
                {"category": r[0], "sub_category": r[1]} for r in cur.fetchall()
            ]
        except Exception as e:
            errors.append({"source": "filter_options", "error": str(e)})

    states = sorted(
        (
            {
                "state": name,
                # `units` kept for back-compat; `value` is the metric-selected
                # number the UI reads. Both carry the same figure.
                "units": round(v["value"], 2),
                "value": round(v["value"], 2),
                "by_platform": {k: round(x, 2) for k, x in v["by_platform"].items()},
            }
            for name, v in by_state.items()
            if v["value"] > 0
        ),
        key=lambda s: s["value"],
        reverse=True,
    )
    mapped_value = round(sum(s["value"] for s in states), 2)
    total_value = round(total_value, 2)

    return Response({
        "metric": metric,
        "metric_label": _STATE_METRICS[metric]["label"],
        "metric_unit": _STATE_METRICS[metric]["unit"],
        "mode": mode,
        **month_echo,
        "platform": platform,
        "brands": brands,
        "categories": cats,
        "sub_categories": subs,
        "states": states,
        # `*_units` kept for back-compat; `*_value` is metric-aware.
        "total_units": total_value,
        "mapped_units": mapped_value,
        "total_value": total_value,
        "mapped_value": mapped_value,
        "pct_mapped": round(mapped_value / total_value * 100, 1) if total_value else 0,
        "filter_options": {
            "brands": ["JIVO", "SANO"],
            "categories": categories_all,
            "sub_categories": sub_categories_all,
        },
        "errors": errors,
    })


# ─── /state-sales/detail ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.state_sales_detail")
def state_sales_detail(request):
    """Raw line-item rows behind one state on the Home map (secondary data).

    Drill-down for a click on the State-wise Sales map. Returns the underlying
    rows for `state` from the same sources as /state-sales — "SecMaster" for the
    QC platforms, amazon_sec_state for Amazon, flipkart_state_sales_master for
    Flipkart — honouring the same platform / brand / category / sub_category /
    month filters, paginated.

    State matching mirrors the map's aggregation exactly: because the raw `state`
    spellings are messy (Jio Mart mixed-case, Amazon codes/misspellings), we pull
    each source's small set of distinct spellings, keep those that _norm_state to
    the requested canonical state, then fetch rows matching those exact spellings
    — so the page totals reconcile with the number the user clicked."""
    today = date.today()
    metric = _state_metric(request)
    mode, periods, month_echo = _state_periods(request, today)

    # Row-level metric expression per source (no SUM — one value per line item).
    sec_row = {
        "units": "COALESCE(s.quantity, 0)::numeric",
        "value": "COALESCE(s.amount, 0)::numeric",
        "litres": "COALESCE(s.ltr_sold, 0)::numeric",
    }[metric]
    az_row = {
        "units": "COALESCE(a.shipped_units, 0)::numeric",
        "value": "COALESCE(a.shipped_revenue, 0)::numeric",
        "litres": ("CASE WHEN UPPER(TRIM(m.is_litre::text)) = 'Y' THEN "
                   "COALESCE(a.shipped_units, 0)::numeric * COALESCE(m.per_unit_value, 0) "
                   "ELSE 0 END"),
    }[metric]
    _fk_q = "NULLIF(regexp_replace(f.item_quantity, '[^0-9.-]', '', 'g'), '')::numeric"
    fk_row = {
        "units": f"COALESCE({_fk_q}, 0)",
        "value": ("COALESCE(NULLIF(regexp_replace(f.final_invoice_amount, "
                  "'[^0-9.-]', '', 'g'), '')::numeric, 0)"),
        "litres": (f"CASE WHEN UPPER(TRIM(f.is_litre::text)) = 'Y' THEN "
                   f"COALESCE({_fk_q}, 0) * COALESCE(f.per_unit_value, 0) ELSE 0 END"),
    }[metric]

    canon = _norm_state(request.GET.get("state"))
    platform = (request.GET.get("platform") or "").strip().lower() or None
    brands = [b.strip().upper() for b in request.GET.getlist("brand")
              if b.strip() and b.strip().lower() != "all"]
    cats = [c.strip().upper() for c in request.GET.getlist("category") if c.strip()]
    subs = [s.strip().upper() for s in request.GET.getlist("sub_category") if s.strip()]
    try:
        limit = min(max(int(request.GET.get("limit") or 50), 1), 1000)
    except (TypeError, ValueError):
        limit = 50
    try:
        offset = max(int(request.GET.get("offset") or 0), 0)
    except (TypeError, ValueError):
        offset = 0

    use_other = platform not in ("amazon", "flipkart")
    use_amazon = platform is None or platform == "amazon"
    use_flipkart = platform is None or platform == "flipkart"

    empty = {
        "state": canon, "metric": metric,
        "metric_label": _STATE_METRICS[metric]["label"],
        "metric_unit": _STATE_METRICS[metric]["unit"], "mode": mode, **month_echo,
        "platform": platform,
        "brands": brands, "categories": cats, "sub_categories": subs,
        "limit": limit, "offset": offset, "total_rows": 0, "total_units": 0,
        "total_value": 0, "rows": [], "errors": [],
    }
    if canon is None:
        return Response(empty)

    errors = []
    with connection.cursor() as cur:
        # 1) Resolve the messy raw spellings that normalise to `canon`, per source.
        sec_raws, az_raws, fk_states = [], [], []
        if use_other:
            sec_mf, sec_mp = _sec_month_filter(periods)
            ds = ("SELECT DISTINCT COALESCE(state::text, '') FROM \"SecMaster\" "
                  "WHERE UPPER(TRIM(format::text)) NOT IN ('AMAZON', 'FLIPKART')")
            ds += sec_mf
            dp = list(sec_mp)
            if platform:
                ds += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                dp.append(_SEC_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper()))
            try:
                cur.execute(ds, dp)
                sec_raws = [r[0] for r in cur.fetchall() if _norm_state(r[0]) == canon]
            except Exception as e:
                errors.append({"source": "secmaster_states", "error": str(e)})
        if use_amazon:
            az_mf, az_mp = _az_month_filter(periods, alias="")
            try:
                cur.execute(
                    "SELECT DISTINCT COALESCE(state::text, '') FROM public.amazon_sec_state "
                    "WHERE 1 = 1" + az_mf,
                    list(az_mp),
                )
                az_raws = [r[0] for r in cur.fetchall() if _norm_state(r[0]) == canon]
            except Exception as e:
                errors.append({"source": "amazon_states", "error": str(e)})
        if use_flipkart:
            try:
                cur.execute(
                    "SELECT DISTINCT COALESCE(customer_delivery_state::text, '') "
                    "FROM public.flipkart_state_sales_master "
                    "WHERE left(order_date, 7) = ANY(%s) AND UPPER(TRIM(event_type::text)) = 'SALE'",
                    [_fk_yms(periods)],
                )
                fk_states = [r[0] for r in cur.fetchall() if _norm_state(r[0]) == canon]
            except Exception as e:
                errors.append({"source": "flipkart_states", "error": str(e)})

        # 2) Build a UNION of the sources that actually carry this state, with
        #    window totals so the page also reports the full row/unit counts.
        branches, params = [], []
        if use_other and sec_raws:
            sec_mf, sec_mp = _sec_month_filter(periods, alias="s.")
            b = f"""
                SELECT s.date::date AS d, UPPER(TRIM(s.format::text)) AS platform,
                       s.sku_code::text AS sku, s.sku_name::text AS name,
                       UPPER(TRIM(s.brand::text)) AS brand, s.category::text AS category,
                       s.sub_category::text AS sub_category,
                       {sec_row} AS units, s.city::text AS city
                FROM "SecMaster" s
                WHERE UPPER(TRIM(s.format::text)) NOT IN ('AMAZON', 'FLIPKART')
            """
            b += sec_mf
            p = list(sec_mp)
            if platform:
                b += " AND LOWER(TRIM(s.format::text)) = LOWER(%s)"
                p.append(_SEC_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper()))
            if brands:
                b += " AND UPPER(TRIM(s.brand::text)) = ANY(%s)"; p.append(brands)
            if cats:
                b += " AND UPPER(TRIM(s.category::text)) = ANY(%s)"; p.append(cats)
            if subs:
                b += " AND UPPER(TRIM(s.sub_category::text)) = ANY(%s)"; p.append(subs)
            b += " AND COALESCE(s.state::text, '') = ANY(%s)"; p.append(sec_raws)
            branches.append(b); params += p
        if use_amazon and az_raws:
            az_mf, az_mp = _az_month_filter(periods)
            b = f"""
                SELECT a.from_date::date AS d, 'AMAZON' AS platform,
                       a.asin::text AS sku, m.product_name::text AS name,
                       UPPER(TRIM(m.brand::text)) AS brand, m.category::text AS category,
                       m.sub_category::text AS sub_category,
                       {az_row} AS units, NULL::text AS city
                FROM public.amazon_sec_state a
                LEFT JOIN public.master_sheet m
                  ON UPPER(TRIM(m.format_sku_code::text)) = UPPER(TRIM(a.asin))
                 AND UPPER(TRIM(m.format::text)) = 'AMAZON'
                WHERE 1 = 1
            """
            b += az_mf
            p = list(az_mp)
            if brands:
                b += " AND UPPER(TRIM(m.brand::text)) = ANY(%s)"; p.append(brands)
            if cats:
                b += " AND UPPER(TRIM(m.category::text)) = ANY(%s)"; p.append(cats)
            if subs:
                b += " AND UPPER(TRIM(m.sub_category::text)) = ANY(%s)"; p.append(subs)
            b += " AND COALESCE(a.state::text, '') = ANY(%s)"; p.append(az_raws)
            branches.append(b); params += p
        if use_flipkart and fk_states:
            b = f"""
                SELECT NULLIF(LEFT(f.order_date, 10), '')::date AS d, 'FLIPKART' AS platform,
                       regexp_replace(upper(f.fsn), '[^A-Z0-9]+', '', 'g')::text AS sku,
                       f.product_title::text AS name,
                       UPPER(TRIM(f.brand::text)) AS brand, f.category::text AS category,
                       f.sub_category::text AS sub_category,
                       {fk_row} AS units,
                       NULL::text AS city
                FROM public.flipkart_state_sales_master f
                WHERE left(f.order_date, 7) = ANY(%s)
                  AND UPPER(TRIM(f.event_type::text)) = 'SALE'
            """
            p = [_fk_yms(periods)]
            if brands:
                b += " AND UPPER(TRIM(f.brand::text)) = ANY(%s)"; p.append(brands)
            if cats:
                b += " AND UPPER(TRIM(f.category::text)) = ANY(%s)"; p.append(cats)
            if subs:
                b += " AND UPPER(TRIM(f.sub_category::text)) = ANY(%s)"; p.append(subs)
            b += " AND COALESCE(f.customer_delivery_state::text, '') = ANY(%s)"; p.append(fk_states)
            branches.append(b); params += p

        if not branches:
            empty["errors"] = errors
            return Response(empty)

        union = " UNION ALL ".join(f"({b})" for b in branches)
        final = f"""
            SELECT d, platform, sku, name, brand, category, sub_category, units, city,
                   COUNT(*) OVER() AS total_rows,
                   COALESCE(SUM(units) OVER(), 0) AS total_units
            FROM ( {union} ) u
            ORDER BY d DESC NULLS LAST, units DESC NULLS LAST
            LIMIT %s OFFSET %s
        """
        params += [limit, offset]
        rows, total_rows, total_units = [], 0, 0.0
        try:
            cur.execute(final, params)
            for r in cur.fetchall():
                total_rows = int(r[9] or 0)
                total_units = float(r[10] or 0)
                rows.append({
                    "date": r[0].isoformat() if r[0] else None,
                    "platform": r[1],
                    "sku": r[2],
                    "name": r[3],
                    "brand": r[4],
                    "category": r[5],
                    "sub_category": r[6],
                    # `units` is the metric-selected figure (kept for back-compat);
                    # `value` is the same number under a metric-neutral key.
                    "units": round(float(r[7] or 0), 2),
                    "value": round(float(r[7] or 0), 2),
                    "city": r[8],
                })
        except Exception as e:
            errors.append({"source": "state_detail_rows", "error": str(e)})

    return Response({
        "state": canon, "metric": metric,
        "metric_label": _STATE_METRICS[metric]["label"],
        "metric_unit": _STATE_METRICS[metric]["unit"], "mode": mode, **month_echo,
        "platform": platform,
        "brands": brands, "categories": cats, "sub_categories": subs,
        "limit": limit, "offset": offset,
        "total_rows": total_rows,
        "total_units": round(total_units, 2),
        "total_value": round(total_units, 2),
        "rows": rows, "errors": errors,
    })


# ─── /category-breakdown ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.category_breakdown")
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


def _friendly_platform_name(slug, fmt_label):
    """Fallback display name for a platform; the frontend overrides this with its
    own platform list (matched by slug) when it has one."""
    if slug == "amazon":
        return "Amazon"
    if fmt_label:
        return str(fmt_label).title()
    return (slug or "Unknown").replace("_", " ").title()


# ─── /category-platform-breakdown ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.category_platform_breakdown")
def category_platform_breakdown(request):
    """Per-platform units + litres for ONE category or sub_category within a head.

    Drill-down for the home "Category Split" card: click a (sub)category row and
    see every platform that sold it, with units sold and litres sold. Same source
    semantics as /category-breakdown (primary = master_po + Amazon PO; secondary =
    SecMaster + amazon_sec_range, latest cumulative snapshot). Deliberately spans
    ALL platforms regardless of any platform filter — the whole point of the
    drill-down is the cross-platform split for the picked item.

    Query params: month, year, source (primary|secondary), head (premium|commodity),
    dimension (category|sub_category), name (the clicked label; '' or 'Uncategorized'
    matches rows with a null/blank value)."""
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

    head_in = (request.GET.get("head") or "premium").strip().lower()
    head_sql = "COMMODITY" if head_in == "commodity" else "PREMIUM"
    head_key = "commodity" if head_in == "commodity" else "premium"

    dimension = "category" if (request.GET.get("dimension") or "").strip().lower() == "category" else "sub_category"
    dim_col = dimension  # column name in every source table

    name = (request.GET.get("name") or "").strip()
    # /category-breakdown surfaces null/blank (sub)categories as "Uncategorized";
    # mirror that so a click on that row matches the same rows here.
    is_uncat = name == "" or name.upper() == "UNCATEGORIZED"
    name_u = name.upper()

    # Format string ('BLINKIT', 'BIG BASKET', …) → frontend slug. Reverse the
    # per-source slug maps the rest of this module uses.
    if source == "primary":
        slug_by_format = {v: k for k, v in _CATEGORY_SLUG_TO_FORMAT.items()}
    else:
        slug_by_format = {v: k for k, v in _SEC_SLUG_TO_FORMAT.items()}

    rows_out = {}  # slug -> {"slug","format","name","units","ltrs"}
    errors = []

    def add(fmt_label, slug, units, ltrs):
        u = float(units or 0)
        litres = float(ltrs or 0)
        if u == 0 and litres == 0:
            return
        key = slug or (fmt_label or "").strip().lower().replace(" ", "_") or "unknown"
        row = rows_out.get(key)
        if row is None:
            row = {
                "slug": key,
                "format": fmt_label,
                "name": _friendly_platform_name(key, fmt_label),
                "units": 0.0,
                "ltrs": 0.0,
            }
            rows_out[key] = row
        row["units"] += u
        row["ltrs"] += litres

    def dim_filter(col):
        """(SQL fragment, extra params) restricting `col` to the picked value."""
        if is_uncat:
            return f" AND COALESCE(NULLIF(TRIM({col}::text), ''), '') = ''", []
        return f" AND UPPER(TRIM({col}::text)) = %s", [name_u]

    def run(label, sql, params, handler):
        try:
            cur.execute(sql, params)
            handler(cur.fetchall())
        except Exception as e:  # noqa: BLE001
            errors.append({"source": label, "error": str(e)})

    with connection.cursor() as cur:
        if source == "primary":
            # master_po — every non-Amazon platform, grouped by format.
            dim_sql, dim_params = dim_filter("sub_category" if dim_col == "sub_category" else "category")
            sql = f"""
                SELECT UPPER(TRIM(format::text)) AS fmt,
                       COALESCE(SUM(delivered_qty), 0) AS units,
                       COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                FROM public.master_po
                WHERE UPPER(TRIM(delivery_month::text)) = %s
                  AND delivered_year = %s
                  AND UPPER(TRIM(item_head::text)) = %s
                  AND UPPER(TRIM(format::text)) <> 'AMAZON'
                  {dim_sql}
                GROUP BY 1
            """
            run(
                "master_po", sql, [month_name, year, head_sql, *dim_params],
                lambda rows: [
                    add(fmt, slug_by_format.get(fmt), units, ltrs)
                    for fmt, units, ltrs in rows
                ],
            )
            # Amazon PO (reporting) — single platform.
            dim_sql, dim_params = dim_filter(dim_col)
            sql = f"""
                SELECT COALESCE(SUM(filled_units), 0) AS units,
                       COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                FROM reporting."Amazon PO"
                WHERE po_month = %s AND year = %s
                  AND UPPER(TRIM(item_head::text)) = %s
                  {dim_sql}
            """
            run(
                "amazon_po", sql, [month_num, year, head_sql, *dim_params],
                lambda rows: [add("AMAZON", "amazon", r[0], r[1]) for r in rows],
            )
        else:  # secondary
            dim_sql, dim_params = dim_filter(dim_col)
            sql = f"""
                SELECT UPPER(TRIM(format::text)) AS fmt,
                       COALESCE(SUM(quantity), 0) AS units,
                       COALESCE(SUM(ltr_sold), 0) AS ltrs
                FROM "SecMaster"
                WHERE UPPER(TRIM(month::text)) = %s
                  AND year::numeric = %s
                  AND UPPER(TRIM(item_head::text)) = %s
                  AND UPPER(TRIM(format::text)) <> 'AMAZON'
                  {dim_sql}
                GROUP BY 1
            """
            run(
                "secmaster", sql, [month_name, year, head_sql, *dim_params],
                lambda rows: [
                    add(fmt, slug_by_format.get(fmt), units, ltrs)
                    for fmt, units, ltrs in rows
                ],
            )
            # Amazon secondary — latest cumulative snapshot, units × per_unit_value.
            dim_sql, dim_params = dim_filter(f"ml.{dim_col}")
            sql = f"""
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
                SELECT COALESCE(SUM(b.units), 0) AS units,
                       COALESCE(SUM(b.units * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs
                FROM base b
                CROSS JOIN latest l
                JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(b.asin::text))
                WHERE b.to_day = l.md
                  AND UPPER(TRIM(ml.item_head::text)) = %s
                  {dim_sql}
            """
            run(
                "amazon_sec_range", sql, [year, month_name, head_sql, *dim_params],
                lambda rows: [add("AMAZON", "amazon", r[0], r[1]) for r in rows],
            )

    platforms_list = sorted(rows_out.values(), key=lambda r: r["ltrs"], reverse=True)
    for r in platforms_list:
        r["units"] = round(r["units"], 2)
        r["ltrs"] = round(r["ltrs"], 2)

    return Response({
        "source": source,
        "head": head_key,
        "dimension": dimension,
        "name": name or "Uncategorized",
        "month": month_num,
        "year": year,
        "platforms": platforms_list,
        "total_units": round(sum(r["units"] for r in platforms_list), 2),
        "total_ltrs": round(sum(r["ltrs"] for r in platforms_list), 2),
        "errors": errors,
    })


def _category_sku_rows(
    cur,
    *,
    source,
    is_amazon,
    fmt,
    head_sql,
    month_num,
    month_name,
    year,
    dim_col,
    is_uncat,
    name_u,
):
    """SKU rows [(code, name, brand, units, ltrs)] for one platform in one month.

    Queries only the single source table backing the platform. Shared by the SKU
    drill-down so the trailing-month comparison runs the same SQL per month."""

    def dim_filter(col):
        if is_uncat:
            return f" AND COALESCE(NULLIF(TRIM({col}::text), ''), '') = ''", []
        return f" AND UPPER(TRIM({col}::text)) = %s", [name_u]

    if source == "primary" and not is_amazon:
        dim_sql, dim_params = dim_filter(dim_col)
        cur.execute(f"""
            SELECT COALESCE(NULLIF(TRIM(sku_code::text), ''), '—') AS code,
                   COALESCE(NULLIF(TRIM(sku_name::text), ''), '') AS sku_name,
                   UPPER(TRIM(COALESCE(brand::text, ''))) AS brand,
                   COALESCE(SUM(delivered_qty), 0) AS units,
                   COALESCE(SUM(total_delivered_liters), 0) AS ltrs
            FROM public.master_po
            WHERE UPPER(TRIM(delivery_month::text)) = %s
              AND delivered_year = %s
              AND UPPER(TRIM(item_head::text)) = %s
              AND UPPER(TRIM(format::text)) = %s
              {dim_sql}
            GROUP BY 1, 2, 3
        """, [month_name, year, head_sql, fmt, *dim_params])
        return cur.fetchall()

    if source == "primary" and is_amazon:
        dim_sql, dim_params = dim_filter(dim_col)
        cur.execute(f"""
            SELECT COALESCE(NULLIF(TRIM(asin::text), ''), '—') AS code,
                   COALESCE(NULLIF(TRIM(sku_name::text), ''), '') AS sku_name,
                   UPPER(TRIM(COALESCE(brand::text, ''))) AS brand,
                   COALESCE(SUM(filled_units), 0) AS units,
                   COALESCE(SUM(total_delivered_liters), 0) AS ltrs
            FROM reporting."Amazon PO"
            WHERE po_month = %s AND year = %s
              AND UPPER(TRIM(item_head::text)) = %s
              {dim_sql}
            GROUP BY 1, 2, 3
        """, [month_num, year, head_sql, *dim_params])
        return cur.fetchall()

    if source == "secondary" and not is_amazon:
        dim_sql, dim_params = dim_filter(dim_col)
        cur.execute(f"""
            SELECT COALESCE(NULLIF(TRIM(sku_code::text), ''), '—') AS code,
                   COALESCE(NULLIF(TRIM(sku_name::text), ''), '') AS sku_name,
                   UPPER(TRIM(COALESCE(brand::text, ''))) AS brand,
                   COALESCE(SUM(quantity), 0) AS units,
                   COALESCE(SUM(ltr_sold), 0) AS ltrs
            FROM "SecMaster"
            WHERE UPPER(TRIM(month::text)) = %s
              AND year::numeric = %s
              AND UPPER(TRIM(item_head::text)) = %s
              AND UPPER(TRIM(format::text)) = %s
              {dim_sql}
            GROUP BY 1, 2, 3
        """, [month_name, year, head_sql, fmt, *dim_params])
        return cur.fetchall()

    # secondary + amazon — latest cumulative snapshot, units × per_unit_value.
    dim_sql, dim_params = dim_filter(f"ml.{dim_col}")
    cur.execute(f"""
        WITH ml AS (
            SELECT DISTINCT ON (format_sku_code)
                   format_sku_code, category, sub_category, item_head,
                   per_unit_value, product_name, brand
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
        SELECT UPPER(TRIM(b.asin::text)) AS code,
               COALESCE(NULLIF(TRIM(ml.product_name::text), ''), '') AS sku_name,
               UPPER(TRIM(COALESCE(ml.brand::text, ''))) AS brand,
               COALESCE(SUM(b.units), 0) AS units,
               COALESCE(SUM(b.units * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs
        FROM base b
        CROSS JOIN latest l
        JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(b.asin::text))
        WHERE b.to_day = l.md
          AND UPPER(TRIM(ml.item_head::text)) = %s
          {dim_sql}
        GROUP BY 1, 2, 3
    """, [year, month_name, head_sql, *dim_params])
    return cur.fetchall()


# ─── /category-sku-breakdown ───
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.category_sku_breakdown")
def category_sku_breakdown(request):
    """SKU-wise units + litres for ONE platform within a category / sub_category,
    optionally across the trailing N months for a month-over-month comparison.

    Second drill level under /category-platform-breakdown: pick a platform and
    list every SKU it sold for the chosen (sub)category, with units sold and
    litres sold. Same source semantics; queries only the single source table that
    backs the requested platform (Amazon → Amazon PO / amazon_sec_range, everyone
    else → master_po / SecMaster filtered by format).

    months=N (1-6, default 1) returns each SKU's `by_month` map keyed YYYY-MM plus
    a top-level `months` list (oldest→newest); `units`/`ltrs` mirror the latest
    (selected) month so they match the platform-level totals."""
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
    month_name = calendar.month_name[month_num].upper()

    source = "secondary" if (request.GET.get("source") or "").strip().lower() == "secondary" else "primary"

    head_in = (request.GET.get("head") or "premium").strip().lower()
    head_sql = "COMMODITY" if head_in == "commodity" else "PREMIUM"
    head_key = "commodity" if head_in == "commodity" else "premium"

    dimension = "category" if (request.GET.get("dimension") or "").strip().lower() == "category" else "sub_category"
    dim_col = dimension

    name = (request.GET.get("name") or "").strip()
    is_uncat = name == "" or name.upper() == "UNCATEGORIZED"
    name_u = name.upper()

    platform = (request.GET.get("platform") or "").strip().lower()
    if not platform:
        return Response({"detail": "platform is required."}, status=400)

    is_amazon = platform == "amazon"
    if source == "primary":
        fmt = _CATEGORY_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper())
    else:
        fmt = _SEC_SLUG_TO_FORMAT.get(platform, platform.replace("_", " ").upper())

    try:
        months_n = int(request.GET.get("months") or 1)
    except (TypeError, ValueError):
        months_n = 1
    months_n = max(1, min(months_n, 6))

    # Trailing N months ending at the selected month (oldest → newest).
    month_list = _trailing_months(month_num, year, months_n)

    month_meta = []
    sku_map = {}  # code -> {code, name, brand, by_month: {key: {units, ltrs}}}
    errors = []
    with connection.cursor() as cur:
        for m, y, mname in month_list:
            key_m = f"{y:04d}-{m:02d}"
            month_meta.append({
                "key": key_m,
                "month": m,
                "year": y,
                "label": f"{calendar.month_abbr[m]} {y}",
            })
            try:
                month_rows = _category_sku_rows(
                    cur,
                    source=source,
                    is_amazon=is_amazon,
                    fmt=fmt,
                    head_sql=head_sql,
                    month_num=m,
                    month_name=mname,
                    year=y,
                    dim_col=dim_col,
                    is_uncat=is_uncat,
                    name_u=name_u,
                )
            except Exception as e:  # noqa: BLE001
                errors.append({"month": key_m, "error": str(e)})
                continue
            for code, sku_name, brand, units, ltrs in month_rows:
                u = float(units or 0)
                litres = float(ltrs or 0)
                if u == 0 and litres == 0:
                    continue
                rec = sku_map.get(code)
                if rec is None:
                    rec = {"code": code, "name": sku_name or "", "brand": brand or "", "by_month": {}}
                    sku_map[code] = rec
                if sku_name:
                    rec["name"] = sku_name
                if brand:
                    rec["brand"] = brand
                cell = rec["by_month"].setdefault(key_m, {"units": 0.0, "ltrs": 0.0})
                cell["units"] += u
                cell["ltrs"] += litres

    latest_key = month_meta[-1]["key"] if month_meta else None
    skus = []
    for rec in sku_map.values():
        by_month = {}
        for mm in month_meta:
            cell = rec["by_month"].get(mm["key"]) or {"units": 0.0, "ltrs": 0.0}
            by_month[mm["key"]] = {
                "units": round(cell["units"], 2),
                "ltrs": round(cell["ltrs"], 2),
            }
        cur_cell = by_month.get(latest_key) or {"units": 0.0, "ltrs": 0.0}
        skus.append({
            "code": rec["code"],
            "name": rec["name"],
            "brand": rec["brand"],
            "units": cur_cell["units"],
            "ltrs": cur_cell["ltrs"],
            "by_month": by_month,
            # Sort by total litres across the window so a SKU that was big last
            # month stays visible even if it sold nothing this month.
            "_sort": sum(c["ltrs"] for c in by_month.values()),
        })
    skus.sort(key=lambda s: (s["_sort"], s["ltrs"]), reverse=True)
    for s in skus:
        del s["_sort"]

    return Response({
        "source": source,
        "head": head_key,
        "dimension": dimension,
        "name": name or "Uncategorized",
        "platform": platform,
        "month": month_num,
        "year": year,
        "months": month_meta,
        "skus": skus,
        "total_units": round(sum(s["units"] for s in skus), 2),
        "total_ltrs": round(sum(s["ltrs"] for s in skus), 2),
        "errors": errors,
    })


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
@cached_get(timeout=120, prefix="dash.category_trend")
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
                secmaster_formats = ("BLINKIT", "SWIGGY", "ZEPTO", "BIG BASKET", "FLIPKART")
                secmaster_fmt = fmt and fmt.upper() in secmaster_formats
                target_ph = ", ".join(["(%s, %s)"] * len(num_year_pairs))
                target_sql = f"""
                    SELECT year::int AS yr, month::int AS mon,
                           UPPER(TRIM(item_head::text)) AS head,
                           COALESCE(SUM(done_ltrs), 0) AS ltrs
                    FROM month_targets
                    WHERE (month, year) IN ({target_ph})
                      AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                """
                target_params = [v for pair in num_year_pairs for v in pair]
                if fmt:
                    target_sql += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                    target_params.append(fmt)
                    if secmaster_fmt:
                        target_sql += " AND month = %s AND year = %s"
                        target_params.extend([end_month, end_year])
                else:
                    excluded_formats = (*secmaster_formats, "AMAZON")
                    fmt_ph = ", ".join(["%s"] * len(excluded_formats))
                    target_sql += (
                        f" AND (UPPER(TRIM(format::text)) NOT IN ({fmt_ph}) "
                        "OR (month = %s AND year = %s AND UPPER(TRIM(format::text)) <> 'AMAZON'))"
                    )
                    target_params.extend([*excluded_formats, end_month, end_year])
                target_sql += " GROUP BY 1, 2, 3"
                run("month_targets", target_sql, target_params)

                mat_window = [(m, y, mon) for (m, y, mon) in window if not (m == end_month and y == end_year)]
                if mat_window and (not fmt or secmaster_fmt):
                    mat_pairs = [(mon, y) for (m, y, mon) in mat_window]
                    mat_ph = ", ".join(["(%s, %s)"] * len(mat_pairs))
                    mat_sql = f"""
                        SELECT year::int AS yr, UPPER(TRIM(month::text)) AS mon,
                               UPPER(TRIM(item_head::text)) AS head,
                               COALESCE(SUM(ltr_sold), 0) AS ltrs
                        FROM "SecMaster_Mat"
                        WHERE (UPPER(TRIM(month::text)), year::numeric) IN ({mat_ph})
                          AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                    """
                    mat_params = [v for pair in mat_pairs for v in pair]
                    if fmt:
                        mat_sql += " AND LOWER(TRIM(format::text)) = LOWER(%s)"
                        mat_params.append(fmt)
                    else:
                        mat_sql += " AND UPPER(TRIM(format::text)) <> 'AMAZON'"
                    mat_sql += " GROUP BY 1, 2, 3"
                    run("secmaster_mat", mat_sql, mat_params)
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


# --- /secondary-yoy-growth ---
# Secondary YOY comparison for the Home page. This intentionally excludes
# JioMart and B2B primary-style channels (Zomato/CityMall). Swiggy is sourced
# from SecMaster here, even though its detailed platform dashboard uses swiggySec.
_SECONDARY_YOY_PLATFORMS = (
    ("amazon", "Amazon"),
    ("amazon_mp", "Amazon MP"),
    ("blinkit", "Blinkit"),
    ("swiggy", "Swiggy"),
    ("zepto", "Zepto"),
    ("bigbasket", "BigBasket"),
    ("flipkart", "Flipkart"),
    ("flipkart_grocery", "Flipkart Grocery"),
)

_SECONDARY_YOY_SECM_FORMATS = {
    "blinkit": "blinkit",
    "swiggy": "swiggy",
    "zepto": "zepto",
    "bigbasket": "bigbasket",
}


def _secondary_yoy_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _secondary_yoy_date(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def _secondary_yoy_growth(current, previous):
    current_num = _secondary_yoy_float(current)
    previous_num = _secondary_yoy_float(previous)
    if current_num is None or previous_num is None or previous_num <= 0:
        return None
    return round(((current_num - previous_num) / previous_num) * 100, 2)


def _secondary_yoy_empty_cell():
    return {
        "actual": None,
        "value": None,
        "units": None,
        "has_data": False,
        "growth_pct": None,
        "projection": None,
        "elapsed_day": None,
        "days_in_month": None,
        "max_date": None,
    }


def _secondary_yoy_pick_platforms(raw_platform: str | None):
    allowed = {slug for slug, _ in _SECONDARY_YOY_PLATFORMS}
    platform = (raw_platform or "").strip().lower()
    if platform and platform in allowed:
        return [
            (slug, name)
            for slug, name in _SECONDARY_YOY_PLATFORMS
            if slug == platform
        ]
    return list(_SECONDARY_YOY_PLATFORMS)


def _secondary_yoy_month_year(params) -> tuple[int, int, bool, list[dict]]:
    errors: list[dict] = []
    today = date.today()
    raw_month = str(params.get("month") or "").strip()
    raw_year = str(params.get("year") or "").strip()

    if re.fullmatch(r"\d{4}-\d{2}", raw_month) and not raw_year:
        raw_year, raw_month = raw_month.split("-")

    if raw_month and raw_year:
        try:
            month = int(raw_month)
            year = int(raw_year)
            if 1 <= month <= 12 and 2000 <= year <= 2100:
                return month, year, False, errors
        except (TypeError, ValueError):
            pass

    latest = None

    def consider(label, sql, sql_params=None):
        nonlocal latest
        try:
            with connection.cursor() as cur:
                cur.execute(sql, sql_params or [])
                row = cur.fetchone()
            candidate = row[0] if row else None
            if candidate and (latest is None or candidate > latest):
                latest = candidate
        except Exception as exc:  # noqa: BLE001
            errors.append({"source": label, "error": str(exc)})

    secmaster_date_expr = """
        CASE
            WHEN REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
             AND TRIM("real_date"::text) ~ '^\\d{2}-\\d{2}-\\d{4}$'
                THEN TO_DATE(TRIM("real_date"::text), 'DD-MM-YYYY')
            WHEN REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
             AND TRIM("real_date"::text) ~ '^\\d{4}-\\d{2}-\\d{2}$'
                THEN TRIM("real_date"::text)::date
            ELSE "date"
        END
    """
    consider(
        "secmaster",
        f"""
        SELECT MAX(({secmaster_date_expr})::date)
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g')
              IN ('blinkit', 'swiggy', 'zepto', 'bigbasket')
        """,
    )
    consider(
        "amazon_sec_range_master_view",
        'SELECT MAX("to_date"::date) FROM "amazon_sec_range_master_view"',
    )
    consider(
        "amazon_mp_master",
        """
        SELECT MAX(
            CASE
                WHEN "shipment_date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}'
                    THEN to_timestamp("shipment_date", 'DD/MM/YY HH24:MI')::date
                WHEN "shipment_date" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN "shipment_date"::date
                WHEN "shipment_date" ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}'
                    THEN to_date("shipment_date", 'DD-MM-YYYY')
                ELSE NULL
            END
        )
        FROM "amazon_mp_master"
        """,
    )
    consider(
        "flipkart_secondary_all",
        'SELECT MAX("Order Date"::date) FROM "flipkart_secondary_all"',
    )
    consider(
        "flipkart_grocery_master",
        'SELECT MAX("real_date"::date) FROM "flipkart_grocery_master"',
    )

    if latest:
        return latest.month, latest.year, True, errors
    return today.month, today.year, True, errors


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.secondary_yoy_growth")
def secondary_yoy_growth(request):
    month, anchor_year, defaulted, errors = _secondary_yoy_month_year(request.GET)
    month_name = calendar.month_name[month].upper()
    years = [anchor_year - 2, anchor_year - 1, anchor_year]
    days_in_month = calendar.monthrange(anchor_year, month)[1]
    platform_filter = (request.GET.get("platform") or "").strip().lower() or None
    selected_platforms = _secondary_yoy_pick_platforms(platform_filter)
    selected_slugs = {slug for slug, _ in selected_platforms}
    cells: dict[str, dict[int, dict]] = defaultdict(dict)

    def put(slug, year, actual, value, units, max_date, source):
        if slug not in selected_slugs:
            return
        actual_num = _secondary_yoy_float(actual)
        max_date_value = _secondary_yoy_date(max_date)
        cell = {
            "actual": actual_num,
            "value": _secondary_yoy_float(value),
            "units": _secondary_yoy_float(units),
            "has_data": actual_num is not None and actual_num != 0,
            "growth_pct": None,
            "projection": None,
            "elapsed_day": None,
            "days_in_month": None,
            "max_date": max_date_value,
            "source": source,
        }
        if year == anchor_year and actual_num is not None:
            elapsed_day = None
            if hasattr(max_date, "day"):
                elapsed_day = max_date.day
            elif isinstance(max_date_value, str):
                match = re.match(r"^\d{4}-\d{2}-(\d{2})", max_date_value)
                if match:
                    elapsed_day = int(match.group(1))
            if elapsed_day:
                cell["elapsed_day"] = elapsed_day
                cell["days_in_month"] = days_in_month
                cell["projection"] = round(
                    (actual_num / elapsed_day) * days_in_month,
                    2,
                )
        cells[slug][int(year)] = cell

    def run(label, sql, params, handler):
        try:
            with connection.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            for row in rows:
                handler(row)
        except Exception as exc:  # noqa: BLE001
            errors.append({"source": label, "error": str(exc)})

    if selected_slugs & set(_SECONDARY_YOY_SECM_FORMATS):
        format_to_slug = {
            fmt: slug for slug, fmt in _SECONDARY_YOY_SECM_FORMATS.items()
        }
        secmaster_date_expr = """
            CASE
                WHEN REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
                 AND TRIM("real_date"::text) ~ '^\\d{2}-\\d{2}-\\d{4}$'
                    THEN TO_DATE(TRIM("real_date"::text), 'DD-MM-YYYY')
                WHEN REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = 'zepto'
                 AND TRIM("real_date"::text) ~ '^\\d{4}-\\d{2}-\\d{2}$'
                    THEN TRIM("real_date"::text)::date
                ELSE "date"
            END
        """
        fmt_values = [
            _SECONDARY_YOY_SECM_FORMATS[slug]
            for slug in selected_slugs
            if slug in _SECONDARY_YOY_SECM_FORMATS
        ]
        fmt_placeholders = ", ".join(["%s"] * len(fmt_values))
        year_placeholders = ", ".join(["%s"] * len(years))
        run(
            "secmaster",
            f"""
            SELECT
                REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') AS fmt,
                "year"::int AS yr,
                COALESCE(SUM("ltr_sold"), 0) AS ltrs,
                COALESCE(
                    NULLIF(SUM("sales_amt_exc"), 0),
                    NULLIF(SUM("sales_amt"), 0),
                    SUM("amount"),
                    0
                ) AS value,
                COALESCE(SUM("quantity"), 0) AS units,
                MAX(({secmaster_date_expr})::date) AS max_date
            FROM "SecMaster"
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g')
                  IN ({fmt_placeholders})
              AND UPPER(TRIM("month"::text)) = %s
              AND "year"::numeric IN ({year_placeholders})
            GROUP BY 1, 2
            """,
            [*fmt_values, month_name, *years],
            lambda row: put(
                format_to_slug.get(row[0]),
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                "SecMaster",
            ),
        )

    if "amazon" in selected_slugs:
        year_placeholders = ", ".join(["%s"] * len(years))
        run(
            "amazon_sec_range_master_view",
            f"""
            WITH base AS (
                SELECT
                    "year"::int AS yr,
                    "to_date"::date AS to_date,
                    COALESCE("shipped_litres", 0) AS ltrs,
                    COALESCE("calculated_shipped_revenue", 0) AS value,
                    COALESCE("shipped_units", 0) AS units
                FROM "amazon_sec_range_master_view"
                WHERE UPPER(TRIM("month"::text)) = %s
                  AND "year"::int IN ({year_placeholders})
            ),
            latest AS (
                SELECT yr, MAX(to_date) AS max_date
                FROM base
                GROUP BY yr
            )
            SELECT b.yr, COALESCE(SUM(b.ltrs), 0), COALESCE(SUM(b.value), 0),
                   COALESCE(SUM(b.units), 0), l.max_date
            FROM base b
            JOIN latest l ON l.yr = b.yr AND l.max_date = b.to_date
            GROUP BY b.yr, l.max_date
            """,
            [month_name, *years],
            lambda row: put(
                "amazon",
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                "amazon_sec_range_master_view",
            ),
        )

    if "amazon_mp" in selected_slugs:
        year_placeholders = ", ".join(["%s"] * len(years))
        run(
            "amazon_mp_master",
            f"""
            SELECT
                "shipment_year"::int AS yr,
                COALESCE(SUM("delivered_ltr"), 0) AS ltrs,
                NULL::numeric AS value,
                COALESCE(SUM("quantity"), 0) AS units,
                MAX(
                    CASE
                        WHEN "shipment_date" ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{2}}'
                            THEN to_timestamp("shipment_date", 'DD/MM/YY HH24:MI')::date
                        WHEN "shipment_date" ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN "shipment_date"::date
                        WHEN "shipment_date" ~ '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}'
                            THEN to_date("shipment_date", 'DD-MM-YYYY')
                        ELSE NULL
                    END
                ) AS max_date
            FROM "amazon_mp_master"
            WHERE UPPER(TRIM("shipment_month"::text)) = %s
              AND "shipment_year"::int IN ({year_placeholders})
            GROUP BY "shipment_year"::int
            """,
            [month_name, *years],
            lambda row: put(
                "amazon_mp",
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                "amazon_mp_master",
            ),
        )

    if "flipkart" in selected_slugs:
        year_placeholders = ", ".join(["%s"] * len(years))
        run(
            "flipkart_secondary_all",
            f"""
            SELECT
                "year"::int AS yr,
                COALESCE(SUM("ltr_sold"), 0) AS ltrs,
                COALESCE(SUM("Final Sale Amount"), 0) AS value,
                COALESCE(SUM("Final Sale Units"), 0) AS units,
                MAX("Order Date"::date) AS max_date
            FROM "flipkart_secondary_all"
            WHERE UPPER(TRIM("month"::text)) = %s
              AND "year"::int IN ({year_placeholders})
            GROUP BY "year"::int
            """,
            [month_name, *years],
            lambda row: put(
                "flipkart",
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                "flipkart_secondary_all",
            ),
        )

    if "flipkart_grocery" in selected_slugs:
        year_placeholders = ", ".join(["%s"] * len(years))
        run(
            "flipkart_grocery_master",
            f"""
            SELECT
                "year"::int AS yr,
                COALESCE(SUM("ltr_sold"), 0) AS ltrs,
                COALESCE(SUM("sale_amt_exclusive"), 0) AS value,
                COALESCE(SUM("qty"), 0) AS units,
                MAX("real_date"::date) AS max_date
            FROM "flipkart_grocery_master"
            WHERE "month"::int = %s
              AND "year"::int IN ({year_placeholders})
            GROUP BY "year"::int
            """,
            [month, *years],
            lambda row: put(
                "flipkart_grocery",
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                "flipkart_grocery_master",
            ),
        )

    rows = []
    total_by_year = {
        year: {
            "actual": None,
            "projection": None,
            "growth_pct": None,
            "has_data": False,
        }
        for year in years
    }

    for slug, name in selected_platforms:
        values = {}
        for year in years:
            cell = cells.get(slug, {}).get(year, _secondary_yoy_empty_cell()).copy()
            if cell["actual"] is not None:
                total_by_year[year]["actual"] = (
                    total_by_year[year]["actual"] or 0
                ) + cell["actual"]
                total_by_year[year]["has_data"] = True
            if year == anchor_year and cell.get("projection") is not None:
                total_by_year[year]["projection"] = (
                    total_by_year[year]["projection"] or 0
                ) + cell["projection"]
            values[str(year)] = cell
        for index, year in enumerate(years):
            if index == 0:
                continue
            values[str(year)]["growth_pct"] = _secondary_yoy_growth(
                values[str(year)]["actual"],
                values[str(years[index - 1])]["actual"],
            )
        rows.append({"slug": slug, "name": name, "values": values})

    for index, year in enumerate(years):
        if index == 0:
            continue
        total_by_year[year]["growth_pct"] = _secondary_yoy_growth(
            total_by_year[year]["actual"],
            total_by_year[years[index - 1]]["actual"],
        )

    return Response({
        "source": "secondary",
        "metric": "ltrs",
        "anchor_month": month,
        "anchor_month_label": calendar.month_name[month],
        "anchor_year": anchor_year,
        "defaulted_to_latest": defaulted,
        "years": years,
        "rows": rows,
        "totals": {str(year): total_by_year[year] for year in years},
        "errors": errors,
    })


# --- /fulfilment-health ---
@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=120, prefix="dash.fulfilment_health")
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
@cached_get(timeout=120, prefix="dash.top_skus")
def top_skus(request):
    """Top SKUs by delivered litres for a month, with prior-period delta.

    Same source semantics as /category-breakdown. Powers the home "Top Movers"
    leaderboard: current-month top-N SKUs (name + item head + litres) plus each
    SKU's previous-period litres so the UI can show % change and risers/fallers.
    Honours the platform filter. compare_months selects a 1/3/6/12-month
    window ending at the selected month end (or today for the current month),
    compared with the previous same-length month window."""
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
    # Cap high enough that callers wanting the full SKU list (e.g. the YoY
    # SKU-wise drill-down) get every SKU with data, while the Top Movers
    # leaderboard still asks for just its small N.
    limit = max(1, min(limit, 1000))
    try:
        compare_months = int(request.GET.get("compare_months") or 0)
    except (TypeError, ValueError):
        compare_months = 0
    if compare_months not in (1, 3, 6, 12):
        try:
            compare_days = int(request.GET.get("compare_days") or 0)
        except (TypeError, ValueError):
            compare_days = 0
        compare_months = {30: 1, 90: 3, 180: 6, 365: 12}.get(compare_days, 0)

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
        # rows: (name, head, ltrs[, code[, brand]]) — code and brand are optional
        # extra columns the secondary queries supply. Rows are keyed by
        # (name, brand) so the same product under two brands (e.g. Jivo vs Sano
        # Canola) stays as two separate rows instead of being merged.
        for row in rows:
            name_val, head_val, ltrs = row[0], row[1], row[2]
            code_val = row[3] if len(row) > 3 else None
            brand_val = row[4] if len(row) > 4 else None
            val = float(ltrs or 0)
            if val == 0:
                continue
            name = (str(name_val).strip() if name_val else "") or "Unknown"
            head = (str(head_val).strip().upper() if head_val else "")
            if head not in ("PREMIUM", "COMMODITY"):
                head = "OTHER"
            code = (str(code_val).strip() if code_val else "") or None
            brand = (str(brand_val).strip() if brand_val else "") or None
            key = (name.upper(), (brand or "").upper())
            slot = dest.get(key)
            if slot is None:
                dest[key] = {
                    "name": name,
                    "head": head,
                    "code": code,
                    "brand": brand,
                    "ltrs": val,
                }
            else:
                slot["ltrs"] += val
                if not slot.get("code") and code:
                    slot["code"] = code

    def run(label, dest, sql, params):
        try:
            cur.execute(sql, params)
            absorb(dest, cur.fetchall())
        except Exception as e:  # noqa: BLE001
            errors.append({"source": label, "error": str(e)})

    def _month_start_months_before(dt, months_before):
        month_index = (dt.year * 12 + dt.month - 1) - months_before
        return date(month_index // 12, month_index % 12 + 1, 1)

    def _period_windows():
        last_day = calendar.monthrange(year, month_num)[1]
        selected_end = date(year, month_num, last_day)
        current_month_start = date(today.year, today.month, 1)
        selected_month_start = date(year, month_num, 1)
        if selected_month_start >= current_month_start:
            selected_end = min(selected_end, today)
        current_start = _month_start_months_before(
            selected_end,
            compare_months - 1,
        )
        previous_end = current_start - timedelta(days=1)
        previous_start = _month_start_months_before(
            previous_end,
            compare_months - 1,
        )
        return current_start, selected_end, previous_start, previous_end

    if compare_months:
        current_start, current_end, previous_start, previous_end = _period_windows()
        acc_days = {"current": {}, "previous": {}}

        with connection.cursor() as cur:
            for bucket, start_dt, end_dt in (
                ("current", current_start, current_end),
                ("previous", previous_start, previous_end),
            ):
                dest = acc_days[bucket]
                if source == "primary":
                    if use_other:
                        sql = """
                            SELECT COALESCE(NULLIF(TRIM(item::text), ''),
                                            NULLIF(TRIM(sku_name::text), ''), 'Unknown') AS name,
                                   UPPER(TRIM(item_head::text)) AS head,
                                   COALESCE(SUM(total_delivered_liters), 0) AS ltrs
                            FROM public.master_po
                            WHERE public._pm_parse_date(delivery_date::text) >= %s
                              AND public._pm_parse_date(delivery_date::text) <= %s
                              AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                        """
                        params = [start_dt, end_dt]
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
                            WHERE public._pm_parse_date(order_date::text) >= %s
                              AND public._pm_parse_date(order_date::text) <= %s
                              AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                            GROUP BY 1, 2
                        """, [start_dt, end_dt])
                else:
                    if use_other:
                        sql = """
                            SELECT COALESCE(NULLIF(TRIM(item::text), ''), 'Unknown') AS name,
                                   UPPER(TRIM(item_head::text)) AS head,
                                   COALESCE(SUM(ltr_sold), 0) AS ltrs
                            FROM "SecMaster"
                            WHERE "date" >= %s
                              AND "date" <= %s
                              AND UPPER(TRIM(item_head::text)) IN ('PREMIUM', 'COMMODITY')
                        """
                        params = [start_dt, end_dt]
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
                                       COALESCE(NULLIF(TRIM(item::text), ''),
                                                NULLIF(TRIM(product_name::text), '')) AS name
                                FROM master_sheet
                                WHERE format_sku_code IS NOT NULL AND format_sku_code::text <> ''
                                ORDER BY format_sku_code
                            )
                            SELECT COALESCE(ml.name, r.asin) AS name,
                                   UPPER(TRIM(ml.item_head::text)) AS head,
                                   COALESCE(SUM(COALESCE(r.shipped_units, 0) * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs
                            FROM amazon_sec_range r
                            JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(r.asin::text))
                            WHERE r.to_date::date >= %s
                              AND r.to_date::date <= %s
                              AND UPPER(TRIM(ml.item_head::text)) IN ('PREMIUM', 'COMMODITY')
                            GROUP BY 1, 2
                        """, [start_dt, end_dt])

        cur_map = acc_days["current"]
        prev_map = acc_days["previous"]
        def build_sku_delta_rows(rows):
            out = []
            for s in rows:
                prev = prev_map.get(
                    (s["name"].upper(), (s.get("brand") or "").upper())
                )
                prev_ltrs = round(prev["ltrs"], 2) if prev else 0.0
                ltrs = round(s["ltrs"], 2)
                if prev_ltrs > 0:
                    delta_pct = round((ltrs - prev_ltrs) / prev_ltrs * 100, 1)
                else:
                    delta_pct = None
                out.append({
                    "name": s["name"],
                    "head": s["head"],
                    "code": s.get("code"),
                    "brand": s.get("brand"),
                    "ltrs": ltrs,
                    "prev_ltrs": prev_ltrs,
                    "delta_pct": delta_pct,
                    "is_new": prev is None,
                })
            return out

        ranked = sorted(cur_map.values(), key=lambda s: s["ltrs"], reverse=True)[:limit]
        skus = build_sku_delta_rows(ranked)
        all_skus = build_sku_delta_rows(cur_map.values())

        movers = [s for s in all_skus if s["delta_pct"] is not None]
        risers = [s for s in movers if s["delta_pct"] > 0]
        fallers = [s for s in movers if s["delta_pct"] < 0]
        top_riser = max(risers, key=lambda s: s["delta_pct"], default=None)
        top_faller = min(fallers, key=lambda s: s["delta_pct"], default=None)
        return Response({
            "source": source, "platform": platform,
            "month": month_num, "year": year,
            "compare_months": compare_months,
            "window": {
                "current_start": current_start.isoformat(),
                "current_end": current_end.isoformat(),
                "previous_start": previous_start.isoformat(),
                "previous_end": previous_end.isoformat(),
            },
            "skus": skus, "top_riser": top_riser, "top_faller": top_faller,
            "errors": errors,
        })

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
                               COALESCE(SUM(ltr_sold), 0) AS ltrs,
                               MAX(NULLIF(TRIM(sku_code::text), '')) AS code,
                               NULLIF(TRIM(brand::text), '') AS brand
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
                    sql += " GROUP BY 1, 2, 5"
                    run("secmaster", dest, sql, params)
                if use_amazon:
                    run("amazon_sec_range", dest, """
                        WITH ml AS (
                            SELECT DISTINCT ON (format_sku_code)
                                   format_sku_code, item_head, per_unit_value,
                                   NULLIF(TRIM(brand::text), '') AS brand,
                                   COALESCE(NULLIF(TRIM(item::text), ''),
                                            NULLIF(TRIM(product_name::text), '')) AS name
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
                               COALESCE(SUM(b.units * COALESCE(ml.per_unit_value::numeric, 0)), 0) AS ltrs,
                               MAX(b.asin) AS code,
                               ml.brand AS brand
                        FROM base b
                        CROSS JOIN latest l
                        JOIN ml ON UPPER(TRIM(ml.format_sku_code::text)) = UPPER(TRIM(b.asin::text))
                        WHERE b.to_day = l.md
                          AND UPPER(TRIM(ml.item_head::text)) IN ('PREMIUM', 'COMMODITY')
                        GROUP BY 1, 2, ml.brand
                    """, [y, mname])

    cur_map = acc[(month_num, year)]
    prev_map = acc[(prev_month, prev_year)]
    def build_sku_delta_rows(rows):
        out = []
        for s in rows:
            prev = prev_map.get(
                (s["name"].upper(), (s.get("brand") or "").upper())
            )
            prev_ltrs = round(prev["ltrs"], 2) if prev else 0.0
            ltrs = round(s["ltrs"], 2)
            if prev_ltrs > 0:
                delta_pct = round((ltrs - prev_ltrs) / prev_ltrs * 100, 1)
            else:
                delta_pct = None  # no prior baseline -> "NEW"
            out.append({
                "name": s["name"],
                "head": s["head"],
                "code": s.get("code"),
                "brand": s.get("brand"),
                "ltrs": ltrs,
                "prev_ltrs": prev_ltrs,
                "delta_pct": delta_pct,
                "is_new": prev is None,
            })
        return out

    ranked = sorted(cur_map.values(), key=lambda s: s["ltrs"], reverse=True)[:limit]
    skus = build_sku_delta_rows(ranked)
    all_skus = build_sku_delta_rows(cur_map.values())

    # A riser must actually have grown (> 0) and a faller must actually have
    # shrunk (< 0). Use all current SKUs for these callouts so the drop card is
    # not hidden just because the dropping SKU is outside the top-N list.
    movers = [s for s in all_skus if s["delta_pct"] is not None]
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
@cached_get(timeout=120, prefix="dash.platform_expiry_alerts")
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
                    COALESCE(SUM(total_order_amt_exclusive), 0)     AS total_units,
                    COALESCE(SUM(order_qty), 0)                     AS total_order_units
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
                    "total_order_units": float(row[4] or 0),
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
                    "total_order_units": float(row[2] or 0),
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
                        COALESCE(SUM(requested_qty), 0)                         AS total_units,
                        COALESCE(SUM(requested_qty), 0)                         AS total_order_units
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
                        COALESCE(SUM(total_order_amt_exclusive), 0)     AS total_units,
                        COALESCE(SUM(order_qty), 0)                     AS total_order_units
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
    # Status (like remark) is editable on every primary format in the UI, so
    # allow it for all formats — not only the CITY MALL / FLIPKART GROCERY
    # "full manual" ones. GRN date / delivered qty stay restricted to those.
    allowed_columns = set(PRIMARY_REMARK_UPDATE_COLUMNS) | {"status"}
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
        # Match on a normalized format (strip spaces/punctuation) so a platform's
        # two stored spellings — e.g. "BIGBASKET" vs "BIG BASKET" — both resolve
        # to the same rows. The UI's format filter carries both variants but the
        # save only sends the first, so an exact match missed rows stored under
        # the other spelling ("Matching editable row not found").
        return (
            "AND REGEXP_REPLACE(UPPER(TRIM(\"format\"::text)), '[^A-Z0-9]+', '', 'g')"
            " = REGEXP_REPLACE(%s, '[^A-Z0-9]+', '', 'g')",
            [expected_format],
        )
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

    # The edited status / GRN date / delivered qty / remark feeds the master_po
    # materialized view (master_po_mv). Refresh it in the background so this save
    # returns immediately; the dashboards/pendency pick up the change a few
    # seconds later when the rebuild finishes. Best-effort (never raises).
    from platforms.master_po_refresh import refresh_master_po_mv_async
    refresh_master_po_mv_async()

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

    # Refresh the master_po materialized view so the edited status / GRN /
    # delivered / remark shows up on the dashboards and pendency. Done in the
    # background (one refresh after the whole batch) so the save returns right
    # away instead of blocking on the multi-second rebuild. Best-effort.
    if updated:
        from platforms.master_po_refresh import refresh_master_po_mv_async
        refresh_master_po_mv_async()

    return Response({"updated": updated, "rows": saved_rows})
