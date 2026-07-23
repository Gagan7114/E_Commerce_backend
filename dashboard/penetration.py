"""Penetration Report — where is each SKU live, by city and format.

Combines the org-wide sources that carry a city:

* Secondary sell-out — `secmaster_mv` (materialized "SecMaster"): rows for the
  selected month/year with a non-blank city. Jio Mart is excluded on purpose
  (user call, 2026-07-21), and Amazon is excluded here because it has its own
  city feed below.
* Amazon city-wise — `amazon_sec_city` (ship-to city feed, ASIN joined to
  master_sheet for item/category, same recipe as /state-sales) → format
  "AMAZON".
* Amazon MP — `amazon_mp_master` (GST MTR ship_to_city; qty/litres summed
  GROSS with ABS, matching the MP dashboard's Done Unit/Ltr convention) →
  format "AMAZON MP".
* Inventory — `all_platform_inventory` (Jio Mart excluded): each format's
  LATEST snapshot date inside the selected month (inventory is a daily
  snapshot, so summing the whole month would over-count — same reasoning as
  the cumulative ads masters). Amazon has no city-level inventory feed, so
  AMAZON / AMAZON MP rows can be "selling" but never "live"/"stocked".

The two sides are FULL OUTER JOINed on (format, city, item) so every
combination shows up once with a status:

    live     — selling in that city AND in stock there
    selling  — secondary sales but no stock on the latest snapshot
    stocked  — stock on hand but no secondary sales that month
    inactive — listed rows with zero movement and zero stock

Filters: platform (multi — the SecMaster `format` spellings; the param is
named `platform` because `?format=` is DRF's reserved content-negotiation
param and 404s), item_head (multi), month, year, plus a free-text search and
a status filter for the UI chips. Results are paginated.
"""

import calendar
from datetime import date

from django.db import connection
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require
from config.perf_cache import cached_get

from .views import _blinkit_city_sql, _city_canon_sql

# Same "Item" fallback chain the state-sales drill-down uses: prefer the clean
# catalogue item, fall back to the platform's raw SKU name.
_SEC_ITEM_EXPR = ("COALESCE(NULLIF(TRIM(s.item::text), ''), "
                  "NULLIF(TRIM(s.sku_name::text), ''))")

_PAGE_SIZE_DEFAULT = 100
_PAGE_SIZE_MAX = 100000  # export asks for everything in one go

# Reference denominator for the coverage ratio card: total cities/towns in
# India per Census 2011 (4,041 statutory towns + 3,894 census towns = 7,935).
# Deliberately NOT the city_state_mapping table — that is a 127k locality list
# (villages included) and would make any coverage % read as ~0. Echoed to the
# frontend so the number lives in exactly one place.
#
# This all-India figure is only a sensible denominator for parcel-delivery
# formats (Amazon / Amazon MP can ship to any town) and for the mixed
# "All formats" view. Dark-store platforms only OPERATE in a limited city set
# (Blinkit ~200, Zepto ~70, Big Basket ~46 clusters), so dividing their
# covered cities by 7,935 read as "2.3% covered" and looked broken. When the
# format filter selects only QC platforms the denominator switches to those
# platforms' own city universe — see `_universe_denominator`.
INDIA_TOTAL_CITIES = 7935

# Formats whose reach is parcel shipping (any Indian town), not dark stores.
# They keep the all-India denominator and never use the platform universe.
_PARCEL_FORMATS = ("AMAZON", "AMAZON MP")

_STATUSES = ("live", "selling", "stocked", "inactive")

# Sortable columns (?sort= & ?dir=asc|desc), whitelisted so they are safe to
# interpolate into ORDER BY. Detail mode sorts pen_joined columns directly;
# grouped mode maps UI keys onto the roll-up query's SELECT aliases.
_DETAIL_SORT_COLS = {
    "city", "item", "category", "sub_category", "format", "item_head",
    "sec_qty", "sec_ltr", "inv_qty", "inv_ltr", "status",
}
_GROUP_SORT_COLS = {
    "key": "key", "count": "total_other", "live": "live", "selling": "selling",
    "stocked": "stocked", "inactive": "inactive", "sec_qty": "sec_qty",
    "sec_ltr": "sec_ltr", "inv_qty": "inv_qty", "inv_ltr": "inv_ltr",
}


def _int_param(request, name, default):
    try:
        return int(request.GET.get(name) or default)
    except (TypeError, ValueError):
        return default


def _month_year(request, today):
    month = _int_param(request, "month", today.month)
    year = _int_param(request, "year", today.year)
    if not 1 <= month <= 12:
        month = today.month
    if not 2000 <= year <= 2100:
        year = today.year
    return month, year


def _multi_param(request, name):
    """Repeatable query param → distinct UPPER-trimmed values ('all' dropped)."""
    out = []
    for raw in request.GET.getlist(name):
        v = (raw or "").strip().upper()
        if v and v != "ALL" and v not in out:
            out.append(v)
    return out


def _base_sql(month, year, month_start, month_end, fmts, heads):
    """(sql, params) for the joined sec × inventory CTE chain.

    The secondary side is a UNION ALL of three city-bearing feeds (SecMaster
    QC platforms, Amazon city-wise, Amazon MP); a format filter includes or
    drops whole branches. Placeholders are appended in exactly the order the
    SQL mentions them."""
    sec_city = _city_canon_sql("s.city")
    az_city = _city_canon_sql("a.city")
    mp_city = _city_canon_sql("mp.ship_to_city")
    # Blinkit inventory `location` is a warehouse code (CPC-GGN4, "Bengaluru B3 -
    # Feeder Warehouse"), not a city — clean it so it matches the secondary
    # feed's real cities; every other format's location is already a city.
    inv_city = (
        "(CASE WHEN UPPER(TRIM(i.format::text)) = 'BLINKIT' "
        f"THEN {_blinkit_city_sql('i.location')} "
        f"ELSE {_city_canon_sql('i.location')} END)"
    )
    month_name = calendar.month_name[month].upper()

    # Which branches does the format filter keep?
    sec_fmts = [f for f in fmts if f not in ("AMAZON", "AMAZON MP")]
    use_amazon = not fmts or "AMAZON" in fmts
    use_mp = not fmts or "AMAZON MP" in fmts

    params = []
    branches = []

    # 1) SecMaster QC platforms (Amazon has its own feed; Jio Mart removed).
    if not fmts or sec_fmts:
        b = f"""
        SELECT UPPER(TRIM(s.format::text))                 AS format,
               {sec_city}                                  AS city,
               UPPER(TRIM({_SEC_ITEM_EXPR}))               AS item_key,
               TRIM({_SEC_ITEM_EXPR})                      AS item,
               NULLIF(TRIM(s.category::text), '')          AS category,
               NULLIF(TRIM(s.sub_category::text), '')      AS sub_category,
               NULLIF(TRIM(s.item_head::text), '')         AS item_head,
               COALESCE(s.quantity, 0)                     AS qty,
               COALESCE(s.ltr_sold, 0)                     AS ltr
        FROM secmaster_mv s
        WHERE NULLIF(TRIM(s.city::text), '') IS NOT NULL
          AND {_SEC_ITEM_EXPR} IS NOT NULL
          AND UPPER(TRIM(s.format::text)) NOT IN ('AMAZON', 'JIO MART')
          AND UPPER(TRIM(s.month::text)) = %s
          AND s.year::numeric = %s
        """
        params += [month_name, year]
        if sec_fmts:
            b += " AND UPPER(TRIM(s.format::text)) = ANY(%s)"
            params.append(sec_fmts)
        if heads:
            b += " AND UPPER(TRIM(s.item_head::text)) = ANY(%s)"
            params.append(heads)
        branches.append(b)

    # 2) Amazon city-wise feed (ship-to city; catalogue via master_sheet ASIN).
    if use_amazon:
        az_name = "COALESCE(NULLIF(TRIM(m.item::text), ''), TRIM(a.asin::text))"
        b = f"""
        SELECT 'AMAZON'                                    AS format,
               {az_city}                                   AS city,
               UPPER(TRIM({az_name}))                      AS item_key,
               TRIM({az_name})                             AS item,
               NULLIF(TRIM(m.category::text), '')          AS category,
               NULLIF(TRIM(m.sub_category::text), '')      AS sub_category,
               NULLIF(TRIM(m.item_head::text), '')         AS item_head,
               COALESCE(a.shipped_units, 0)                AS qty,
               CASE WHEN UPPER(TRIM(m.is_litre::text)) = 'Y'
                    THEN COALESCE(a.shipped_units::numeric * m.per_unit_value, 0)
                    ELSE 0 END                             AS ltr
        FROM public.amazon_sec_city a
        LEFT JOIN public.master_sheet m
          ON UPPER(TRIM(m.format_sku_code::text)) = UPPER(TRIM(a.asin::text))
         AND UPPER(TRIM(m.format::text)) = 'AMAZON'
        WHERE NULLIF(TRIM(a.city::text), '') IS NOT NULL
          AND EXTRACT(MONTH FROM a.from_date) = %s
          AND EXTRACT(YEAR FROM a.from_date) = %s
        """
        params += [month, year]
        if heads:
            b += " AND UPPER(TRIM(m.item_head::text)) = ANY(%s)"
            params.append(heads)
        branches.append(b)

    # 3) Amazon MP (GST MTR). Qty/litres gross (ABS) like the MP dashboard;
    #    master_sheet joined only for the clean catalogue item name.
    if use_mp:
        mp_name = ("COALESCE(NULLIF(TRIM(m.item::text), ''), "
                   "NULLIF(TRIM(mp.item_description::text), ''))")
        b = f"""
        SELECT 'AMAZON MP'                                 AS format,
               {mp_city}                                   AS city,
               UPPER(TRIM({mp_name}))                      AS item_key,
               TRIM({mp_name})                             AS item,
               NULLIF(TRIM(mp.category::text), '')         AS category,
               NULLIF(TRIM(mp.sub_category::text), '')     AS sub_category,
               NULLIF(TRIM(mp.item_head::text), '')        AS item_head,
               ABS(COALESCE(mp.quantity, 0))               AS qty,
               ABS(COALESCE(mp.delivered_ltr, 0))          AS ltr
        FROM public.amazon_mp_master mp
        LEFT JOIN public.master_sheet m
          ON UPPER(TRIM(m.format_sku_code::text)) = UPPER(TRIM(mp.asin::text))
         AND UPPER(TRIM(m.format::text)) = 'AMAZON'
        WHERE NULLIF(TRIM(mp.ship_to_city::text), '') IS NOT NULL
          AND {mp_name} IS NOT NULL
          AND UPPER(TRIM(mp.shipment_month::text)) = %s
          AND mp.shipment_year = %s
        """
        params += [month_name, year]
        if heads:
            b += " AND UPPER(TRIM(mp.item_head::text)) = ANY(%s)"
            params.append(heads)
        branches.append(b)

    if not branches:  # format filter excluded everything secondary-side
        branches.append("""
        SELECT NULL::text AS format, NULL::text AS city, NULL::text AS item_key,
               NULL::text AS item, NULL::text AS category, NULL::text AS sub_category,
               NULL::text AS item_head, 0 AS qty, 0 AS ltr
        WHERE FALSE
        """)

    union_sql = " UNION ALL ".join(branches)

    inv_where = ""
    if fmts:
        inv_where += " AND UPPER(TRIM(i.format::text)) = ANY(%s)"
    if heads:
        inv_where += " AND UPPER(TRIM(i.item_head::text)) = ANY(%s)"

    sql = f"""
    WITH sec AS (
        SELECT format, city, item_key,
               MAX(item)          AS item,
               MAX(category)      AS category,
               MAX(sub_category)  AS sub_category,
               MAX(item_head)     AS item_head,
               COALESCE(SUM(qty), 0) AS sec_qty,
               COALESCE(SUM(ltr), 0) AS sec_ltr
        FROM ({union_sql}) src
        GROUP BY 1, 2, 3
    ),
    inv_latest AS (
        SELECT UPPER(TRIM(format::text)) AS format, MAX(inventory_date) AS latest_date
        FROM all_platform_inventory
        WHERE inventory_date BETWEEN %s AND %s
          AND UPPER(TRIM(format::text)) <> 'JIO MART'
        GROUP BY 1
    ),
    inv AS (
        SELECT UPPER(TRIM(i.format::text))                  AS format,
               {inv_city}                                   AS city,
               UPPER(TRIM(i.item::text))                    AS item_key,
               MAX(TRIM(i.item::text))                      AS item,
               MAX(NULLIF(TRIM(i.category::text), ''))      AS category,
               MAX(NULLIF(TRIM(i.sub_category::text), ''))  AS sub_category,
               MAX(NULLIF(TRIM(i.item_head::text), ''))     AS item_head,
               COALESCE(SUM(i.soh_unit), 0)                 AS inv_qty,
               COALESCE(SUM(i.soh_ltr), 0)                  AS inv_ltr
        FROM all_platform_inventory i
        JOIN inv_latest l
          ON UPPER(TRIM(i.format::text)) = l.format
         AND i.inventory_date = l.latest_date
        WHERE NULLIF(TRIM(i.location::text), '') IS NOT NULL
          AND NULLIF(TRIM(i.item::text), '') IS NOT NULL
          {inv_where}
        GROUP BY 1, 2, 3
    ),
    joined AS (
        SELECT COALESCE(s.format, i.format)                 AS format,
               COALESCE(s.city, i.city)                     AS city,
               COALESCE(s.item, i.item)                     AS item,
               COALESCE(s.category, i.category)             AS category,
               COALESCE(s.sub_category, i.sub_category)     AS sub_category,
               COALESCE(s.item_head, i.item_head)           AS item_head,
               COALESCE(s.sec_qty, 0)                       AS sec_qty,
               COALESCE(s.sec_ltr, 0)                       AS sec_ltr,
               COALESCE(i.inv_qty, 0)                       AS inv_qty,
               COALESCE(i.inv_ltr, 0)                       AS inv_ltr,
               CASE
                 WHEN COALESCE(s.sec_qty, 0) > 0 AND COALESCE(i.inv_qty, 0) > 0 THEN 'live'
                 WHEN COALESCE(s.sec_qty, 0) > 0 THEN 'selling'
                 WHEN COALESCE(i.inv_qty, 0) > 0 THEN 'stocked'
                 ELSE 'inactive'
               END                                          AS status
        FROM sec s
        FULL OUTER JOIN inv i
          ON s.format = i.format
         AND UPPER(s.city) = UPPER(i.city)
         AND s.item_key = i.item_key
    )
    """

    params += [month_start, month_end]
    if fmts:
        params.append(fmts)
    if heads:
        params.append(heads)
    return sql, params


def _universe_cte_sql(fmts, has_upload_tbl):
    """(sql, params) for a `uni(format, city)` CTE — the selected QC platforms'
    serviceable-city universe.

    Two sources, uploaded winning per platform:

    * `up` — rows uploaded into platform_city_universe (the official operating
      city list from each platform's seller portal). Authoritative when present.
    * `der` — derived fallback: every city that platform has EVER shown in
      secondary sales or an inventory snapshot (all months, not just the
      selected one). Zero-maintenance, but it can only see our own footprint —
      cities the platform serves where we never sold are invisible to it.

    A platform with uploaded rows uses ONLY those; platforms without any use
    the derived set. Cities go through the same canonicalisation as the main
    report so covered-vs-universe subtraction lines up spelling-for-spelling.
    Blinkit inventory locations get the warehouse→city cleanup for the same
    reason."""
    uni_city = _city_canon_sql("u.city")
    sec_city = _city_canon_sql("s.city")
    inv_city = (
        "(CASE WHEN UPPER(TRIM(i.format::text)) = 'BLINKIT' "
        f"THEN {_blinkit_city_sql('i.location')} "
        f"ELSE {_city_canon_sql('i.location')} END)"
    )
    params = []
    if has_upload_tbl:
        up_sql = f"""
        SELECT UPPER(TRIM(u.platform::text)) AS format, {uni_city} AS city
        FROM public.platform_city_universe u
        WHERE u.active
          AND NULLIF(TRIM(u.city::text), '') IS NOT NULL
          AND UPPER(TRIM(u.platform::text)) = ANY(%s)
        GROUP BY 1, 2
        """
        params.append(fmts)
    else:  # migration not applied yet — derived fallback only
        up_sql = "SELECT NULL::text AS format, NULL::text AS city WHERE FALSE"
    sql = f"""
    WITH up AS ({up_sql}),
    der AS (
        SELECT UPPER(TRIM(s.format::text)) AS format, {sec_city} AS city
        FROM secmaster_mv s
        WHERE NULLIF(TRIM(s.city::text), '') IS NOT NULL
          AND UPPER(TRIM(s.format::text)) = ANY(%s)
        GROUP BY 1, 2
        UNION
        SELECT UPPER(TRIM(i.format::text)) AS format, {inv_city} AS city
        FROM all_platform_inventory i
        WHERE NULLIF(TRIM(i.location::text), '') IS NOT NULL
          AND UPPER(TRIM(i.format::text)) = ANY(%s)
        GROUP BY 1, 2
    ),
    uni AS (
        SELECT format, city FROM up
        UNION
        SELECT format, city FROM der
        WHERE der.format NOT IN (SELECT up2.format FROM up up2)
    )
    """
    params += [fmts, fmts]
    return sql, params


def _universe_denominator(cur, fmts):
    """(universe_total, universe_source) for the coverage cards.

    Only kicks in when the format filter selects dark-store platforms
    exclusively; "All formats", no filter, or any parcel format keeps the
    all-India census denominator. `universe_source` tells the frontend what
    the number is: 'india' | 'uploaded' | 'derived' | 'mixed' (some selected
    platforms uploaded, the rest derived)."""
    if not fmts or any(f in _PARCEL_FORMATS for f in fmts):
        return INDIA_TOTAL_CITIES, "india"
    cur.execute("SELECT to_regclass('public.platform_city_universe') IS NOT NULL")
    has_upload_tbl = bool(cur.fetchone()[0])
    cte, params = _universe_cte_sql(fmts, has_upload_tbl)
    cur.execute(
        cte + " SELECT COUNT(DISTINCT city),"
        "        (SELECT COUNT(DISTINCT up3.format) FROM up up3)"
        " FROM uni",
        params,
    )
    total, uploaded_formats = cur.fetchone()
    total = int(total or 0)
    if total <= 0:  # nothing known about these platforms — census fallback
        return INDIA_TOTAL_CITIES, "india"
    if uploaded_formats >= len(fmts):
        source = "uploaded"
    elif uploaded_formats == 0:
        source = "derived"
    else:
        source = "mixed"
    return total, source


_PENDING_CITIES_CAP = 300


def _pending_cities(cur, fmts, has_upload_tbl, summary_where, summary_params):
    """(cities, truncated, total) — universe cities with NO penetration row
    this month, the actionable "pending" list behind the coverage card.
    Uploaded universe ⇒ expansion whitespace; derived universe ⇒ cities we
    sold in before but not now. Reads the pen_joined temp table, so the
    covered side honours the same search/category filters as the summary
    cards. `total` is the full set-difference count (the list is capped) —
    used for the card instead of `universe - covered` arithmetic, which
    undercounts when covered cities are missing from an uploaded universe."""
    cte, params = _universe_cte_sql(fmts, has_upload_tbl)
    pending_sql = (
        " SELECT city FROM uni"
        " EXCEPT"
        " SELECT DISTINCT city FROM pen_joined"
        " WHERE city IS NOT NULL" + summary_where
    )
    cur.execute(
        cte + " SELECT COUNT(*) FROM (" + pending_sql + ") x",
        params + summary_params,
    )
    total = int(cur.fetchone()[0] or 0)
    cur.execute(
        cte + pending_sql + " ORDER BY 1 LIMIT %s",
        params + summary_params + [_PENDING_CITIES_CAP],
    )
    cities = [r[0] for r in cur.fetchall()]
    return cities, total > len(cities), total


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=300, prefix="dash.penetration", shared=True)
def penetration_report(request):
    """City × Item × Format penetration rows + summary for the selected month."""
    today = timezone.localdate()
    month, year = _month_year(request, today)
    fmts = _multi_param(request, "platform")
    heads = _multi_param(request, "item_head")
    cats = _multi_param(request, "category")
    subcats = _multi_param(request, "sub_category")
    status = (request.GET.get("status") or "").strip().lower()
    if status not in _STATUSES:
        status = ""
    # Roll-up dimension. '' = detail (one row per city×item×format, default);
    # 'city' = one row per city; 'sku' = one row per item. Whitelisted, so it's
    # safe to interpolate as a column name.
    group_by = (request.GET.get("group_by") or "").strip().lower()
    if group_by not in ("city", "sku"):
        group_by = ""
    search = (request.GET.get("search") or "").strip()
    # Column sort. No/unknown ?sort= keeps each mode's default ordering, so the
    # default state stays exactly what it was before sorting existed.
    sort = (request.GET.get("sort") or "").strip().lower()
    sdir = "DESC" if (request.GET.get("dir") or "").strip().lower() == "desc" else "ASC"
    page = max(_int_param(request, "page", 0), 0)
    page_size = min(max(_int_param(request, "page_size", _PAGE_SIZE_DEFAULT), 1),
                    _PAGE_SIZE_MAX)

    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])

    base, base_params = _base_sql(month, year, month_start, month_end, fmts, heads)

    # The summary (status-chip counts) ignores the status filter on purpose —
    # picking "Live" must not zero the other chips — while the rows honour it.
    summary_where, summary_params = "", []
    if search:
        summary_where += " AND (city ILIKE %s OR item ILIKE %s)"
        like = f"%{search}%"
        summary_params += [like, like]
    # Exact city / item filters power the grouped-row drill-down (click a city to
    # list its SKUs, or a SKU to list its cities). They filter the detail rows.
    city_exact = (request.GET.get("city") or "").strip()
    item_exact = (request.GET.get("item") or "").strip()
    if city_exact:
        summary_where += " AND UPPER(city) = UPPER(%s)"
        summary_params.append(city_exact)
    if item_exact:
        summary_where += " AND UPPER(item) = UPPER(%s)"
        summary_params.append(item_exact)
    # Category / sub-category filters (multi). Applied on the joined set so they
    # narrow the summary, rows and grouped roll-up alike.
    if cats:
        summary_where += " AND UPPER(TRIM(category)) = ANY(%s)"
        summary_params.append(cats)
    if subcats:
        summary_where += " AND UPPER(TRIM(sub_category)) = ANY(%s)"
        summary_params.append(subcats)
    rows_where, rows_params = summary_where, list(summary_params)
    if status:
        rows_where += " AND status = %s"
        rows_params.append(status)

    rows, summary, errors = [], {}, []
    grouped_count = None
    with connection.cursor() as cur:
        try:
            # The joined sec×inventory base is the expensive part (~2.4s). We used
            # to recompute it for every aggregate (summary, rows, count, grouped
            # summary) — 2-4× the cost per request. Instead materialize it ONCE
            # into a session-temp table; every aggregation below is then a ~50ms
            # scan of ~24k local rows. Dropped explicitly (and defensively at the
            # top) so a reused pooled connection never sees a stale copy.
            cur.execute("DROP TABLE IF EXISTS pen_joined")
            cur.execute(
                "CREATE TEMP TABLE pen_joined AS " + base + " SELECT * FROM joined",
                base_params,
            )

            cur.execute(
                " SELECT COUNT(*),"
                "        COUNT(*) FILTER (WHERE status = 'live'),"
                "        COUNT(*) FILTER (WHERE status = 'selling'),"
                "        COUNT(*) FILTER (WHERE status = 'stocked'),"
                "        COUNT(*) FILTER (WHERE status = 'inactive'),"
                "        COUNT(DISTINCT city),"
                "        COUNT(DISTINCT item),"
                "        COUNT(DISTINCT format)"
                " FROM pen_joined WHERE 1 = 1" + summary_where,
                summary_params,
            )
            (total, live, selling, stocked, inactive,
             cities, items, formats_n) = cur.fetchone()
            # Denominator: the selected platforms' own city universe when the
            # filter is QC-only, the all-India census figure otherwise. An
            # uploaded universe can be smaller than reality, so clamp at 100%.
            universe_total, universe_source = _universe_denominator(cur, fmts)
            covered_pct = min(round(cities / universe_total * 100, 1), 100.0)
            summary = {
                "total": total, "live": live, "selling": selling,
                "stocked": stocked, "inactive": inactive,
                "cities": cities, "items": items, "formats": formats_n,
                "india_cities_total": INDIA_TOTAL_CITIES,
                "universe_total": universe_total,
                "universe_source": universe_source,
                "cities_pending": max(universe_total - cities, 0),
                "covered_pct": covered_pct,
                "pending_pct": round(100 - covered_pct, 1),
            }
            if universe_source in ("uploaded", "derived", "mixed"):
                pending, truncated, pending_total = _pending_cities(
                    cur, fmts, universe_source != "derived",
                    summary_where, summary_params,
                )
                summary["pending_cities"] = pending
                summary["pending_cities_truncated"] = truncated
                summary["cities_pending"] = pending_total

            if group_by:
                # Roll up to one row per city (dim=city, count items per status)
                # or per item (dim=item, count CITIES per status — "in how many
                # cities is it live"). Each (dim, other) pair is first collapsed
                # to its best status across formats (live > selling > stocked >
                # inactive) so the per-status counts don't overlap.
                dim, other = ("city", "item") if group_by == "city" else ("item", "city")
                grp_cte = f"""WITH grp AS (
                    SELECT {dim} AS key, {other} AS other,
                           CASE WHEN bool_or(status = 'live')    THEN 'live'
                                WHEN bool_or(status = 'selling') THEN 'selling'
                                WHEN bool_or(status = 'stocked') THEN 'stocked'
                                ELSE 'inactive' END           AS ostatus,
                           COALESCE(SUM(sec_qty), 0) AS sec_qty,
                           COALESCE(SUM(sec_ltr), 0) AS sec_ltr,
                           COALESCE(SUM(inv_qty), 0) AS inv_qty,
                           COALESCE(SUM(inv_ltr), 0) AS inv_ltr
                    FROM pen_joined WHERE 1 = 1{summary_where}
                    GROUP BY {dim}, {other}
                )"""
                # Status chip in grouped mode = keep only keys that have at least
                # one `other` in that status (e.g. cities with ≥1 live item).
                having = " HAVING COUNT(*) FILTER (WHERE ostatus = %s) > 0" if status else ""
                grp_params = summary_params + ([status] if status else [])

                cur.execute(
                    grp_cte +
                    " SELECT key, COUNT(*) AS total_other,"
                    "        COUNT(*) FILTER (WHERE ostatus = 'live')     AS live,"
                    "        COUNT(*) FILTER (WHERE ostatus = 'selling')  AS selling,"
                    "        COUNT(*) FILTER (WHERE ostatus = 'stocked')  AS stocked,"
                    "        COUNT(*) FILTER (WHERE ostatus = 'inactive') AS inactive,"
                    "        COALESCE(SUM(sec_qty), 0) AS sec_qty,"
                    "        COALESCE(SUM(sec_ltr), 0) AS sec_ltr,"
                    "        COALESCE(SUM(inv_qty), 0) AS inv_qty,"
                    "        COALESCE(SUM(inv_ltr), 0) AS inv_ltr"
                    " FROM grp GROUP BY key" + having +
                    (
                        f" ORDER BY {_GROUP_SORT_COLS[sort]} {sdir} NULLS LAST, key ASC"
                        if sort in _GROUP_SORT_COLS
                        else " ORDER BY live DESC, sec_ltr DESC, key ASC"
                    ) +
                    " LIMIT %s OFFSET %s",
                    grp_params + [page_size, page * page_size],
                )
                for r in cur.fetchall():
                    rows.append({
                        "key": r[0],
                        "count": int(r[1] or 0),      # distinct items (city) / cities (sku)
                        "live": int(r[2] or 0),
                        "selling": int(r[3] or 0),
                        "stocked": int(r[4] or 0),
                        "inactive": int(r[5] or 0),
                        "sec_qty": round(float(r[6] or 0), 2),
                        "sec_ltr": round(float(r[7] or 0), 2),
                        "inv_qty": round(float(r[8] or 0), 2),
                        "inv_ltr": round(float(r[9] or 0), 2),
                    })
                cur.execute(
                    grp_cte + " SELECT COUNT(*) FROM ("
                    "SELECT key FROM grp GROUP BY key" + having + ") x",
                    grp_params,
                )
                grouped_count = cur.fetchone()[0]

                # Dimension-aware summary for the KPI cards: how many distinct
                # cities (city mode) / SKUs (sku mode) have >=1 `other` in each
                # status. Overlaps across statuses on purpose ("cities WITH a live
                # item"), and ignores the status chip like the detail summary.
                cur.execute(
                    grp_cte +
                    " SELECT COUNT(DISTINCT key),"
                    "        COUNT(DISTINCT key) FILTER (WHERE ostatus = 'live'),"
                    "        COUNT(DISTINCT key) FILTER (WHERE ostatus = 'selling'),"
                    "        COUNT(DISTINCT key) FILTER (WHERE ostatus = 'stocked'),"
                    "        COUNT(DISTINCT key) FILTER (WHERE ostatus = 'inactive')"
                    " FROM grp",
                    summary_params,
                )
                gk = cur.fetchone()
                summary["grouped"] = {
                    "dimension": group_by,
                    "total": int(gk[0] or 0),
                    "live": int(gk[1] or 0),
                    "selling": int(gk[2] or 0),
                    "stocked": int(gk[3] or 0),
                    "inactive": int(gk[4] or 0),
                }
            else:
                cur.execute(
                    " SELECT city, item, category, sub_category, format, item_head,"
                    "        sec_qty, sec_ltr, inv_qty, inv_ltr, status"
                    " FROM pen_joined WHERE 1 = 1" + rows_where +
                    (
                        f" ORDER BY {sort} {sdir} NULLS LAST, city ASC, item ASC, format ASC"
                        if sort in _DETAIL_SORT_COLS
                        else " ORDER BY city ASC, item ASC, format ASC"
                    ) +
                    " LIMIT %s OFFSET %s",
                    rows_params + [page_size, page * page_size],
                )
                for r in cur.fetchall():
                    rows.append({
                        "city": r[0],
                        "item": r[1],
                        "category": r[2],
                        "sub_category": r[3],
                        "format": r[4],
                        "item_head": r[5],
                        "sec_qty": round(float(r[6] or 0), 2),
                        "sec_ltr": round(float(r[7] or 0), 2),
                        "inv_qty": round(float(r[8] or 0), 2),
                        "inv_ltr": round(float(r[9] or 0), 2),
                        "status": r[10],
                    })
        except Exception as e:  # pragma: no cover - surfaced to the UI
            errors.append(str(e))
        finally:
            try:
                cur.execute("DROP TABLE IF EXISTS pen_joined")
            except Exception:
                pass

    # Pager count. Detail mode = rows matching the status filter (the summary
    # keeps every chip's count, so the active status's figure is already there).
    # Grouped mode = number of distinct keys (cities/items) after any HAVING.
    total = summary.get("total", 0)
    if group_by:
        count = grouped_count or 0
    else:
        count = summary.get(status, total) if status else total
    return Response({
        "rows": rows,
        "count": count,
        "page": page,
        "page_size": page_size,
        "group_by": group_by,
        "summary": summary,
        "month": month,
        "year": year,
        "filters": {"formats": fmts, "item_heads": heads,
                    "categories": cats, "sub_categories": subcats,
                    "status": status, "search": search, "group_by": group_by,
                    "sort": sort, "dir": sdir.lower()},
        "inventory_window": {"from": month_start.isoformat(),
                             "to": month_end.isoformat()},
        "errors": errors,
    })


@api_view(["GET"])
@permission_classes([require("dashboard.view")])
@cached_get(timeout=600, prefix="dash.penetration.options", shared=True)
def penetration_report_options(request):
    """Dropdown options: formats (both sources), item heads, categories,
    sub-categories, years."""
    formats, heads, categories, sub_categories, years = [], [], [], [], []
    errors = []
    with connection.cursor() as cur:
        try:
            # Formats that can actually place an item in a city: QC secondary
            # rows with a city plus every inventory format (minus Jio Mart —
            # removed from this report), and the two Amazon city feeds when
            # they hold city-bearing rows.
            cur.execute("""
                SELECT DISTINCT f FROM (
                    SELECT UPPER(TRIM(format::text)) AS f
                    FROM secmaster_mv
                    WHERE NULLIF(TRIM(city::text), '') IS NOT NULL
                    UNION
                    SELECT UPPER(TRIM(format::text)) AS f
                    FROM all_platform_inventory
                    UNION
                    SELECT 'AMAZON' AS f
                    WHERE EXISTS (SELECT 1 FROM public.amazon_sec_city
                                  WHERE NULLIF(TRIM(city::text), '') IS NOT NULL)
                    UNION
                    SELECT 'AMAZON MP' AS f
                    WHERE EXISTS (SELECT 1 FROM public.amazon_mp_master
                                  WHERE NULLIF(TRIM(ship_to_city::text), '') IS NOT NULL)
                ) u
                WHERE NULLIF(f, '') IS NOT NULL
                  AND f NOT IN ('JIO MART')
                ORDER BY f
            """)
            formats = [r[0] for r in cur.fetchall()]
        except Exception as e:
            errors.append(f"formats: {e}")
        try:
            cur.execute("""
                SELECT DISTINCT UPPER(TRIM(item_head::text)) AS h
                FROM master_sheet
                WHERE NULLIF(TRIM(item_head::text), '') IS NOT NULL
                ORDER BY h
            """)
            heads = [r[0] for r in cur.fetchall()]
        except Exception as e:
            errors.append(f"item_heads: {e}")
        # Category / sub-category options — the distinct values that appear in the
        # report's item sources (secmaster_mv ∪ master_sheet), so the pickers only
        # offer values that can actually match a row.
        try:
            cur.execute("""
                SELECT DISTINCT c FROM (
                    SELECT UPPER(TRIM(category::text)) AS c FROM secmaster_mv
                    WHERE NULLIF(TRIM(category::text), '') IS NOT NULL
                    UNION
                    SELECT UPPER(TRIM(category::text)) AS c FROM master_sheet
                    WHERE NULLIF(TRIM(category::text), '') IS NOT NULL
                ) u
                ORDER BY c
            """)
            categories = [r[0] for r in cur.fetchall()]
        except Exception as e:
            errors.append(f"categories: {e}")
        try:
            cur.execute("""
                SELECT DISTINCT c FROM (
                    SELECT UPPER(TRIM(sub_category::text)) AS c FROM secmaster_mv
                    WHERE NULLIF(TRIM(sub_category::text), '') IS NOT NULL
                    UNION
                    SELECT UPPER(TRIM(sub_category::text)) AS c FROM master_sheet
                    WHERE NULLIF(TRIM(sub_category::text), '') IS NOT NULL
                ) u
                ORDER BY c
            """)
            sub_categories = [r[0] for r in cur.fetchall()]
        except Exception as e:
            errors.append(f"sub_categories: {e}")
        try:
            cur.execute("""
                SELECT DISTINCT TRIM(year::text)::int FROM secmaster_mv
                WHERE TRIM(year::text) ~ '^[0-9]{4}$'
                ORDER BY 1 DESC
            """)
            years = [r[0] for r in cur.fetchall()]
        except Exception as e:
            errors.append(f"years: {e}")

    current_year = timezone.localdate().year
    if current_year not in years:
        years.insert(0, current_year)
        years.sort(reverse=True)

    return Response({
        "formats": formats,
        "item_heads": heads,
        "categories": categories,
        "sub_categories": sub_categories,
        "years": years,
        "errors": errors,
    })
