"""Blinkit Campaigns Optimization — Marketing section, Blinkit only.

Serves the raw rows the page's calculation engine needs. All the arithmetic
(ROAS, TACoS, paid/organic split, the performance flag) stays in the browser in
`utils/blinkitCampaignOptimization.js`, which is a verified port of the
Aug_Blinkit_Ads workbook. This endpoint only assembles that workbook's four
source tabs out of tables the daily crons already fill.

Workbook tab → source
---------------------
    SKU_Master   master_sheet (BLINKIT) + monthly_landing_rate.basic_rate
    Raw_BF       blinkitSec (city x item x day) + blinkit_brandfund (fund/day)
    Raw_PB       blinkit_ads_keyword     (migration 0086)
    Raw_RA       blinkit_ads_asset       (migration 0086)

Two things worth knowing before reading a number here:

1. `blinkit_brandfund` is deduped to (date, item_id) by migration 0041, so the
   brand-fund amount has no city breakdown left. The city detail the Cities view
   needs comes from `blinkitSec` instead, which carries it and agrees with the
   workbook exactly (3,404 units over 99 cities on 2026-08-01, every city
   equal). The day's fund is therefore attached to one city row per item — the
   totals are right and no view splits fund by city.

2. `blinkit_ads_keyword` / `blinkit_ads_asset` only start filling from the first
   ads upload after migration 0086. Before that they are empty and the Keywords
   and Assets views have nothing to show; every other view still works.

Basic price
-----------
`monthly_landing_rate.basic_rate` for format BLINKIT, carried forward from the
newest month at or before the sales month (the same carry-forward the landing
rate uses elsewhere). Its June 2026 values match the workbook's Jul-Aug rate
card to the paisa on six of nine SKUs. The three that differ:

  * Mustard 1L / 5L — the workbook blends Delhi NCR and Punjab into one price;
    this table holds the Delhi NCR rate. A published rate beats an average of
    two, so the table wins. `blinkitSec` has real cities, so a proper
    city-to-region price could replace both later.
  * Sunflower 1L — a genuine rate change the DB has not been given yet. Loading
    the Jul/Aug 2026 Blinkit rate card fixes it.

Net effect on Sales (Basic) for 1-7 Aug 2026: Rs 67,38,592 -> Rs 66,69,652,
about 1% lower than the workbook.

Contract
--------
  GET /api/platform/<slug>/blinkit-campaigns-optimization
        ?from=YYYY-MM-DD&to=YYYY-MM-DD   (default: the current month)

  -> { skuMaster, brandFund, productBooster, recommendationAds,
       momHistory: [], mtdSpend: [], coverage: {...} }

The shape is exactly what `buildModel()` consumes, so the page can feed it the
response unchanged. momHistory and mtdSpend are always empty: the Months view
was removed, and the MTD budget tab of the workbook is a separate Blinkit report
that no daily upload carries.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require
from config.perf_cache import cached_get

FORMAT = "BLINKIT"

# "JIVO POMACE 1L" -> brand "POMACE", size "1L". The campaign-to-SKU rule the
# workbook uses needs both a brand word and a size word to appear in the
# campaign name, and Blinkit names campaigns "Pomace Oil 1L" — never "Jivo".
_BRAND_PREFIX = "JIVO "


def _split_item(item: str) -> tuple[str, str]:
    parts = str(item or "").strip().split()
    if len(parts) < 2:
        return str(item or "").strip(), ""
    size = parts[-1]
    brand = " ".join(parts[:-1])
    if brand.upper().startswith(_BRAND_PREFIX):
        brand = brand[len(_BRAND_PREFIX):]
    return brand.title(), size.upper()


def _parse_date(value, fallback: date) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return fallback


def _num(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sku_master(cur, month_start: date) -> list[dict]:
    """The 9 Blinkit SKUs with litres, category and basic price.

    Price is the newest rate at or before the requested month, so a month whose
    card has not been loaded yet keeps using the last one published rather than
    silently costing everything at zero.
    """
    cur.execute(
        """
        SELECT m.format_sku_code,
               m.item,
               m.per_unit_value,
               m.item_head,
               (SELECT r.basic_rate
                  FROM monthly_landing_rate r
                 WHERE r.format ILIKE %s
                   AND r.sku_code = m.format_sku_code
                   AND r.basic_rate IS NOT NULL
                   AND r.month::date <= %s
                 ORDER BY r.month::date DESC
                 LIMIT 1) AS basic_rate
          FROM master_sheet m
         WHERE m.format ILIKE %s
         ORDER BY m.format_sku_code
        """,
        [FORMAT, month_start, FORMAT],
    )
    out = []
    for code, item, per_unit, item_head, basic_rate in cur.fetchall():
        brand, size = _split_item(item)
        out.append(
            {
                "sku": str(item or "").title(),
                "brand": brand,
                "size": size,
                "itemCode": str(code or "").strip(),
                "litres": _num(per_unit),
                # The workbook spells these Premium / Commodity; master_sheet
                # shouts them. The engine compares on the workbook's casing.
                "category": str(item_head or "").strip().title(),
                "basicPrice": _num(basic_rate),
                "note": "",
            }
        )
    return out


def _brand_fund(cur, start: date, end: date) -> list[dict]:
    """Raw_BF: city x item x day quantity, with the day's brand fund attached.

    Quantity and city come from blinkitSec. The fund is per (date, item_id) —
    see the module docstring — so it rides on the first city row of that item
    and day, keeping day and item totals correct.
    """
    cur.execute(
        """
        SELECT date, city_name, item_id::text, SUM(qty_sold)
          FROM "blinkitSec"
         WHERE date BETWEEN %s AND %s
         GROUP BY date, city_name, item_id
         ORDER BY date, item_id, city_name
        """,
        [start, end],
    )
    sales = cur.fetchall()

    cur.execute(
        """
        SELECT date, item_id::text, SUM(total_brand_fund)
          FROM "blinkit_brandfund"
         WHERE date BETWEEN %s AND %s
         GROUP BY date, item_id
        """,
        [start, end],
    )
    fund = {(d, item): _num(v) for d, item, v in cur.fetchall()}

    rows = []
    seen: set[tuple] = set()
    for d, city, item, qty in sales:
        key = (d, item)
        first = key not in seen
        seen.add(key)
        rows.append(
            {
                "date": d.isoformat(),
                "city": city or "",
                "itemId": item,
                "qty": _num(qty),
                "brandFund": fund.pop(key, 0.0) if first else 0.0,
            }
        )
    # Brand fund on an item/day with no sales row still has to reach the totals.
    for (d, item), value in fund.items():
        if value:
            rows.append(
                {"date": d.isoformat(), "city": "", "itemId": item, "qty": 0.0, "brandFund": value}
            )
    return rows


def _keyword_rows(cur, start: date, end: date) -> list[dict]:
    cur.execute(
        """
        SELECT date, campaign_name, keyword, cpm, total_budget, impression,
               COALESCE(direct_gmv, 0) + COALESCE(indirect_gmv, 0),
               COALESCE(direct_qty_sold, 0) + COALESCE(indirect_qty_sold, 0),
               ad_spent
          FROM blinkit_ads_keyword
         WHERE date BETWEEN %s AND %s
         ORDER BY date, campaign_name, keyword
        """,
        [start, end],
    )
    return [
        {
            "date": d.isoformat(),
            "campaign": camp or "",
            "keyword": kw or "",
            "cpm": _num(cpm),
            "budget": _num(budget),
            "impressions": _num(impr),
            "sales": _num(sales),
            "qty": _num(qty),
            "spend": _num(spend),
        }
        for d, camp, kw, cpm, budget, impr, sales, qty, spend in cur.fetchall()
    ]


def _asset_rows(cur, start: date, end: date) -> list[dict]:
    cur.execute(
        """
        SELECT date, campaign_name, asset, cpm, total_budget, impression,
               COALESCE(direct_gmv, 0) + COALESCE(indirect_gmv, 0),
               COALESCE(direct_qty_sold, 0) + COALESCE(indirect_qty_sold, 0),
               ad_spent
          FROM blinkit_ads_asset
         WHERE date BETWEEN %s AND %s
         ORDER BY date, campaign_name, asset
        """,
        [start, end],
    )
    return [
        {
            "date": d.isoformat(),
            "campaign": camp or "",
            "asset": asset or "",
            "cpm": _num(cpm),
            "budget": _num(budget),
            "impressions": _num(impr),
            "sales": _num(sales),
            "qty": _num(qty),
            "spend": _num(spend),
        }
        for d, camp, asset, cpm, budget, impr, sales, qty, spend in cur.fetchall()
    ]


@api_view(["GET"])
@permission_classes([require("platform.stats.view")])
@cached_get(timeout=60, prefix="plat.blinkit_campaigns_optimization")
def blinkit_campaigns_optimization(request, slug: str):
    today = date.today()
    default_start = today.replace(day=1)
    default_end = today.replace(day=monthrange(today.year, today.month)[1])

    start = _parse_date(request.GET.get("from"), default_start)
    end = _parse_date(request.GET.get("to"), default_end)
    if end < start:
        start, end = end, start

    with connection.cursor() as cur:
        payload = {
            "skuMaster": _sku_master(cur, start.replace(day=1)),
            "brandFund": _brand_fund(cur, start, end),
            "productBooster": _keyword_rows(cur, start, end),
            "recommendationAds": _asset_rows(cur, start, end),
            # The Months view was removed and no daily upload carries the
            # workbook's MTD budget tab, so both stay empty by design.
            "momHistory": [],
            "mtdSpend": [],
        }

    payload["coverage"] = {
        "from": start.isoformat(),
        "to": end.isoformat(),
        # Lets the page say "keyword data starts after the next ads upload"
        # instead of showing an unexplained empty Keywords tab.
        "keywordRows": len(payload["productBooster"]),
        "assetRows": len(payload["recommendationAds"]),
    }
    return Response(payload)
