"""Natural-language understanding for the chatbot.

Pure-Python intent + entity extraction — no external AI required. Parses a user
message into a ``ParsedQuery`` (intent, platform, date range, metric, flags)
that the engine routes to the right data tool.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass, field
from datetime import date, timedelta

from django.utils import timezone

# --- Platform aliases --------------------------------------------------------
# Base aliases; merged at runtime with PlatformConfig (slug, name) so any
# admin-configured platform also resolves. Keyed by canonical slug.
_BASE_PLATFORM_ALIASES: dict[str, list[str]] = {
    "blinkit": ["blinkit", "blink it", "grofers"],
    "zepto": ["zepto"],
    "swiggy": ["swiggy", "instamart", "swiggy instamart"],
    "bigbasket": ["bigbasket", "big basket", "bb now", "bbnow", "bbdaily", "bb"],
    "amazon": ["amazon", "amzn", "amazon fresh"],
    "flipkart": ["flipkart", "flipkart grocery", "fk"],
    "jiomart": ["jiomart", "jio mart", "jio"],
    "zomato": ["zomato"],
    "citymall": ["citymall", "city mall"],
}

_MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
_MONTHS.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})


@dataclass
class ParsedQuery:
    text: str
    intent: str = "unknown"
    platforms: list[dict] = field(default_factory=list)  # [{"slug","name"}]
    date_from: date | None = None
    date_to: date | None = None
    date_label: str = ""
    metric: str = ""          # "liters" | "units" | ""
    movement: str = ""        # "delivered" | "sold" | ""
    dimension: str = ""       # "state" | "city" | "sku" | "brand" | ... (ranking)
    severity: str = ""        # "critical" | "warning" | ""
    active_only: bool | None = None
    wants_excel: bool = False
    top_n: int | None = None
    group_by_month: bool = False   # "month wise" / "monthly" / "all months" breakdown
    group_by_platform: bool = False  # "platform wise" breakdown
    wants_amount: bool = False     # "order amount" / "value" / "revenue" question
    item_head: str = ""            # PREMIUM / COMMODITY filter
    product: str = ""              # item-family filter, e.g. "extra light", "canola"

    @property
    def platform_slugs(self) -> list[str]:
        return [p["slug"] for p in self.platforms]

    @property
    def primary_platform(self) -> dict | None:
        return self.platforms[0] if self.platforms else None


def _today() -> date:
    return timezone.localdate()


def _platform_alias_map(db_platforms: list[dict] | None) -> list[tuple[str, str, str]]:
    """Return ``[(alias, slug, name), ...]`` sorted longest-alias-first."""
    aliases: dict[str, tuple[str, str]] = {}
    names: dict[str, str] = {}
    for slug, al in _BASE_PLATFORM_ALIASES.items():
        names[slug] = slug.title()
        for a in al:
            aliases[a.lower()] = (slug, slug.title())
    for row in db_platforms or []:
        slug = (row.get("slug") or "").lower()
        name = row.get("name") or slug.title()
        if not slug:
            continue
        names[slug] = name
        aliases.setdefault(slug, (slug, name))
        aliases[slug.replace("_", " ")] = (slug, name)
        aliases[name.lower()] = (slug, name)
    out = [(alias, slug, names.get(slug, name)) for alias, (slug, name) in aliases.items()]
    out.sort(key=lambda t: len(t[0]), reverse=True)
    return out


def _match_platforms(text: str, db_platforms: list[dict] | None) -> list[dict]:
    found: list[dict] = []
    seen: set[str] = set()
    haystack = f" {text} "
    for alias, slug, name in _platform_alias_map(db_platforms):
        if slug in seen:
            continue
        pattern = r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])"
        if re.search(pattern, haystack):
            found.append({"slug": slug, "name": name})
            seen.add(slug)
    return found


def _parse_explicit_date(token: str) -> date | None:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            from datetime import datetime

            return datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    return None


def _month_range(year: int, month: int) -> tuple[date, date]:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def parse_date_range(text: str) -> tuple[date | None, date | None, str]:
    """Return ``(date_from, date_to, label)`` — any may be None/empty."""
    t = text.lower()
    today = _today()

    if re.search(r"\btoday\b", t):
        return today, today, "today"
    if re.search(r"\byesterday\b", t):
        y = today - timedelta(days=1)
        return y, y, "yesterday"

    m = re.search(r"\b(?:last|past|previous)\s+(\d{1,4})\s*days?\b", t)
    if m:
        n = int(m.group(1))
        return today - timedelta(days=n), today, f"last {n} days"

    if re.search(r"\blast\s+week\b", t):
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=6), "last week"
    if re.search(r"\bthis\s+week\b|\bcurrent\s+week\b", t):
        start = today - timedelta(days=today.weekday())
        return start, today, "this week"

    if re.search(r"\blast\s+month\b|\bprevious\s+month\b", t):
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        return last_prev.replace(day=1), last_prev, "last month"
    if re.search(r"\bthis\s+month\b|\bcurrent\s+month\b", t):
        return today.replace(day=1), today, "this month"

    if re.search(r"\blast\s+year\b", t):
        y = today.year - 1
        return date(y, 1, 1), date(y, 12, 31), "last year"
    if re.search(r"\bthis\s+year\b|\bcurrent\s+year\b", t):
        return date(today.year, 1, 1), today, "this year"

    # Explicit dates (YYYY-MM-DD, DD/MM/YYYY, ...). If two present, treat as range.
    tokens = re.findall(r"\b\d{4}-\d{1,2}-\d{1,2}\b|\b\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}\b", t)
    dates = [d for d in (_parse_explicit_date(tok) for tok in tokens) if d]
    if len(dates) >= 2:
        lo, hi = sorted(dates)[:2]
        return lo, hi, f"{lo} to {hi}"
    if len(dates) == 1:
        return dates[0], dates[0], str(dates[0])

    # Month name, optionally with a year: "in june", "march 2025".
    mm = re.search(r"\b(" + "|".join(sorted(_MONTHS, key=len, reverse=True)) + r")\b\.?\s*(\d{4})?", t)
    if mm:
        month = _MONTHS[mm.group(1)]
        year = int(mm.group(2)) if mm.group(2) else today.year
        lo, hi = _month_range(year, month)
        if hi > today and year == today.year:
            hi = today
        return lo, hi, mm.group(0).strip()

    return None, None, ""


_EXCEL_WORDS = ("excel", "xlsx", ".xls", "spreadsheet", "workbook", "download", "export", "csv", "sheet file")
_ALERT_WORDS = ("alert", "notification", "low stock", "low doh", "doh alert", "out of stock",
                "stockout", "doh", "days on hand", "days of hand")
_PO_WORDS = ("purchase order", "po ", " po", "pos", "p.o", "master po", "orders")
_SHIPMENT_WORDS = ("shipment", "truck", "dispatch", "load", "vehicle")
_INVENTORY_WORDS = ("inventory", "stock on hand", " soh", "on hand", "in stock", "stock level",
                    "stock by", "stock across", "stock in ", "soh unit", "soh ltr")
_SALES_WORDS = ("secondary sales", "sales", "sold", "revenue")
_PLATFORM_LIST_WORDS = ("list platforms", "which platforms", "what platforms", "all platforms", "platform list")
_HELP_WORDS = ("help", "what can you do", "how do you work", "who are you", "what do you do",
               "what data", "what can you show", "what can i ask", "how can you help")
_GREETING_WORDS = ("hi", "hello", "hey", "hii", "namaste", "yo")

# Dimensions the bot can rank ("top states", "best brands", ...). Maps a logical
# dimension to the words that trigger it; the tool maps it to a master_po column.
_DIMENSION_WORDS = {
    "state": ["state", "states"],
    "city": ["city", "cities"],
    "location": ["location", "locations"],
    "sku": ["sku", "skus"],
    "brand": ["brand", "brands"],
    "category": ["category", "categories"],
    "item": ["item", "items", "product", "products"],
    "vendor": ["vendor", "vendors", "supplier", "suppliers"],
    "platform": ["platform", "platforms", "format", "formats"],
}
_RANK_RE = re.compile(r"\b(top|best|highest|most|leading|largest|rank|ranking|compare)\b")

# Jivo product families -> the text to match against the `item` column. Each
# tuple is (item-filter, [aliases users type]). Longest/most-specific first.
_PRODUCTS = [
    ("extra light", ["extra light", "extralight", "extra-light"]),
    ("extra virgin", ["extra virgin", "extravirgin"]),
    ("rice bran", ["rice bran", "ricebran"]),
    ("cotton seed", ["cotton seed", "cottonseed"]),
    ("desi ghee", ["desi ghee", "ghee"]),
    ("black olives", ["black olive", "black olives"]),
    ("canola", ["canola"]),
    ("pomace", ["pomace"]),
    ("groundnut", ["groundnut", "peanut"]),
    ("mustard", ["mustard", "kachi ghani", "kacchi ghani"]),
    ("sunflower", ["sunflower"]),
    ("gold", ["gold"]),
]
_STATE_RE = re.compile(
    r"\b(maharashtra|gujarat|goa|rajasthan|delhi|punjab|haryana|uttar pradesh|uttarakhand|"
    r"himachal pradesh|jammu and kashmir|chandigarh|karnataka|tamil nadu|kerala|andhra pradesh|"
    r"telangana|puducherry|west bengal|bihar|odisha|orissa|jharkhand|madhya pradesh|chhattisgarh|"
    r"chattisgarh|assam|tripura|meghalaya|manipur|nagaland|mizoram|sikkim|arunachal pradesh)\b")


def _has(text: str, words) -> bool:
    return any(w in text for w in words)


def parse(message: str, db_platforms: list[dict] | None = None) -> ParsedQuery:
    text = (message or "").strip()
    low = f" {text.lower()} "

    q = ParsedQuery(text=text)
    q.platforms = _match_platforms(low, db_platforms)
    q.date_from, q.date_to, q.date_label = parse_date_range(low)
    q.wants_excel = _has(low, _EXCEL_WORDS)

    if re.search(r"\bliters?\b|\blitres?\b|\bltrs?\b|\bvolume\b", low):
        q.metric = "liters"
    elif re.search(r"\bunits?\b|\bqty\b|\bquantit|\bpcs\b|\bpieces?\b", low):
        q.metric = "units"

    # "fill rate" / "miss rate" / "fill %" is a liters-table question (the liters
    # tool reports fill %); route it there even without an explicit "liters" word.
    if re.search(r"\bfill\s*rate\b|\bfillrate\b|\bmiss\s*rate\b|\bmissrate\b|\bfill\s*%|\bfill percentage\b", low):
        q.metric = q.metric or "liters"

    # Order/delivered amount ("amount", "value", "revenue", "inclusive"...).
    q.wants_amount = bool(re.search(
        r"\bamount\b|\bamt\b|\brevenue\b|\bworth\b|order value|sales value|"
        r"\binclusive\b|\bexclusive\b", low,
    ))

    if re.search(r"\bdeliver", low) or re.search(r"\bdispatch", low):
        q.movement = "delivered"
    elif re.search(r"\bsold\b|\bselling\b|\bsale", low):
        q.movement = "sold"

    if "critical" in low:
        q.severity = "critical"
    elif "warning" in low:
        q.severity = "warning"

    if re.search(r"\bresolved\b|\bclosed\b", low):
        q.active_only = False
    elif re.search(r"\bactive\b|\bunresolved\b|\bopen\b|\bpending\b", low):
        q.active_only = True

    m = re.search(r"\btop\s*(\d{1,4})\b", low)
    if m:
        q.top_n = int(m.group(1))

    # Month-wise breakdown ("month wise", "monthly", "by month", "all months",
    # "month on month"). Deliberately does NOT match "this month" / "last month"
    # / "june month" — those select a single period, not a breakdown.
    q.group_by_month = bool(re.search(
        r"\bmonth[\s\-]?wise\b|\bmonthly\b|\bby\s+month\b|\bper\s+month\b|"
        r"\beach\s+month\b|\bevery\s+month\b|\bmonth\s+by\s+month\b|"
        r"\bmonth\s+on\s+month\b|\ball\s+months?\b",
        low,
    ))

    for dim, words in _DIMENSION_WORDS.items():
        if any(re.search(r"\b" + re.escape(w) + r"\b", low) for w in words):
            q.dimension = dim
            break

    # Item-head filter (premium / commodity). If BOTH appear it's a split, not a
    # filter — leave blank so the split tool handles it.
    if "premium" in low and "commodity" not in low:
        q.item_head = "PREMIUM"
    elif "commodity" in low and "premium" not in low:
        q.item_head = "COMMODITY"

    # Product-family filter ("extra light", "canola", ...).
    for canon, aliases in _PRODUCTS:
        if any(a in low for a in aliases):
            q.product = canon
            break

    # --- Intent (order matters: most specific first) ---
    # "all platforms" only means "list the platforms" when the question isn't
    # actually asking for data across them ("delivered liters ... all platforms"
    # is a liters query, not a platform-list request). A bare dimension word like
    # "platforms" does NOT count — only a real ranking (dimension + rank word).
    is_ranking = bool(q.dimension and _RANK_RE.search(low))
    data_signal = bool(q.metric or q.movement or q.group_by_month or q.wants_amount or is_ranking)
    movers = bool(re.search(r"\brisers?\b|\bfallers?\b|\bmovers?\b|\bgainers?\b|\bdecliners?\b|"
                            r"biggest\s+(drop|jump|gain|fall|rise)", low))
    split = bool(re.search(r"premium\s*(vs|versus|and|&|/|\s)\s*commodity|"
                           r"commodity\s*(vs|versus|and|&|/|\s)\s*premium|"
                           r"item\s*head\s+(split|wise|breakup|breakdown)", low))
    drr_flag = bool(re.search(r"\bdrr\b|run\s*rate|day\s*wise|daywise|per\s*day|\bdaily\b|\bops\b", low))
    secondary_flag = bool(re.search(r"\bsecondary\b|sell\s*out|sellout|sell-out|"
                                    r"\bsold\b|\bshipped\b|\bshpd\b|\bsec sales\b|\breturns?\b", low))
    targets_flag = bool("target" in low or "achieved %" in low or "achievement" in low
                        or "require drr" in low or "req drr" in low or "behind on" in low)
    landing_flag = bool(re.search(r"landing rate|basic rate|landing price", low))
    pendency_flag = bool("pendency" in low or ("pending" in low and "approval" not in low
                                               and "shipment" not in low))
    coupon_flag = bool("coupon" in low or "clips" in low or "redemption" in low)
    brandfund_flag = bool("brand fund" in low or "brandfund" in low or "brand-fund" in low)
    ads_flag = bool(re.search(r"\bads?\b|ad spent|ad spend|\broas\b|\bacos\b|\btacos\b|\bgmv\b|"
                              r"impressions|\bcpc\b|\bctr\b|\bntb\b|advertis|detail page view", low))
    is_amazon = any(p.get("slug") == "amazon" for p in q.platforms)
    expiry_flag = bool("expiring" in low or "expiry" in low or "expire" in low
                       or "days to expiry" in low or "about to expire" in low)
    appt_flag = bool("appointment" in low)
    amazon_mp_flag = bool("amazon mp" in low or "mp sales" in low or "marketplace" in low
                          or (is_amazon and re.search(r"\bmp\b", low)))
    leadtime_flag = bool("lead time" in low or "leadtime" in low or "lead-time" in low)
    amazon_po_flag = bool("mov" in low or "fulfillment center" in low or "fulfilment center" in low
                          or ("requested" in low and "received" in low)
                          or (is_amazon and re.search(r"\bpos?\b|pending|new po|\bfc\b|status|fill rate", low)))
    sap_flag = bool("jm primary" in low or "hana" in low or "wellness billing" in low
                    or "mart source" in low or "oil source" in low or "below min" in low
                    or "zero stock" in low or "finished goods" in low or "fifo" in low
                    or "credit line" in low or "stock value" in low or re.search(r"\bsap\b", low)
                    or ("distributor" in low and ("balance" in low or "credit" in low or "invoice" in low)))
    realise_flag = bool("realise" in low or "realize" in low or "realisation" in low or "commission" in low)
    state_flag = bool("state wise" in low or "statewise" in low or "state-wise" in low
                      or ("region" in low and any(d in low for d in ("north", "south", "east", "west", "which region")))
                      or ("jivo" in low and "sano" in low) or bool(_STATE_RE.search(low)))
    q.group_by_platform = bool(re.search(r"platform\s*wise|platformwise|platform-wise|by\s+platform|"
                                         r"platform\s+break", low))
    explain_flag = bool(
        (re.search(r"\b(explain|define|definition|meaning of|what do you mean)\b", low)
         or re.search(r"^\s*(what is|what's|whats)\b", low))
        and not q.platforms and not q.date_from
        and re.search(r"secondary|secandary|secndary|primary|\bdrr\b|\bdoh\b|\bsoh\b|fill rate|"
                      r"miss rate|pendency|realise|realize|brand fund|item head|\broas\b|\bacos\b|"
                      r"\btacos\b|lead time|\bmov\b|master po", low))
    maxdate_flag = bool(re.search(r"\bmax date\b|\blatest date\b|last updated|data date|max updated|"
                                  r"till which date|up ?to which date|latest data", low))
    datetime_flag = bool(re.search(r"what.{0,8}\b(day|date|time)\b|\btime now\b|current (time|date)|"
                                   r"today'?s? date|what day is|what.?s the day|which day (is|it)", low))
    appcontrol_flag = bool(re.search(r"\blog\s?out\b|\bsign\s?out\b|logout|log me out|"
                                     r"(refresh|reload).{0,15}(app|page|ecom|dashboard|site)|"
                                     r"close the app|open the (\w+) (page|dashboard)|navigate to", low))
    smalltalk_flag = bool(re.search(r"\bhlo+\b|\bhlooo+\b|how are (you|u)\b|how r u\b|how\s?are\s?u\b|"
                                    r"\bhru\b|hor vi kida|sat sri akal|kiddan|\bthanks?\b|thank you|thankyou|"
                                    r"\bwhat r u\b|\bwhat are you\b|who (are|r) (you|u)|are you (a )?(bot|ai|ready|there)|"
                                    r"is (you|u) ready|you ready|ready for deploy|busine\w* mode|"
                                    r"\bsup\b|what'?s up|\bnice\b|\bgood (job|bot|work)\b|\bcool\b|"
                                    r"this is (incorrect|wrong)|not correct|that'?s wrong", low))
    ack_flag = bool(re.fullmatch(r"\s*(ok|okay|k|kk|yes|yep|no|nope|hmm+|great|fine|got it|thx|ty)\s*[.! ]*", low))
    if _has(low, _PLATFORM_LIST_WORDS) and not data_signal:
        q.intent = "list_platforms"
    elif explain_flag:
        q.intent = "explain"
    elif _has(low, _ALERT_WORDS):
        q.intent = "alerts"
    elif sap_flag:
        q.intent = "sap"
    elif movers:
        q.intent = "movers"
    elif split and not (coupon_flag or brandfund_flag):
        q.intent = "split"
    elif _has(low, _INVENTORY_WORDS):
        q.intent = "inventory"
    elif targets_flag:
        q.intent = "targets"
    elif landing_flag:
        q.intent = "landing"
    elif coupon_flag:
        q.intent = "coupon"
    elif brandfund_flag:
        q.intent = "brand_fund"
    elif ads_flag:
        q.intent = "ads"
    elif realise_flag:
        q.intent = "realise"
    elif state_flag:
        q.intent = "state_sales"
    elif drr_flag:
        q.intent = "drr"
    elif secondary_flag:
        q.intent = "sales"
    elif expiry_flag:
        q.intent = "expiry"
    elif appt_flag:
        q.intent = "appointments"
    elif amazon_mp_flag:
        q.intent = "amazon_mp"
    elif leadtime_flag:
        q.intent = "lead_time"
    elif amazon_po_flag:
        q.intent = "amazon_po"
    elif pendency_flag:
        q.intent = "pendency"
    elif q.dimension and _RANK_RE.search(low):
        q.intent = "ranking"
    elif q.metric == "liters" or q.movement in ("delivered", "sold") or q.group_by_month or q.group_by_platform or q.wants_amount:
        q.intent = "liters"
    elif _has(low, _SHIPMENT_WORDS):
        q.intent = "shipments"
    elif _has(low, _PO_WORDS) or "master po" in low:
        q.intent = "pos"
    elif _has(low, _INVENTORY_WORDS):
        q.intent = "inventory"
    elif _has(low, _SALES_WORDS):
        q.intent = "sales"
    elif maxdate_flag:
        q.intent = "maxdate"
    elif appcontrol_flag:
        q.intent = "appcontrol"
    elif datetime_flag:
        q.intent = "datetime"
    elif _has(low, _HELP_WORDS):
        q.intent = "help"
    elif smalltalk_flag or ack_flag or (text and text.lower().strip(" !.?") in _GREETING_WORDS):
        q.intent = "greeting"
    else:
        q.intent = "unknown"

    return q
