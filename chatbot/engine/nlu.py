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
    severity: str = ""        # "critical" | "warning" | ""
    active_only: bool | None = None
    wants_excel: bool = False
    top_n: int | None = None

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
_ALERT_WORDS = ("alert", "notification", "low stock", "low doh", "doh alert", "out of stock", "stockout")
_PO_WORDS = ("purchase order", "po ", " po", "pos", "p.o", "master po", "orders")
_SHIPMENT_WORDS = ("shipment", "truck", "dispatch", "load", "vehicle", "appointment")
_INVENTORY_WORDS = ("inventory", "stock on hand", " soh", "on hand", "in stock", "stock level")
_SALES_WORDS = ("secondary sales", "sales", "sold", "revenue")
_PLATFORM_LIST_WORDS = ("list platforms", "which platforms", "what platforms", "all platforms", "platform list")
_HELP_WORDS = ("help", "what can you do", "how do you work", "who are you", "what do you do")
_GREETING_WORDS = ("hi", "hello", "hey", "hii", "namaste", "yo")


def _has(text: str, words) -> bool:
    return any(w in text for w in words)


def parse(message: str, db_platforms: list[dict] | None = None) -> ParsedQuery:
    text = (message or "").strip()
    low = f" {text.lower()} "

    q = ParsedQuery(text=text)
    q.platforms = _match_platforms(low, db_platforms)
    q.date_from, q.date_to, q.date_label = parse_date_range(low)
    q.wants_excel = _has(low, _EXCEL_WORDS)

    if re.search(r"\bliters?\b|\blitres?\b|\bltr\b|\bvolume\b", low):
        q.metric = "liters"
    elif re.search(r"\bunits?\b|\bqty\b|\bquantit|\bpcs\b|\bpieces?\b", low):
        q.metric = "units"

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

    # --- Intent (order matters: most specific first) ---
    if _has(low, _PLATFORM_LIST_WORDS):
        q.intent = "list_platforms"
    elif _has(low, _ALERT_WORDS):
        q.intent = "alerts"
    elif q.metric == "liters" or q.movement in ("delivered", "sold"):
        q.intent = "liters"
    elif _has(low, _SHIPMENT_WORDS):
        q.intent = "shipments"
    elif _has(low, _PO_WORDS) or "master po" in low:
        q.intent = "pos"
    elif _has(low, _INVENTORY_WORDS):
        q.intent = "inventory"
    elif _has(low, _SALES_WORDS):
        q.intent = "sales"
    elif _has(low, _HELP_WORDS):
        q.intent = "help"
    elif text and text.lower().strip(" !.?") in _GREETING_WORDS:
        q.intent = "greeting"
    else:
        q.intent = "unknown"

    return q
