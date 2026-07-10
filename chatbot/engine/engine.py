"""Chatbot orchestrator.

`answer_question` parses a message, routes it to the right data tool, composes a
natural-language reply, and (when asked) generates a downloadable Excel file.

Runs fully offline on the built-in rules engine. If an ANTHROPIC_API_KEY is
configured AND the `anthropic` package is installed, it transparently upgrades
to Claude tool-use (same tools) and falls back to the rules engine on any error.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from django.conf import settings
from django.utils import timezone

from ..models import ChatConversation, ChatFile
from . import nlu, tools
from .excel import build_workbook
from .tools import DataResult, PREVIEW_ROWS

logger = logging.getLogger(__name__)

HELP_TEXT = (
    "Hi! I'm your data assistant. I can read your live operations data and answer "
    "questions — and turn any answer into a downloadable Excel file.\n\n"
    "Try asking:\n"
    "• \"Show critical DOH alerts for Blinkit\"\n"
    "• \"How many liters were delivered this month?\"\n"
    "• \"Excel of Zepto alerts\"\n"
    "• \"Blinkit purchase orders this week\"\n"
    "• \"Top states by order liters\"\n"
    "• \"Top 10 brands in Zepto\"\n"
    "• \"List all platforms\"\n"
    "• \"Amazon shipments last 7 days\"\n\n"
    "Add the word \"excel\" (or \"download\") to any question and I'll build a spreadsheet."
)

SUGGESTIONS = [
    "Top states by order liters",
    "Critical DOH alerts for Blinkit",
    "Top 10 brands in Zepto",
    "List all platforms",
]


@dataclass
class EngineResult:
    text: str
    data: dict = field(default_factory=dict)
    intent: str = "unknown"
    engine: str = "builtin"
    is_error: bool = False
    file: ChatFile | None = None
    suggestions: list = field(default_factory=list)


def engine_mode() -> str:
    """'claude' when an API key + SDK are both available, else 'builtin'."""
    key = getattr(settings, "ANTHROPIC_API_KEY", "") or ""
    if not key:
        return "builtin"
    try:
        import anthropic  # noqa: F401
    except Exception:
        return "builtin"
    return "claude"


def _json_safe(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        f = float(value)
        return int(f) if f.is_integer() else round(f, 4)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return str(value)


def _preview(result: DataResult) -> dict:
    rows = [[_json_safe(c) for c in row] for row in result.rows[:PREVIEW_ROWS]]
    return {
        "columns": list(result.columns),
        "rows": rows,
        "row_count": len(result.rows),
        "source": result.source,
        "truncated": len(result.rows) > PREVIEW_ROWS,
    }


def _safe_filename(title: str) -> str:
    stamp = timezone.localtime().strftime("%Y%m%d_%H%M")
    base = re.sub(r"[^A-Za-z0-9]+", "_", (title or "export")).strip("_").lower() or "export"
    return f"{base}_{stamp}.xlsx"


def _make_excel(user, conversation, result: DataResult) -> ChatFile | None:
    if not result.rows:
        return None
    try:
        content, total = build_workbook(
            [(result.excel_title or "Data", result.columns, result.rows)],
            meta=[("Generated", timezone.localtime().strftime("%Y-%m-%d %H:%M")),
                  ("Source", result.source),
                  ("Rows", len(result.rows))] + list(result.meta),
        )
    except Exception:
        logger.exception("excel build failed")
        return None
    return ChatFile.objects.create(
        user=user,
        conversation=conversation,
        filename=_safe_filename(result.excel_title),
        content=content,
        size_bytes=len(content),
        row_count=total,
    )


# intent -> tool
_ROUTES = {
    "list_platforms": tools.list_platforms,
    "alerts": tools.alerts,
    "liters": tools.liters,
    "shipments": tools.shipments,
    "pos": tools.purchase_orders,
    "inventory": tools.inventory,
    "sales": tools.secondary_sales,
    "master_sheet": tools.master_po_sheet,
    "ranking": tools.ranking,
    "movers": tools.movers,
    "split": tools.premium_commodity_split,
    "drr": tools.drr,
    "targets": tools.targets,
    "landing": tools.landing_rate,
    "pendency": tools.pendency,
    "ads": tools.ads,
    "brand_fund": tools.brand_fund,
    "coupon": tools.coupon,
    "expiry": tools.expiry,
    "appointments": tools.appointments,
    "amazon_mp": tools.amazon_mp,
    "lead_time": tools.lead_time,
    "amazon_po": tools.amazon_po,
    "state_sales": tools.state_sales,
    "realise": tools.realise,
    "sap": tools.sap_info,
    "datetime": tools.datetime_now,
    "appcontrol": tools.app_control,
    "maxdate": tools.max_date,
    "explain": tools.explain,
}


def _related_suggestions(q) -> list:
    """Context-aware follow-up questions shown as chips below each answer. Every
    suggestion is phrased so the bot can actually answer it when clicked."""
    p = (q.primary_platform["name"] if q.primary_platform else "") or "Blinkit"
    intent = q.intent
    by_intent = {
        "liters": [f"{p} fill rate", f"{p} order liters month wise",
                   "Top states by order liters", f"Excel of {p} purchase orders"],
        "ranking": ["Top 10 brands by order amount", "Top cities by order liters",
                    "Premium vs commodity by platform", "Top vendors by order qty"],
        "movers": ["Top states by order liters", "Premium vs commodity by platform",
                   "Top 10 skus by delivered liters", "Top 10 brands in Zepto"],
        "split": ["Top states by order liters", "Top 10 brands by order amount",
                  "Compare platforms by order liters", "Top 10 skus by delivered liters"],
        "alerts": [f"Excel of {p} alerts", f"Critical DOH alerts for {p}",
                   "Which skus have the lowest doh", f"{p} inventory"],
        "inventory": [f"Items with soh units less than 10 in {p}", f"{p} stock by city",
                      f"Critical DOH alerts for {p}", "Stock by city top 15 across platforms"],
        "sales": [f"{p} DRR this month", f"{p} secondary sales premium",
                  "Top 10 skus by ltr sold", f"{p} order liters"],
        "drr": [f"{p} secondary sales", f"{p} day wise ops and ltr",
                "Top skus by ltr sold", f"{p} DRR premium"],
        "ads": [f"{p} roas and acos", f"Total brand fund spent on {p}",
                "Which platform had highest ad spent", f"Top items by ad spent on {p}"],
        "brand_fund": [f"Total ad spent on {p}", f"{p} brand fund sub category wise",
                       "Which platform had highest ad spent", f"Top 5 items by brand fund on {p}"],
        "coupon": ["Total coupon redemptions and clips on amazon",
                   "Which coupon has highest budget used", "Amazon roas and acos", "Amazon mp delivered litres"],
        "targets": [f"{p} done ltrs vs target", "Which platforms are behind on target",
                    f"{p} pendency", f"{p} drr"],
        "pendency": [f"{p} pendency by city", "Pos expiring in next 5 days",
                     f"Top 10 vendors by pending value in {p}", f"{p} fill rate"],
        "state_sales": ["Top states by order liters", "Which region sold more north or south",
                        "Jivo vs sano state sales split", "Total distributor commission for june"],
        "realise": ["Total distributor commission for june", "Top states by order liters",
                    f"{p} fill rate", "Top 10 brands by order amount"],
        "list_platforms": ["Total order ltrs in blinkit", "Top states by order liters",
                            "Critical DOH alerts for Blinkit", "Which platform had highest ad spent"],
        "landing": [f"{p} skus with no landing rate", "Total order ltrs in blinkit", f"{p} fill rate"],
    }
    amazon_bucket = ["How many amazon pos are pending", "Which amazon pos are expiring in 7 days",
                     "How many appointments today", "Amazon fill rate by fulfillment center"]
    for k in ("amazon_po", "expiry", "appointments", "amazon_mp", "lead_time"):
        by_intent[k] = amazon_bucket
    return by_intent.get(intent, list(SUGGESTIONS))[:4]


def _smalltalk_reply(message: str) -> str:
    """A warm, varied reply for greetings / thanks / identity / feedback so the
    bot doesn't feel dull. Falls back to the friendly intro."""
    t = (message or "").lower()
    if any(w in t for w in ("thank", "thx", " ty", "nice", "good job", "good bot", "cool", "great work", "well done")):
        return "You're welcome! Anything else you'd like to pull from your data?"
    if "incorrect" in t or "wrong" in t or "not correct" in t:
        return ("Sorry about that. I read the live database — tell me the platform + month you expected "
                "and I'll recheck, or rephrase and I'll try again.")
    if re.search(r"how are (you|u)|how r u|how ?are ?u|hor vi kida|sat sri akal|kiddan|how'?s it going", t):
        return ("Doing great and ready to dig into your data! Ask me about POs, liters, alerts, ads, "
                "targets, pendency and more.")
    if re.search(r"busine\w* mode", t):
        return ("I always read the live database directly, so my numbers aren't affected by the app's "
                "Business Mode display toggle — you'll get the real figures either way.")
    if re.search(r"what r u|what are you|who (are|r) (you|u)|are you (a )?(bot|ai)|"
                 r"is (you|u) ready|are you ready|ready for deploy|are you there", t):
        return ("I'm JivoBot — your data assistant. I read your live Jivo operations data and answer questions "
                "about POs, liters, secondary sales, inventory, alerts, ads, targets, pendency, state sales and "
                "more, and can export any answer to Excel. What would you like to know?")
    return "Hi there! " + HELP_TEXT.removeprefix("Hi! ")


def _previous_user_question(conversation, current_text: str) -> str | None:
    """The last thing the user asked in this conversation, other than the current
    message — used to give context to a bare follow-up like 'in june'."""
    if conversation is None:
        return None
    try:
        from ..models import ChatMessage

        texts = list(
            ChatMessage.objects.filter(conversation=conversation, role="user")
            .order_by("-id").values_list("text", flat=True)[:6]
        )
    except Exception:
        return None
    cur = (current_text or "").strip().lower()
    for prev in texts:
        if (prev or "").strip().lower() != cur:
            return prev
    return None


def _try_continuation(user, conversation, message, db_platforms) -> EngineResult | None:
    """Combine a bare follow-up with the previous question and answer that, e.g.
    prev 'total order ltrs in blinkit' + 'in june' -> answered for June."""
    prev = _previous_user_question(conversation, message)
    if not prev:
        return None
    q2 = nlu.parse(f"{prev} {message}", db_platforms=db_platforms)
    if q2.intent in ("help", "greeting", "unknown"):
        return None
    tool = _ROUTES.get(q2.intent)
    if tool is None:
        return None
    try:
        result: DataResult = tool(q2)
    except Exception:
        logger.exception("continuation tool %s failed", q2.intent)
        return None
    text = result.summary
    file_obj = None
    if q2.wants_excel and result.ok and result.rows:
        file_obj = _make_excel(user, conversation, result)
        if file_obj:
            text += "\n\n📊 Your Excel file is ready — use the download button below."
    data = _preview(result)
    sugg = result.suggestions or (_related_suggestions(q2) if result.ok else [])
    if sugg:
        data["suggestions"] = sugg
    return EngineResult(text=text, data=data, intent=q2.intent, engine="builtin",
                        is_error=not result.ok, file=file_obj, suggestions=sugg)


def _run_builtin(user, conversation: ChatConversation, message: str) -> EngineResult:
    db_platforms = tools.get_active_platforms()
    q = nlu.parse(message, db_platforms=db_platforms)
    low = message.lower()

    # Explicit Master-PO Google Sheet routing overrides generic PO handling.
    if "google sheet" in low or "master po sheet" in low or "from the sheet" in low:
        q.intent = "master_sheet"

    if q.intent == "greeting":
        return EngineResult(
            text=_smalltalk_reply(message), intent="greeting", engine="builtin",
            suggestions=SUGGESTIONS, data={"columns": [], "rows": [], "suggestions": SUGGESTIONS},
        )
    if q.intent == "help":
        return EngineResult(
            text=HELP_TEXT, intent="help", engine="builtin", suggestions=SUGGESTIONS,
            data={"columns": [], "rows": [], "suggestions": SUGGESTIONS},
        )
    if q.intent == "unknown":
        # A bare follow-up ("in june", "platform wise") only makes sense with the
        # previous question — try combining it with the last thing the user asked.
        cont = _try_continuation(user, conversation, message, db_platforms)
        if cont is not None:
            return cont
        return EngineResult(
            text=HELP_TEXT, intent="unknown", engine="builtin", suggestions=SUGGESTIONS,
            data={"columns": [], "rows": [], "suggestions": SUGGESTIONS},
        )

    tool = _ROUTES.get(q.intent)
    if tool is None:
        return EngineResult(text=HELP_TEXT, intent="unknown", engine="builtin",
                            suggestions=SUGGESTIONS)

    try:
        result: DataResult = tool(q)
    except Exception as exc:
        logger.exception("tool %s failed", q.intent)
        return EngineResult(
            text=f"Sorry — I hit an error reading that data: {exc}",
            intent=q.intent, engine="builtin", is_error=True,
        )

    text = result.summary
    file_obj = None
    if q.wants_excel and result.ok and result.rows:
        file_obj = _make_excel(user, conversation, result)
        if file_obj:
            text += "\n\n📊 Your Excel file is ready — use the download button below."
        else:
            text += "\n\n(I couldn't build an Excel file for this result.)"

    data = _preview(result)
    sugg = result.suggestions or (_related_suggestions(q) if result.ok else [])
    if sugg:
        data["suggestions"] = sugg

    return EngineResult(
        text=text, data=data, intent=q.intent, engine="builtin",
        is_error=not result.ok, file=file_obj, suggestions=sugg,
    )


def answer_question(user, conversation: ChatConversation, message: str) -> EngineResult:
    """Main entry point. Never raises — returns an error EngineResult instead."""
    mode = engine_mode()
    if mode == "claude":
        try:
            from .llm import answer_with_claude

            res = answer_with_claude(user, conversation, message)
            if res is not None:
                return res
        except Exception:
            logger.exception("Claude engine failed; falling back to built-in")

    try:
        return _run_builtin(user, conversation, message)
    except Exception as exc:
        logger.exception("built-in engine failed")
        return EngineResult(
            text=f"Sorry — something went wrong: {exc}",
            intent="error", engine="builtin", is_error=True,
        )
