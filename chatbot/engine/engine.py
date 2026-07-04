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
}


def _run_builtin(user, conversation: ChatConversation, message: str) -> EngineResult:
    db_platforms = tools.get_active_platforms()
    q = nlu.parse(message, db_platforms=db_platforms)
    low = message.lower()

    # Explicit Master-PO Google Sheet routing overrides generic PO handling.
    if "google sheet" in low or "master po sheet" in low or "from the sheet" in low:
        q.intent = "master_sheet"

    if q.intent in ("help", "greeting", "unknown"):
        return EngineResult(
            text=HELP_TEXT if q.intent != "greeting" else "Hi! " + HELP_TEXT,
            intent=q.intent, engine="builtin", suggestions=SUGGESTIONS,
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
    if result.suggestions:
        data["suggestions"] = result.suggestions

    return EngineResult(
        text=text, data=data, intent=q.intent, engine="builtin",
        is_error=not result.ok, file=file_obj, suggestions=result.suggestions,
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
