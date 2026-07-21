"""Optional Claude (Anthropic) engine.

Activated only when ANTHROPIC_API_KEY is set and the `anthropic` package is
installed. Uses the same curated data tools via Claude tool-use so deep, novel
questions get real multi-step reasoning. Any failure returns None so the caller
falls back to the built-in rules engine.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal

from django.conf import settings

from ..models import ChatConversation
from . import tools
from .nlu import ParsedQuery

logger = logging.getLogger(__name__)

MODEL = getattr(settings, "CHATBOT_CLAUDE_MODEL", "claude-opus-4-8")
MAX_TOOL_ROWS = 60
MAX_ITERS = 6

SYSTEM_PROMPT = (
    "You are the data assistant embedded in an e-commerce operations dashboard "
    "(platforms like Blinkit, Zepto, Amazon, BigBasket, Swiggy, Flipkart, JioMart). "
    "Answer questions about purchase orders, inventory DOH alerts, delivered/sold "
    "liters, shipments, and secondary sales. "
    "ALWAYS call a tool to get real numbers — never invent data. State which source "
    "a figure comes from. If the user asks for an Excel/spreadsheet/download, call "
    "generate_excel after fetching the data. Be concise and specific."
)

_TOOL_DEFS = [
    {"name": "list_platforms", "description": "List configured platforms.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "query_alerts",
     "description": "Inventory DOH (days-on-hand) alerts / low-stock notifications.",
     "input_schema": {"type": "object", "properties": {
         "platform": {"type": "string", "description": "platform slug e.g. blinkit"},
         "severity": {"type": "string", "enum": ["critical", "warning"]},
         "active_only": {"type": "boolean"},
         "date_from": {"type": "string", "description": "YYYY-MM-DD"},
         "date_to": {"type": "string"}, "top_n": {"type": "integer"}}}},
    {"name": "query_liters",
     "description": "Delivered or sold liters. movement='delivered' uses shipments; 'sold' uses sales/DOH snapshots.",
     "input_schema": {"type": "object", "properties": {
         "platform": {"type": "string"}, "movement": {"type": "string", "enum": ["delivered", "sold"]},
         "date_from": {"type": "string"}, "date_to": {"type": "string"}}}},
    {"name": "query_shipments", "description": "Shipment/dispatch records with liters and status.",
     "input_schema": {"type": "object", "properties": {
         "date_from": {"type": "string"}, "date_to": {"type": "string"}, "top_n": {"type": "integer"}}}},
    {"name": "query_purchase_orders", "description": "Purchase orders (master_po). Optional platform + date range.",
     "input_schema": {"type": "object", "properties": {
         "platform": {"type": "string"}, "date_from": {"type": "string"}, "date_to": {"type": "string"},
         "top_n": {"type": "integer"}}}},
    {"name": "query_inventory", "description": "Current inventory / stock on hand for a platform.",
     "input_schema": {"type": "object", "properties": {"platform": {"type": "string"}}, "required": ["platform"]}},
    {"name": "query_jm_inventory",
     "description": ("JM Inventory finished-goods stock ON HAND from SAP HANA (the JM Inventory dashboard "
                     "source). Give a warehouse code (e.g. 'DL-FG', 'BH-JM', 'KT-FG') for one warehouse, or "
                     "omit it for a per-warehouse breakdown. OnHand is a stock-unit count (the item's stock "
                     "UOM, e.g. PCS) — NOT litres. source selects the company DB: mart (default) or oil."),
     "input_schema": {"type": "object", "properties": {
         "warehouse": {"type": "string", "description": "FG warehouse code, e.g. DL-FG"},
         "source": {"type": "string", "enum": ["mart", "oil"]}}}},
    {"name": "query_secondary_sales", "description": "Secondary (sell-out) sales for a platform.",
     "input_schema": {"type": "object", "properties": {"platform": {"type": "string"}}, "required": ["platform"]}},
    {"name": "read_master_po_sheet", "description": "Read the Master PO Google Sheet.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "generate_excel",
     "description": "Build a downloadable Excel from the most recent query result.",
     "input_schema": {"type": "object", "properties": {"title": {"type": "string"}}}},
]


def _json_safe(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, Decimal):
        f = float(v)
        return int(f) if f.is_integer() else round(f, 4)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


def _mk_query(text: str, args: dict) -> ParsedQuery:
    q = ParsedQuery(text=text)
    slug = (args.get("platform") or "").strip().lower()
    if slug:
        name = next((p["name"] for p in tools.get_active_platforms() if p["slug"] == slug), slug.title())
        q.platforms = [{"slug": slug, "name": name}]
    for k in ("date_from", "date_to"):
        val = args.get(k)
        if val:
            try:
                setattr(q, k, datetime.strptime(val[:10], "%Y-%m-%d").date())
            except Exception:
                pass
    q.severity = args.get("severity", "") or ""
    q.movement = args.get("movement", "") or ""
    if q.movement == "delivered" or q.metric == "":
        q.metric = "liters" if args.get("movement") else q.metric
    if args.get("movement") == "sold":
        q.metric = "liters"
    q.top_n = args.get("top_n")
    src = (args.get("source") or "").strip().lower()
    if src in ("mart", "oil"):
        q.sap_source = src
    return q


def _run_tool(name: str, args: dict, question: str):
    q = _mk_query(question, args)
    if name == "list_platforms":
        return tools.list_platforms(q)
    if name == "query_alerts":
        if "active_only" in args:
            q.active_only = bool(args["active_only"])
        return tools.alerts(q)
    if name == "query_liters":
        return tools.liters(q)
    if name == "query_shipments":
        return tools.shipments(q)
    if name == "query_purchase_orders":
        return tools.purchase_orders(q)
    if name == "query_inventory":
        return tools.inventory(q)
    if name == "query_jm_inventory":
        whs = (args.get("warehouse") or "").strip()
        if whs:
            q.text = f"{q.text} {whs}".strip()
        return tools.jm_inventory(q)
    if name == "query_secondary_sales":
        return tools.secondary_sales(q)
    if name == "read_master_po_sheet":
        return tools.master_po_sheet(q)
    return None


def answer_with_claude(user, conversation: ChatConversation, message: str):
    from .engine import EngineResult, _make_excel, _preview

    try:
        import anthropic
    except Exception:
        return None
    api_key = getattr(settings, "ANTHROPIC_API_KEY", "") or ""
    if not api_key:
        return None

    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": message}]
    last_result = None
    last_file = None

    for _ in range(MAX_ITERS):
        resp = client.messages.create(
            model=MODEL, max_tokens=4096, system=SYSTEM_PROMPT,
            tools=_TOOL_DEFS, messages=messages,
        )
        messages.append({"role": "assistant", "content": resp.content})
        if resp.stop_reason != "tool_use":
            break

        tool_results = []
        for block in resp.content:
            if getattr(block, "type", None) != "tool_use":
                continue
            name, args = block.name, (block.input or {})
            try:
                if name == "generate_excel":
                    if last_result and last_result.rows:
                        title = args.get("title") or last_result.excel_title
                        last_result.excel_title = title
                        last_file = _make_excel(user, conversation, last_result)
                        payload = {"ok": True, "message": f"Excel '{last_file.filename}' created."}
                    else:
                        payload = {"ok": False, "message": "No data to export yet; query first."}
                else:
                    res = _run_tool(name, args, message)
                    if res is None:
                        payload = {"ok": False, "message": f"Unknown tool {name}"}
                    else:
                        last_result = res
                        payload = {
                            "ok": res.ok, "summary": res.summary, "source": res.source,
                            "columns": res.columns,
                            "rows": [[_json_safe(c) for c in r] for r in res.rows[:MAX_TOOL_ROWS]],
                            "row_count": len(res.rows),
                        }
            except Exception as exc:
                logger.exception("claude tool %s failed", name)
                payload = {"ok": False, "message": str(exc)}
            tool_results.append({
                "type": "tool_result", "tool_use_id": block.id,
                "content": json.dumps(payload, default=str),
            })
        messages.append({"role": "user", "content": tool_results})

    final_text = "".join(
        getattr(b, "text", "") for b in resp.content if getattr(b, "type", None) == "text"
    ).strip() or "Done."

    data = _preview(last_result) if last_result else {"columns": [], "rows": []}
    return EngineResult(
        text=final_text, data=data, intent="claude", engine="claude",
        is_error=False, file=last_file,
    )
