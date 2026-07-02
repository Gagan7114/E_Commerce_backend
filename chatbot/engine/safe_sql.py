"""Guarded read-only access to Postgres for the chatbot.

Same philosophy as sap/service.py's HANA guard: only SELECT/WITH statements
run, a per-statement timeout bounds runaway queries, and results are capped.
Adds `information_schema` introspection so the PO / inventory tools can work
against the real (externally-managed) table columns without hard-coding names
— the reflected warehouse models only carry placeholder columns.
"""

from __future__ import annotations

import re

from django.conf import settings
from django.db import connection, transaction

# Reject anything that isn't a read. A single statement only (a trailing ';' is
# tolerated but an interior one — stacked statements — is not).
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|CREATE|ALTER|GRANT|REVOKE|"
    r"CALL|COPY|VACUUM|COMMENT|SET|LOCK|DO)\b",
    re.IGNORECASE,
)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def default_max_rows() -> int:
    return int(getattr(settings, "CHATBOT_MAX_ROWS", 5000))


def default_timeout_ms() -> int:
    return int(getattr(settings, "CHATBOT_SQL_TIMEOUT_MS", 8000))


class UnsafeQuery(RuntimeError):
    pass


def is_safe_identifier(name: str) -> bool:
    return bool(name) and bool(_IDENTIFIER.match(name)) and len(name) <= 63


def assert_readonly(sql: str) -> None:
    stripped = (sql or "").strip().rstrip(";").strip()
    if not stripped:
        raise UnsafeQuery("Empty query.")
    if ";" in stripped:
        raise UnsafeQuery("Multiple statements are not allowed.")
    lowered = stripped.lstrip("(").lstrip()
    if not re.match(r"^(SELECT|WITH)\b", lowered, re.IGNORECASE):
        raise UnsafeQuery("Only SELECT / WITH queries are allowed.")
    if _WRITE_KEYWORDS.search(stripped):
        raise UnsafeQuery("Only read-only SELECT queries are allowed.")


def run_select(
    sql: str,
    params: list | tuple | None = None,
    max_rows: int | None = None,
    timeout_ms: int | None = None,
) -> tuple[list[str], list[list], bool]:
    """Run a SELECT and return ``(columns, rows, truncated)``.

    ``rows`` is a list of lists. ``truncated`` is True when more rows existed
    than ``max_rows`` (one extra row is fetched to detect this).
    """
    assert_readonly(sql)
    cap = int(max_rows or default_max_rows())
    tmo = int(timeout_ms or default_timeout_ms())

    with transaction.atomic():
        with connection.cursor() as cur:
            # SET LOCAL is scoped to this transaction and auto-resets after it,
            # so we never leave a lingering statement_timeout on the pooled
            # connection. tmo is an int we control — safe to inline.
            cur.execute(f"SET LOCAL statement_timeout = {int(tmo)}")
            cur.execute(sql, params or [])
            columns = [d[0] for d in cur.description] if cur.description else []
            fetched = cur.fetchmany(cap + 1)

    truncated = len(fetched) > cap
    rows = [list(r) for r in fetched[:cap]]
    return columns, rows, truncated


def table_exists(table: str) -> bool:
    if not is_safe_identifier(table):
        return False
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            LIMIT 1
            """,
            [table],
        )
        return cur.fetchone() is not None


def table_columns(table: str) -> list[dict]:
    """Return ``[{"name": str, "type": str}, ...]`` for a public table/view."""
    if not is_safe_identifier(table):
        raise UnsafeQuery(f"Invalid table name: {table!r}")
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            [table],
        )
        return [{"name": r[0], "type": r[1]} for r in cur.fetchall()]


def find_column(columns: list[dict], *candidates: str) -> str | None:
    """Best-effort match a logical column to a real one.

    Tries exact (case-insensitive) names first, then substring contains.
    """
    names = [c["name"] for c in columns]
    lower = {n.lower(): n for n in names}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    for cand in candidates:
        for n in names:
            if cand.lower() in n.lower():
                return n
    return None


def date_like_columns(columns: list[dict]) -> list[str]:
    out = []
    for c in columns:
        t = (c["type"] or "").lower()
        if "date" in t or "timestamp" in t or "date" in c["name"].lower():
            out.append(c["name"])
    return out
