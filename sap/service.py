"""Read-only SAP HANA service layer.

We don't use the Django ORM for HANA — no maintained HANA backend exists for
Django 5. Instead, this module opens hdbcli connections per-request, runs
parameterized SELECTs, and returns plain dicts. All queries are SELECT-only
and reject any statement that isn't.

Connection pooling is deliberately omitted (keep-alives with hdbcli can leak
sessions in gunicorn workers). Open, use, close. If you need a pool, wrap
with `sqlalchemy` or add `hdbcli`'s own connection pool separately.
"""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from typing import Any, Iterator

from django.conf import settings

logger = logging.getLogger(__name__)

try:
    from hdbcli import dbapi
except ImportError:  # pragma: no cover - only hits when package not yet installed
    dbapi = None

_WRITE_KEYWORDS = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|CREATE|ALTER|GRANT|REVOKE|CALL)\b",
    re.IGNORECASE,
)


@contextmanager
def hana_connection(schema: str | None = None) -> Iterator[Any]:
    if dbapi is None:
        raise RuntimeError("hdbcli is not installed. pip install hdbcli.")
    cfg = settings.HANA
    conn = dbapi.connect(
        address=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        currentSchema=(schema or cfg["schema"] or None),
        autocommit=False,
    )
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _assert_readonly(sql: str) -> None:
    if _WRITE_KEYWORDS.search(sql):
        raise RuntimeError("Only SELECT statements are allowed against SAP HANA.")


def select(
    sql: str, params: list | tuple | None = None, schema: str | None = None
) -> list[dict]:
    """Run a parameterized SELECT and return rows as list[dict]. `schema`
    overrides the connection's default currentSchema for this query, so the
    same unqualified-table SQL can target either company DB (mart / oil)."""
    _assert_readonly(sql)
    with hana_connection(schema) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


# All HANA company schemas we can target, keyed by the `source` the frontend
# sends (mart / oil). Single place to add a new company DB — both the
# sales-analysis procedure names and the inventory grid derive from this.
HANA_SCHEMAS: dict[str, str] = {
    "mart": "JIVO_MART_HANADB",
    "oil":  "JIVO_OIL_HANADB",
}
DEFAULT_SOURCE = "mart"


def resolve_schema(source: str | None) -> tuple[str, str]:
    """Map a `source` key to (source_key, schema_name). Unknown/blank sources
    fall back to the default so a stray value never 500s the grid."""
    key = (source or DEFAULT_SOURCE).strip().lower()
    if key not in HANA_SCHEMAS:
        key = DEFAULT_SOURCE
    return key, HANA_SCHEMAS[key]


# Allow-listed HANA procedures the sales-analysis endpoint may call — one per
# schema, derived from HANA_SCHEMAS so there's a single source of truth.
SALES_ANALYSIS_PROCEDURES: dict[str, str] = {
    key: f'"{schema}"."REPORT_SALES_ANALYSIS"'
    for key, schema in HANA_SCHEMAS.items()
}
SALES_ANALYSIS_DEFAULT_SOURCE = DEFAULT_SOURCE


def _resolve_sales_analysis_procedure(source: str | None) -> tuple[str, str]:
    """Returns (source_key, fully_quoted_procedure_name). Raises ValueError
    for unknown sources so the view can surface a clean 400."""
    key = (source or SALES_ANALYSIS_DEFAULT_SOURCE).strip().lower()
    proc = SALES_ANALYSIS_PROCEDURES.get(key)
    if not proc:
        allowed = ", ".join(sorted(SALES_ANALYSIS_PROCEDURES))
        raise ValueError(f"Unknown sales-analysis source '{source}'. Allowed: {allowed}.")
    return key, proc


def report_sales_analysis(
    from_date: str,
    to_date: str,
    source: str = SALES_ANALYSIS_DEFAULT_SOURCE,
) -> list[dict]:
    """Run an allow-listed SAP HANA sales analysis procedure.

    `source` picks which schema's REPORT_SALES_ANALYSIS to call — one of
    the keys in SALES_ANALYSIS_PROCEDURES (mart / oil). Defaults to mart.

    Logs the raw row count returned by HANA so we can verify whether the
    procedure itself caps results or our pipeline drops some downstream.
    """
    source_key, procedure = _resolve_sales_analysis_procedure(source)
    with hana_connection() as conn:
        cur = conn.cursor()
        cur.execute(f'CALL {procedure}(?, ?)', [from_date, to_date])
        cols = [d[0] for d in cur.description] if cur.description else []
        raw_rows = cur.fetchall() if cur.description else []
        rowcount_attr = getattr(cur, "rowcount", "n/a")
        cur.close()
    result = [dict(zip(cols, r)) for r in raw_rows]
    logger.warning(
        "[SAP] report_sales_analysis(%s, %s, source=%s) -> fetchall=%d rows, cur.rowcount=%s, cols=%d",
        from_date,
        to_date,
        source_key,
        len(raw_rows),
        rowcount_attr,
        len(cols),
    )
    return result


def scalar(sql: str, params: list | tuple | None = None):
    rows = select(sql, params)
    if not rows:
        return None
    return next(iter(rows[0].values()))


# --- Query helpers -----------------------------------------------------------

def distributors(search: str = "", limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
    """Return (rows, total_count) for OSLP distributor list."""
    where = ""
    params: list = []
    if search:
        where = "WHERE CardCode LIKE ? OR CardName LIKE ?"
        params.extend([f"%{search}%", f"%{search}%"])

    total = scalar(f"SELECT COUNT(*) FROM OCRD {where}", params) or 0
    rows = select(
        f"""
        SELECT CardCode, CardName, Phone1 AS phone, City, Country, Balance
        FROM OCRD
        {where}
        ORDER BY CardCode
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    )
    return rows, int(total)


def invoices(search: str = "", limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
    where = ""
    params: list = []
    if search:
        where = "WHERE DocNum LIKE ? OR CardCode LIKE ?"
        params.extend([f"%{search}%", f"%{search}%"])

    total = scalar(f"SELECT COUNT(*) FROM OINV {where}", params) or 0
    rows = select(
        f"""
        SELECT DocNum, CardCode, CardName, DocDate, DocTotal, DocStatus
        FROM OINV
        {where}
        ORDER BY DocDate DESC
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    )
    return rows, int(total)
