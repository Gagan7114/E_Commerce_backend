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
from django.core.cache import cache

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
        # Give up quickly if HANA is unreachable so a down VPN/host fails fast
        # (raises a connect error) instead of freezing the request for ~30s.
        connectTimeout=cfg.get("connect_timeout_ms", 5000),
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


def _apply_query_timeout(cur) -> None:
    """Bound how long a single statement may run on the worker thread. Without
    this, connectTimeout only protects the handshake — a slow query/proc would
    pin a gunicorn worker until HANA returns. Best-effort: never break the query
    if the driver doesn't expose setquerytimeout."""
    try:
        timeout_s = int(settings.HANA.get("query_timeout_s", 0) or 0)
    except (TypeError, ValueError):
        timeout_s = 0
    if timeout_s <= 0:
        return
    try:
        cur.setquerytimeout(timeout_s)
    except Exception:
        # Older/newer hdbcli without setquerytimeout — leave the query unbounded
        # rather than fail it.
        pass


def select(
    sql: str, params: list | tuple | None = None, schema: str | None = None
) -> list[dict]:
    """Run a parameterized SELECT and return rows as list[dict]. `schema`
    overrides the connection's default currentSchema for this query, so the
    same unqualified-table SQL can target either company DB (mart / oil)."""
    _assert_readonly(sql)
    with hana_connection(schema) as conn:
        cur = conn.cursor()
        _apply_query_timeout(cur)
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

# Finished-goods warehouse codes shown as the columns of the JM Inventory
# dashboard's FG pivot. Single source of truth: the dashboard view
# (sap.views.inventory_finished_goods) and the chatbot's jm_inventory tool +
# NLU both import these so they never drift apart.
FG_WAREHOUSE_CODES: tuple[str, ...] = (
    "BH-FGM", "DL-MP", "DL-EC", "DL-GR", "DL-FG", "BH-JM",
    "FBF-HR", "KT-FG", "DL-INT", "KT-FBF", "PB-FG", "BH-GR", "BH-FG",
)
# OITB item-group name that marks a finished good (matched case-insensitively).
FG_GROUP_NAME = "FINISHED"

# Match an FG warehouse code named in free text: 'dl fg' / 'dl-fg' / 'DLFG' ->
# 'DL-FG'. Longest codes first (in the alternation) so 'BH-FGM' wins over
# 'BH-FG', and the word-boundary lookarounds stop a shorter code from matching
# inside a longer word. Used by the chatbot's NLU (intent detection) and its
# jm_inventory tool (which code to filter), so both agree on the code list.
_FG_WHS_RE = re.compile(
    r"(?<![a-z0-9])(?:"
    + "|".join(
        code.lower().replace("-", r"[\s\-]?")
        for code in sorted(FG_WAREHOUSE_CODES, key=len, reverse=True)
    )
    + r")(?![a-z0-9])",
    re.IGNORECASE,
)


def match_fg_warehouse(text: str) -> str | None:
    """Return the canonical FG warehouse code named in ``text`` (e.g. 'dl fg' ->
    'DL-FG'), or None when no code is present."""
    if not text:
        return None
    m = _FG_WHS_RE.search(text)
    if not m:
        return None
    norm = re.sub(r"[\s\-]", "", m.group(0)).upper()
    for code in FG_WAREHOUSE_CODES:
        if code.replace("-", "") == norm:
            return code
    return None


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

    The raw proc result is cached on (source, from_date, to_date) so that all
    filter/search/page/aggregate permutations of the same date range share a
    single HANA CALL within the cache TTL. Python filtering in the view is
    unchanged — equivalent by construction (same proc + same inputs = same rows).

    Logs the raw row count returned by HANA so we can verify whether the
    procedure itself caps results or our pipeline drops some downstream.
    """
    source_key, procedure = _resolve_sales_analysis_procedure(source)

    # Proc-level cache: key only on the inputs that actually reach HANA.
    # Filter/search/page params never change what the proc returns, so caching
    # on those (as the view-level @cached_get does) re-calls HANA needlessly.
    _proc_key = f"sap:proc:{source_key}:{from_date}:{to_date}"
    try:
        _cached = cache.get(_proc_key)
    except Exception:
        _cached = None
    if _cached is not None:
        return _cached

    with hana_connection() as conn:
        cur = conn.cursor()
        _apply_query_timeout(cur)
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
    try:
        cache.set(_proc_key, result, timeout=120)
    except Exception:
        pass
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
