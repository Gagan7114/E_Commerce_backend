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

import re
from contextlib import contextmanager
from typing import Any, Iterator

from django.conf import settings

try:
    from hdbcli import dbapi
except ImportError:  # pragma: no cover - only hits when package not yet installed
    dbapi = None

_WRITE_KEYWORDS = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|CREATE|ALTER|GRANT|REVOKE|CALL)\b",
    re.IGNORECASE,
)


@contextmanager
def hana_connection() -> Iterator[Any]:
    if dbapi is None:
        raise RuntimeError("hdbcli is not installed. pip install hdbcli.")
    cfg = settings.HANA
    conn = dbapi.connect(
        address=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        currentSchema=cfg["schema"] or None,
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


def select(sql: str, params: list | tuple | None = None) -> list[dict]:
    """Run a parameterized SELECT and return rows as list[dict]."""
    _assert_readonly(sql)
    with hana_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


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
