"""Raw-rows reports endpoint.

Exposes a whitelisted set of database views as JSON for the global Reports page
(Frontend/src/pages/Reports.jsx). No formulas, no transformations - just
SELECT <cols> FROM <view> WHERE <filters> LIMIT N.
"""

import re

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from . import reports_sap


REPORT_VIEW_CATALOG = {
    "all_platform_inventory": {
        "date_column": "inventory_date",
        "format_column": "format",
        "max_rows": 50000,
    },
    "master_po": {
        "date_column": "po_date",
        "format_column": "format",
        "max_rows": 50000,
    },
    "prim_master_po": {
        "date_column": "po_date",
        "date_expr": "public._pm_parse_date(\"po_date\")",
        "format_column": "format",
        "max_rows": 50000,
    },
    "SecMaster": {
        "date_column": "date",
        "format_column": "format",
        "max_rows": 50000,
    },
    "amazon_sec_range_master_view": {
        "date_column": "to_date",
        "format_column": None,
        "max_rows": 50000,
    },
    "amazon_sec_daily_master_view": {
        "date_column": "to_date",
        "format_column": None,
        "max_rows": 50000,
    },
}

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _safe_view(name: str) -> str:
    name = (name or "").strip()
    if name not in REPORT_VIEW_CATALOG:
        raise ValidationError(f"Unknown report view: {name!r}")
    return name


def _safe_col(name: str) -> str:
    if not name or not _IDENT.match(name):
        raise ValidationError(f"Invalid column name: {name!r}")
    return name


def _normalised_format(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def report_columns(request):
    view_raw = request.query_params.get("view", "")
    if reports_sap.is_sap_view(view_raw):
        try:
            sap_cols = reports_sap.columns_for(view_raw)
        except ValueError as exc:
            raise ValidationError(str(exc))
        return Response({
            "view": view_raw,
            "columns": [{"key": name, "type": dtype} for name, dtype in sap_cols],
        })
    view = _safe_view(view_raw)
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            [view],
        )
        cols = cur.fetchall()
    return Response({
        "view": view,
        "columns": [{"key": name, "type": dtype} for name, dtype in cols],
    })


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def report_raw(request):
    view_raw = request.query_params.get("view", "")
    if reports_sap.is_sap_view(view_raw):
        try:
            page = max(0, int(request.query_params.get("page") or 0))
        except ValueError:
            page = 0
        try:
            page_size = int(request.query_params.get("page_size") or 200)
        except ValueError:
            page_size = 200
        page_size = max(1, min(50000, page_size))
        date_from = (request.query_params.get("date_from") or "").strip()
        date_to = (request.query_params.get("date_to") or "").strip()
        if date_from and not _DATE.match(date_from):
            raise ValidationError("`date_from` must be YYYY-MM-DD.")
        if date_to and not _DATE.match(date_to):
            raise ValidationError("`date_to` must be YYYY-MM-DD.")
        try:
            rows, count = reports_sap.fetch_for(
                view_raw,
                from_date=date_from,
                to_date=date_to,
                page=page,
                page_size=page_size,
            )
        except ValueError as exc:
            raise ValidationError(str(exc))
        except Exception as exc:  # noqa: BLE001 — surface SAP errors to the UI
            return Response({
                "view": view_raw,
                "rows": [],
                "columns": [],
                "count": 0,
                "page": page,
                "page_size": page_size,
                "error": str(exc),
            })
        response_columns = list(rows[0].keys()) if rows else []
        return Response({
            "view": view_raw,
            "rows": rows,
            "columns": response_columns,
            "count": count,
            "page": page,
            "page_size": page_size,
        })
    view = _safe_view(view_raw)
    catalog = REPORT_VIEW_CATALOG[view]

    requested_columns = (request.query_params.get("columns") or "").strip()
    columns: list[str] = []
    if requested_columns:
        for c in requested_columns.split(","):
            c = c.strip()
            if not c:
                continue
            _safe_col(c)
            columns.append(c)
    select_clause = ", ".join(f'"{c}"' for c in columns) if columns else "*"

    where_parts: list[str] = []
    params: list = []

    fmt = (request.query_params.get("platform") or request.query_params.get("fmt") or "").strip()
    if fmt and catalog["format_column"]:
        col = catalog["format_column"]
        where_parts.append(
            f"REGEXP_REPLACE(LOWER(TRIM(\"{col}\"::text)), '[^a-z0-9]+', '', 'g') = %s"
        )
        params.append(_normalised_format(fmt))

    date_from = (request.query_params.get("date_from") or "").strip()
    date_to = (request.query_params.get("date_to") or "").strip()
    if catalog["date_column"]:
        date_expr = catalog.get("date_expr") or f'("{catalog["date_column"]}")::date'
        if date_from:
            if not _DATE.match(date_from):
                raise ValidationError("`date_from` must be YYYY-MM-DD.")
            where_parts.append(f"{date_expr} >= %s")
            params.append(date_from)
        if date_to:
            if not _DATE.match(date_to):
                raise ValidationError("`date_to` must be YYYY-MM-DD.")
            where_parts.append(f"{date_expr} <= %s")
            params.append(date_to)

    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    try:
        page = max(0, int(request.query_params.get("page") or 0))
    except ValueError:
        page = 0
    try:
        page_size = int(request.query_params.get("page_size") or 200)
    except ValueError:
        page_size = 200
    page_size = max(1, min(catalog["max_rows"], page_size))
    offset = page * page_size

    try:
        with connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{view}" {where_clause}', params)
            count_row = cur.fetchone()
            count = int(count_row[0]) if count_row else 0
            cur.execute(
                f'SELECT {select_clause} FROM "{view}" {where_clause} LIMIT %s OFFSET %s',
                params + [page_size, offset],
            )
            description = cur.description or []
            response_columns = [c[0] for c in description]
            rows = [dict(zip(response_columns, row)) for row in cur.fetchall()]
    except Exception as exc:
        return Response({
            "view": view,
            "rows": [],
            "columns": [],
            "count": 0,
            "page": page,
            "page_size": page_size,
            "error": str(exc),
        })

    return Response({
        "view": view,
        "rows": rows,
        "columns": response_columns,
        "count": count,
        "page": page,
        "page_size": page_size,
    })
