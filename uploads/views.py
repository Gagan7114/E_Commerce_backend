"""Batch upload endpoint. Mirrors FastAPI routes/upload.py.

Contract (JSON body):
  {
    "table":       "blinkit_inventory",
    "data":        [ {...}, {...} ],
    "unique_key":  "sku,warehouse",   // comma-separated; optional
    "upsert":      true                // default true
  }

Returns: {"success": N, "failed": M, "error": "..." | null}
"""

from __future__ import annotations

import re

from django.db import connection
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Tables the uploader is allowed to write to (mirrors FastAPI).
UPLOAD_ALLOWED_TABLES = {
    # Inventory
    "blinkit_inventory", "zepto_inventory", "swiggy_inventory",
    "bigbasket_inventory", "jiomart_inventory", "amazon_inventory",
    # Secondary sells
    "blinkitSec", "zeptoSec", "swiggySec", "flipkartSec",
    "jiomartSec", "bigbasketSec", "amazon_sec_daily", "amazon_sec_range",
}

BATCH_SIZE = 50


@api_view(["POST"])
@permission_classes([require("upload.use")])
def batch_upload(request):
    body = request.data or {}
    table = body.get("table")
    data = body.get("data") or []
    unique_key = body.get("unique_key") or ""
    upsert = bool(body.get("upsert", True))

    if table not in UPLOAD_ALLOWED_TABLES:
        return Response(
            {"detail": f"Table '{table}' is not allowed for upload"},
            status=400,
        )
    if not _IDENT.match(table):
        return Response({"detail": "Invalid table name."}, status=400)
    if not isinstance(data, list) or not data:
        return Response({"success": 0, "failed": 0, "error": None})

    columns = list(data[0].keys())
    invalid = [c for c in columns if not _IDENT.match(c)]
    if invalid:
        return Response({"detail": f"Invalid column identifiers: {invalid}"}, status=400)

    quoted_cols = [f'"{c}"' for c in columns]
    col_list = ", ".join(quoted_cols)
    placeholders = ", ".join(["%s"] * len(columns))

    upsert_clause = ""
    if upsert and unique_key:
        keys = [k.strip() for k in unique_key.split(",") if k.strip()]
        for k in keys:
            if not _IDENT.match(k):
                return Response(
                    {"detail": f"Invalid unique_key identifier: {k!r}"}, status=400
                )
        conflict_cols = ", ".join(f'"{k}"' for k in keys)
        update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in keys]
        if update_cols:
            upsert_clause = (
                f' ON CONFLICT ({conflict_cols}) DO UPDATE SET {", ".join(update_cols)}'
            )
        else:
            upsert_clause = f" ON CONFLICT ({conflict_cols}) DO NOTHING"

    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}){upsert_clause}'

    success = 0
    failed = 0
    last_error: str | None = None

    with connection.cursor() as cur:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            for row in batch:
                try:
                    cur.execute(sql, [row.get(c) for c in columns])
                    success += 1
                except Exception as e:
                    failed += 1
                    last_error = str(e)

    return Response({"success": success, "failed": failed, "error": last_error})
