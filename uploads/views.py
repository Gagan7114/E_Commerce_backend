"""Batch upsert endpoint for the external uploader tool.

Contract (JSON body):
  {
    "table":      "blinkit_inventory",   // warehouse table name
    "unique_keys": ["sku"],              // columns to match on for upsert
    "rows": [ {...}, {...} ]             // list of row dicts; max 5000/request
  }

Returns: {"inserted": N, "updated": M, "failed": 0}

The endpoint is intentionally generic — it validates the target table is
known (via PlatformConfig), all columns in `rows` are real columns on that
table, and then issues a single INSERT ... ON CONFLICT ... DO UPDATE inside
a transaction.
"""

from __future__ import annotations

import re

from django.db import connection, transaction
from rest_framework import serializers, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require
from platforms.models import PlatformConfig

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAX_ROWS = 5000


class BatchSerializer(serializers.Serializer):
    table = serializers.CharField(max_length=80)
    unique_keys = serializers.ListField(child=serializers.CharField(max_length=80), min_length=1)
    rows = serializers.ListField(child=serializers.DictField(), min_length=1, max_length=MAX_ROWS)

    def validate_table(self, value):
        if not _IDENT.match(value):
            raise serializers.ValidationError("Invalid table name.")
        known = set(PlatformConfig.objects.values_list("inventory_table", flat=True)) | \
                set(PlatformConfig.objects.values_list("secondary_table", flat=True)) | \
                set(PlatformConfig.objects.values_list("master_po_table", flat=True))
        if value not in known:
            raise serializers.ValidationError(f"Table {value!r} is not a registered warehouse table.")
        return value

    def validate_unique_keys(self, value):
        for k in value:
            if not _IDENT.match(k):
                raise serializers.ValidationError(f"Invalid column identifier: {k!r}")
        return value


def _table_columns(table: str) -> set[str]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND table_schema = current_schema()
            """,
            [table],
        )
        return {r[0] for r in cur.fetchall()}


@api_view(["POST"])
@permission_classes([require("upload.use")])
def batch_upsert(request):
    ser = BatchSerializer(data=request.data)
    ser.is_valid(raise_exception=True)
    data = ser.validated_data

    table = data["table"]
    unique_keys = data["unique_keys"]
    rows = data["rows"]

    all_cols = _table_columns(table)
    if not all_cols:
        return Response({"detail": f"Table {table!r} has no columns / does not exist."},
                        status=status.HTTP_400_BAD_REQUEST)
    if not set(unique_keys).issubset(all_cols):
        return Response({"detail": f"unique_keys not all present in table: {set(unique_keys) - all_cols}"},
                        status=status.HTTP_400_BAD_REQUEST)

    # Pick the columns we actually will write — intersection of provided row keys and real columns.
    provided_cols: set[str] = set()
    for r in rows:
        provided_cols.update(r.keys())
    invalid = [c for c in provided_cols if not _IDENT.match(c)]
    if invalid:
        return Response({"detail": f"Invalid column identifiers: {invalid}"},
                        status=status.HTTP_400_BAD_REQUEST)
    cols = sorted(provided_cols & all_cols)
    if not cols:
        return Response({"detail": "No matching columns between rows and target table."},
                        status=status.HTTP_400_BAD_REQUEST)
    for uk in unique_keys:
        if uk not in cols:
            cols.append(uk)

    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = "(" + ", ".join(["%s"] * len(cols)) + ")"
    update_cols = [c for c in cols if c not in unique_keys]
    conflict_cols = ", ".join(f'"{c}"' for c in unique_keys)
    if update_cols:
        update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        do_clause = f"DO UPDATE SET {update_clause}"
    else:
        do_clause = "DO NOTHING"

    values: list = []
    for r in rows:
        values.append(tuple(r.get(c) for c in cols))

    sql = (
        f'INSERT INTO "{table}" ({col_list}) VALUES {placeholders} '
        f'ON CONFLICT ({conflict_cols}) {do_clause}'
    )

    # Execute one row at a time so we can report inserted vs updated. Batched
    # execute_values would be faster but blurs the insert/update split.
    inserted = updated = 0
    with transaction.atomic(), connection.cursor() as cur:
        for vals in values:
            cur.execute(sql + " RETURNING (xmax = 0) AS was_insert", vals)
            row = cur.fetchone()
            if row is None:
                continue  # DO NOTHING path with no updatable cols
            if row[0]:
                inserted += 1
            else:
                updated += 1

    return Response({"inserted": inserted, "updated": updated, "failed": 0})
