"""Call Center monthly figures — a single, isolated editable store.

Self-contained on purpose: this module does NOT read or write the existing
`month_targets` / `primary_month_targets` tables or any platform/dashboard
logic. It backs the editable cells of the frontend's Call Center row — the
`targets`, `done_ltrs` and source `date` per (month, year, item_head) — stored
in `call_center_targets` (see migrations 0043 + 0044). Every other column the
row shows (Achieved %, Est.Ltr, DRR…) is derived on the frontend from these
three, exactly like the real platform rows.

Contract (the frontend depends on this exactly):
  GET  /api/platform/call-center-targets?month=<int>&year=<int>
       -> {"premium":   {"targets": <num|null>, "done_ltrs": <num|null>,
                         "date": "YYYY-MM-DD"|null} | null,
           "commodity": { ...same... } | null}
       (a section is null when no row has been saved for it yet)
  POST /api/platform/call-center-targets
       body {"month": <int>, "year": <int>,
             "item_head": "PREMIUM"|"COMMODITY",
             "field": "targets"|"done_ltrs"|"date",
             "value": <number|string|null>}
       -> {"ok": true, "item_head": "...",
           "targets": <num|null>, "done_ltrs": <num|null>, "date": <str|null>}

Permissions mirror the existing targets endpoints (see monthly_targets.py /
primary_monthly_targets.py): the GET path requires the view permission
`platform.month_targets.view`; the POST (edit) path requires `target_sheet.edit`.
Because this is a single GET+POST view, the per-method permission is enforced
explicitly via the same `require(...)` classes those endpoints use as their
`permission_classes`.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.db import connection
from rest_framework.decorators import api_view
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from accounts.permissions import require


ITEM_HEADS = ("PREMIUM", "COMMODITY")

# Editable field name -> physical column. Whitelisted so the column can be
# interpolated into the upsert SQL safely (never user-controlled SQL).
FIELD_COLUMNS = {
    "targets": "targets",
    "done_ltrs": "done_ltrs",
    "date": "data_date",
}
NUMERIC_FIELDS = ("targets", "done_ltrs")

# Same permission classes the existing targets endpoints use:
#   GET  -> require("platform.month_targets.view")  (the targets-list/dashboard view perm)
#   POST -> require("target_sheet.edit")            (the targets-edit perm)
_VIEW_PERMISSION = require("platform.month_targets.view")
_EDIT_PERMISSION = require("target_sheet.edit")


def _enforce(request, permission_cls) -> None:
    """Run the same check DRF would for `permission_classes=[permission_cls]`.

    This view is a single GET+POST endpoint, so the permission differs per
    method and can't be a single view-level `@permission_classes`.
    """
    if not permission_cls().has_permission(request, None):
        raise PermissionDenied("You do not have permission to perform this action.")


def _parse_month_year(source) -> tuple[int, int]:
    try:
        month = int(source.get("month"))
        year = int(source.get("year"))
    except (TypeError, ValueError):
        raise ValidationError("`month` (1-12) and `year` (YYYY) are required integers.")
    if not 1 <= month <= 12:
        raise ValidationError("`month` must be 1-12.")
    if year < 2000 or year > 2100:
        raise ValidationError("`year` looks out of range.")
    return month, year


def _as_number(value) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _as_date_str(value) -> str | None:
    """A DATE column comes back as a datetime.date; expose it as 'YYYY-MM-DD'."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _section(row) -> dict | None:
    """Shape one DB row (targets, done_ltrs, data_date) for the frontend, or
    None when no row has been saved for that item_head yet."""
    if row is None:
        return None
    targets, done_ltrs, data_date = row
    return {
        "targets": _as_number(targets),
        "done_ltrs": _as_number(done_ltrs),
        "date": _as_date_str(data_date),
    }


@api_view(["GET", "POST"])
def call_center_targets(request):
    if request.method == "POST":
        return _post(request)
    return _get(request)


def _get(request):
    """GET /api/platform/call-center-targets?month=<int>&year=<int>

    Returns the saved PREMIUM / COMMODITY figures for the month (targets,
    done_ltrs, date), or null per section when no row exists yet.
    """
    _enforce(request, _VIEW_PERMISSION)
    month, year = _parse_month_year(request.query_params)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT UPPER(TRIM(item_head)), targets, done_ltrs, data_date
              FROM call_center_targets
             WHERE month = %s AND year = %s
               AND UPPER(TRIM(item_head)) IN ('PREMIUM', 'COMMODITY')
            """,
            [month, year],
        )
        rows = cur.fetchall()

    saved = {head: (targets, done_ltrs, data_date)
             for head, targets, done_ltrs, data_date in rows}
    return Response({
        "premium": _section(saved.get("PREMIUM")),
        "commodity": _section(saved.get("COMMODITY")),
    })


def _coerce_value(field: str, raw):
    """Validate + coerce a field's incoming value to the type its column needs.

    Returns a value safe to bind for `targets`/`done_ltrs` (Decimal or None) or
    `date` (a 'YYYY-MM-DD' string or None — Postgres casts it to DATE).
    """
    if raw is None or raw == "":
        return None
    if field in NUMERIC_FIELDS:
        try:
            return Decimal(str(raw))
        except (InvalidOperation, ValueError, TypeError):
            raise ValidationError(f"`{field}` must be a number (or null to clear).")
    # field == "date": keep the string; the column is DATE so an invalid string
    # surfaces as a DB error, but normal input is 'YYYY-MM-DD'.
    return str(raw)


def _post(request):
    """POST /api/platform/call-center-targets

    Upsert ONE editable field (targets | done_ltrs | date) for one
    (month, year, item_head). `value` may be null to clear that field.
    """
    _enforce(request, _EDIT_PERMISSION)
    body = request.data or {}
    month, year = _parse_month_year(body)

    item_head = str(body.get("item_head") or "").strip().upper()
    if item_head not in ITEM_HEADS:
        raise ValidationError(f"`item_head` must be one of {ITEM_HEADS}.")

    field = str(body.get("field") or "targets").strip().lower()
    if field not in FIELD_COLUMNS:
        raise ValidationError(f"`field` must be one of {tuple(FIELD_COLUMNS)}.")
    column = FIELD_COLUMNS[field]

    value = _coerce_value(field, body.get("value"))

    # `column` is whitelisted via FIELD_COLUMNS, never user SQL.
    with connection.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO call_center_targets
                (month, year, item_head, {column}, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (month, year, item_head)
            DO UPDATE SET {column} = EXCLUDED.{column}, updated_at = NOW()
            RETURNING targets, done_ltrs, data_date
            """,
            [month, year, item_head, value],
        )
        targets, done_ltrs, data_date = cur.fetchone()

    return Response({
        "ok": True,
        "item_head": item_head,
        "targets": _as_number(targets),
        "done_ltrs": _as_number(done_ltrs),
        "date": _as_date_str(data_date),
    })
