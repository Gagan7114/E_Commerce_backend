"""Call Center monthly targets — a single, isolated target store.

Self-contained on purpose: this module does NOT read or write the existing
`month_targets` / `primary_month_targets` tables or any platform/dashboard
logic. It backs one numeric target per (month, year, item_head) for the
frontend's Call Center card, stored in `call_center_targets` (see migration
0043_call_center_targets).

Contract (the frontend depends on this exactly):
  GET  /api/platform/call-center-targets?month=<int>&year=<int>
       -> {"premium": <number|null>, "commodity": <number|null>}
  POST /api/platform/call-center-targets
       body {"month": <int>, "year": <int>,
             "item_head": "PREMIUM"|"COMMODITY", "targets": <number|null>}
       -> {"ok": true, "item_head": "...", "targets": <number|null>}

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


@api_view(["GET", "POST"])
def call_center_targets(request):
    if request.method == "POST":
        return _post(request)
    return _get(request)


def _get(request):
    """GET /api/platform/call-center-targets?month=<int>&year=<int>

    Returns the saved PREMIUM / COMMODITY targets for the month, or null each
    when no row exists yet.
    """
    _enforce(request, _VIEW_PERMISSION)
    month, year = _parse_month_year(request.query_params)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT UPPER(TRIM(item_head)), targets
              FROM call_center_targets
             WHERE month = %s AND year = %s
               AND UPPER(TRIM(item_head)) IN ('PREMIUM', 'COMMODITY')
            """,
            [month, year],
        )
        rows = cur.fetchall()

    saved = {head: targets for head, targets in rows}
    return Response({
        "premium": _as_number(saved.get("PREMIUM")),
        "commodity": _as_number(saved.get("COMMODITY")),
    })


def _post(request):
    """POST /api/platform/call-center-targets

    Upsert one (month, year, item_head) target. `targets` may be null to clear.
    """
    _enforce(request, _EDIT_PERMISSION)
    body = request.data or {}
    month, year = _parse_month_year(body)

    item_head = str(body.get("item_head") or "").strip().upper()
    if item_head not in ITEM_HEADS:
        raise ValidationError(f"`item_head` must be one of {ITEM_HEADS}.")

    raw_targets = body.get("targets", None)
    if raw_targets is None or raw_targets == "":
        targets: Decimal | None = None
    else:
        try:
            targets = Decimal(str(raw_targets))
        except (InvalidOperation, ValueError, TypeError):
            raise ValidationError("`targets` must be a number (or null to clear).")

    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO call_center_targets (month, year, item_head, targets, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (month, year, item_head)
            DO UPDATE SET targets = EXCLUDED.targets, updated_at = NOW()
            RETURNING targets
            """,
            [month, year, item_head, targets],
        )
        saved_targets = cur.fetchone()[0]

    return Response({
        "ok": True,
        "item_head": item_head,
        "targets": _as_number(saved_targets),
    })
