from __future__ import annotations

import hmac
import json
import math
import time
from datetime import date as _date, timedelta
from decimal import Decimal

import logging

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.db import connection, transaction, DatabaseError
from django.http import Http404, HttpResponse
from rest_framework import status
from rest_framework.exceptions import APIException
from rest_framework.permissions import IsAuthenticated, AllowAny, BasePermission
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser

from accounts.permissions import has_permission_code
from .models import Shipment, ShipmentAuditLog, ShipmentItem, ShipmentPoDocument
from .serializers import (
    ShipmentAuditLogSerializer,
    ShipmentItemSerializer,
    ShipmentListSerializer,
    ShipmentSerializer,
)

TRUCK_CAPACITIES = {'10_ton': 10000.0, '15_ton': 15000.0}
LOCKED_STATUSES = ('approved', 'dispatched', 'in_transit', 'delivered')

# Fixed key for the Postgres transaction-scoped advisory lock that serializes the
# shipment claim+create critical section. Without it, two planners can both pass
# the "is this PO line still free?" check and then both insert the same rows
# (a phantom race a plain re-check cannot prevent) → over-commitment.
SHIPMENT_CLAIM_LOCK = 738214

logger = logging.getLogger(__name__)


def _safe_int(value, default, *, lo=None, hi=None):
    """Parse a query-param int without 500-ing on garbage; clamp to [lo, hi]."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    if lo is not None:
        n = max(lo, n)
    if hi is not None:
        n = min(hi, n)
    return n


# Base view that turns an unexpected (non-DRF) error into a clean JSON response
# instead of a raw 500 stack trace. DRF's own exceptions (validation, auth, 404)
# still render normally. Aliasing APIView keeps the class swap below from
# rewriting this definition. Scoped to the shipment app — global behaviour
# is unchanged (other apps still use the plain APIView).
_BaseAPIView = APIView

# Permission code that gates the whole Amazon Shipment Planner section. Granted
# only to selected users (plus admins) — see accounts/catalog.py.
SHIPMENT_PLANNING_PERMISSION = "amazon.shipment_planning.view"


class CanViewShipmentPlanning(BasePermission):
    """Every Shipment Planner endpoint requires this permission, so a user who
    can't see the section can't reach its data either."""
    message = 'You do not have access to the Amazon Shipment Planner.'

    def has_permission(self, request, view):
        return has_permission_code(getattr(request, 'user', None), SHIPMENT_PLANNING_PERMISSION)


class _SafeAPIView(_BaseAPIView):
    def get_permissions(self):
        # Add the Shipment Planner gate to whatever a view already declares
        # (IsAuthenticated / IsShipmentManager …), so it's enforced in ONE place
        # across every endpoint. Public shared-secret endpoints opt out with
        # AllowAny (e.g. the Vendor Central importer) and are left untouched.
        perms = super().get_permissions()
        if any(isinstance(p, AllowAny) for p in perms):
            return perms
        return perms + [CanViewShipmentPlanning()]

    def handle_exception(self, exc):
        if isinstance(exc, (APIException, Http404, PermissionDenied)):
            return super().handle_exception(exc)
        if isinstance(exc, DatabaseError):
            logger.exception('shipment: database error in %s', self.__class__.__name__)
            return Response(
                {'error': 'A database error occurred. Please try again in a moment.'},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        logger.exception('shipment: unhandled error in %s', self.__class__.__name__)
        return Response(
            {'error': 'Something went wrong while processing your request.'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


class IsShipmentManager(BasePermission):
    """Manager-only actions (approve / reject / dispatch). A user qualifies if
    they are a superuser, Django staff, OR belong to the 'shipment_managers'
    group — so a proper role can be granted without handing out staff/admin."""
    message = 'Manager access required to approve, reject, or dispatch shipments.'

    def has_permission(self, request, view):
        u = getattr(request, 'user', None)
        return bool(
            u and u.is_authenticated and (
                u.is_superuser
                or u.is_staff
                or u.groups.filter(name='shipment_managers').exists()
            )
        )


# ---------------------------------------------------------------------------
# Priority helpers
# ---------------------------------------------------------------------------

# Vendor Central commit caps may be exceeded by up to this factor (7% over).
# Single source of truth — the frontend uses the same 0.07 tolerance.
CAP_TOLERANCE = 1.07


def _compute_priority(drr_unit, soh_unit, doh, days_to_expiry, po_status):
    drr = float(drr_unit or 0)
    soh = float(soh_unit or 0)
    d = float(doh or 0)
    dte = int(days_to_expiry or 999)

    if drr > 0 and soh == 0:
        bucket, doh_score = 'CRITICAL', 100
    elif drr > 0 and d <= 7:
        bucket, doh_score = 'VERY HIGH', 90
    elif drr > 0 and d <= 14:
        bucket, doh_score = 'HIGH', 75
    elif drr > 0 and d <= 30:
        bucket, doh_score = 'MEDIUM', 50
    elif drr > 0 and d > 30:
        bucket, doh_score = 'LOW', 20
    else:
        bucket, doh_score = 'HOLD', 5

    fefo = 100 if dte <= 7 else 80 if dte <= 30 else 50 if dte <= 90 else 20
    po_urgency = (
        100 if po_status == 'PENDING' and dte <= 30
        else 50 if po_status == 'PENDING'
        else 10
    )

    score = (doh_score * 0.60) + (fefo * 0.25) + (po_urgency * 0.15)

    if bucket == 'HOLD':
        reason = 'No active demand (DRR=0)'
    elif bucket == 'CRITICAL':
        reason = 'Out of stock with active demand'
    else:
        reason = f'DOH={d:.1f} days, DRR={drr:.2f}'

    return bucket, round(score, 2), reason


def _resolve_capacity(truck_size, capacity_override=None):
    """Resolve truck capacity in liters. Falls back to known sizes; honors custom override."""
    if capacity_override is not None:
        try:
            v = float(capacity_override)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    return TRUCK_CAPACITIES.get(truck_size, 15000.0)


def _item_head_bucket(item):
    """Map an item's item_head to one of PREMIUM / COMMODITY / OTHER."""
    raw = str(item.get('item_head') or '').strip().upper()
    if 'PREMIUM' in raw:
        return 'PREMIUM'
    if 'COMMODITY' in raw:
        return 'COMMODITY'
    return 'OTHER'


def _pack_into_capacity(items, capacity_lt):
    """
    Greedy pack a list of pre-sorted items into the given liter capacity.
    Returns (loaded_subset, not_loaded_subset, used_liters).
    Mutates each item with planned_qty / planned_liters.
    """
    remaining = float(capacity_lt)
    loaded, not_loaded = [], []
    for item in items:
        per_liter    = float(item.get('per_liter') or 0)
        accepted_qty = float(item.get('accepted_qty') or 0)
        # Effective shippable units = ordered, capped by live stock when set.
        # accepted_qty itself is never changed (Ordered/Short stay correct).
        sc = item.get('stock_cap')
        # ship_cap: user-chosen units to ship (manual short-supply). Caps the
        # shippable qty exactly like stock_cap; accepted_qty is left untouched so
        # Ordered/Short stay correct and the user's short_reason (carried on the
        # item) is preserved through to the saved record.
        uc = item.get('ship_cap')
        caps = [accepted_qty]
        if sc is not None:
            caps.append(max(0.0, float(sc)))
        if uc is not None:
            caps.append(max(0.0, float(uc)))
        cap_units = min(caps)
        capped = (sc is not None) or (uc is not None)
        total_liters = (round(cap_units * per_liter, 4) if capped
                        else float(item.get('total_accepted_liters') or 0))

        if accepted_qty == 0:
            item['planned_qty'] = 0
            item['planned_liters'] = 0
            item['unfit_reason'] = (
                'Already fully committed to another shipment — nothing left to ship.'
            )
            not_loaded.append(item)
            continue

        if sc is not None and cap_units <= 0:
            # No live stock for this SKU — can't ship it.
            item['planned_qty'] = 0
            item['planned_liters'] = 0
            item['unfit_reason'] = item.get('stock_unfit') or (
                f"Out of stock at {item.get('source_warehouse')}." if item.get('source_warehouse')
                else 'Out of stock.'
            )
            not_loaded.append(item)
            continue

        if total_liters == 0:
            # Zero-volume items (e.g. no per-litre value in the master sheet)
            # normally can't be packed. Exception: OTHER-bucket items still ship
            # at full qty — they consume no truck capacity, so they always fit.
            if _item_head_bucket(item) == 'OTHER':
                item['planned_qty'] = cap_units
                item['planned_liters'] = 0
                loaded.append(item)
            else:
                item['planned_qty'] = 0
                item['planned_liters'] = 0
                item['unfit_reason'] = (
                    'No per-liter data in the master sheet — planner cannot fit '
                    'this item without knowing its volume.'
                )
                not_loaded.append(item)
            continue

        if total_liters <= remaining + 0.001:
            # All shippable (in-stock) units fit — ship them.
            item['planned_qty'] = cap_units
            item['planned_liters'] = round(total_liters, 4)
            remaining -= total_liters
            loaded.append(item)
        else:
            if per_liter > 0:
                partial_qty = math.floor(remaining / per_liter)
                if partial_qty > 0:
                    partial_liters = round(partial_qty * per_liter, 4)
                    item['planned_qty'] = partial_qty
                    item['planned_liters'] = partial_liters
                    short_units = int(accepted_qty - partial_qty)
                    item['short_reason'] = (
                        f'Truck out of capacity — only {int(partial_qty)} of '
                        f'{int(accepted_qty)} units fit before the truck '
                        f'filled up. {short_units} units left for the next '
                        f'shipment.'
                    )
                    remaining -= partial_liters
                    loaded.append(item)
                else:
                    item['planned_qty'] = 0
                    item['planned_liters'] = 0
                    item['unfit_reason'] = (
                        'Truck is full — no remaining capacity for this item.'
                    )
                    not_loaded.append(item)
            else:
                item['planned_qty'] = 0
                item['planned_liters'] = 0
                item['unfit_reason'] = (
                    'No per-liter data — cannot pack this item.'
                )
                not_loaded.append(item)
    used = float(capacity_lt) - remaining
    return loaded, not_loaded, used


def _auto_plan_truck(items, truck_size, capacity_override=None, priority=None, strict=False):
    """
    Plan a truck load.

    `priority` (optional): {'PREMIUM': pct, 'COMMODITY': pct, 'OTHER': pct} — each
    percentage 0..100, summing to 100. When provided, the loader carves the truck
    into three bucket slices and packs each bucket's items into its slice.

    `strict` controls what happens with capacity left over after each bucket is
    packed:
      - strict=True  -> hard adherence to the slider split. Leftover bucket slices
        stay empty, items from other buckets are NOT borrowed. Truck may ship
        under-loaded if a bucket's pool is too small.
      - strict=False (default) -> "best-effort". After bucket-greedy packing, any
        un-used capacity (from any slice) is pooled and a second pass fills it
        with the highest-scoring un-loaded items regardless of bucket, until the
        truck is full or no more items fit. The Priority Adherence panel still
        reports requested vs actually-used per bucket so users see the trade-off.

    When `priority` is None, falls back to a flat greedy pack across all items.
    """
    capacity = _resolve_capacity(truck_size, capacity_override)

    if not priority:
        loaded, not_loaded, used = _pack_into_capacity(items, capacity)
        planned = round(used, 4)
        load_pct = round((planned / capacity * 100) if capacity > 0 else 0, 2)
        return loaded, not_loaded, capacity, planned, load_pct, None

    # Bucket the candidates
    buckets = {'PREMIUM': [], 'COMMODITY': [], 'OTHER': []}
    for it in items:
        buckets[_item_head_bucket(it)].append(it)

    bucket_caps = {
        k: round(capacity * (float(priority.get(k, 0) or 0) / 100.0), 4)
        for k in buckets
    }

    loaded_all, not_loaded_all = [], []
    priority_actual = {}
    bucket_used = {}
    for k, bucket_items in buckets.items():
        cap_k = bucket_caps.get(k, 0)
        if cap_k <= 0:
            # Bucket not requested — push everything to not_loaded (kept for
            # best-effort second pass if strict=False). Exception: zero-volume
            # OTHER items still ship — they take no capacity, so a 0% slice
            # doesn't apply to them.
            for it in bucket_items:
                if (k == 'OTHER'
                        and float(it.get('total_accepted_liters') or 0) == 0
                        and float(it.get('accepted_qty') or 0) > 0):
                    it['planned_qty'] = float(it.get('accepted_qty') or 0)
                    it['planned_liters'] = 0
                    loaded_all.append(it)
                else:
                    it['planned_qty'] = 0
                    it['planned_liters'] = 0
                    it['unfit_reason'] = (
                        f'{k} bucket has 0% allocation in the priority slider — '
                        'this item belongs to a bucket you didn\'t pick.'
                    )
                    not_loaded_all.append(it)
            priority_actual[k] = {'requested_liters': 0, 'used_liters': 0}
            bucket_used[k] = 0.0
            continue
        l, nl, used = _pack_into_capacity(bucket_items, cap_k)
        loaded_all.extend(l)
        not_loaded_all.extend(nl)
        priority_actual[k] = {'requested_liters': cap_k, 'used_liters': round(used, 4)}
        bucket_used[k] = float(used)

    # Best-effort second pass — fill leftover capacity from any bucket's not-loaded
    # items, highest-scoring first. Caller has already sorted `items` by score so
    # `not_loaded_all` is roughly score-ordered per bucket; re-sort for safety.
    if not strict:
        first_pass_used = sum(bucket_used.values())
        leftover_capacity = max(0.0, capacity - first_pass_used)
        if leftover_capacity > 0 and not_loaded_all:
            # Sort the remaining pool by priority score (high first), then expiry,
            # then accepted qty — same key the candidate pool uses upstream.
            spill_pool = sorted(
                not_loaded_all,
                key=lambda x: (
                    -float(x.get('priority_score') or 0),
                    int(x.get('days_to_expiry') or 999),
                    -float(x.get('accepted_qty') or 0),
                ),
            )
            spill_loaded, spill_not_loaded, spill_used = _pack_into_capacity(
                spill_pool, leftover_capacity
            )
            # Credit the spill to whichever bucket each spilled item belongs to,
            # so adherence reporting reflects the real bucket split that shipped.
            for it in spill_loaded:
                bkt = _item_head_bucket(it)
                if bkt in priority_actual:
                    priority_actual[bkt]['used_liters'] = round(
                        priority_actual[bkt]['used_liters'] + float(it.get('planned_liters') or 0),
                        4,
                    )
            loaded_all.extend(spill_loaded)
            not_loaded_all = spill_not_loaded

    planned = round(sum(p['used_liters'] for p in priority_actual.values()), 4)
    load_pct = round((planned / capacity * 100) if capacity > 0 else 0, 2)

    return loaded_all, not_loaded_all, capacity, planned, load_pct, priority_actual


def _row_to_dict(cur, rows):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _serialize_row(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, 'isoformat'):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


DRR_WINDOW_DAYS = 30  # rolling-window length for daily run-rate computation


def _doh_snapshot_meta(effective_date):
    """Snapshot metadata so the UI can warn when DOH data is stale."""
    if not effective_date:
        return {
            'effective_date': None,
            'window_days': DRR_WINDOW_DAYS,
            'snapshot_age_days': None,
            'is_stale': True,
            'message': 'No inventory snapshot found yet.',
        }
    today = _date.today()
    age = (today - effective_date).days
    return {
        'effective_date': effective_date.isoformat(),
        'window_days': DRR_WINDOW_DAYS,
        'snapshot_age_days': age,
        'is_stale': age > 1,
        'message': (
            'Live snapshot.' if age <= 0
            else f'Snapshot is {age} day{"s" if age != 1 else ""} old.'
        ),
    }


def _rolling_window_date_keys(effective_date, days=DRR_WINDOW_DAYS):
    """
    Produce the list of (year, month_upper, month_day_upper) tuples for the
    last `days` calendar days ending at `effective_date`. Used to query the
    daily-grain `amazon_sec_range_master_view` over a rolling window.
    """
    keys = []
    for i in range(days):
        d = effective_date - timedelta(days=i)
        keys.append((
            d.year,
            d.strftime('%B').upper(),
            # month_day in amazon_sec_range_master_view stores the FULL month name
            # (e.g. "08-JULY"), so use %B not %b — %b ("08-JUL") matches nothing and
            # collapses DRR/DOH to 0 for every month except May.
            f"{d.day:02d}-{d.strftime('%B').upper()}",
        ))
    return keys


_DOH_CACHE = {'at': 0.0, 'data': None}
_DOH_TTL = 120  # seconds — DOH/DRR change ~daily; avoid recomputing this heavy
#                 rolling-window query on every plan / 30s auto-refresh.


def _live_doh_by_asin():
    """Cached wrapper (TTL _DOH_TTL) over the heavy DOH/DRR computation below.
    The numbers only change daily, so recomputing on every appointment-items /
    auto-plan / 30s poll is wasteful. Call sites are unchanged."""
    now = time.time()
    cached = _DOH_CACHE['data']
    if cached is not None and (now - _DOH_CACHE['at'] < _DOH_TTL):
        return cached
    result = _compute_live_doh_by_asin()
    _DOH_CACHE['at'] = now
    _DOH_CACHE['data'] = result
    return result


def _compute_live_doh_by_asin():
    """
    Returns (by_asin, meta).

    by_asin: {asin_upper: {soh_unit, soh_ltr, drr_unit, drr_ltr, doh, units_sold, ltr_sold}}
        sourced from amazon_master_inventory + amazon_sec_range_master_view.
    meta:    {effective_date, window_days, snapshot_age_days, is_stale, message}

    DRR is computed over a rolling DRR_WINDOW_DAYS window so the first days of a
    new month no longer collapse DRR to ~0 (month-to-date used to divide by the
    day-of-month). All four surfaces (SOH/DOH dashboard, Manual PO, Appointment
    plan, DOH Auto-Fill) call this helper so the numbers stay in sync.

    Returns ({}, meta) if no inventory snapshot is available yet.
    """
    with connection.cursor() as cur:
        cur.execute(
            "SELECT MAX(inventory_date) FROM amazon_master_inventory"
        )
        eff_row = cur.fetchone()
        effective_date = eff_row[0] if eff_row else None
        meta = _doh_snapshot_meta(effective_date)
        if not effective_date:
            return {}, meta

        month_name = effective_date.strftime('%B').upper()
        year = effective_date.year

        date_keys = _rolling_window_date_keys(effective_date, DRR_WINDOW_DAYS)
        # Build a (year, month, month_day) IN-list for the trailing window
        placeholders = ', '.join(['(%s, %s, %s)'] * len(date_keys))
        flat_params = [v for triple in date_keys for v in triple]

        cur.execute(
            f"""
            WITH sales AS (
                SELECT
                    UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                    COALESCE(SUM(shipped_units), 0)::numeric  AS units_sold,
                    COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
                FROM amazon_sec_range_master_view
                WHERE ("year", UPPER(TRIM("month"::text)), UPPER(TRIM(month_day::text))) IN ({placeholders})
                GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
            ),
            inventory AS (
                SELECT
                    UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                    COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                    COALESCE(SUM(soh_ltr), 0)::numeric                AS soh_ltr
                FROM amazon_master_inventory
                WHERE "year" = %s
                  AND UPPER(TRIM("month"::text)) = %s
                  AND inventory_date = %s
                  AND NULLIF(TRIM(COALESCE(asin::text, '')), '') IS NOT NULL
                GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
            )
            SELECT i.asin_key,
                   i.soh_unit, i.soh_ltr,
                   COALESCE(s.units_sold, 0) AS units_sold,
                   COALESCE(s.ltr_sold,  0) AS ltr_sold
            FROM inventory i
            LEFT JOIN sales s ON s.asin_key = i.asin_key
            """,
            flat_params + [year, month_name, effective_date],
        )
        rows = cur.fetchall()

    by_asin = {}
    window = float(DRR_WINDOW_DAYS)
    for asin_key, soh_unit, soh_ltr, units_sold, ltr_sold in rows:
        if not asin_key:
            continue
        soh_unit_f = float(soh_unit or 0)
        soh_ltr_f  = float(soh_ltr or 0)
        units_sold_f = float(units_sold or 0)
        ltr_sold_f   = float(ltr_sold or 0)
        drr_unit = units_sold_f / window
        drr_ltr  = ltr_sold_f / window
        doh = ((soh_unit_f / drr_unit) - 2) if drr_unit > 0 else 0.0
        by_asin[asin_key] = {
            'soh_unit': soh_unit_f,
            'soh_ltr':  soh_ltr_f,
            'drr_unit': drr_unit,
            'drr_ltr':  drr_ltr,
            'doh':      doh,
            'units_sold': units_sold_f,
            'ltr_sold':   ltr_sold_f,
        }
    return by_asin, meta


_DOH_AUTOFILL_CACHE = {'date': None, 'at': 0.0, 'data': None}


def _doh_autofill_by_asin(effective_date):
    """Rolling-window DOH per ASIN WITH inventory attributes (item_head/category/
    brand/per_unit) for the DOH Auto-Fill view. Same heavy aggregate as
    `_compute_live_doh_by_asin` but carrying the extra columns Auto-Fill needs.
    TTL-cached by snapshot date — DOH changes only daily, so repeated Auto-Fill
    runs reuse it instead of re-running the rolling-window aggregate each time."""
    now = time.time()
    c = _DOH_AUTOFILL_CACHE
    if c['data'] is not None and c['date'] == effective_date and (now - c['at'] < _DOH_TTL):
        return c['data']

    month_name = effective_date.strftime('%B').upper()
    year = effective_date.year
    date_keys = _rolling_window_date_keys(effective_date, DRR_WINDOW_DAYS)
    placeholders = ', '.join(['(%s, %s, %s)'] * len(date_keys))
    flat_date_params = [v for triple in date_keys for v in triple]

    with connection.cursor() as cur:
        cur.execute(f"""
            WITH sales AS (
                SELECT
                    UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                    COALESCE(SUM(shipped_units), 0)::numeric  AS units_sold,
                    COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
                FROM amazon_sec_range_master_view
                WHERE ("year", UPPER(TRIM("month"::text)), UPPER(TRIM(month_day::text))) IN ({placeholders})
                GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
            ),
            inventory AS (
                SELECT
                    UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                    MIN(NULLIF(TRIM(item_head::text), ''))    AS item_head,
                    MIN(NULLIF(TRIM(category::text), ''))     AS category,
                    MIN(NULLIF(TRIM(sub_category::text), '')) AS sub_category,
                    MIN(NULLIF(TRIM(brand_2::text), ''))      AS brand,
                    MIN(NULLIF(TRIM(per_unit::text), ''))     AS per_unit,
                    MIN(NULLIF(TRIM(asin::text), ''))         AS asin,
                    COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                    COALESCE(SUM(soh_ltr), 0)::numeric                AS soh_ltr
                FROM amazon_master_inventory
                WHERE "year" = %s
                  AND UPPER(TRIM("month"::text)) = %s
                  AND inventory_date = %s
                  AND NULLIF(TRIM(COALESCE(asin::text, '')), '') IS NOT NULL
                GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
            )
            SELECT
                i.asin_key,
                i.asin, i.item_head, i.category, i.sub_category, i.brand, i.per_unit,
                i.soh_unit, i.soh_ltr,
                COALESCE(s.units_sold, 0) AS units_sold,
                COALESCE(s.ltr_sold,  0) AS ltr_sold
            FROM inventory i
            LEFT JOIN sales s ON s.asin_key = i.asin_key
        """, flat_date_params + [year, month_name, effective_date])
        doh_rows = _row_to_dict(cur, cur.fetchall())

    window = float(DRR_WINDOW_DAYS)
    doh_by_asin = {}
    for r in doh_rows:
        row = _serialize_row(r)
        units_sold = float(row.get('units_sold') or 0)
        ltr_sold = float(row.get('ltr_sold') or 0)
        soh_unit = float(row.get('soh_unit') or 0)
        soh_ltr = float(row.get('soh_ltr') or 0)
        drr_unit = units_sold / window
        drr_ltr = ltr_sold / window
        doh = ((soh_unit / drr_unit) - 2) if drr_unit > 0 else 0.0
        asin_up = str(row.get('asin_key') or '').upper()
        if not asin_up:
            continue
        doh_by_asin[asin_up] = {
            'asin': row.get('asin'),
            'item_head_live': row.get('item_head'),
            'category_live': row.get('category'),
            'sub_category_live': row.get('sub_category'),
            'brand_live': row.get('brand'),
            'per_unit_live': row.get('per_unit'),
            'units_sold': units_sold,
            'ltr_sold': ltr_sold,
            'soh_unit': soh_unit,
            'soh_ltr': soh_ltr,
            'drr_unit': drr_unit,
            'drr_ltr': drr_ltr,
            'doh': doh,
        }
    c['date'] = effective_date
    c['at'] = now
    c['data'] = doh_by_asin
    return doh_by_asin


# ---------------------------------------------------------------------------
# Appointment endpoints
# ---------------------------------------------------------------------------

class AppointmentDatesView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        with connection.cursor() as cur:
            cur.execute("""
                SELECT DATE(appointment_time) AS appt_date,
                       COUNT(DISTINCT CASE WHEN status = 'Confirmed' THEN appointment_id END) AS confirmed_count,
                       COUNT(DISTINCT CASE WHEN status = 'Cancelled' THEN appointment_id END) AS cancelled_count
                FROM reporting."appointment"
                WHERE status IN ('Confirmed','Cancelled')
                  AND appointment_time IS NOT NULL
                GROUP BY DATE(appointment_time)
                ORDER BY appt_date
            """)
            rows = cur.fetchall()
        dates = [r[0].isoformat() for r in rows if r[0] and r[1] > 0]
        counts = {r[0].isoformat(): r[1] for r in rows if r[0]}
        cancelled = {r[0].isoformat(): r[2] for r in rows if r[0] and r[2] > 0}
        channels = {}

        with connection.cursor() as cur:
            cur.execute("""
                SELECT DATE(a.appointment_time) AS appt_date,
                       UPPER(COALESCE(NULLIF(TRIM(fcm.channel::text), ''), 'UNMAPPED')) AS channel,
                       COUNT(DISTINCT a.appointment_id) AS appointment_count
                FROM reporting."appointment" a
                LEFT JOIN public.fc_city_state_channel_master fcm
                  ON UPPER(TRIM(fcm.fc::text)) = UPPER(TRIM(a.destination_fc::text))
                WHERE a.status = 'Confirmed'
                  AND a.appointment_time IS NOT NULL
                GROUP BY DATE(a.appointment_time),
                         UPPER(COALESCE(NULLIF(TRIM(fcm.channel::text), ''), 'UNMAPPED'))
                ORDER BY appt_date, channel
            """)
            for appt_date, channel, channel_count in cur.fetchall():
                if not appt_date or not channel:
                    continue
                date_key = appt_date.isoformat()
                channels.setdefault(date_key, {})[channel] = channel_count

        # Per-date count of appointments already in a non-rejected shipment.
        # Powers the "X planned" mark on the upcoming-dates tiles so planners
        # can see at a glance which days already have plans.
        planned = {}
        if dates:
            # Pull all date+appointment pairs once, then walk shipments to
            # count which dates have planned appointments. Cheap aggregation.
            with connection.cursor() as cur:
                cur.execute("""
                    SELECT DATE(a.appointment_time) AS appt_date, a.appointment_id
                    FROM reporting."appointment" a
                    WHERE a.appointment_time IS NOT NULL
                      AND a.status = 'Confirmed'
                """)
                appt_date_by_id = {}
                for d, aid in cur.fetchall():
                    if d and aid:
                        appt_date_by_id.setdefault(aid, set()).add(d.isoformat())

                cur.execute("""
                    SELECT appointment_id, additional_appointment_ids
                    FROM sp_shipments
                    WHERE status != 'rejected'
                """)
                planned_appt_ids = set()
                for primary, additional in cur.fetchall():
                    if primary:
                        planned_appt_ids.add(str(primary).strip())
                    if additional:
                        for a in str(additional).split(','):
                            a = a.strip()
                            if a:
                                planned_appt_ids.add(a)

            for aid in planned_appt_ids:
                for d_iso in appt_date_by_id.get(aid, set()):
                    planned[d_iso] = planned.get(d_iso, 0) + 1

        return Response({
            'dates': dates,
            'counts': counts,
            'cancelled': cancelled,
            'channels': channels,
            'planned': planned,
        })


def _explain_ineligibility(c):
    """
    Build a short, human-friendly reason string explaining why an appointment
    has zero eligible POs. The frontend shows this on the appointment card so
    planners can see WHY a slot is unusable before they invest time configuring
    the truck.

    Order of detection matters: FC-mismatch (no PO data at the appointment's
    FC) is the FIRST thing we check, because the underlying SQL counts can't
    distinguish "PO row says out-of-stock" from "PO row doesn't exist at this
    FC at all" — both end up looking like is_in_stock = FALSE. We use a
    dedicated `no_fc_match_count` signal for the latter.
    """
    total = int(c.get('po_count') or 0)
    if total == 0:
        return 'No POs linked to this appointment'

    no_fc_match = int(c.get('no_fc_match_count') or 0)
    if no_fc_match == total:
        appt_fc = (c.get('destination_fc') or '').strip()
        other_fcs = c.get('pos_actual_fcs') or []
        if other_fcs:
            fc_str = ', '.join(other_fcs[:3])
            if len(other_fcs) > 3:
                fc_str += f', +{len(other_fcs) - 3} more'
            return (
                f"PO data not found at FC {appt_fc} — Amazon's PO Report has "
                f"these POs at: {fc_str}. Re-upload the Amazon PO Report, or "
                f"fix the appointment's FC."
            )
        return (
            f"PO data not found at FC {appt_fc} in Amazon's PO Report. "
            f"Re-upload the report or verify the appointment FC."
        )

    not_pending = int(c.get('not_pending_count') or 0)
    not_in_stock = int(c.get('not_in_stock_count') or 0)
    no_qty = int(c.get('no_qty_count') or 0)
    locked = int(c.get('locked_count') or 0)

    # Dominant-cause cases — read more clearly than a list of fragments
    if locked == total:
        return f'All {total} POs are locked in other shipments'
    if not_in_stock == total:
        return f'All {total} POs are out of stock'
    if not_pending == total:
        return f'All {total} POs are already closed or dispatched'
    if no_qty == total:
        return f'All {total} POs have zero accepted qty'

    parts = []
    if locked:       parts.append(f'{locked} locked in other shipments')
    if not_in_stock: parts.append(f'{not_in_stock} out of stock')
    if not_pending:  parts.append(f'{not_pending} closed/dispatched')
    if no_qty:       parts.append(f'{no_qty} with no accepted qty')
    return f'Of {total} POs: ' + (', '.join(parts) if parts else 'all unavailable')


def _filler_pass(loaded, leftover_pool, capacity, primary_fc=None, mark_key='_filler', reason=None):
    """
    Second-stage pack that fills any unused truck capacity from `leftover_pool`.

    `mark_key` controls how loaded fillers are tagged so the UI can render
    different badges (filler vs DOH-filler vs anything future). Defaults to
    `_filler` for back-compat with the first filler pass.

    Items kept: same FC as the rest of the truck (single-FC trucks only).
    Sort: priority_score desc, days_to_expiry asc, accepted_qty desc.

    Returns (new_loaded, new_not_loaded). Items that didn't fit go back into
    not-loaded so the UI can still surface them.
    """
    planned_lt = sum(float(it.get('planned_liters') or 0) for it in loaded)
    remaining = float(capacity) - planned_lt
    if remaining <= 0.001 or not leftover_pool:
        return list(loaded), list(leftover_pool)

    # Enforce single-FC for fillers too — a truck physically ships to one FC
    pool = list(leftover_pool)
    if primary_fc:
        pf = str(primary_fc).strip().upper()
        pool = [
            it for it in pool
            if str(it.get('destination_fc') or '').strip().upper() == pf
        ]

    pool.sort(key=lambda x: (
        -float(x.get('priority_score') or 0),
        int(x.get('days_to_expiry') or 999),
        -float(x.get('accepted_qty') or 0),
    ))

    filler_loaded, filler_unfit, _used = _pack_into_capacity(pool, remaining)
    default_reason = (
        'Filler · added to fill leftover truck capacity '
        '(not part of the priority-driven plan).'
    )
    for it in filler_loaded:
        it[mark_key] = True
        it['filler_reason'] = reason or default_reason

    # Anything in leftover_pool not at primary_fc stays in not_loaded
    if primary_fc:
        wrong_fc = [
            it for it in leftover_pool
            if str(it.get('destination_fc') or '').strip().upper() != str(primary_fc).strip().upper()
        ]
    else:
        wrong_fc = []
    return list(loaded) + filler_loaded, filler_unfit + wrong_fc


def _enforce_commit_caps(loaded, not_loaded, commit_caps, key_field='appointment_id'):
    """Trim ``loaded`` so each capped group respects its Vendor Central commit,
    allowing up to CAP_TOLERANCE (7%) over:
    sum(planned_qty) ≤ units_cap×1.07 AND sum(planned_qty/case_pack) ≤ cartons_cap×1.07.
    Lowest-priority items are dropped first; removed items go to ``not_loaded``
    with a clear ``unfit_reason`` so the UI can explain them.

    ``commit_caps`` is ``{group_key: {'units': N, 'cartons': N}}``. Items are
    grouped by ``key_field`` (default ``appointment_id`` for auto; ``po_number``
    for manual). For ``po_number`` the comparison is uppercase-trimmed. Zero
    caps mean "no cap" for that field. DOH fillers (which have no appointment of
    their own) are counted toward the single appointment's cap, so the truck
    total — fillers included — respects the Vendor Central commit ×1.07.
    """
    if not commit_caps:
        return loaded, not_loaded

    norm_caps = {}
    for k, v in commit_caps.items():
        if key_field == 'po_number':
            norm_caps[str(k or '').strip().upper()] = v
        else:
            norm_caps[str(k or '').strip()] = v

    def _key(it):
        raw = str(it.get(key_field) or '').strip()
        return raw.upper() if key_field == 'po_number' else raw

    indexed = list(enumerate(loaded))
    indexed.sort(key=lambda pair: (
        1 if pair[1].get('_doh_filler') else 0,
        -(pair[1].get('priority_score') or 0),
        (pair[1].get('days_to_expiry') or 999),
        -(pair[1].get('accepted_qty') or 0),
    ))

    totals = {k: {'u': 0.0, 'c': 0.0} for k in norm_caps}
    keep_flags = [True] * len(loaded)
    extras = []
    # DOH fillers have no appointment of their own; with exactly one cap they're
    # attributed to it so the truck total (fillers included) respects the commit.
    # The sort above keeps appointment items first and drops fillers first when
    # the cap is reached. With multiple caps we can't attribute, so they pass.
    single_cap_key = next(iter(norm_caps)) if len(norm_caps) == 1 else None

    for orig_idx, it in indexed:
        gk = _key(it)
        if gk not in norm_caps:
            if it.get('_doh_filler') and single_cap_key is not None:
                gk = single_cap_key
            else:
                continue
        cap = norm_caps[gk] or {}
        # Allow up to 7% over the Vendor Central commit (units AND cartons).
        cap_u = (float(cap.get('units') or 0) * CAP_TOLERANCE) or float('inf')
        cap_c = (float(cap.get('cartons') or 0) * CAP_TOLERANCE) or float('inf')

        # planned_qty / case_pack are DecimalFields — keep them as floats so the
        # running cap comparison stays accurate (int() truncation undercounts
        # units and overcounts cartons, weakening the cap).
        pq = float(it.get('planned_qty') or 0)
        cp = max(float(it.get('case_pack') or 1), 1.0)
        c_units = pq / cp

        t = totals[gk]
        label = 'PO' if key_field == 'po_number' else 'appointment'
        if t['u'] + pq <= cap_u and t['c'] + c_units <= cap_c:
            t['u'] += pq
            t['c'] += c_units
        else:
            # Item would breach the cap. Rather than dropping it whole, fill it
            # PARTIALLY up to whatever headroom is left (units AND cartons) so
            # the commit is respected exactly, and short-supply the remainder.
            # (A partial of an item that already fit the truck can't overflow it.)
            ru = max(0.0, cap_u - t['u'])                  # units headroom
            rc_units = max(0.0, cap_c - t['c']) * cp        # carton headroom, in units
            allow = math.floor(min(pq, ru, rc_units))       # whole units only
            if allow > 0:
                per_liter = float(it.get('per_liter') or 0)
                short = int(round(pq - allow))
                it['planned_qty'] = allow
                it['planned_liters'] = round(allow * per_liter, 4)
                it['short_reason'] = (
                    f'Capped at Vendor Central commit for this {label} '
                    f'(cap: {int(cap.get("units") or 0)} units / '
                    f'{int(cap.get("cartons") or 0)} cartons, +7% allowed) — '
                    f'{short} units short-supplied.'
                )
                t['u'] += allow
                t['c'] += allow / cp
                # keep_flags[orig_idx] stays True — item remains loaded (partial)
            else:
                keep_flags[orig_idx] = False
                removed = dict(it)
                removed['planned_qty'] = 0
                removed['planned_liters'] = 0
                removed['not_loaded'] = True
                removed['unfit_reason'] = (
                    f'Exceeds Vendor Central commit cap for this {label} '
                    f'(cap: {int(cap.get("units") or 0)} units / '
                    f'{int(cap.get("cartons") or 0)} cartons, +7% allowed).'
                )
                extras.append(removed)

    new_loaded = [it for i, it in enumerate(loaded) if keep_flags[i]]
    return new_loaded, list(not_loaded) + extras


def _lookup_appointment_commit(appointment_id):
    """Vendor Central commit (units / cartons) for an appointment, read live from
    public.appointment_commit. This is the single source of truth for manual-plan
    and draft enforcement — never trust commit caps sent by the client. Returns
    {'units': float, 'cartons': float} or None when the appointment has no commit."""
    aid = str(appointment_id or '').strip()
    if not aid:
        return None
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT MAX(unit_count), MAX(carton_count) FROM public.appointment_commit "
                "WHERE TRIM(appointment_id) = %s",
                [aid],
            )
            r = cur.fetchone()
    except Exception:
        return None
    if not r:
        return None
    units = float(r[0] or 0)
    cartons = float(r[1] or 0)
    if units <= 0 and cartons <= 0:
        return None
    return {'units': units, 'cartons': cartons}


# ── Live planner warehouse stock, bridged to Amazon ASINs ────────────────────
# The planner pools finished-goods stock from these warehouses, in PREFERENCE
# order: for each ASIN the FIRST warehouse (left→right) that has on-hand > 0 is
# chosen as the primary fulfilment source. Warehouses: BH-FGM, GP-FG, BH-EC.
# Planner warehouses and the company DB (schema) each lives in. Stock is pooled
# across ALL of these. BH-FGM is in the mart book; GP-FG + BH-EC are in oil.
_PLANNER_WAREHOUSE_SOURCES = (
    ('BH-FGM', 'mart'),
    ('GP-FG',  'oil'),
    ('BH-EC',  'oil'),
)
# Preference order (used only to pick a primary source when nothing has on-hand).
_PLANNER_WAREHOUSES = tuple(code for code, _src in _PLANNER_WAREHOUSE_SOURCES)
_WAREHOUSE_INVENTORY_LABEL = {
    'BH-FGM': 'Jivo Mart',
    'GP-FG':  'Gupta Godown',
    'BH-EC':  'Bhakharpur E-Commerce',
}
_STOCK_CACHE_KEY = 'sp:stock_detail_v2'   # shared snapshot: {'at': epoch, 'detail': {...}}
_STOCK_TTL = 60          # seconds fresh — avoids hitting HANA on every plan / 30s refresh
_STOCK_STALE_MAX = 3600  # keep serving a stale snapshot up to 1h if HANA is unreachable


def _inventory_label(whs_code):
    """Human inventory name for a warehouse code (e.g. BH-FGM → 'Jivo Mart')."""
    return _WAREHOUSE_INVENTORY_LABEL.get(str(whs_code or '').strip().upper())


def _stock_snapshot_meta():
    """(at_epoch, is_stale) for the last live-stock snapshot, or (None, True) if we
    have never fetched one. Lets the plan endpoint surface the snapshot's age so the
    UI can flag stock that was served from cache because HANA was unreachable."""
    try:
        from django.core.cache import cache
        snap = cache.get(_STOCK_CACHE_KEY)
    except Exception:
        snap = None
    if not snap or not snap.get('detail'):
        return None, True
    at = float(snap.get('at') or 0)
    return at, (time.time() - at) >= _STOCK_TTL


def _stock_meta_payload(stock_detail):
    """Response payload describing the live-stock snapshot's freshness, so the UI can
    flag a plan built on CACHED stock (SAP was briefly unreachable) or on NO stock at
    all (SAP down before any snapshot). ``stale`` / ``unavailable`` drive the banner."""
    at, stale = _stock_snapshot_meta()
    return {
        'as_of': at,                                    # epoch seconds, or None
        'age_seconds': (time.time() - at) if at else None,
        'stale': bool(stale),
        'unavailable': not stock_detail,                # couldn't verify any stock
    }


def _bh_fgm_stock_detail():
    """ASIN (upper) → {'onhand', 'onorder', 'source_warehouse', 'sources'}.

    Pools live SAP stock across ALL planner warehouses (``_PLANNER_WAREHOUSE_SOURCES``),
    which span two company DBs — BH-FGM (mart), GP-FG + BH-EC (oil). ``onhand`` and
    ``onorder`` are the SUM across every warehouse and every SAP code that maps to the
    ASIN, so the planner can ship the pooled total. ``source_warehouse`` is the single
    warehouse holding the most on-hand (the primary pick location); ``sources`` is the
    full {warehouse: on-hand} breakdown.

    Bridge: master_sheet maps format_sku_code (ASIN) → sku_sap_code; SAP OITW gives
    OnHand / OnOrder per SAP code per warehouse (same unit as Amazon sellable units).

    Resilience (SAP down): the snapshot lives in the shared Django cache (Redis in
    prod) so all workers agree. On a HANA failure the last good snapshot is returned
    as-is — never zeroing every item — and a successful-but-EMPTY pull is ignored so a
    partial outage can't wipe a good snapshot. ``_stock_snapshot_meta()`` exposes its age.
    """
    from django.core.cache import cache
    now = time.time()
    try:
        snap = cache.get(_STOCK_CACHE_KEY)
    except Exception:
        snap = None
    if snap and snap.get('detail') and (now - float(snap.get('at') or 0) < _STOCK_TTL):
        return snap['detail']

    try:
        from sap.service import select, resolve_schema
        # Query each schema once for its warehouses, then pool by SAP item code.
        by_schema = {}
        for code, src in _PLANNER_WAREHOUSE_SOURCES:
            by_schema.setdefault(src, []).append(code)
        sap_stock = {}   # SAP code -> {WhsCode: (onhand, onorder)}
        for src, whs_list in by_schema.items():
            _s, schema = resolve_schema(src)
            ph = ', '.join(['?'] * len(whs_list))
            oh_rows = select(
                f'SELECT "ItemCode", "WhsCode", "OnHand", "OnOrder" FROM OITW '
                f'WHERE "WhsCode" IN ({ph})',
                whs_list, schema=schema,
            )
            for r in oh_rows:
                code = str(r['ItemCode']).strip().upper()
                whs = str(r['WhsCode']).strip().upper()
                sap_stock.setdefault(code, {})[whs] = (
                    float(r['OnHand'] or 0), float(r['OnOrder'] or 0)
                )
    except Exception:
        # HANA unreachable → keep serving the last good snapshot (however old)
        # rather than zeroing every item. {} only if we've never had one.
        return (snap or {}).get('detail', {})

    asin_map = {}
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT UPPER(TRIM(format_sku_code)) AS asin,
                            UPPER(TRIM(sku_sap_code))    AS sap
            FROM public.master_sheet
            WHERE UPPER(format) = 'AMAZON'
              AND format_sku_code IS NOT NULL
              AND sku_sap_code   IS NOT NULL
        """)
        for asin, sap in cur.fetchall():
            per_whs = sap_stock.get(sap)
            if not per_whs:
                continue
            d = asin_map.setdefault(asin, {'onhand': 0.0, 'onorder': 0.0, 'sources': {}})
            # Pool this SAP code's stock across every planner warehouse into the ASIN.
            for whs, (oh, oo) in per_whs.items():
                d['onhand'] += oh
                d['onorder'] += oo
                d['sources'][whs] = d['sources'].get(whs, 0.0) + oh
    # Primary source = warehouse with the most pooled on-hand (picklist label);
    # if nothing has on-hand, keep preference order among the carriers.
    for d in asin_map.values():
        srcs = d.get('sources') or {}
        best = max(srcs.items(), key=lambda kv: kv[1]) if srcs else None
        if best and best[1] > 0:
            d['source_warehouse'] = best[0]
        elif srcs:
            d['source_warehouse'] = next((w for w in _PLANNER_WAREHOUSES if w in srcs), best[0])
        else:
            d['source_warehouse'] = ''

    # A successful-but-empty pull is almost always a partial SAP glitch, not a real
    # "everything is zero" — don't clobber a good snapshot with it.
    if not asin_map and snap and snap.get('detail'):
        return snap['detail']
    try:
        cache.set(_STOCK_CACHE_KEY, {'at': now, 'detail': asin_map}, timeout=_STOCK_STALE_MAX)
    except Exception:
        pass
    return asin_map


def _reserved_stock_by_asin():
    """ASIN (upper) → units already reserved by ACTIVE shipments not yet
    dispatched (draft / pending_approval / approved). Those units are spoken for,
    so a new plan shouldn't claim them again. Dispatched/delivered shipments have
    physically left the warehouse and are assumed reflected in SAP OnHand."""
    reserved = {}
    with connection.cursor() as cur:
        cur.execute("""
            SELECT UPPER(TRIM(si.asin)) AS asin, SUM(COALESCE(si.planned_qty, 0)) AS qty
            FROM sp_items si
            JOIN sp_shipments s ON s.id = si.shipment_id
            WHERE si.not_loaded = FALSE
              AND si.asin IS NOT NULL
              AND s.status IN ('draft', 'pending_approval', 'approved')
            GROUP BY UPPER(TRIM(si.asin))
        """)
        for asin, qty in cur.fetchall():
            reserved[asin] = float(qty or 0)
    return reserved


def _apply_stock_caps(items, avail_total, avail_remaining, respect, detail, reserved):
    """Tag each item with live stock figures (on-hand, reserved-elsewhere,
    available, incoming on-order). When ``respect``, set ``stock_cap`` = units
    still AVAILABLE (on-hand − reserved) for that ASIN so the packer plans no
    more than that. ``accepted_qty`` is left untouched so Ordered/Short stay
    correct. Stock is consumed in item order (priority) so one ASIN across rows
    shares one pool. Each item is tagged with the ``source_warehouse`` it is
    pooled from (the primary of BH-FGM / GP-FG / BH-EC). ASINs with no
    planner-warehouse stock record are capped to 0 so they drop to not_loaded
    rather than shipping unverified. Mutates ``items``.
    """
    for it in items:
        asin = str(it.get('asin') or '').strip().upper()
        d = detail.get(asin)
        it['sap_stock'] = d['onhand'] if d else None          # physical on hand
        it['sap_on_order'] = d['onorder'] if d else None       # inbound
        it['sap_reserved'] = (reserved.get(asin, 0.0) if d else None)
        it['sap_available'] = (avail_total.get(asin) if d else None)  # on-hand − reserved
        # Which inventory this line is auto-pooled from (prefer BH-FGM; see
        # _bh_fgm_stock_detail). None when the ASIN maps to no planner warehouse.
        src_whs = d['source_warehouse'] if d else None
        it['source_warehouse'] = src_whs
        it['source_inventory'] = _inventory_label(src_whs)
        if not respect:
            continue
        if d is None:
            # Not mapped to ANY planner warehouse (BH-FGM / GP-FG / BH-EC):
            # availability can't be verified, so don't ship it blind. Cap to 0 →
            # the packer drops it into not_loaded with this reason instead of
            # shipping the full ordered qty unverified.
            it['stock_cap'] = 0.0
            it['stock_limited'] = True
            it['stock_unfit'] = (
                'Not mapped to any planner warehouse stock (Jivo Mart / Gupta '
                'Godown / Bhakharpur E-Commerce) — availability cannot be '
                'verified, so it was left out of the plan.'
            )
            continue
        avail = avail_remaining.get(asin, 0.0)
        orderable = float(it.get('accepted_qty') or 0)
        it['stock_cap'] = avail
        # Reserve what this row could ship so later rows of the same ASIN see less.
        avail_remaining[asin] = max(0.0, avail - min(orderable, max(0.0, avail)))
        if avail < orderable - 1e-6:
            it['stock_limited'] = True
            short = int(round(orderable - max(0.0, avail)))
            _where = f'{_inventory_label(src_whs) or src_whs} ({src_whs})' if src_whs else 'the warehouse'
            it['stock_unfit'] = (
                f'No free stock in {_where} (0 available).' if avail <= 0
                else f'Limited to {int(round(avail))} available in {_where} ({short} short).'
            )


def _fetch_doh_filler_pool(fc, exclude_po_uppers, doh_by_asin):
    """
    Pull all PENDING in-stock POs at the given FC that ARE NOT already in the
    `exclude_po_uppers` set (typically the current appointment's own POs) and
    that aren't locked in another active shipment. Enriches each row with
    DOH/DRR/SOH from the live snapshot and assigns a priority bucket + score.

    Used as a second-stage filler pool when an appointment-anchored plan
    leaves capacity on the truck — these are 'extra' POs at the same FC that
    can ride the same truck, ranked by DOH urgency.
    """
    if not fc:
        return []
    exclude_list = [str(x).strip().upper() for x in (exclude_po_uppers or []) if x]

    with connection.cursor() as cur:
        cur.execute("""
            WITH locked_pairs AS (
                SELECT DISTINCT si.asin, UPPER(TRIM(si.po_number)) AS po_number
                FROM sp_items si
                JOIN sp_shipments s ON s.id = si.shipment_id
                WHERE si.not_loaded = FALSE
                  AND s.status != 'rejected'
            )
            SELECT
                p.po_number,
                p.asin,
                p.merchant_sku        AS internal_sku,
                p.sap_sku_code,
                p.sku_name            AS product_name,
                p.accepted_qty,
                p.case_pack,
                p.per_liter,
                p.cost_price,
                p.total_accepted_liters,
                p.days_to_expiry,
                p.expiry_date,
                p.category,
                p.sub_category,
                p.brand,
                p.item_head,
                p.item,
                p.availability_status,
                p.po_status,
                p.status,
                p.fulfillment_center AS destination_fc
            FROM reporting."Amazon PO" p
            LEFT JOIN locked_pairs lp
                ON lp.asin = p.asin
               AND lp.po_number = UPPER(TRIM(p.po_number))
            WHERE p.status = 'Confirmed'
              AND p.availability_status = 'AC - Accepted: In stock'
              AND p.accepted_qty > 0
              AND p.po_status = 'PENDING'
              AND p.per_liter IS NOT NULL
              AND p.per_liter > 0
              AND p.fulfillment_center = %s
              AND NOT (UPPER(TRIM(p.po_number)) = ANY(%s::text[]))
              AND lp.asin IS NULL
        """, [fc, exclude_list])
        raw = _row_to_dict(cur, cur.fetchall())

    pool = []
    for r in raw:
        row = _serialize_row(r)
        asin_up = str(row.get('asin') or '').upper().strip()
        live = doh_by_asin.get(asin_up, {}) if doh_by_asin else {}
        row['soh_unit'] = live.get('soh_unit', 0) or 0
        row['soh_ltr']  = live.get('soh_ltr',  0) or 0
        row['drr_unit'] = live.get('drr_unit', 0) or 0
        row['drr_ltr']  = live.get('drr_ltr',  0) or 0
        row['doh']      = live.get('doh',      0) or 0
        bucket, score, reason = _compute_priority(
            row['drr_unit'], row['soh_unit'], row['doh'],
            row.get('days_to_expiry'), row.get('po_status'),
        )
        row['priority_bucket'] = bucket
        row['priority_score']  = score
        row['priority_reason'] = reason
        pool.append(row)
    return pool


# Smaller-truck options the planner can suggest when a load comes out very thin.
# Tuple of (size_key, liters). Kept ascending so the loop below finds the
# smallest size that would still hold the current load.
_SMALLER_TRUCK_SUGGESTIONS = (('10_ton', 10000.0),)


def _suggest_smaller_truck(planned_liters, current_capacity, current_truck_size):
    """
    When a plan ends up loading <70% of the chosen truck, suggest a smaller
    truck that would pack to ~80%+. Two-step search:
      1. Try stock sizes (10-ton) first — they're easier for ops to source.
      2. If no stock size hits the threshold, suggest a CUSTOM size sized to
         the actual loaded liters + 10% headroom, rounded to nearest 100 L.
         That guarantees we always offer a path to a full truck, even when
         the candidate pool is genuinely tiny.
    Returns a dict suitable for the API response, or None if not meaningful.
    """
    if planned_liters <= 0 or current_capacity <= 0:
        return None
    current_pct = (planned_liters / current_capacity) * 100
    # Show the "not enough POs" warning whenever the truck isn't essentially
    # full. 95% is the cutoff — above that, the gap is normal case-pack
    # rounding and a warning would just be noise.
    if current_pct >= 95:
        return None

    # 1) Stock-size pass
    for size_key, cap in _SMALLER_TRUCK_SUGGESTIONS:
        if size_key == current_truck_size:
            continue
        if cap >= current_capacity:
            continue  # not actually smaller
        if cap < planned_liters:
            continue  # can't fit current plan either
        new_pct = (planned_liters / cap) * 100
        if new_pct >= 75:
            return {
                'truck_size': size_key,
                'capacity_liters': cap,
                'estimated_fill_pct': round(new_pct, 1),
                'current_fill_pct': round(current_pct, 1),
                'is_custom': False,
                'reason': (
                    f'Pool is small ({int(planned_liters)} L) — a smaller '
                    f'{size_key.replace("_", " ")} truck would ship full.'
                ),
            }

    # 2) Custom-size fallback — round the actual load UP to the nearest 100 L.
    # The truck is already packed, so no headroom needed; this gives the
    # tightest sensible fit (typically 98-100% load on the suggested size).
    suggested = max(500, int(math.ceil(planned_liters / 100.0)) * 100)
    if suggested >= current_capacity:
        # Already pretty close to current — no meaningful smaller option
        return None
    new_pct = round((planned_liters / suggested) * 100, 1)
    return {
        'truck_size': 'custom',
        'capacity_liters': suggested,
        'estimated_fill_pct': new_pct,
        'current_fill_pct': round(current_pct, 1),
        'is_custom': True,
        'reason': (
            f'Pool exhausted at {int(planned_liters)} L. No standard truck '
            f'is small enough — a custom {suggested:,} L truck would ship full.'
        ),
    }


def _record_po_flips(flips):
    """Upsert detected FC flips into public.po_fc_flip (audit log).

    `flips` is an iterable of (po_number, from_fc, to_fc). A flip is when a PO
    is on an appointment whose FC differs from the PO's Amazon-PO-sheet FC —
    i.e. the team intentionally moved (flipped) the PO to the sister FC.
    """
    rows = [
        (str(po or '').strip().upper(), str(frm or '').strip().upper(), str(to or '').strip().upper())
        for (po, frm, to) in (flips or [])
    ]
    rows = [r for r in rows if r[0] and r[1] and r[2] and r[1] != r[2]]
    if not rows:
        return
    try:
        with connection.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.po_fc_flip (po_number, from_fc, to_fc, first_seen, last_seen)
                VALUES (%s, %s, %s, now(), now())
                ON CONFLICT (po_number, from_fc, to_fc)
                DO UPDATE SET last_seen = now()
            """, rows)
    except Exception:
        # Never let flip bookkeeping break planning.
        pass


def _row_eligibility_reason(row):
    """
    Per-(PO, ASIN) reason string for the eligibility detail drawer.

    A "flip" (PO booked on an appointment at a different FC than its PO-sheet FC)
    is treated as VALID — the team intentionally moved the PO to that FC — so it
    no longer blocks eligibility; we just tag it "Flipped <from> → <to>".
    """
    actual = (row.get('actual_fc') or '').strip()
    expected = (row.get('expected_fc') or '').strip()
    flip = f"Flipped {actual} → {expected or '?'}" if (row.get('is_fc_mismatch') and actual) else ''

    if row.get('is_eligible'):
        return f"{flip} · ready to ship" if flip else 'OK · ready to ship'

    if row.get('is_locked'):
        sid = row.get('locked_shipment_id')
        base = f'Locked in shipment #{sid}' if sid else 'Locked in another shipment'
    elif not row.get('is_pending'):
        po_status = (row.get('po_status') or '').strip() or 'unknown'
        base = f'PO closed/dispatched (po_status={po_status})'
    elif not row.get('is_in_stock'):
        avail = (row.get('availability_status') or '').strip() or 'unknown'
        base = f'Out of stock (availability={avail})'
    elif not row.get('has_qty'):
        base = 'Zero accepted qty'
    else:
        base = 'Unknown reason'
    return f"{flip} · {base}" if flip else base


class AppointmentListView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        date_str = request.query_params.get('date')
        if not date_str:
            return Response({'error': 'date parameter required'}, status=400)

        # Single round-trip: dedup appointments for the date, explode the
        # comma-separated POs, evaluate eligibility per (appointment, PO),
        # then aggregate counts back per appointment.
        with connection.cursor() as cur:
            cur.execute("""
                WITH appt_dedup AS (
                    -- Ingest stores one row per (appointment_id, PO). Aggregate
                    -- to one row per appointment_id, stitching POs into a single
                    -- comma list so the LATERAL split below sees the full PO set.
                    SELECT a.appointment_id,
                           MAX(a.status)           AS status,
                           MAX(a.appointment_time) AS appointment_time,
                           MAX(a.destination_fc)   AS destination_fc,
                           MAX(a.pro)              AS pro,
                           STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),''), ',') AS pos
                    FROM reporting."appointment" a
                    WHERE DATE(a.appointment_time) = %s
                    GROUP BY a.appointment_id
                ),
                appt_po_pairs AS (
                    SELECT
                        ad.appointment_id,
                        ad.destination_fc,
                        UPPER(TRIM(pv)) AS po_upper
                    FROM appt_dedup ad,
                    LATERAL unnest(
                        regexp_split_to_array(COALESCE(ad.pos, ''), '\s*[,;]\s*')
                    ) AS pv
                    WHERE NULLIF(TRIM(pv), '') IS NOT NULL
                ),
                po_status AS (
                    SELECT
                        app.appointment_id,
                        app.po_upper,
                        BOOL_OR(p.po_number IS NOT NULL) AS has_fc_match,
                        BOOL_OR(p.status = 'Confirmed' AND p.po_status = 'PENDING') AS is_pending,
                        BOOL_OR(p.availability_status = 'AC - Accepted: In stock') AS is_in_stock,
                        BOOL_OR(COALESCE(p.accepted_qty, 0) > 0) AS has_qty,
                        BOOL_OR(
                            p.status = 'Confirmed'
                            AND p.po_status = 'PENDING'
                            AND p.availability_status = 'AC - Accepted: In stock'
                            AND COALESCE(p.accepted_qty, 0) > 0
                            AND NOT EXISTS (
                                SELECT 1
                                FROM sp_items si
                                JOIN sp_shipments s ON s.id = si.shipment_id
                                WHERE UPPER(TRIM(si.po_number)) = app.po_upper
                                  AND UPPER(TRIM(si.asin))      = UPPER(TRIM(p.asin))
                                  AND si.not_loaded = FALSE
                                  AND s.status != 'rejected'
                            )
                        ) AS is_eligible
                    FROM appt_po_pairs app
                    LEFT JOIN reporting."Amazon PO" p
                        ON UPPER(TRIM(p.po_number)) = app.po_upper
                        -- No FC filter: a PO on this appointment at another FC is a
                        -- flip (intentionally moved), so it still counts as matched.
                    GROUP BY app.appointment_id, app.po_upper
                ),
                appt_counts AS (
                    SELECT
                        appointment_id,
                        COUNT(*) AS total_po,
                        COUNT(*) FILTER (WHERE is_eligible) AS eligible_po,
                        -- POs with NO row at the appointment's FC in Amazon's PO Report.
                        -- Distinguished from "out of stock" so the warning can be accurate.
                        COUNT(*) FILTER (WHERE NOT COALESCE(has_fc_match, FALSE)) AS no_fc_match_po,
                        COUNT(*) FILTER (WHERE NOT COALESCE(is_pending, FALSE))   AS not_pending_po,
                        COUNT(*) FILTER (WHERE NOT COALESCE(is_in_stock, FALSE))  AS not_in_stock_po,
                        COUNT(*) FILTER (WHERE NOT COALESCE(has_qty, FALSE))      AS no_qty_po,
                        COUNT(*) FILTER (
                            WHERE COALESCE(is_pending, FALSE)
                              AND COALESCE(is_in_stock, FALSE)
                              AND COALESCE(has_qty, FALSE)
                              AND NOT COALESCE(is_eligible, FALSE)
                        ) AS locked_po
                    FROM po_status
                    GROUP BY appointment_id
                )
                SELECT
                    ad.appointment_id,
                    ad.status,
                    ad.appointment_time,
                    ad.destination_fc,
                    UPPER(COALESCE(NULLIF(TRIM(fcm.channel::text), ''), 'UNMAPPED')) AS channel,
                    ad.pro,
                    ad.pos,
                    acm.carton_count AS amazon_carton_count,
                    acm.unit_count   AS amazon_unit_count,
                    COALESCE(ac.total_po,        0) AS po_count,
                    COALESCE(ac.eligible_po,     0) AS eligible_po_count,
                    COALESCE(ac.no_fc_match_po,  0) AS no_fc_match_count,
                    COALESCE(ac.not_pending_po,  0) AS not_pending_count,
                    COALESCE(ac.not_in_stock_po, 0) AS not_in_stock_count,
                    COALESCE(ac.no_qty_po,       0) AS no_qty_count,
                    COALESCE(ac.locked_po,       0) AS locked_count
                FROM appt_dedup ad
                LEFT JOIN appt_counts ac USING (appointment_id)
                LEFT JOIN public.appointment_commit acm USING (appointment_id)
                LEFT JOIN public.fc_city_state_channel_master fcm
                    ON UPPER(TRIM(fcm.fc::text)) = UPPER(TRIM(ad.destination_fc::text))
                ORDER BY ad.appointment_time, ad.appointment_id
            """, [date_str])
            rows = _row_to_dict(cur, cur.fetchall())

        # Second pass — fetch per-(appointment, PO, ASIN) details so the
        # frontend can show a drawer with EXACTLY which SKUs are blocked,
        # by which shipment, and how much was ordered. Joined with the
        # latest inventory snapshot so users see "how much less" too.
        with connection.cursor() as cur:
            cur.execute("""
                WITH appt_dedup AS (
                    -- Aggregate per-PO rows into one row per appointment_id,
                    -- stitching POs so the LATERAL split sees the full set.
                    SELECT a.appointment_id,
                           MAX(a.appointment_time) AS appointment_time,
                           MAX(a.destination_fc)   AS destination_fc,
                           STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),''), ',') AS pos
                    FROM reporting."appointment" a
                    WHERE DATE(a.appointment_time) = %s
                    GROUP BY a.appointment_id
                ),
                appt_po_pairs AS (
                    SELECT
                        ad.appointment_id,
                        ad.destination_fc,
                        UPPER(TRIM(pv)) AS po_upper,
                        TRUE AS in_appointment
                    FROM appt_dedup ad,
                    LATERAL unnest(
                        regexp_split_to_array(COALESCE(ad.pos, ''), '\s*[,;]\s*')
                    ) AS pv
                    WHERE NULLIF(TRIM(pv), '') IS NOT NULL
                ),
                latest_inv AS (
                    SELECT
                        UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                        COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit
                    FROM amazon_master_inventory
                    WHERE inventory_date = (SELECT MAX(inventory_date) FROM amazon_master_inventory)
                      AND NULLIF(TRIM(COALESCE(asin::text, '')), '') IS NOT NULL
                    GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
                ),
                locked_lookup AS (
                    SELECT
                        UPPER(TRIM(si.po_number)) AS po_upper,
                        UPPER(TRIM(si.asin))      AS asin_upper,
                        MIN(si.shipment_id)        AS locked_shipment_id
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE si.not_loaded = FALSE
                      AND s.status != 'rejected'
                    GROUP BY UPPER(TRIM(si.po_number)), UPPER(TRIM(si.asin))
                )
                SELECT
                    app.appointment_id,
                    app.destination_fc      AS expected_fc,
                    app.in_appointment,
                    p.po_number,
                    p.asin,
                    p.sku_name             AS product_name,
                    p.accepted_qty,
                    p.case_pack,
                    p.per_liter,
                    p.availability_status,
                    p.po_status,
                    p.status               AS po_record_status,
                    p.days_to_expiry,
                    p.fulfillment_center   AS actual_fc,
                    COALESCE(li.soh_unit, 0) AS soh_unit,
                    (p.fulfillment_center = app.destination_fc)                                AS fc_match,
                    (p.fulfillment_center IS NOT NULL
                       AND p.fulfillment_center <> app.destination_fc)                         AS is_fc_mismatch,
                    (p.status = 'Confirmed' AND p.po_status = 'PENDING')                       AS is_pending,
                    (p.availability_status = 'AC - Accepted: In stock')                        AS is_in_stock,
                    (COALESCE(p.accepted_qty, 0) > 0)                                          AS has_qty,
                    (lk.po_upper IS NOT NULL)                                                  AS is_locked,
                    lk.locked_shipment_id,
                    (
                        -- FC match is NOT required: a PO on this appointment at a
                        -- different FC is a "flip" (intentionally moved), still valid.
                        p.status = 'Confirmed'
                        AND p.po_status = 'PENDING'
                        AND p.availability_status = 'AC - Accepted: In stock'
                        AND COALESCE(p.accepted_qty, 0) > 0
                        AND lk.po_upper IS NULL
                    ) AS is_eligible
                FROM appt_po_pairs app
                LEFT JOIN reporting."Amazon PO" p
                    ON UPPER(TRIM(p.po_number)) = app.po_upper
                LEFT JOIN latest_inv li
                    ON li.asin_key = UPPER(TRIM(COALESCE(p.asin::text, '')))
                LEFT JOIN locked_lookup lk
                    ON lk.po_upper   = app.po_upper
                   AND lk.asin_upper = UPPER(TRIM(COALESCE(p.asin::text, '')))
                WHERE p.po_number IS NOT NULL
                ORDER BY app.appointment_id, app.in_appointment DESC, p.po_number, p.asin
            """, [date_str])
            detail_rows = _row_to_dict(cur, cur.fetchall())

        # Group per-row details by appointment, enriching each row with a
        # human-readable reason and a "shortfall" (accepted_qty − soh_unit).
        details_by_appt = {}
        for r in detail_rows:
            d = _serialize_row(r)
            appt_id = d.pop('appointment_id', None)
            if appt_id is None:
                continue
            d['reason'] = _row_eligibility_reason(d)
            # Surface the flip explicitly (from/to FC) for the UI tag.
            d['is_flipped'] = bool(d.get('is_fc_mismatch'))
            d['flipped_from'] = (d.get('actual_fc') or '').strip() if d['is_flipped'] else None
            d['flipped_to'] = (d.get('expected_fc') or '').strip() if d['is_flipped'] else None
            accepted = float(d.get('accepted_qty') or 0)
            soh = float(d.get('soh_unit') or 0)
            d['shortfall_unit'] = max(0.0, accepted - soh)
            d['soh_covers_pct'] = (
                round((soh / accepted) * 100, 1) if accepted > 0 else None
            )
            details_by_appt.setdefault(appt_id, []).append(d)

        # Lookup: for each appointment, which FCs do its POs ACTUALLY live at
        # in the Amazon PO Report? When the appointment's FC has no matching
        # PO rows, we surface this list in the warning so planners know where
        # the POs really exist ("appointment says DED5, POs are at DED3").
        pos_actual_fcs_by_appt = {}
        with connection.cursor() as cur:
            cur.execute("""
                WITH appt_dedup AS (
                    -- Aggregate per-PO rows into one row per appointment_id so
                    -- the LATERAL split below sees the full PO list.
                    SELECT a.appointment_id,
                           STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),''), ',') AS pos
                    FROM reporting."appointment" a
                    WHERE DATE(a.appointment_time) = %s
                    GROUP BY a.appointment_id
                ),
                appt_po_pairs AS (
                    SELECT ad.appointment_id, UPPER(TRIM(pv)) AS po_upper
                    FROM appt_dedup ad,
                    LATERAL unnest(
                        regexp_split_to_array(COALESCE(ad.pos, ''), '\s*[,;]\s*')
                    ) AS pv
                    WHERE NULLIF(TRIM(pv), '') IS NOT NULL
                )
                SELECT
                    app.appointment_id,
                    ARRAY_AGG(DISTINCT p.fulfillment_center)
                        FILTER (WHERE p.fulfillment_center IS NOT NULL
                                  AND TRIM(p.fulfillment_center) <> '')
                        AS actual_fcs
                FROM appt_po_pairs app
                LEFT JOIN reporting."Amazon PO" p
                    ON UPPER(TRIM(p.po_number)) = app.po_upper
                GROUP BY app.appointment_id
            """, [date_str])
            for appt_id, fcs in cur.fetchall():
                pos_actual_fcs_by_appt[appt_id] = list(fcs or [])

        # Lookup: which appointments already have a shipment? Surfaces a
        # visual "already planned" indicator on the appointment cards so
        # planners can tell at a glance whether they're re-planning vs
        # creating new. Includes primary and combined (additional) appointment
        # IDs from any non-rejected shipment.
        appt_ids_today = [r.get('appointment_id') for r in rows if r.get('appointment_id')]
        existing_by_appt = {}
        if appt_ids_today:
            ids_set = {str(x).strip() for x in appt_ids_today}
            with connection.cursor() as cur:
                cur.execute("""
                    SELECT id, status, appointment_id, additional_appointment_ids
                    FROM sp_shipments
                    WHERE status != 'rejected'
                """)
                for sid, sstatus, primary, additional in cur.fetchall():
                    candidates = set()
                    if primary:
                        candidates.add(str(primary).strip())
                    if additional:
                        for a in str(additional).split(','):
                            a = a.strip()
                            if a:
                                candidates.add(a)
                    for a in candidates & ids_set:
                        existing_by_appt.setdefault(a, []).append({
                            'shipment_id': sid,
                            'status': sstatus,
                        })

        # Attach an `ineligible_reason` string when eligible_po_count == 0 so
        # the frontend can display it directly on the appointment card. Also
        # attach the per-(PO, ASIN) detail rows so a click on the warning
        # opens a drawer showing exactly which SKUs are blocked and by how much.
        # `existing_shipments` lets the UI mark already-planned appointments
        # distinctly so users don't accidentally re-plan one.
        out = []
        for r in rows:
            data = _serialize_row(r)
            elig = int(data.get('eligible_po_count') or 0)
            data['has_eligible'] = elig > 0
            # Stash actual-FC list BEFORE _explain_ineligibility so it can
            # surface the FC-mismatch reason with the real FC names.
            actual_fcs = pos_actual_fcs_by_appt.get(data.get('appointment_id'), [])
            # Filter out the appointment's own FC — only "other" FCs are useful
            appt_fc = (data.get('destination_fc') or '').strip()
            data['pos_actual_fcs'] = [f for f in actual_fcs if f and f != appt_fc]
            data['ineligible_reason'] = '' if elig > 0 else _explain_ineligibility(data)
            data['po_details'] = details_by_appt.get(data.get('appointment_id'), [])
            data['existing_shipments'] = existing_by_appt.get(
                str(data.get('appointment_id') or '').strip(), []
            )
            data['has_existing_plan'] = len(data['existing_shipments']) > 0
            out.append(data)
        return Response(out)


class AppointmentItemsView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, appointment_id):
        truck_size = request.query_params.get('truck_size', '15_ton')
        capacity_override = request.query_params.get('truck_capacity_liters')

        # Optional priority allocation (PREMIUM/COMMODITY/OTHER pct, summing to 100)
        priority = None
        try:
            p_premium = float(request.query_params.get('priority_premium_pct') or -1)
            p_commodity = float(request.query_params.get('priority_commodity_pct') or -1)
            p_other = float(request.query_params.get('priority_other_pct') or -1)
            if p_premium >= 0 and p_commodity >= 0 and p_other >= 0:
                total_pct = p_premium + p_commodity + p_other
                if abs(total_pct - 100) <= 0.5:
                    priority = {
                        'PREMIUM': p_premium,
                        'COMMODITY': p_commodity,
                        'OTHER': p_other,
                    }
        except (TypeError, ValueError):
            priority = None

        # Strict-adherence toggle (default best-effort: leftover capacity fills
        # from other buckets after the per-bucket pack).
        strict_param = str(request.query_params.get('priority_strict') or '').lower()
        priority_strict = strict_param in ('1', 'true', 'yes', 'on')

        # Vendor Central commit caps: per-appointment units & cartons ceiling.
        # Format: {"<appointment_id>": {"units": N, "cartons": N}}. Missing /
        # malformed entries are ignored — the planner just runs uncapped.
        commit_caps = {}
        caps_raw = request.query_params.get('commit_caps_json') or ''
        if caps_raw:
            try:
                parsed = json.loads(caps_raw)
                if isinstance(parsed, dict):
                    for k, v in parsed.items():
                        if not isinstance(v, dict):
                            continue
                        units = int(v.get('units') or 0)
                        cartons = int(v.get('cartons') or 0)
                        if units > 0 or cartons > 0:
                            commit_caps[str(k)] = {'units': units, 'cartons': cartons}
            except (ValueError, TypeError):
                pass

        # Maximize-fill toggle: after the priority-driven plan, top up any
        # remaining capacity with NO-DEMAND / leftover items from the same FC.
        # Default ON so trucks ship full rather than 30% loaded.
        fill_param = str(request.query_params.get('maximize_fill') or '1').lower()
        maximize_fill = fill_param in ('1', 'true', 'yes', 'on')

        # Respect live BH-FGM warehouse stock (default ON): cap planned qty by
        # what's physically available. Off = plan against PO qty only.
        stock_param = str(request.query_params.get('respect_stock') or '1').lower()
        respect_stock = stock_param in ('1', 'true', 'yes', 'on')

        # Multi-appointment support: the URL still carries one appointment_id
        # (the primary entry point) but the caller can pass additional IDs via
        # the `appointment_ids` query param (comma-separated). All appointments
        # must be at the same FC — single-FC trucks only.
        extra_ids_raw = request.query_params.get('appointment_ids') or ''
        extra_ids = [
            x.strip() for x in extra_ids_raw.split(',')
            if x.strip() and x.strip() != appointment_id
        ]
        all_appt_ids = [appointment_id] + extra_ids

        # Authoritative commit caps: public.appointment_commit is the single source
        # of truth (see _lookup_appointment_commit) — never trust the client-sent
        # commit_caps_json above, which can be stale (e.g. the appointment was loaded
        # before its commit changed in Vendor Central). Rebuild from the live DB so
        # the auto plan uses the SAME cap the save guard and the manual planner
        # enforce — a plan that generates can always be saved.
        commit_caps = {
            str(_aid).strip(): _live
            for _aid in all_appt_ids
            if (_live := _lookup_appointment_commit(_aid))
        }

        # Optional explicit PO selection: when provided, the candidate pool is
        # built from this list (still scoped to the appointment's FC, still
        # PENDING+in-stock) instead of the appointment's own PO list. Lets the
        # planner add same-FC extras, drop appointment POs, or completely replace.
        selected_pos_raw = request.query_params.get('selected_pos') or ''
        selected_pos = [
            x.strip().upper() for x in selected_pos_raw.split(',')
            if x.strip()
        ]

        with connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (appointment_id)
                    appointment_id, status, appointment_time, destination_fc, pro
                FROM reporting."appointment"
                WHERE appointment_id = ANY(%s::text[])
                ORDER BY appointment_id, appointment_time DESC NULLS LAST
            """, [all_appt_ids])
            appt_rows = cur.fetchall()

        if not appt_rows:
            return Response({'error': 'Appointment not found'}, status=404)

        # Build the appointments list, validate single-FC + all-Confirmed
        appts_by_id = {}
        for r in appt_rows:
            appts_by_id[r[0]] = {
                'appointment_id': r[0],
                'status': r[1],
                'appointment_time': r[2].isoformat() if r[2] else None,
                'destination_fc': r[3],
                'pro': r[4],
            }

        if appointment_id not in appts_by_id:
            return Response({'error': 'Primary appointment not found'}, status=404)

        appt = appts_by_id[appointment_id]
        if appt['status'] != 'Confirmed':
            return Response({'error': 'Appointment is not Confirmed'}, status=400)

        # FC consistency check across all combined appointments
        primary_fc_value = appt['destination_fc']
        for aid in extra_ids:
            other = appts_by_id.get(aid)
            if not other:
                return Response(
                    {'error': f'Additional appointment {aid} not found'},
                    status=400,
                )
            if other['status'] != 'Confirmed':
                return Response(
                    {'error': f'Appointment {aid} is not Confirmed'},
                    status=400,
                )
            if other['destination_fc'] != primary_fc_value:
                return Response(
                    {
                        'error': (
                            f'Cannot combine appointments at different FCs '
                            f'({appointment_id} at {primary_fc_value} vs '
                            f'{aid} at {other["destination_fc"]})'
                        ),
                    },
                    status=400,
                )

        all_appts = [appts_by_id[a] for a in all_appt_ids if a in appts_by_id]

        # Build the appointment's own PO set in Python so we can both override
        # the candidate pool with selected_pos AND know which candidates were
        # "from the appointment" vs "extras" for downstream tagging.
        with connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT UPPER(TRIM(pv)) AS po_number
                FROM reporting."appointment" a,
                LATERAL unnest(
                    regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
                ) AS pv
                WHERE a.appointment_id = ANY(%s::text[])
                  AND NULLIF(TRIM(pv), '') IS NOT NULL
            """, [all_appt_ids])
            appt_pos_set = {r[0] for r in cur.fetchall() if r[0]}

        # Final candidate-PO list: caller's explicit selection (if any) else the
        # appointment's own POs.
        candidate_pos = selected_pos if selected_pos else sorted(appt_pos_set)

        with connection.cursor() as cur:
            cur.execute("""
                WITH appt_pos AS (
                    -- Candidate PO pool. When the caller passed selected_pos the
                    -- list is the explicit selection; otherwise it's the union of
                    -- all selected appointments' POs (mapping back to which
                    -- appointment each PO came from is done via appt_po_map).
                    SELECT DISTINCT UPPER(TRIM(po_number)) AS po_number,
                           %s AS appointment_id  -- default: primary appt as source
                    FROM unnest(%s::text[]) AS po_number
                    WHERE NULLIF(TRIM(po_number), '') IS NOT NULL
                ),
                appt_po_map AS (
                    -- For multi-appointment combine without selected_pos, map each
                    -- PO back to the appointment it originally came from so the
                    -- source_appointment_id below is per-appointment, not primary.
                    SELECT DISTINCT
                        UPPER(TRIM(pv)) AS po_number,
                        a.appointment_id
                    FROM reporting."appointment" a,
                    LATERAL unnest(
                        regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
                    ) AS pv
                    WHERE a.appointment_id = ANY(%s::text[])
                      AND NULLIF(TRIM(pv), '') IS NOT NULL
                ),
                committed AS (
                    -- PO-fulfilment "committed": units already put on ANY
                    -- non-rejected shipment (incl. dispatched/delivered — gone from
                    -- the order). DISTINCT from physical stock reservation in
                    -- _reserved_stock_by_asin() (which counts only not-yet-dispatched
                    -- shipments). Don't merge the two.
                    -- Keyed by (ASIN, PO) ONLY, summed across every FC. NOT FC-keyed:
                    -- a flipped PO ships under the appointment's FC, not its PO-sheet
                    -- FC, so an FC-keyed match would miss the flip and wrongly
                    -- re-offer the already-shipped units. A PO has one total ordered
                    -- qty, so its commitments across all FCs subtract together; the
                    -- remainder (accepted - committed) is what's still shippable.
                    SELECT si.asin,
                           UPPER(TRIM(si.po_number)) AS po_number,
                           SUM(COALESCE(si.planned_qty, 0)) AS committed_qty
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE si.not_loaded = FALSE
                      AND s.status != 'rejected'
                    GROUP BY si.asin,
                             UPPER(TRIM(si.po_number))
                ),
                doh_data AS (
                    -- placeholder; DOH joined in Python via _live_doh_by_asin() below
                    SELECT NULL::text AS asin
                )
                SELECT
                    p.po_number,
                    p.asin,
                    p.merchant_sku        AS internal_sku,
                    p.sap_sku_code,
                    p.sku_name            AS product_name,
                    -- Orderable amount this plan = leftover after prior commitments.
                    (p.accepted_qty - COALESCE(c.committed_qty, 0)) AS accepted_qty,
                    p.accepted_qty        AS original_accepted_qty,
                    COALESCE(c.committed_qty, 0) AS committed_qty,
                    p.case_pack,
                    p.per_liter,
                    p.cost_price,
                    -- Liters for the leftover so the packer fills against remaining.
                    round((p.accepted_qty - COALESCE(c.committed_qty, 0)) * COALESCE(p.per_liter, 0), 4) AS total_accepted_liters,
                    p.days_to_expiry,
                    p.expiry_date,
                    p.category,
                    p.sub_category,
                    p.brand,
                    p.item_head,
                    p.item,
                    p.availability_status,
                    p.po_status,
                    p.status,
                    p.fulfillment_center,
                    p.fulfillment_center  AS destination_fc,
                    -- Source appointment: real per-PO mapping when the PO is on
                    -- one of the selected appointments; primary appointment when
                    -- it's a planner-added extra (not on any selected appt).
                    COALESCE(m.appointment_id, ap.appointment_id) AS source_appointment_id,
                    -- Tag the row so the UI can render "IN APPT" vs "EXTRA" chips
                    -- on the loaded items without re-querying.
                    (m.appointment_id IS NOT NULL)                 AS is_appointment_po
                FROM appt_pos ap
                JOIN reporting."Amazon PO" p
                    ON UPPER(TRIM(p.po_number)) = ap.po_number
                    -- PO at the appointment's FC (normal) OR a PO genuinely on the
                    -- appointment but at another FC (a "flip" — intentionally moved
                    -- to this FC). Planner-added extras still require an FC match.
                    AND (
                        p.fulfillment_center = %s
                        OR EXISTS (SELECT 1 FROM appt_po_map m2 WHERE m2.po_number = ap.po_number)
                    )
                LEFT JOIN appt_po_map m
                    ON m.po_number = ap.po_number
                LEFT JOIN committed c
                    ON c.asin = p.asin
                    AND c.po_number = UPPER(TRIM(p.po_number))
                WHERE p.status = 'Confirmed'
                  AND p.availability_status = 'AC - Accepted: In stock'
                  AND p.accepted_qty > 0
                  AND p.po_status = 'PENDING'
                  AND (p.accepted_qty - COALESCE(c.committed_qty, 0)) > 0
            """, [appointment_id, candidate_pos, all_appt_ids, primary_fc_value])
            raw = _row_to_dict(cur, cur.fetchall())

        # Attach LIVE DOH/DRR/SOH (matches SOH/DOH dashboard exactly)
        doh_by_asin, doh_meta = _live_doh_by_asin()
        appt_fc_up = str(primary_fc_value or '').strip().upper()
        flips_seen = []
        for r in raw:
            asin_up = str(r.get('asin') or '').upper().strip()
            live = doh_by_asin.get(asin_up, {})
            r['soh_unit'] = live.get('soh_unit', 0) or 0
            r['soh_ltr']  = live.get('soh_ltr', 0) or 0
            r['drr_unit'] = live.get('drr_unit', 0) or 0
            r['drr_ltr']  = live.get('drr_ltr', 0) or 0
            r['doh']      = live.get('doh', 0) or 0
            # Flip detection: PO's actual (sheet) FC differs from the appointment FC
            # it's being shipped on. Tag it and ship it to the appointment's FC.
            actual_fc = str(r.get('fulfillment_center') or '').strip()
            if actual_fc and actual_fc.upper() != appt_fc_up:
                r['is_flipped'] = True
                r['flipped_from'] = actual_fc
                r['flipped_to'] = primary_fc_value
                r['destination_fc'] = primary_fc_value  # ships to the appointment's FC
                flips_seen.append((r.get('po_number'), actual_fc, primary_fc_value))
            else:
                r['is_flipped'] = False
                r['flipped_from'] = None
                r['flipped_to'] = None
        _record_po_flips(flips_seen)

        if not raw:
            return Response({
                'appointment': appt,
                'loaded_items': [],
                'not_loaded_items': [],
                'load_summary': {
                    'truck_size': truck_size,
                    'capacity': _resolve_capacity(truck_size, capacity_override),
                    'planned_liters': 0,
                    'load_percentage': 0,
                },
                'message': 'No eligible SKUs found. All POs may be out of stock, cancelled, or already dispatched.',
            })

        items = [_serialize_row(r) for r in raw]
        for item in items:
            bucket, score, reason = _compute_priority(
                item['drr_unit'], item['soh_unit'], item['doh'],
                item['days_to_expiry'], item['po_status'],
            )
            item['priority_bucket'] = bucket
            item['priority_score'] = score
            item['priority_reason'] = reason
            # Track the source appointment so the UI can show "from appt X"
            # tags + we can compute the majority appointment for the saved
            # shipment's primary appointment_id field.
            item['appointment_id'] = item.get('source_appointment_id') or appointment_id

        items.sort(key=lambda x: (
            -x['priority_score'],
            x.get('days_to_expiry') or 999,
            -(x.get('accepted_qty') or 0),
        ))

        # Live warehouse stock: tag every item with BH-FGM on-hand / reserved /
        # available / incoming, and (when respect_stock) cap the orderable qty to
        # what's AVAILABLE (on-hand − reserved by other active shipments),
        # consumed in priority order. avail_remaining is shared with the DOH
        # fillers below so one ASIN's stock isn't double-counted.
        stock_detail = _bh_fgm_stock_detail()
        reserved = _reserved_stock_by_asin()
        avail_total = {a: max(0.0, d['onhand'] - reserved.get(a, 0.0)) for a, d in stock_detail.items()}
        avail_remaining = dict(avail_total)
        _apply_stock_caps(items, avail_total, avail_remaining, respect_stock, stock_detail, reserved)

        # Appointment POs come FIRST and in full: pack the appointment's own POs
        # (highest priority_score first) straight into the truck, limited only by
        # physical capacity. By design `priority=None` here — the PREMIUM/COMMODITY/
        # OTHER slider intentionally does NOT shrink the appointment's own POs (that
        # would drop committed goods); it only steers the discretionary DOH-filler
        # waterfall below and the standalone DOH Auto-Fill view. The Vendor Central
        # units/cartons cap (with +7% tolerance) is still applied at the end.
        loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
            items, truck_size, capacity_override, priority=None,
        )

        # Maximize-fill — three-stage waterfall:
        #   1) NO-DEMAND + leftover items from THIS appointment's own pool.
        #   2) DOH-driven fillers: other PENDING POs at the same FC that
        #      aren't part of this appointment. Lets the truck fill close to
        #      100% when the appointment itself is small. Items still ship
        #      on the same truck — single-FC enforced.
        filler_count = 0
        doh_filler_count = 0
        primary_fc = appt.get('destination_fc') if appt else None
        if maximize_fill:
            # Stage 1 — same-appointment fillers
            if not_loaded:
                loaded, not_loaded = _filler_pass(
                    loaded, not_loaded, capacity,
                    primary_fc=primary_fc,
                    mark_key='_filler',
                )
                filler_count = sum(1 for it in loaded if it.get('_filler'))

            # Stage 2 — DOH-driven fillers (non-appointment PENDING POs at same FC)
            cur_planned = sum(float(it.get('planned_liters') or 0) for it in loaded)
            if cur_planned < float(capacity) and primary_fc:
                appt_po_uppers = sorted({
                    str(it.get('po_number') or '').strip().upper()
                    for it in items
                    if it.get('po_number')
                })
                doh_pool = _fetch_doh_filler_pool(primary_fc, appt_po_uppers, doh_by_asin)
                # Cap fillers by the same live stock (shared remaining pool).
                _apply_stock_caps(doh_pool, avail_total, avail_remaining, respect_stock, stock_detail, reserved)
                if doh_pool:
                    loaded, _doh_unfit = _filler_pass(
                        loaded, doh_pool, capacity,
                        primary_fc=primary_fc,
                        mark_key='_doh_filler',
                        reason=(
                            'DOH filler · pulled from same-FC PENDING POs not '
                            'tied to this appointment, ranked by DOH urgency.'
                        ),
                    )
                    doh_filler_count = sum(1 for it in loaded if it.get('_doh_filler'))

            # Recompute totals so the load meter reflects all fillers
            planned_liters = round(sum(float(it.get('planned_liters') or 0) for it in loaded), 4)
            load_pct = round((planned_liters / capacity * 100) if capacity > 0 else 0, 2)

        # Apply Vendor Central commit caps as the FINAL filter so anything
        # that maximize_fill pulled in respects the per-appointment cap too.
        if commit_caps:
            loaded, not_loaded = _enforce_commit_caps(loaded, not_loaded, commit_caps)
            planned_liters = round(sum(float(it.get('planned_liters') or 0) for it in loaded), 4)
            load_pct = round((planned_liters / capacity * 100) if capacity > 0 else 0, 2)

        # Surface the stock reason: out-of-stock items get it as their not-loaded
        # reason; partially-stocked items get it as their short reason.
        if respect_stock:
            for it in not_loaded:
                if it.get('stock_unfit') and float(it.get('planned_qty') or 0) <= 0:
                    it['unfit_reason'] = it['stock_unfit']
            for it in loaded:
                if it.get('stock_limited') and it.get('stock_unfit') and not it.get('short_reason'):
                    it['short_reason'] = it['stock_unfit']

        # If load is still thin, suggest a smaller truck size
        truck_suggestion = _suggest_smaller_truck(planned_liters, capacity, truck_size)

        # Multi-truck: how many trucks the appointment's OWN available-stock demand
        # needs (ignores DOH fillers — those only top off truck 1). Walks the
        # stock-capped demand in priority order, filling trucks of `capacity`;
        # an item's liters may split across trucks. Purely informational here.
        trucks_breakdown = []
        if capacity > 0:
            t_units = 0.0
            t_liters = 0.0
            remaining_cap = float(capacity)
            for it in items:  # already priority-sorted
                pl = float(it.get('per_liter') or 0)
                units = float(it.get('accepted_qty') or 0)
                sc = it.get('stock_cap')
                if sc is not None:
                    units = min(units, max(0.0, float(sc)))
                if units <= 0:
                    continue
                if pl <= 0:
                    t_units += units  # zero-volume rides any truck free
                    continue
                liters = units * pl
                while liters > 1e-6:
                    if remaining_cap <= 1e-6:
                        trucks_breakdown.append({'liters': round(t_liters, 1), 'units': int(round(t_units))})
                        t_units = 0.0
                        t_liters = 0.0
                        remaining_cap = float(capacity)
                    take = min(liters, remaining_cap)
                    t_liters += take
                    t_units += take / pl
                    remaining_cap -= take
                    liters -= take
            if t_liters > 1e-6 or t_units > 0:
                trucks_breakdown.append({'liters': round(t_liters, 1), 'units': int(round(t_units))})
        trucks_needed = max(1, len(trucks_breakdown))

        # Multi-appointment: compute the majority by loaded liters so the
        # saved shipment can store the right primary appointment_id, and
        # build per-appointment counts so the UI can show "appt A 3500L,
        # appt B 1200L · DOH filler 2500L" breakdowns.
        liters_by_appt = {}
        for it in loaded:
            if it.get('_doh_filler'):
                continue  # DOH fillers don't belong to any appointment
            aid = str(it.get('appointment_id') or '').strip() or appointment_id
            liters_by_appt[aid] = liters_by_appt.get(aid, 0.0) + float(it.get('planned_liters') or 0)

        # Majority = appointment with the most loaded liters (ties → URL primary)
        primary_appt_id = appointment_id
        if liters_by_appt:
            sorted_appts = sorted(liters_by_appt.items(), key=lambda x: -x[1])
            if sorted_appts[0][0] and sorted_appts[0][1] > 0:
                primary_appt_id = sorted_appts[0][0]

        appointments_meta = []
        for a in all_appts:
            a_id = a['appointment_id']
            appointments_meta.append({
                'appointment_id': a_id,
                'appointment_time': a.get('appointment_time'),
                'destination_fc': a.get('destination_fc'),
                'pro': a.get('pro'),
                'loaded_liters': round(liters_by_appt.get(a_id, 0.0), 4),
                'is_primary': a_id == primary_appt_id,
            })
        # Sort: primary first, then by loaded liters desc
        appointments_meta.sort(key=lambda x: (not x['is_primary'], -x['loaded_liters']))

        primary_appt = appts_by_id.get(primary_appt_id, appt)

        return Response({
            'appointment': primary_appt,
            'appointments_meta': appointments_meta,
            'primary_appointment_id': primary_appt_id,
            'doh_snapshot': doh_meta,
            'stock_snapshot': _stock_meta_payload(stock_detail),
            'priority_strict': priority_strict,
            'maximize_fill': maximize_fill,
            'filler_count': filler_count,
            'doh_filler_count': doh_filler_count,
            'commit_caps': commit_caps,
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
            'priority_requested': priority,
            'priority_actual': priority_actual,
            'load_summary': {
                'truck_size': truck_size,
                'capacity': capacity,
                'planned_liters': planned_liters,
                'load_percentage': load_pct,
            },
            'truck_suggestion': truck_suggestion,
            'trucks_needed': trucks_needed,
            'trucks_breakdown': trucks_breakdown,
        })


class AppointmentExtraPosView(_SafeAPIView):
    """
    Lists same-FC PENDING + in-stock POs that AREN'T on the appointment(s).
    Powers the PO picker that lets a planner add "extra" POs alongside (or in
    place of) the appointment's own PO list. Same shape as the appointment
    items, minus DOH (the planner doesn't need it for the picker view).
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, appointment_id):
        extra_ids_raw = request.query_params.get('appointment_ids') or ''
        extra_ids = [
            x.strip() for x in extra_ids_raw.split(',')
            if x.strip() and x.strip() != appointment_id
        ]
        all_appt_ids = [appointment_id] + extra_ids

        with connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (appointment_id)
                    appointment_id, status, destination_fc, pos
                FROM reporting."appointment"
                WHERE appointment_id = ANY(%s::text[])
                ORDER BY appointment_id, appointment_time DESC NULLS LAST
            """, [all_appt_ids])
            appt_rows = cur.fetchall()

        if not appt_rows:
            return Response({'error': 'Appointment not found'}, status=404)

        fcs = {r[2] for r in appt_rows if r[2]}
        if len(fcs) > 1:
            return Response({'error': 'Combined appointments must share an FC'}, status=400)
        fc = next(iter(fcs), None)
        if not fc:
            return Response({'extra_pos': [], 'count': 0, 'fc': None})

        # Collect the appointments' own POs to exclude from the "extra" list.
        own_pos = set()
        for _, _, _, pos_str in appt_rows:
            for p in (pos_str or '').replace(';', ',').split(','):
                p = p.strip().upper()
                if p:
                    own_pos.add(p)

        with connection.cursor() as cur:
            cur.execute("""
                SELECT
                    p.po_number,
                    MAX(p.sku_name) AS product_name,
                    COUNT(DISTINCT p.asin) AS sku_count,
                    SUM(COALESCE(p.accepted_qty, 0))::bigint AS total_accepted_qty,
                    ROUND(SUM(COALESCE(p.accepted_qty, 0) * COALESCE(p.per_liter, 0))::numeric, 2) AS total_liters,
                    MIN(p.days_to_expiry) AS earliest_days_to_expiry,
                    MAX(p.order_date)     AS order_date,
                    MAX(p.item_head)      AS item_head,
                    -- Per-SKU breakdown so the picker can expand a PO and show every
                    -- ASIN with its (short) item name and line detail.
                    json_agg(
                        json_build_object(
                            'asin', p.asin,
                            'item', p.item,
                            'product_name', p.sku_name,
                            'internal_sku', p.merchant_sku,
                            'sap_sku_code', p.sap_sku_code,
                            'item_head', p.item_head,
                            'accepted_qty', p.accepted_qty,
                            'case_pack', p.case_pack,
                            'per_liter', p.per_liter,
                            'total_liters', ROUND((COALESCE(p.accepted_qty, 0) * COALESCE(p.per_liter, 0))::numeric, 2),
                            'days_to_expiry', p.days_to_expiry,
                            'expiry_date', p.expiry_date
                        )
                        ORDER BY p.days_to_expiry NULLS LAST, p.asin
                    ) AS skus
                FROM reporting."Amazon PO" p
                WHERE p.fulfillment_center = %s
                  AND p.status = 'Confirmed'
                  AND p.po_status = 'PENDING'
                  AND p.availability_status = 'AC - Accepted: In stock'
                  AND COALESCE(p.accepted_qty, 0) > 0
                  AND NOT (UPPER(TRIM(p.po_number)) = ANY(%s::text[]))
                GROUP BY p.po_number
                ORDER BY MIN(p.days_to_expiry) NULLS LAST, p.po_number
            """, [fc, sorted(own_pos)])
            raw = _row_to_dict(cur, cur.fetchall())

        # Enrich each SKU with live DOH (the same rolling-window snapshot the
        # planner uses) so the picker can show a DOH column.
        doh_by_asin, _ = _live_doh_by_asin()
        for r in raw:
            skus = r.get('skus')
            if isinstance(skus, str):
                try:
                    skus = json.loads(skus)
                except (ValueError, TypeError):
                    skus = []
                r['skus'] = skus
            for sk in (skus or []):
                a = str(sk.get('asin') or '').strip().upper()
                live = doh_by_asin.get(a) if doh_by_asin else None
                sk['doh'] = (round(float(live['doh']), 1)
                             if live and live.get('doh') is not None else None)

        return Response({
            'fc': fc,
            'count': len(raw),
            'extra_pos': [_serialize_row(r) for r in raw],
        })


# ---------------------------------------------------------------------------
# Shipment CRUD
# ---------------------------------------------------------------------------

class ShipmentListCreateView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        from django.db.models import Count, Q, Prefetch
        # Annotate the loaded-item count once in SQL instead of letting the
        # serializer run one COUNT(*) per shipment (N+1: 200 shipments -> 201
        # queries). ShipmentListSerializer.get_item_count reads this annotation.
        # Also prefetch the loaded items (minimal fields) into `loaded_items_pref`
        # so the list-card summary (category tonnes, PO/SKU counts, units/cartons)
        # is computed from ONE extra query, not one per shipment.
        qs = (
            Shipment.objects
            .select_related('created_by', 'approved_by')
            .annotate(loaded_item_count=Count('items', filter=Q(items__not_loaded=False)))
            .prefetch_related(Prefetch(
                'items',
                queryset=ShipmentItem.objects.filter(not_loaded=False).only(
                    'shipment_id', 'item_head', 'planned_liters', 'planned_qty',
                    'case_pack', 'po_number', 'asin', 'internal_sku',
                ),
                to_attr='loaded_items_pref',
            ))
        )
        status_filter = request.query_params.get('status')
        if status_filter:
            qs = qs.filter(status=status_filter)
        serializer = ShipmentListSerializer(qs, many=True)
        return Response(serializer.data)

    def post(self, request):
        data = request.data
        appointment_id = data.get('appointment_id')
        truck_size = data.get('truck_size', '15_ton')
        loaded_items = data.get('loaded_items', [])
        not_loaded_items = data.get('not_loaded_items', [])
        appointment = data.get('appointment', {})
        load_summary = data.get('load_summary', {})
        # Multi-appointment payload: full meta array + extra IDs (excluding
        # the primary). Frontend sends both; backend uses them to populate
        # the new `additional_appointment_ids` + `appointments_meta` fields.
        appointments_meta = data.get('appointments_meta') or []
        commitment_snapshot = data.get('commitment_snapshot') or []
        if not isinstance(commitment_snapshot, list):
            commitment_snapshot = []
        additional_ids = data.get('additional_appointment_ids') or ''
        if isinstance(additional_ids, list):
            additional_ids = ','.join(str(x) for x in additional_ids if x)

        # Derive destination_fc: explicit > appointment > most common FC across loaded items
        explicit_fc = (appointment or {}).get('destination_fc') or data.get('destination_fc')
        if not explicit_fc and loaded_items:
            from collections import Counter
            fcs = [i.get('destination_fc') for i in loaded_items if i.get('destination_fc')]
            explicit_fc = Counter(fcs).most_common(1)[0][0] if fcs else ''
        destination_fc = explicit_fc or ''

        # Resolve planning_mode: explicit from frontend wins; otherwise infer from payload shape
        planning_mode = data.get('planning_mode')
        if planning_mode not in dict(Shipment.PlanningMode.choices):
            planning_mode = (
                Shipment.PlanningMode.APPOINTMENT if appointment_id
                else Shipment.PlanningMode.MANUAL
            )

        # Serialize the claim+create critical section with a transaction-scoped
        # advisory lock so two planners can't both pass the "is this PO+ASIN
        # free?" check and then both insert the same rows (phantom race →
        # over-commitment). Concurrent saves queue here; the second one re-checks
        # below — now inside the lock — and sees the first planner's rows.
        with transaction.atomic():
            with connection.cursor() as _lock_cur:
                _lock_cur.execute('SELECT pg_advisory_xact_lock(%s)', [SHIPMENT_CLAIM_LOCK])

            # Lock re-check at draft time. Between the moment the plan was
            # generated and this Save call, another planner may have claimed some
            # of the same ASIN+PO rows. Fail fast with details so the UI can
            # guide the user rather than surfacing the conflict later at Submit.
            if loaded_items:
                # A PO line may legitimately span MULTIPLE shipments — e.g. it is
                # short-supplied on one appointment's truck and its leftover ships on
                # a later same-FC appointment. So we do NOT block on mere overlap; we
                # block only when the COMBINED commitment across all active shipments
                # would exceed what Amazon ordered (a real over-commit). Serialized by
                # the advisory lock above, so two concurrent saves can't both slip past.
                new_by_key = {}
                invalid_lines = []   # fix #4: loaded lines with a blank PO / ASIN
                for it in loaded_items:
                    q = float(it.get('planned_qty') or 0)
                    a = str(it.get('asin') or '').strip().upper()
                    p = str(it.get('po_number') or '').strip().upper()
                    if q > 1e-6 and (not a or not p):
                        invalid_lines.append({'asin': a or None, 'po_number': p or None, 'planned_qty': q})
                    if a and p:
                        new_by_key[(a, p)] = new_by_key.get((a, p), 0.0) + q
                if invalid_lines:
                    return Response(
                        {
                            'error': 'Loaded lines with a blank PO or ASIN',
                            'invalid_lines': invalid_lines,
                            'detail': (
                                f'{len(invalid_lines)} loaded line(s) have a blank PO or ASIN and '
                                'cannot be validated against the Amazon PO list. Refresh the plan '
                                'and try again.'
                            ),
                        },
                        status=409,
                    )
                if new_by_key:
                    po_uppers = list({p for (_a, p) in new_by_key})
                    committed_map, ordered_map, ship_map = {}, {}, {}
                    with connection.cursor() as _claim_cur:
                        # Units already committed to OTHER active shipments (this new
                        # one isn't inserted yet), per (asin, po).
                        _claim_cur.execute(
                            """
                            SELECT UPPER(TRIM(si.asin)), UPPER(TRIM(si.po_number)),
                                   COALESCE(SUM(COALESCE(si.planned_qty, 0)), 0),
                                   MIN(s.id)
                            FROM sp_items si JOIN sp_shipments s ON s.id = si.shipment_id
                            WHERE UPPER(TRIM(si.po_number)) = ANY(%s)
                              AND si.not_loaded = FALSE AND s.status != 'rejected'
                            GROUP BY UPPER(TRIM(si.asin)), UPPER(TRIM(si.po_number))
                            """,
                            [po_uppers],
                        )
                        for a, p, q, sid in _claim_cur.fetchall():
                            committed_map[(a, p)] = float(q or 0)
                            ship_map[(a, p)] = sid
                        # Ordered (Amazon-accepted) qty per (asin, po).
                        _claim_cur.execute(
                            """
                            SELECT UPPER(TRIM(asin)), UPPER(TRIM(po_number)), MAX(accepted_qty)
                            FROM reporting."Amazon PO"
                            WHERE UPPER(TRIM(po_number)) = ANY(%s)
                            GROUP BY UPPER(TRIM(asin)), UPPER(TRIM(po_number))
                            """,
                            [po_uppers],
                        )
                        for a, p, q in _claim_cur.fetchall():
                            if q is not None:
                                ordered_map[(a, p)] = float(q)
                    # fix #4: every loaded line must reference a real Amazon PO, else
                    # its ordered qty can't be verified and it could bypass the cap.
                    unknown_po = [
                        {'asin': a, 'po_number': p}
                        for (a, p), nq in new_by_key.items()
                        if nq > 1e-6 and (a, p) not in ordered_map
                    ]
                    if unknown_po:
                        return Response(
                            {
                                'error': 'Loaded lines reference an unknown PO',
                                'unknown_po_lines': unknown_po,
                                'detail': (
                                    f'{len(unknown_po)} loaded line(s) reference a (PO, ASIN) not '
                                    'found in the Amazon PO list, so the ordered quantity cannot be '
                                    'verified. Refresh the plan and try again.'
                                ),
                            },
                            status=409,
                        )
                    conflicts = []
                    for (a, p), new_qty in new_by_key.items():
                        existing = committed_map.get((a, p), 0.0)
                        ordered = ordered_map.get((a, p))
                        if ordered is not None and (existing + new_qty) > ordered + 1e-6:
                            conflicts.append({
                                'asin': a, 'po_number': p,
                                'shipment_id': ship_map.get((a, p)),
                                'ordered_qty': ordered,
                                'already_committed': round(existing, 4),
                                'this_plan': round(new_qty, 4),
                            })
                    if conflicts:
                        return Response(
                            {
                                'error': 'Some items would exceed the ordered quantity',
                                'conflicts': conflicts,
                                'detail': (
                                    f'{len(conflicts)} line(s) would over-commit the PO — '
                                    'already-shipped units plus this plan exceed the ordered '
                                    'quantity. Reduce the quantity and try again.'
                                ),
                            },
                            status=409,
                        )

            # Live-stock re-check (fix #1 — same physical stock double-committed).
            # The plan was stock-capped when generated, but another plan may have
            # claimed some of the same stock since. Inside the advisory lock, reject
            # if any ASIN's total in THIS plan, plus what OTHER active shipments
            # already reserve, exceeds live pooled on-hand. Skipped only when live
            # stock is entirely unverifiable (SAP down + no snapshot) so an outage
            # doesn't block every save — matching the "serve stale, keep working"
            # policy. Serialized by the lock, so two concurrent saves can't both pass.
            if loaded_items:
                _save_stock = _bh_fgm_stock_detail()
                if _save_stock:
                    _reserved_other = _reserved_stock_by_asin()   # excludes this unsaved plan
                    _plan_by_asin = {}
                    for it in loaded_items:
                        a = str(it.get('asin') or '').strip().upper()
                        if a:
                            _plan_by_asin[a] = _plan_by_asin.get(a, 0.0) + float(it.get('planned_qty') or 0)
                    stock_conflicts = []
                    for a, want in _plan_by_asin.items():
                        if want <= 1e-6:
                            continue
                        d = _save_stock.get(a)
                        onhand = float(d['onhand']) if d else 0.0
                        reserved = float(_reserved_other.get(a, 0.0))
                        available = onhand - reserved
                        if want > available + 1e-6:
                            stock_conflicts.append({
                                'asin': a,
                                'wanted': round(want, 2),
                                'on_hand': round(onhand, 2),
                                'reserved_elsewhere': round(reserved, 2),
                                'available': round(max(0.0, available), 2),
                            })
                    if stock_conflicts:
                        return Response(
                            {
                                'error': 'Not enough live stock to save this plan',
                                'stock_conflicts': stock_conflicts,
                                'detail': (
                                    f'{len(stock_conflicts)} item(s) exceed available warehouse '
                                    'stock — another plan may have claimed it since this plan was '
                                    'generated. Refresh the plan and try again.'
                                ),
                            },
                            status=409,
                        )

            # Appointment-commitment guard (units + cartons), PER appointment on the
            # truck (fix #3). Each loaded line is attributed to its SOURCE appointment
            # (source_appointment_id; fillers / extras with none fall to the primary),
            # so a combined truck can no longer over-commit a secondary appointment
            # whose figures were previously invisible. For each appointment that has a
            # commit, the total across ALL active shipments' items tagged to it (this
            # plan included) must stay within the Vendor Central commit (+7%).
            # Serialized by the advisory lock; no-op when an appointment has no commit.
            if loaded_items:
                add_by_appt = {}
                for it in loaded_items:
                    aid = str(it.get('source_appointment_id') or appointment_id or '').strip()
                    if not aid:
                        continue
                    q = float(it.get('planned_qty') or 0)
                    cp = max(float(it.get('case_pack') or 1), 1.0)
                    slot = add_by_appt.setdefault(aid, {'u': 0.0, 'c': 0.0})
                    slot['u'] += q
                    slot['c'] += q / cp
                over_commit = []
                for aid, add in add_by_appt.items():
                    cap = _lookup_appointment_commit(aid)
                    if not cap:
                        continue
                    with connection.cursor() as _agg_cur:
                        _agg_cur.execute(
                            """
                            SELECT COALESCE(SUM(COALESCE(si.planned_qty, 0)), 0),
                                   COALESCE(SUM(COALESCE(si.planned_qty, 0)
                                            / GREATEST(COALESCE(si.case_pack, 1), 1)), 0)
                            FROM sp_items si
                            JOIN sp_shipments s ON s.id = si.shipment_id
                            WHERE si.not_loaded = FALSE
                              AND s.status != 'rejected'
                              AND TRIM(si.appointment_id) = %s
                            """,
                            [aid],
                        )
                        _row = _agg_cur.fetchone()
                    exist_u = float(_row[0] or 0)
                    exist_c = float(_row[1] or 0)
                    cap_u = cap['units'] * CAP_TOLERANCE if cap['units'] > 0 else float('inf')
                    cap_c = cap['cartons'] * CAP_TOLERANCE if cap['cartons'] > 0 else float('inf')
                    if (exist_u + add['u']) > cap_u + 1e-6 or (exist_c + add['c']) > cap_c + 1e-6:
                        over_commit.append(
                            f'appointment {aid} commit {int(cap["units"])} units / '
                            f'{int(cap["cartons"])} cartons (+7%): already {int(round(exist_u))} units / '
                            f'{int(round(exist_c))} cartons, this plan adds {int(round(add["u"]))} units / '
                            f'{int(round(add["c"]))} cartons'
                        )
                if over_commit:
                    return Response(
                        {
                            'error': 'Exceeds the appointment commitment',
                            'detail': ('One or more appointments would exceed their Vendor Central '
                                       'commit — ' + '; '.join(over_commit) + '.'),
                        },
                        status=409,
                    )

            shipment = Shipment.objects.create(
                appointment_id=appointment_id or '',
                appointment_time=appointment.get('appointment_time') if appointment else None,
                destination_fc=destination_fc,
                pro=(appointment or {}).get('pro', ''),
                additional_appointment_ids=additional_ids,
                appointments_meta=appointments_meta,
                commitment_snapshot=commitment_snapshot,
                truck_size=truck_size,
                truck_capacity_liters=load_summary.get('capacity'),
                planned_liters=load_summary.get('planned_liters'),
                load_percentage=load_summary.get('load_percentage'),
                auto_planned=planning_mode != Shipment.PlanningMode.MANUAL,
                planning_mode=planning_mode,
                vehicle_type=data.get('vehicle_type', truck_size),
                vehicle_number=data.get('vehicle_number', ''),
                driver_name=data.get('driver_name', ''),
                driver_phone=data.get('driver_phone', ''),
                dispatch_date_planned=data.get('dispatch_date_planned') or None,
                notes=data.get('notes', ''),
                status=Shipment.Status.DRAFT,
                created_by=request.user,
            )

            # Source-inventory tag for each saved line: prefer the value the
            # planner already computed (echoed by the client); else re-derive from
            # live pooled stock by ASIN. Fetched once (cached ~60s).
            _save_stock_detail = _bh_fgm_stock_detail()

            def _make_item(item_data, not_loaded=False):
                dte = item_data.get('days_to_expiry')
                try:
                    dte_int = int(dte) if dte is not None else None
                except (TypeError, ValueError):
                    dte_int = None
                # Parse expiry_date — accepts ISO date string or None
                exp_raw = item_data.get('expiry_date')
                expiry_date_val = None
                if exp_raw:
                    try:
                        from datetime import date, datetime
                        if isinstance(exp_raw, (date, datetime)):
                            expiry_date_val = exp_raw if isinstance(exp_raw, date) and not isinstance(exp_raw, datetime) else exp_raw.date()
                        else:
                            # Strip time component if present (e.g. "2026-05-26T00:00:00")
                            expiry_date_val = datetime.fromisoformat(str(exp_raw).split('T')[0]).date()
                    except (ValueError, TypeError):
                        expiry_date_val = None
                return ShipmentItem(
                    shipment=shipment,
                    # Fix #3: tag each line with its SOURCE appointment (not just the
                    # truck's primary) so per-appointment commit accounting is correct
                    # on combined trucks. Fillers/extras with no source fall to primary.
                    appointment_id=str(item_data.get('source_appointment_id') or appointment_id or ''),
                    po_number=item_data.get('po_number') or '',
                    asin=item_data.get('asin') or '',
                    internal_sku=item_data.get('internal_sku') or item_data.get('merchant_sku') or '',
                    product_name=item_data.get('product_name') or item_data.get('sku_name') or '',
                    destination_fc=item_data.get('destination_fc') or '',
                    category=item_data.get('category') or '',
                    sub_category=item_data.get('sub_category') or '',
                    brand=item_data.get('brand') or '',
                    item_head=item_data.get('item_head') or '',
                    item=item_data.get('item') or '',
                    source_warehouse=(
                        item_data.get('source_warehouse')
                        or (_save_stock_detail.get(str(item_data.get('asin') or '').strip().upper()) or {}).get('source_warehouse')
                        or ''
                    ),
                    availability_status=item_data.get('availability_status') or '',
                    po_status=item_data.get('po_status') or '',
                    status=item_data.get('status') or '',
                    accepted_qty=item_data.get('accepted_qty'),
                    available_qty=item_data.get('accepted_qty'),
                    planned_qty=item_data.get('planned_qty', 0) if not not_loaded else 0,
                    planned_liters=item_data.get('planned_liters', 0) if not not_loaded else 0,
                    per_liter=item_data.get('per_liter'),
                    case_pack=item_data.get('case_pack'),
                    doh=item_data.get('doh'),
                    drr_unit=item_data.get('drr_unit'),
                    soh_unit=item_data.get('soh_unit'),
                    days_to_expiry=dte_int,
                    expiry_date=expiry_date_val,
                    priority_bucket=item_data.get('priority_bucket') or '',
                    priority_score=item_data.get('priority_score'),
                    priority_reason=item_data.get('priority_reason') or '',
                    is_auto_selected=True,
                    not_loaded=not_loaded,
                    # Record/audit: why this line wasn't fully shipped (from the planner).
                    unfit_reason=item_data.get('unfit_reason') or '',
                    short_reason=item_data.get('short_reason') or '',
                )

            all_items = (
                [_make_item(i, False) for i in loaded_items]
                + [_make_item(i, True) for i in not_loaded_items]
            )
            ShipmentItem.objects.bulk_create(all_items)

        serializer = ShipmentSerializer(shipment)
        return Response(serializer.data, status=201)


class ShipmentDetailView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def _get_shipment(self, pk):
        try:
            return Shipment.objects.prefetch_related('items', 'audit_logs').get(pk=pk)
        except Shipment.DoesNotExist:
            return None

    def get(self, request, pk):
        shipment = self._get_shipment(pk)
        if not shipment:
            return Response({'error': 'Not found'}, status=404)
        return Response(ShipmentSerializer(shipment).data)

    def patch(self, request, pk):
        shipment = self._get_shipment(pk)
        if not shipment:
            return Response({'error': 'Not found'}, status=404)
        if shipment.status not in (Shipment.Status.DRAFT, Shipment.Status.REJECTED):
            return Response({'error': 'Only draft or rejected shipments can be edited'}, status=400)

        allowed = [
            'driver_name', 'driver_phone', 'vehicle_number', 'vehicle_type',
            'appointment_id', 'appointment_time', 'destination_fc', 'pro',
            'dispatch_date_planned', 'notes',
        ]
        update_fields = []
        for field in allowed:
            if field in request.data:
                value = request.data[field]
                # Normalize empty strings for nullable date/time fields
                if field in ('appointment_time', 'dispatch_date_planned') and value == '':
                    value = None
                setattr(shipment, field, value if value is not None else ('' if field not in ('appointment_time', 'dispatch_date_planned') else None))
                update_fields.append(field)
        if update_fields:
            shipment.save(update_fields=update_fields)
        return Response(ShipmentSerializer(shipment).data)

    def delete(self, request, pk):
        shipment = self._get_shipment(pk)
        if not shipment:
            return Response({'error': 'Not found'}, status=404)
        # DRAFT, PENDING_APPROVAL and REJECTED shipments can be deleted;
        # approved/dispatched/delivered are protected.
        deletable_statuses = {
            Shipment.Status.DRAFT,
            Shipment.Status.PENDING_APPROVAL,
            Shipment.Status.REJECTED,
        }
        if shipment.status not in deletable_statuses:
            return Response(
                {'error': f'Only draft, pending-approval or rejected shipments can be deleted. This shipment is "{shipment.get_status_display()}".'},
                status=400,
            )
        # Only the creator (or staff) can delete.
        if shipment.created_by_id and shipment.created_by_id != request.user.id and not request.user.is_staff:
            return Response({'error': 'Only the creator or staff can delete this shipment.'}, status=403)
        sid = shipment.id
        # Durable audit trail: the ShipmentAuditLog FK cascades on delete, so a
        # DB row would vanish with the shipment — log to the server instead.
        logger.info(
            'shipment delete: id=%s status=%s appointment=%s fc=%s by user_id=%s (%s)',
            sid, shipment.status, shipment.appointment_id, shipment.destination_fc,
            getattr(request.user, 'id', None), getattr(request.user, 'username', ''),
        )
        shipment.delete()  # cascades to items + audit_logs via FK
        return Response({'deleted': True, 'shipment_id': sid}, status=200)


class ShipmentItemUpdateView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def patch(self, request, pk, item_id):
        try:
            shipment = Shipment.objects.get(pk=pk)
            item = ShipmentItem.objects.get(pk=item_id, shipment=shipment)
        except (Shipment.DoesNotExist, ShipmentItem.DoesNotExist):
            return Response({'error': 'Not found'}, status=404)

        if shipment.status not in (Shipment.Status.DRAFT, Shipment.Status.REJECTED):
            return Response({'error': 'Shipment is approved. Changes require re-approval.'}, status=400)

        data = request.data
        reason = data.get('reason')
        if not reason:
            return Response({'error': 'reason is required'}, status=400)

        old_asin = item.asin
        old_sku = item.internal_sku
        old_qty = item.planned_qty

        if 'new_qty' in data:
            try:
                new_qty = float(data['new_qty'])
            except (TypeError, ValueError):
                return Response({'error': 'new_qty must be a number.'}, status=400)
            # Can't ship more than ordered (accepted); the difference is the
            # short-supply qty shown to planners. Cartons are not counted, so
            # the entered quantity ships as-is (clamped to the ordered qty).
            ordered = float(item.accepted_qty or 0)
            if ordered > 0:
                new_qty = min(new_qty, ordered)
            new_qty = max(new_qty, 0)
            item.planned_qty = new_qty
            item.planned_liters = round(new_qty * float(item.per_liter or 0), 4)

        if 'new_asin' in data:
            item.asin = data['new_asin']
        if 'new_sku' in data:
            item.internal_sku = data['new_sku']
        if 'remove' in data and data['remove']:
            item.not_loaded = True
            item.planned_qty = 0
            item.planned_liters = 0

        item.is_changed = True
        item.change_reason = reason
        item.save()

        _recalc_shipment_totals(shipment)

        ShipmentAuditLog.objects.create(
            shipment=shipment,
            changed_by=request.user.email,
            change_type=reason,
            old_asin=old_asin,
            new_asin=item.asin,
            old_sku=old_sku,
            new_sku=item.internal_sku,
            old_qty=old_qty,
            new_qty=item.planned_qty,
            reason=reason,
            reason_note=data.get('reason_note', ''),
        )

        return Response(ShipmentItemSerializer(item).data)


def _recalc_shipment_totals(shipment):
    items = shipment.items.filter(not_loaded=False)
    total_liters = sum(float(i.planned_liters or 0) for i in items)
    capacity = float(shipment.truck_capacity_liters or 15000)
    shipment.planned_liters = round(total_liters, 4)
    shipment.load_percentage = round((total_liters / capacity * 100) if capacity > 0 else 0, 2)
    shipment.save(update_fields=['planned_liters', 'load_percentage'])


# ---------------------------------------------------------------------------
# Shipment workflow actions
# ---------------------------------------------------------------------------

class ShipmentSubmitView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            shipment = Shipment.objects.prefetch_related('items').get(pk=pk)
        except Shipment.DoesNotExist:
            return Response({'error': 'Not found'}, status=404)

        if shipment.status != Shipment.Status.DRAFT:
            return Response({'error': 'Only draft shipments can be submitted'}, status=400)

        conflicts = _check_qty_conflicts(shipment)
        if conflicts:
            return Response({'error': 'Quantity conflicts detected', 'conflicts': conflicts}, status=409)

        # Mandatory PO documents: every loaded PO must have an uploaded PDF before
        # the shipment can go up for approval (the "Upload POs" wizard step).
        loaded_pos = {
            str(it.po_number).strip()
            for it in shipment.items.all()
            if not it.not_loaded and str(it.po_number or '').strip()
        }
        have = {
            str(p).strip()
            for p in ShipmentPoDocument.objects.filter(shipment=shipment).values_list('po_number', flat=True)
        }
        missing = sorted(loaded_pos - have)
        if missing:
            return Response(
                {'error': 'Upload a PO document (PDF) for every PO before submitting.',
                 'missing_pos': missing},
                status=400,
            )

        shipment.status = Shipment.Status.PENDING_APPROVAL
        # Record when it was put up for approval. auto_now fields are only written
        # when named in update_fields, so include updated_at explicitly.
        shipment.save(update_fields=['status', 'updated_at'])
        return Response(ShipmentListSerializer(shipment).data)


class ShipmentPoDocumentsView(_SafeAPIView):
    """List PO documents (metadata only) for a shipment, and upload/replace one
    PDF per PO. One document per (shipment, PO); re-upload overwrites it."""
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    MAX_BYTES = 10 * 1024 * 1024  # 10 MB

    def get(self, request, pk):
        docs = (
            ShipmentPoDocument.objects
            .filter(shipment_id=pk)
            .values('po_number', 'file_name', 'size', 'content_type', 'uploaded_at')
        )
        return Response(list(docs))

    def post(self, request, pk):
        try:
            shipment = Shipment.objects.get(pk=pk)
        except Shipment.DoesNotExist:
            return Response({'error': 'Not found'}, status=404)
        po_number = str(request.data.get('po_number') or '').strip()
        f = request.FILES.get('file')
        if not po_number or f is None:
            return Response({'error': 'po_number and file are required'}, status=400)
        if f.size > self.MAX_BYTES:
            return Response({'error': 'File exceeds the 10 MB limit'}, status=400)
        ct = (f.content_type or '').lower()
        if 'pdf' not in ct and not (f.name or '').lower().endswith('.pdf'):
            return Response({'error': 'Only PDF files are allowed'}, status=400)
        ShipmentPoDocument.objects.update_or_create(
            shipment=shipment, po_number=po_number,
            defaults={
                'file_name': (f.name or 'document.pdf')[:255],
                'content_type': f.content_type or 'application/pdf',
                'size': f.size,
                'data': f.read(),
                'uploaded_by': request.user if getattr(request.user, 'is_authenticated', False) else None,
            },
        )
        return Response({'po_number': po_number, 'file_name': f.name, 'size': f.size}, status=201)


class ShipmentPoDocumentFileView(_SafeAPIView):
    """Download (GET) or delete (DELETE) the PDF for one PO of a shipment."""
    permission_classes = [IsAuthenticated]

    def get(self, request, pk, po_number):
        try:
            doc = ShipmentPoDocument.objects.get(shipment_id=pk, po_number=str(po_number).strip())
        except ShipmentPoDocument.DoesNotExist:
            return Response({'error': 'Not found'}, status=404)
        resp = HttpResponse(bytes(doc.data), content_type=doc.content_type or 'application/pdf')
        resp['Content-Disposition'] = f'inline; filename="{doc.file_name}"'
        return resp

    def delete(self, request, pk, po_number):
        ShipmentPoDocument.objects.filter(shipment_id=pk, po_number=str(po_number).strip()).delete()
        return Response(status=204)


class ShipmentApproveView(_SafeAPIView):
    permission_classes = [IsAuthenticated, IsShipmentManager]

    def post(self, request, pk):
        try:
            shipment = Shipment.objects.get(pk=pk)
        except Shipment.DoesNotExist:
            return Response({'error': 'Not found'}, status=404)

        if shipment.status != Shipment.Status.PENDING_APPROVAL:
            return Response({'error': 'Shipment is not pending approval'}, status=400)

        conflicts = _check_qty_conflicts(shipment)
        if conflicts:
            return Response({'error': 'Quantity conflicts detected', 'conflicts': conflicts}, status=409)

        shipment.status = Shipment.Status.APPROVED
        shipment.approved_by = request.user
        shipment.save(update_fields=['status', 'approved_by'])
        return Response(ShipmentListSerializer(shipment).data)


class ShipmentRejectView(_SafeAPIView):
    permission_classes = [IsAuthenticated, IsShipmentManager]

    def post(self, request, pk):
        try:
            shipment = Shipment.objects.get(pk=pk)
        except Shipment.DoesNotExist:
            return Response({'error': 'Not found'}, status=404)

        if shipment.status != Shipment.Status.PENDING_APPROVAL:
            return Response({'error': 'Shipment is not pending approval'}, status=400)

        reason = request.data.get('reason', '')
        shipment.status = Shipment.Status.REJECTED
        shipment.rejection_reason = reason
        shipment.save(update_fields=['status', 'rejection_reason'])
        return Response(ShipmentListSerializer(shipment).data)


class ShipmentDispatchView(_SafeAPIView):
    permission_classes = [IsAuthenticated, IsShipmentManager]

    def post(self, request, pk):
        try:
            shipment = Shipment.objects.get(pk=pk)
        except Shipment.DoesNotExist:
            return Response({'error': 'Not found'}, status=404)

        if shipment.status != Shipment.Status.APPROVED:
            return Response({'error': 'Shipment must be approved before dispatch'}, status=400)

        shipment.status = Shipment.Status.DISPATCHED
        shipment.save(update_fields=['status'])
        return Response(ShipmentListSerializer(shipment).data)


def _check_qty_conflicts(shipment):
    conflicts = []
    loaded_items = list(shipment.items.filter(not_loaded=False))
    # Batched availability check (was 2 queries PER item → N+1 on submit/approve):
    # one grouped query for OTHER non-rejected shipments' committed qty per
    # (ASIN, PO, FC), and one for the PO's original accepted qty. FC is part of the
    # key so a commitment at one FC never reduces another FC's availability.
    committed = {}    # (asin, po_up, fc_up) -> {'qty': float, 'ids': [shipment ids]}
    po_accepted = {}  # (asin, po_up, fc_up) -> original accepted_qty
    po_uppers = list({(it.po_number or '').strip().upper()
                      for it in loaded_items if (it.po_number or '').strip()})
    if po_uppers:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT si.asin,
                       UPPER(TRIM(si.po_number)) AS po_up,
                       UPPER(TRIM(COALESCE(si.destination_fc, ''))) AS fc_up,
                       COALESCE(SUM(COALESCE(si.planned_qty, 0)), 0) AS qty,
                       ARRAY_AGG(DISTINCT s.id) AS ids
                FROM sp_items si
                JOIN sp_shipments s ON s.id = si.shipment_id
                WHERE UPPER(TRIM(si.po_number)) = ANY(%s)
                  AND s.status != 'rejected'
                  AND s.id != %s
                  AND si.not_loaded = FALSE
                GROUP BY si.asin, UPPER(TRIM(si.po_number)),
                         UPPER(TRIM(COALESCE(si.destination_fc, '')))
            """, [po_uppers, shipment.id])
            for asin, po_up, fc_up, qty, ids in cur.fetchall():
                committed[(asin, po_up, fc_up)] = {'qty': float(qty or 0), 'ids': list(ids or [])}
            cur.execute("""
                SELECT asin,
                       UPPER(TRIM(po_number)) AS po_up,
                       UPPER(TRIM(COALESCE(fulfillment_center, ''))) AS fc_up,
                       MAX(accepted_qty) AS accepted
                FROM reporting."Amazon PO"
                WHERE UPPER(TRIM(po_number)) = ANY(%s)
                GROUP BY asin, UPPER(TRIM(po_number)),
                         UPPER(TRIM(COALESCE(fulfillment_center, '')))
            """, [po_uppers])
            for asin, po_up, fc_up, accepted in cur.fetchall():
                if accepted is not None:
                    po_accepted[(asin, po_up, fc_up)] = float(accepted)

    for item in loaded_items:
        key = (item.asin or '',
               (item.po_number or '').strip().upper(),
               (item.destination_fc or '').strip().upper())
        c = committed.get(key) or {'qty': 0.0, 'ids': []}
        locked_qty = c['qty']
        planned = float(item.planned_qty or 0)
        if key in po_accepted:
            original = po_accepted[key]
            available = original - locked_qty
        else:
            # Source row not found — fall back to the item's stored orderable qty
            # (already net of prior commitments) and don't double-subtract locked.
            original = float(item.accepted_qty or 0)
            available = original

        if planned > available + 1e-6:
            conflicts.append({
                'asin': item.asin,
                'po_number': item.po_number,
                'accepted_qty': original,
                'locked_qty': locked_qty,
                'available_qty': available,
                'planned_qty': planned,
                'locked_shipment_ids': c['ids'],
            })

    # Live-stock re-check (fix #1 at submit / approve — the last gate before dispatch).
    # For each ASIN this shipment ships, the TOTAL reserved across ALL active shipments
    # (this one included) must not exceed live pooled on-hand. Catches drafts that each
    # fit their own PO but together over-commit the same physical stock. Skipped only
    # when live stock is unverifiable (SAP down), matching the Save-time gate.
    _stock = _bh_fgm_stock_detail()
    if _stock and loaded_items:
        _reserved = _reserved_stock_by_asin()   # every active shipment, incl this one
        _seen = set()
        for item in loaded_items:
            a = (item.asin or '').strip().upper()
            if not a or a in _seen:
                continue
            _seen.add(a)
            d = _stock.get(a)
            onhand = float(d['onhand']) if d else 0.0
            total_reserved = float(_reserved.get(a, 0.0))
            if total_reserved > onhand + 1e-6:
                conflicts.append({
                    'reason': 'stock_over_committed',
                    'asin': a,
                    'on_hand': round(onhand, 2),
                    'total_reserved': round(total_reserved, 2),
                    'over_by': round(total_reserved - onhand, 2),
                })

    # Appointment-commitment guard: the total committed across all active shipments
    # for this appointment must stay within the Vendor Central commit (+7%). Catches
    # over-commit that per-line (ASIN, PO, FC) checks miss (e.g. two shipments that
    # individually fit but together exceed the appointment), and qty edits on Review.
    aid = str(getattr(shipment, 'appointment_id', '') or '').strip()
    if aid:
        appt_cap = _lookup_appointment_commit(aid)
        if appt_cap:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(SUM(COALESCE(si.planned_qty, 0)), 0),
                           COALESCE(SUM(COALESCE(si.planned_qty, 0)
                                    / GREATEST(COALESCE(si.case_pack, 1), 1)), 0)
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE si.not_loaded = FALSE
                      AND s.status != 'rejected'
                      AND TRIM(s.appointment_id) = %s
                    """,
                    [aid],
                )
                row = cur.fetchone()
            tot_u = float(row[0] or 0)
            tot_c = float(row[1] or 0)
            cap_u = appt_cap['units'] * CAP_TOLERANCE if appt_cap['units'] > 0 else float('inf')
            cap_c = appt_cap['cartons'] * CAP_TOLERANCE if appt_cap['cartons'] > 0 else float('inf')
            if tot_u > cap_u + 1e-6 or tot_c > cap_c + 1e-6:
                conflicts.append({
                    'reason': 'appointment_commit_exceeded',
                    'appointment_id': aid,
                    'commit_units': appt_cap['units'],
                    'commit_cartons': appt_cap['cartons'],
                    'total_units': round(tot_u, 2),
                    'total_cartons': round(tot_c, 2),
                })
    return conflicts


class ShipmentStatsView(_SafeAPIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        from django.db.models import Count, Q
        stats = Shipment.objects.aggregate(
            total=Count('id'),
            draft=Count('id', filter=Q(status='draft')),
            pending_approval=Count('id', filter=Q(status='pending_approval')),
            approved=Count('id', filter=Q(status='approved')),
            dispatched=Count('id', filter=Q(status='dispatched')),
            in_transit=Count('id', filter=Q(status='in_transit')),
            delivered=Count('id', filter=Q(status='delivered')),
            rejected=Count('id', filter=Q(status='rejected')),
        )
        # Backwards-compat: keep `pending` alias for any older client.
        stats['pending'] = stats['pending_approval']
        return Response(stats)


class AsinCatalogView(_SafeAPIView):
    """Returns distinct ASIN → per_liter + DOH data for PO List calculations."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        with connection.cursor() as cur:
            # per_liter from Amazon PO table (latest non-null value per ASIN)
            cur.execute("""
                SELECT DISTINCT ON (asin) asin, per_liter, case_pack,
                    sku_name AS product_name, merchant_sku, category, sub_category, brand
                FROM reporting."Amazon PO"
                WHERE per_liter IS NOT NULL AND per_liter > 0
                ORDER BY asin, order_date DESC NULLS LAST
            """)
            po_rows = _row_to_dict(cur, cur.fetchall())

        # DOH/DRR/SOH — LIVE from amazon_master_inventory + amazon_sec_range_master_view
        # so the Manual PO planner matches the SOH/DOH dashboard exactly.
        doh_by_asin, _doh_meta = _live_doh_by_asin()

        catalog = {}
        for r in po_rows:
            entry = _serialize_row(r)
            asin_up = str(r.get('asin') or '').upper().strip()
            live = doh_by_asin.get(asin_up, {})
            entry['doh']      = live.get('doh')
            entry['drr_unit'] = live.get('drr_unit')
            entry['soh_unit'] = live.get('soh_unit')
            entry['soh_ltr']  = live.get('soh_ltr')
            entry['drr_ltr']  = live.get('drr_ltr')
            catalog[r['asin']] = entry

        return Response(catalog)


class POListView(_SafeAPIView):
    """Paginated list of POs from reporting."Amazon PO"."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        po_number = request.query_params.get('po_number', '').strip()
        po_status = request.query_params.get('po_status', '').strip()
        status = request.query_params.get('status', '').strip()
        fc = request.query_params.get('fc', '').strip()
        asin = request.query_params.get('asin', '').strip()
        no_paginate = request.query_params.get('no_paginate', '').lower() == 'true'
        page = _safe_int(request.query_params.get('page'), 1, lo=1)
        page_size = 9999 if no_paginate else _safe_int(request.query_params.get('page_size'), 50, lo=1, hi=5000)
        offset = 0 if no_paginate else (page - 1) * page_size

        where = ["1=1"]
        params = []
        if po_number:
            where.append("LOWER(ap.po_number) LIKE LOWER(%s)")
            params.append(f'%{po_number}%')
        if po_status:
            where.append("LOWER(ap.po_status) LIKE LOWER(%s)")
            params.append(f'%{po_status}%')
        if status:
            where.append("LOWER(ap.status) LIKE LOWER(%s)")
            params.append(f'%{status}%')
        if fc:
            where.append("LOWER(ap.fulfillment_center) LIKE LOWER(%s)")
            params.append(f'%{fc}%')
        if asin:
            where.append("LOWER(ap.asin) LIKE LOWER(%s)")
            params.append(f'%{asin}%')

        where_sql = ' AND '.join(where)

        with connection.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM reporting."Amazon PO" ap WHERE {where_sql}
            """, params)
            total = cur.fetchone()[0]

            cur.execute(f"""
                WITH po_appt AS (
                    -- A PO's EFFECTIVE FC is the FC of the appointment it's booked on.
                    -- A "swapped/flipped" PO (its Amazon-sheet FC differs from the FC of
                    -- the appointment it sits on) physically ships to the appointment FC,
                    -- so it should be treated as belonging to that FC everywhere — incl.
                    -- appearing on every appointment at that FC.
                    SELECT UPPER(TRIM(pv)) AS po_up,
                           MAX(UPPER(TRIM(a.destination_fc))) AS appt_fc
                    FROM reporting."appointment" a,
                         LATERAL unnest(regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')) AS pv
                    WHERE a.status = 'Confirmed' AND NULLIF(TRIM(pv), '') IS NOT NULL
                    GROUP BY UPPER(TRIM(pv))
                )
                SELECT
                    ap.po_number, ap.asin, ap.merchant_sku, ap.sku_code, ap.sap_sku_code,
                    ap.sku_name        AS product_name,
                    ap.accepted_qty, ap.cancelled_qty, ap.requested_qty, ap.received_qty,
                    ap.fulfillment_center AS destination_fc,
                    pa.appt_fc            AS appt_fc,
                    ap.availability_status,
                    ap.status, ap.po_status, ap.item_status,
                    ap.case_pack, ap.per_liter,
                    ap.total_accepted_liters, ap.total_order_liters, ap.days_to_expiry,
                    ap.expiry_date, ap.category, ap.sub_category, ap.brand,
                    ap.item_head, ap.item, ap.order_date,
                    ap.fill_rate, ap.total_accepted_cost,
                    COALESCE(NULLIF(ap.city,''), fcm.city)   AS city,
                    COALESCE(NULLIF(ap.state,''), fcm.state) AS state
                FROM reporting."Amazon PO" ap
                LEFT JOIN po_appt pa
                    ON pa.po_up = UPPER(TRIM(ap.po_number))
                LEFT JOIN public.fc_city_state_channel_master fcm
                    ON UPPER(TRIM(fcm.fc::text)) = UPPER(TRIM(ap.fulfillment_center::text))
                WHERE {where_sql}
                ORDER BY ap.order_date DESC NULLS LAST, ap.po_number
                LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            rows = _row_to_dict(cur, cur.fetchall())

        # Tag each PO line with live BH-FGM stock (informational here — no cap):
        # on-hand, reserved by active shipments, available (on-hand − reserved),
        # and inbound on-order.
        stock_detail = _bh_fgm_stock_detail()
        reserved = _reserved_stock_by_asin()
        # Live DRR / SOH / DOH per ASIN — the SAME snapshot the auto planner and
        # the SOH/DOH dashboard use. Without this the Manual PO picker had no
        # demand data, so the manual planner's _compute_priority saw DRR=0 and
        # bucketed EVERY line as HOLD. Now manual priority matches auto exactly.
        # Use the TTL-cached wrapper (DOH changes only daily) so the 60s picker
        # poll doesn't re-run the heavy rolling-window aggregate every time.
        doh_by_asin, _ = _live_doh_by_asin()
        for r in rows:
            a = str(r.get('asin') or '').strip().upper()
            d = stock_detail.get(a)
            if d:
                r['sap_stock'] = d['onhand']
                r['sap_on_order'] = d['onorder']
                r['sap_reserved'] = reserved.get(a, 0.0)
                r['sap_available'] = max(0.0, d['onhand'] - reserved.get(a, 0.0))
                r['source_warehouse'] = d['source_warehouse']
                r['source_inventory'] = _inventory_label(d['source_warehouse'])
            else:
                r['sap_stock'] = r['sap_on_order'] = r['sap_reserved'] = r['sap_available'] = None
                r['source_warehouse'] = r['source_inventory'] = None
            live = doh_by_asin.get(a, {}) if doh_by_asin else {}
            r['soh_unit'] = live.get('soh_unit', 0) or 0
            r['soh_ltr']  = live.get('soh_ltr',  0) or 0
            r['drr_unit'] = live.get('drr_unit', 0) or 0
            r['drr_ltr']  = live.get('drr_ltr',  0) or 0
            r['doh']      = live.get('doh',      0) or 0
            # Compute the priority bucket/score/reason here (identical to the auto
            # planner's pool) so the Manual PO picker shows the right badges and
            # sorts by priority_score, instead of treating every row as HOLD.
            bucket, score, reason = _compute_priority(
                r['drr_unit'], r['soh_unit'], r['doh'],
                r.get('days_to_expiry'), r.get('po_status'),
            )
            r['priority_bucket'] = bucket
            r['priority_score']  = score
            r['priority_reason'] = reason

        return Response({
            'results': [_serialize_row(r) for r in rows],
            'count': total,
            'page': page,
            'page_size': page_size,
            'total_pages': math.ceil(total / page_size) if page_size else 1,
        })


class AllAppointmentsView(_SafeAPIView):
    """All appointments from reporting.appointment with filters."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        status = request.query_params.get('status', '').strip()
        fc = request.query_params.get('fc', '').strip()
        appt_id = request.query_params.get('appointment_id', '').strip()
        date_from = request.query_params.get('date_from', '').strip()
        date_to = request.query_params.get('date_to', '').strip()
        no_paginate = request.query_params.get('no_paginate', '').lower() == 'true'
        page = _safe_int(request.query_params.get('page'), 1, lo=1)
        page_size = 9999 if no_paginate else _safe_int(request.query_params.get('page_size'), 50, lo=1, hi=100)
        offset = 0 if no_paginate else (page - 1) * page_size

        # Qualify with the `a` alias so the appointment_commit LEFT JOIN (which
        # also has appointment_id / destination_fc) stays unambiguous.
        where = ["a.appointment_time IS NOT NULL"]
        params = []
        if status:
            where.append("LOWER(a.status) LIKE LOWER(%s)")
            params.append(f'%{status}%')
        if fc:
            where.append("LOWER(a.destination_fc) LIKE LOWER(%s)")
            params.append(f'%{fc}%')
        if appt_id:
            where.append("LOWER(a.appointment_id) LIKE LOWER(%s)")
            params.append(f'%{appt_id}%')
        if date_from:
            where.append("DATE(a.appointment_time) >= %s")
            params.append(date_from)
        if date_to:
            where.append("DATE(a.appointment_time) <= %s")
            params.append(date_to)

        where_sql = ' AND '.join(where)

        with connection.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT a.appointment_id
                    FROM reporting."appointment" a
                    WHERE {where_sql}
                ) _distinct
            """, params)
            total = cur.fetchone()[0]

            # The ingest stores one row per (appointment_id, PO). Aggregate
            # back to one row per appointment_id by stitching the POs with
            # STRING_AGG. LEFT JOIN appointment_commit to surface the Amazon
            # carton/unit counts on the same combined page.
            cur.execute(f"""
                SELECT a.appointment_id,
                       MAX(a.status)            AS status,
                       MAX(a.appointment_time)  AS appointment_time,
                       MAX(a.creation_date)     AS creation_date,
                       MAX(a.destination_fc)    AS destination_fc,
                       MAX(a.pro)               AS pro,
                       STRING_AGG(
                           DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),''),
                           ', '
                           ORDER BY NULLIF(TRIM(COALESCE(a.pos,'')),'')
                       ) AS pos,
                       COUNT(DISTINCT NULLIF(TRIM(COALESCE(a.pos,'')),'')) AS po_count,
                       MAX(acm.carton_count)    AS amazon_carton_count,
                       MAX(acm.unit_count)      AS amazon_unit_count,
                       -- Estimated carton count from this appointment's PO line
                       -- items: sum of (accepted_qty / case_pack) per SKU. Used
                       -- only when Amazon VC has no carton count for the appt.
                       (
                           SELECT ROUND(SUM(p.accepted_qty::numeric / GREATEST(p.case_pack, 1)))
                           FROM reporting."Amazon PO" p
                           WHERE UPPER(TRIM(p.po_number)) IN (
                               SELECT UPPER(TRIM(NULLIF(a2.pos, '')))
                               FROM reporting."appointment" a2
                               WHERE a2.appointment_id = a.appointment_id
                           )
                       ) AS calc_carton_count
                FROM reporting."appointment" a
                LEFT JOIN public.appointment_commit acm
                       ON acm.appointment_id = a.appointment_id
                WHERE {where_sql}
                GROUP BY a.appointment_id
                ORDER BY MAX(a.appointment_time) DESC NULLS LAST
                LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            rows = _row_to_dict(cur, cur.fetchall())

            cur.execute("""
                SELECT updated_at, updated_by
                FROM public.appointment_commit
                WHERE updated_at IS NOT NULL
                ORDER BY updated_at DESC LIMIT 1
            """)
            lr = cur.fetchone()
        last_update = (
            {'at': lr[0].isoformat() if lr[0] else None, 'by': lr[1]} if lr else None
        )

        # Carton count: when Amazon VC has no carton count for an appointment,
        # estimate it from the appointment's PO line items
        # (sum of accepted_qty / case_pack). Units are never calculated. Flagged
        # with carton_is_calc so the UI can mark it as an estimate.
        for r in rows:
            cc = r.get('amazon_carton_count')
            calc_raw = r.pop('calc_carton_count', None)
            calc = None
            if cc is None and calc_raw is not None:
                try:
                    calc = int(round(float(calc_raw)))
                    if calc <= 0:
                        calc = None
                except (TypeError, ValueError):
                    calc = None
            r['amazon_carton_count_calc'] = calc
            r['carton_is_calc'] = calc is not None

        return Response({
            'results': [_serialize_row(r) for r in rows],
            'count': total,
            'page': page,
            'page_size': page_size,
            'total_pages': math.ceil(total / page_size) if page_size else 1,
            'last_update': last_update,
        })


class AppointmentCommitImportView(_SafeAPIView):
    """Unattended importer for Amazon Vendor Central carton/unit commitments.

    Authenticated by a shared-secret header (``X-Import-Key``) instead of a user
    JWT, so the Tampermonkey auto-run script can POST from vendorcentral.in
    without the app login. Scoped to ONLY upsert public.appointment_commit — it
    cannot touch any other table (unlike the generic /api/upload/batch).

    No CORS change is needed: the userscript uses GM_xmlhttpRequest, which is
    not subject to the browser's same-origin policy.

    Body: { "rows": [ {appointment_id, destination_fc, carton_count, unit_count}, … ] }
    """
    authentication_classes = []          # no session auth → no CSRF; key check below
    permission_classes = [AllowAny]

    @staticmethod
    def _pos_int(value):
        try:
            n = int(round(float(value)))
        except (TypeError, ValueError):
            return None
        return n if n > 0 else None

    def post(self, request):
        expected = (getattr(settings, "APPOINTMENT_COMMIT_IMPORT_KEY", "") or "").strip()
        if not expected:
            return Response({"detail": "Import endpoint is disabled (no key configured)."}, status=503)
        provided = (request.headers.get("X-Import-Key") or "").strip()
        if not provided or not hmac.compare_digest(provided, expected):
            return Response({"detail": "Invalid or missing import key."}, status=401)

        payload = request.data or {}
        rows_in = payload.get("rows") if isinstance(payload, dict) else payload
        if not isinstance(rows_in, list):
            rows_in = []

        cleaned = []
        for r in rows_in:
            if not isinstance(r, dict):
                continue
            aid = str(r.get("appointment_id") or "").strip()
            if not aid:
                continue
            fc = str(r.get("destination_fc") or "").strip() or None
            carton = self._pos_int(r.get("carton_count"))
            unit = self._pos_int(r.get("unit_count"))
            if carton is None and unit is None:
                continue
            cleaned.append((aid, fc, carton, unit))

        if not cleaned:
            return Response({"imported": 0, "updated": 0, "received": len(rows_in), "detail": "No usable rows."})

        created = 0
        updated = 0
        with connection.cursor() as cur:
            for aid, fc, carton, unit in cleaned:
                # COALESCE keeps any existing value when a re-import omits a
                # field, so partial scrapes never wipe good data.
                cur.execute(
                    """
                    INSERT INTO public.appointment_commit
                        (appointment_id, destination_fc, carton_count, unit_count, source, updated_at)
                    VALUES (%s, %s, %s, %s, 'amazon', now())
                    ON CONFLICT (appointment_id) DO UPDATE SET
                        destination_fc = COALESCE(EXCLUDED.destination_fc, public.appointment_commit.destination_fc),
                        carton_count   = COALESCE(EXCLUDED.carton_count, public.appointment_commit.carton_count),
                        unit_count     = COALESCE(EXCLUDED.unit_count, public.appointment_commit.unit_count),
                        source         = 'amazon',
                        updated_at     = now()
                    RETURNING (xmax::text = '0') AS inserted
                    """,
                    [aid, fc, carton, unit],
                )
                row = cur.fetchone()
                if row and row[0]:
                    created += 1
                else:
                    updated += 1

        return Response({
            "imported": created,
            "updated": updated,
            "stored": created + updated,
            "received": len(rows_in),
        })


class AppointmentCommitListView(_SafeAPIView):
    """Read-only list of Amazon Vendor Central carton/unit commitments
    (the public.appointment_commit table) for the standalone
    'Cartons/Unit Count VC' page. Deliberately kept separate from the
    appointment list — no join — per product requirement.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        with connection.cursor() as cur:
            cur.execute("""
                SELECT appointment_id,
                       destination_fc,
                       carton_count,
                       unit_count,
                       source,
                       updated_at,
                       updated_by
                FROM public.appointment_commit
                ORDER BY updated_at DESC NULLS LAST, appointment_id
            """)
            rows = _row_to_dict(cur, cur.fetchall())
            cur.execute("""
                SELECT updated_at, updated_by
                FROM public.appointment_commit
                WHERE updated_at IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            lr = cur.fetchone()
        last_update = (
            {'at': lr[0].isoformat() if lr[0] else None, 'by': lr[1]}
            if lr else None
        )
        return Response({
            'results': [_serialize_row(r) for r in rows],
            'count': len(rows),
            'last_update': last_update,
        })


class AppointmentCommitManualImportView(_SafeAPIView):
    """Logged-in (paste-flow) importer for Vendor Central carton/unit data.

    Upserts public.appointment_commit AND stamps updated_at + updated_by with
    the current user, so the VC page can show "Last updated <when> by <who>"
    and warn on same-day re-runs (Amazon ToS exposure). Distinct from the
    key-authed AppointmentCommitImportView used by the unattended script.
    """
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _pos_int(value):
        try:
            n = int(round(float(value)))
        except (TypeError, ValueError):
            return None
        return n if n > 0 else None

    def post(self, request):
        u = request.user
        who = (
            (getattr(u, 'get_full_name', lambda: '')() or '').strip()
            or getattr(u, 'email', '') or getattr(u, 'username', '') or str(u)
        ).strip() or 'unknown'

        payload = request.data or {}
        rows_in = payload.get('rows') if isinstance(payload, dict) else payload
        if not isinstance(rows_in, list):
            rows_in = []

        cleaned = []
        for r in rows_in:
            if not isinstance(r, dict):
                continue
            aid = str(r.get('appointment_id') or '').strip()
            if not aid:
                continue
            fc = str(r.get('destination_fc') or '').strip() or None
            carton = self._pos_int(r.get('carton_count'))
            unit = self._pos_int(r.get('unit_count'))
            if carton is None and unit is None:
                continue
            cleaned.append((aid, fc, carton, unit, who))

        created = 0
        updated = 0
        lr = None
        if cleaned:
            with connection.cursor() as cur:
                for aid, fc, carton, unit, who in cleaned:
                    cur.execute(
                        """
                        INSERT INTO public.appointment_commit
                            (appointment_id, destination_fc, carton_count, unit_count, source, updated_at, updated_by)
                        VALUES (%s, %s, %s, %s, 'amazon', now(), %s)
                        ON CONFLICT (appointment_id) DO UPDATE SET
                            destination_fc = COALESCE(EXCLUDED.destination_fc, public.appointment_commit.destination_fc),
                            carton_count   = COALESCE(EXCLUDED.carton_count, public.appointment_commit.carton_count),
                            unit_count     = COALESCE(EXCLUDED.unit_count, public.appointment_commit.unit_count),
                            source         = 'amazon',
                            updated_at     = now(),
                            updated_by     = EXCLUDED.updated_by
                        RETURNING (xmax::text = '0') AS inserted
                        """,
                        [aid, fc, carton, unit, who],
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        created += 1
                    else:
                        updated += 1
                cur.execute("""
                    SELECT updated_at, updated_by FROM public.appointment_commit
                    WHERE updated_at IS NOT NULL ORDER BY updated_at DESC LIMIT 1
                """)
                lr = cur.fetchone()

        last_update = (
            {'at': lr[0].isoformat() if lr[0] else None, 'by': lr[1]} if lr else None
        )
        return Response({
            'imported': created,
            'updated': updated,
            'stored': created + updated,
            'received': len(rows_in),
            'last_update': last_update,
        })


class SetFcChannelView(_SafeAPIView):
    """Manually map a fulfillment center to a sales channel (one channel per FC).

    Persisted in public.fc_city_state_channel_master so an unmapped ('Other')
    FC only needs to be assigned once — every current and future appointment at
    that FC then inherits the channel automatically (no re-asking).
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        body = request.data or {}
        fc = str(body.get('fc') or '').strip()
        channel = str(body.get('channel') or '').strip().upper()
        if not fc:
            return Response({'detail': 'fc is required.'}, status=400)

        with connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT UPPER(TRIM(channel))
                FROM public.fc_city_state_channel_master
                WHERE channel IS NOT NULL AND TRIM(channel) <> ''
            """)
            allowed = {r[0] for r in cur.fetchall()}
            if channel not in allowed:
                return Response(
                    {'detail': f'Unknown channel "{channel}". Allowed: {sorted(allowed)}'},
                    status=400,
                )
            # One row per FC: update if it exists, else insert.
            cur.execute(
                "UPDATE public.fc_city_state_channel_master SET channel = %s WHERE UPPER(TRIM(fc)) = UPPER(TRIM(%s))",
                [channel, fc],
            )
            if cur.rowcount == 0:
                cur.execute(
                    "INSERT INTO public.fc_city_state_channel_master (fc, channel) VALUES (%s, %s)",
                    [fc, channel],
                )
        return Response({'ok': True, 'fc': fc.upper(), 'channel': channel})


class ManualPlanView(_SafeAPIView):
    """Preview a plan from manually selected PO items (no DB writes — Save as Draft persists it)."""
    permission_classes = [IsAuthenticated]

    def post(self, request):
        selected_items = request.data.get('items', [])
        truck_size = request.data.get('truck_size', '15_ton')
        capacity_override = request.data.get('truck_capacity_liters')
        # Appointment-driven manual: the appointment's Vendor Central commit is
        # enforced from the DB (not from client-sent caps) — see below.
        appointment_id = str(request.data.get('appointment_id') or '').strip()
        # Respect live BH-FGM stock by default — same constraint as the auto /
        # appointment planner, so a manual plan can't ship more than is physically
        # available. Toggleable via respect_stock (default on).
        respect_stock = str(request.data.get('respect_stock', True)).lower() not in ('0', 'false', 'no', 'off')
        # DOH filler (from the Plan Review "DOH filler" button) — top up leftover truck
        # capacity with same-FC PENDING in-stock POs NOT in this selection, ranked by
        # DOH urgency. Same engine as the auto planner. Off by default.
        doh_fill = str(request.data.get('doh_fill', False)).lower() in ('1', 'true', 'yes', 'on')

        # Vendor Central commit caps per PO (manual planner). Same shape as the
        # auto endpoint, just keyed by PO number instead of appointment_id.
        commit_caps = {}
        raw_caps = request.data.get('commit_caps_per_po') or {}
        if isinstance(raw_caps, dict):
            for k, v in raw_caps.items():
                if not isinstance(v, dict):
                    continue
                try:
                    units = int(v.get('units') or 0)
                    cartons = int(v.get('cartons') or 0)
                except (TypeError, ValueError):
                    continue
                if units > 0 or cartons > 0:
                    commit_caps[str(k)] = {'units': units, 'cartons': cartons}

        if not selected_items:
            return Response({'error': 'No items selected'}, status=400)

        # No-per-litre items are NOT stripped out here. They flow through the same
        # packer as the auto planner (_pack_into_capacity), which ships zero-volume
        # OTHER-bucket items at full qty and sets the rest aside as not-loaded with a
        # clear "No per-liter data…" reason AND a proper computed priority badge.
        # This makes manual handle missing per-litre identically to auto.

        # A single truck ships from ONE fulfillment center. Reject mixed-FC payloads
        # (the auto planner enforces this too) so a direct API call can't bypass it.
        fcs = {str(it.get('destination_fc') or '').strip().upper()
               for it in selected_items if it.get('destination_fc')}
        if len(fcs) > 1:
            return Response(
                {'error': f'Items span multiple fulfillment centers ({", ".join(sorted(fcs))}); a truck must be a single FC.'},
                status=400,
            )

        for item in selected_items:
            bucket, score, reason = _compute_priority(
                item.get('drr_unit', 0), item.get('soh_unit', 0),
                item.get('doh', 0), item.get('days_to_expiry'),
                item.get('po_status', ''),
            )
            item['priority_bucket'] = bucket
            item['priority_score'] = score
            item['priority_reason'] = reason

        selected_items.sort(key=lambda x: (
            -x.get('priority_score', 0),
            x.get('days_to_expiry') or 999,
            -(float(x.get('accepted_qty') or 0)),
        ))

        # Live warehouse stock cap — identical to the appointment / auto planner.
        # Tags each item with on-hand / reserved / available / incoming and, when
        # respect_stock, caps the shippable qty to what's AVAILABLE (on-hand −
        # reserved by other active shipments). Out-of-stock items drop to
        # not_loaded; partials are short-supplied — exactly the same rules as auto.
        stock_detail = _bh_fgm_stock_detail()
        reserved = _reserved_stock_by_asin()
        avail_total = {a: max(0.0, d['onhand'] - reserved.get(a, 0.0)) for a, d in stock_detail.items()}
        avail_remaining = dict(avail_total)
        _apply_stock_caps(selected_items, avail_total, avail_remaining, respect_stock, stock_detail, reserved)

        loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
            selected_items, truck_size, capacity_override
        )

        # DOH filler — top up leftover truck capacity with same-FC PENDING in-stock
        # POs not already in this selection, ranked by DOH (the SAME engine the auto
        # planner uses). Runs BEFORE the commit caps so the fillers are trimmed to the
        # appointment commit too, and shares avail_remaining so it never re-allocates
        # stock already claimed by the selected items.
        if doh_fill and capacity > planned_liters + 0.001 and fcs:
            fc = next(iter(fcs))
            selected_po_uppers = sorted({
                str(it.get('po_number') or '').strip().upper()
                for it in selected_items if it.get('po_number')
            })
            doh_by_asin, _ = _live_doh_by_asin()
            doh_pool = _fetch_doh_filler_pool(fc, selected_po_uppers, doh_by_asin)
            _apply_stock_caps(doh_pool, avail_total, avail_remaining, respect_stock, stock_detail, reserved)
            if doh_pool:
                loaded, _doh_unfit = _filler_pass(
                    loaded, doh_pool, capacity,
                    primary_fc=fc,
                    mark_key='_doh_filler',
                    reason='DOH filler · same-FC PENDING POs not in this selection, ranked by DOH urgency.',
                )
                planned_liters = round(sum(float(it.get('planned_liters') or 0) for it in loaded), 4)
                load_pct = round((planned_liters / capacity * 100) if capacity > 0 else 0, 2)

        if commit_caps:
            loaded, not_loaded = _enforce_commit_caps(
                loaded, not_loaded, commit_caps, key_field='po_number',
            )
            planned_liters = round(sum(float(it.get('planned_liters') or 0) for it in loaded), 4)
            load_pct = round((planned_liters / capacity * 100) if capacity > 0 else 0, 2)

        # Authoritative appointment-commitment cap (units + cartons), read live from
        # the DB regardless of any client-sent caps. Trims the plan to the Vendor
        # Central commit (+7%) so even a direct API call stays within commitment.
        appt_cap = _lookup_appointment_commit(appointment_id) if appointment_id else None
        if appt_cap:
            for it in loaded:
                it['appointment_id'] = appointment_id  # tag so the cap groups them
            loaded, not_loaded = _enforce_commit_caps(
                loaded, not_loaded, {appointment_id: appt_cap}, key_field='appointment_id',
            )
            planned_liters = round(sum(float(it.get('planned_liters') or 0) for it in loaded), 4)
            load_pct = round((planned_liters / capacity * 100) if capacity > 0 else 0, 2)

        return Response({
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
            'priority_actual': priority_actual,
            'commit_caps': commit_caps,
            'appointment_commit': appt_cap,
            'respect_stock': respect_stock,
            'stock_snapshot': _stock_meta_payload(stock_detail),
            'load_summary': {
                'truck_size': truck_size,
                'capacity': capacity,
                'planned_liters': planned_liters,
                'load_percentage': load_pct,
            },
        })


def _doh_bucket(doh, drr):
    """4-bucket priority based purely on live DOH (matches SOH/DOH page color thresholds)."""
    drr = float(drr or 0)
    if drr <= 0:
        return 'NO DEMAND', 'No demand (DRR = 0)'
    d = float(doh if doh is not None else 0)
    if d < 7:
        return 'CRITICAL', f'DOH {d:.1f} — restock urgently'
    if d < 14:
        return 'HIGH',     f'DOH {d:.1f} — low cover'
    if d < 30:
        return 'MEDIUM',   f'DOH {d:.1f} — comfortable'
    return 'LOW', f'DOH {d:.1f} — well stocked'


class DOHAutoFillView(_SafeAPIView):
    """
    Auto-fill a truck using LIVE DOH from amazon_master_inventory + amazon_sec_range_master_view
    (same source as the SOH/DOH dashboard so numbers match exactly).

    FC filter scopes both PO availability AND inventory (if amazon_master_inventory has an fc column);
    otherwise DOH is platform-wide and only POs are FC-filtered.

    Response also includes 'urgent_no_po' — ASINs where DOH < 14 but no eligible PO exists.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        truck_size = request.query_params.get('truck_size', '15_ton')
        capacity_override = request.query_params.get('truck_capacity_liters')
        fc = request.query_params.get('fc', '').strip()

        # Optional priority allocation (PREMIUM/COMMODITY/OTHER pct, summing to 100)
        priority = None
        try:
            p_premium = float(request.query_params.get('priority_premium_pct') or -1)
            p_commodity = float(request.query_params.get('priority_commodity_pct') or -1)
            p_other = float(request.query_params.get('priority_other_pct') or -1)
            if p_premium >= 0 and p_commodity >= 0 and p_other >= 0:
                total_pct = p_premium + p_commodity + p_other
                if abs(total_pct - 100) <= 0.5:
                    priority = {
                        'PREMIUM': p_premium,
                        'COMMODITY': p_commodity,
                        'OTHER': p_other,
                    }
        except (TypeError, ValueError):
            priority = None

        # Strict-adherence toggle (default best-effort: leftover capacity fills
        # from other buckets after the per-bucket pack).
        strict_param = str(request.query_params.get('priority_strict') or '').lower()
        priority_strict = strict_param in ('1', 'true', 'yes', 'on')

        # Maximize-fill toggle: top up the truck with NO-DEMAND / leftover
        # items at the chosen FC after the priority-driven pack. Default ON.
        fill_param = str(request.query_params.get('maximize_fill') or '1').lower()
        maximize_fill = fill_param in ('1', 'true', 'yes', 'on')

        # 1) Resolve the effective inventory snapshot date (latest available)
        with connection.cursor() as cur:
            cur.execute("""
                SELECT MAX(inventory_date) FROM amazon_master_inventory
            """)
            effective_date = cur.fetchone()[0]

        doh_meta = _doh_snapshot_meta(effective_date)
        if not effective_date:
            return Response({
                'loaded_items': [],
                'not_loaded_items': [],
                'urgent_no_po': [],
                'load_summary': {'truck_size': truck_size, 'capacity': _resolve_capacity(truck_size, capacity_override), 'planned_liters': 0, 'load_percentage': 0},
                'priority_breakdown': {},
                'priority_strict': priority_strict,
                'doh_snapshot': doh_meta,
                'fc_used': None,
                'fc_options': [],
                'stats': {'total_candidates': 0, 'loaded_count': 0, 'not_loaded_count': 0, 'urgent_no_po_count': 0},
                'source': {'sales': 'amazon_sec_range_master_view', 'inventory': 'amazon_master_inventory'},
                'message': 'No inventory snapshots found in amazon_master_inventory.',
            })

        # Heavy rolling-window DOH aggregate (WITH inventory attributes) — TTL-cached
        # by snapshot date so repeated Auto-Fill runs reuse it (DOH changes daily).
        doh_by_asin = _doh_autofill_by_asin(effective_date)

        # 3) Fetch available POs (FC-scoped if fc provided)
        po_where = [
            "p.status = 'Confirmed'",
            "p.availability_status = 'AC - Accepted: In stock'",
            "p.accepted_qty > 0",
            "p.po_status = 'PENDING'",
            "p.per_liter IS NOT NULL",
            "p.per_liter > 0",
        ]
        po_params = []
        if fc:
            po_where.append("LOWER(p.fulfillment_center) LIKE LOWER(%s)")
            po_params.append(f'%{fc}%')
        po_where_sql = ' AND '.join(po_where)

        with connection.cursor() as cur:
            cur.execute(f"""
                WITH committed AS (
                    -- Quantity already committed per (ASIN, PO, FC); the leftover
                    -- (accepted - committed) stays shippable. FC is in the key so
                    -- a commitment at one FC never reduces another FC's availability.
                    SELECT si.asin,
                           UPPER(TRIM(si.po_number)) AS po_number,
                           UPPER(TRIM(COALESCE(si.destination_fc, ''))) AS fc_key,
                           SUM(COALESCE(si.planned_qty, 0)) AS committed_qty
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE si.not_loaded = FALSE
                      AND s.status != 'rejected'
                    GROUP BY si.asin,
                             UPPER(TRIM(si.po_number)),
                             UPPER(TRIM(COALESCE(si.destination_fc, '')))
                )
                SELECT
                    p.po_number, p.asin,
                    p.merchant_sku       AS internal_sku,
                    p.sku_name           AS product_name,
                    (p.accepted_qty - COALESCE(c.committed_qty, 0)) AS accepted_qty,
                    p.accepted_qty       AS original_accepted_qty,
                    COALESCE(c.committed_qty, 0) AS committed_qty,
                    p.case_pack, p.per_liter,
                    round((p.accepted_qty - COALESCE(c.committed_qty, 0)) * COALESCE(p.per_liter, 0), 4) AS total_accepted_liters,
                    p.days_to_expiry, p.expiry_date,
                    p.fulfillment_center AS destination_fc,
                    p.category, p.sub_category, p.brand,
                    p.item_head, p.item,
                    p.availability_status, p.po_status, p.status
                FROM reporting."Amazon PO" p
                LEFT JOIN committed c
                    ON c.asin = p.asin
                    AND c.po_number = UPPER(TRIM(p.po_number))
                    AND c.fc_key = UPPER(TRIM(COALESCE(p.fulfillment_center, '')))
                WHERE {po_where_sql} AND (p.accepted_qty - COALESCE(c.committed_qty, 0)) > 0
            """, po_params)
            po_raw = _row_to_dict(cur, cur.fetchall())

        # 4) Merge live DOH into each PO item, compute bucket
        items = []
        asins_with_po = set()
        for r in po_raw:
            row = _serialize_row(r)
            asin_up = str(row.get('asin') or '').upper()
            asins_with_po.add(asin_up)
            live = doh_by_asin.get(asin_up, {})
            row.update({
                'soh_unit': live.get('soh_unit', 0),
                'soh_ltr':  live.get('soh_ltr',  0),
                'drr_unit': live.get('drr_unit', 0),
                'drr_ltr':  live.get('drr_ltr',  0),
                'doh':      live.get('doh',      0),
                'units_sold': live.get('units_sold', 0),
                'ltr_sold':   live.get('ltr_sold',   0),
            })
            bucket, reason = _doh_bucket(row['doh'], row['drr_unit'])
            row['priority_bucket'] = bucket
            row['priority_reason'] = reason
            items.append(row)

        # 5) Sort: NO-DEMAND items skipped; rest by DOH ASC (most urgent first), FEFO tiebreaker
        actionable = [it for it in items if it['priority_bucket'] != 'NO DEMAND']
        no_demand = [it for it in items if it['priority_bucket'] == 'NO DEMAND']
        actionable.sort(key=lambda x: (
            float(x.get('doh') if x.get('doh') is not None else 9999),
            x.get('days_to_expiry') or 999,
            -(float(x.get('accepted_qty') or 0)),
        ))

        # Compute per-FC urgency summary so the frontend can show a dropdown of
        # selectable FCs with how many critical items each contains. Treat lower
        # DOH as more urgent: weight count by inverse-DOH.
        fc_summary = {}
        for it in actionable:
            fc_key = (it.get('destination_fc') or '').strip()
            if not fc_key:
                continue
            entry = fc_summary.setdefault(fc_key, {
                'fc': fc_key,
                'item_count': 0,
                'liters': 0.0,
                'critical_count': 0,
                'min_doh': None,
            })
            entry['item_count'] += 1
            entry['liters'] += float(it.get('total_accepted_liters') or 0)
            if it.get('priority_bucket') in ('CRITICAL', 'VERY HIGH', 'HIGH'):
                entry['critical_count'] += 1
            doh_val = it.get('doh')
            if doh_val is not None:
                cur_min = entry['min_doh']
                entry['min_doh'] = doh_val if cur_min is None else min(cur_min, doh_val)
        # Rank FCs by "most urgent first": critical_count desc, min_doh asc, liters desc.
        fc_options = sorted(
            fc_summary.values(),
            key=lambda x: (
                -x['critical_count'],
                float(x['min_doh']) if x['min_doh'] is not None else 9999.0,
                -x['liters'],
            ),
        )

        # 6) Single-FC constraint: a truck must contain items from one FC only.
        #    If the user explicitly passed `fc`, the candidate pool was already
        #    filtered to it. Otherwise pick the FC whose items are most urgent
        #    (top of fc_options).
        if fc and actionable:
            primary_fc = (actionable[0].get('destination_fc') or '').strip().upper()
        elif fc_options:
            primary_fc = fc_options[0]['fc'].strip().upper()
        elif actionable:
            primary_fc = (actionable[0].get('destination_fc') or '').strip().upper()
        else:
            primary_fc = ''

        if primary_fc:
            same_fc = []
            other_fc = []
            for it in actionable:
                if (it.get('destination_fc') or '').strip().upper() == primary_fc:
                    same_fc.append(it)
                else:
                    it_copy = dict(it)
                    it_copy['skipped_reason'] = f'Different FC ({it.get("destination_fc")}); truck is locked to {primary_fc}'
                    other_fc.append(it_copy)
            loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
                same_fc, truck_size, capacity_override, priority=priority, strict=priority_strict,
            )
            not_loaded = not_loaded + other_fc + no_demand
        else:
            loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
                actionable, truck_size, capacity_override, priority=priority, strict=priority_strict,
            )
            not_loaded = not_loaded + no_demand

        # Maximize-fill: top up remaining truck capacity with NO-DEMAND items
        # + leftover not_loaded items at the chosen FC. Single-FC constraint
        # still enforced — _filler_pass filters by primary_fc internally.
        filler_count = 0
        if maximize_fill and not_loaded:
            loaded, not_loaded = _filler_pass(loaded, not_loaded, capacity, primary_fc=primary_fc)
            filler_count = sum(1 for it in loaded if it.get('_filler'))
            planned_liters = round(sum(float(it.get('planned_liters') or 0) for it in loaded), 4)
            load_pct = round((planned_liters / capacity * 100) if capacity > 0 else 0, 2)

        # 7) Build urgent-no-PO list (CRITICAL or HIGH DOH but no eligible PO)
        urgent_no_po = []
        for asin_up, live in doh_by_asin.items():
            if asin_up in asins_with_po:
                continue
            bucket, reason = _doh_bucket(live.get('doh'), live.get('drr_unit'))
            if bucket in ('CRITICAL', 'HIGH'):
                urgent_no_po.append({
                    'asin': live.get('asin'),
                    'item_head': live.get('item_head_live'),
                    'category': live.get('category_live'),
                    'sub_category': live.get('sub_category_live'),
                    'brand': live.get('brand_live'),
                    'per_unit': live.get('per_unit_live'),
                    'soh_unit': live.get('soh_unit'),
                    'soh_ltr': live.get('soh_ltr'),
                    'drr_unit': live.get('drr_unit'),
                    'drr_ltr': live.get('drr_ltr'),
                    'doh': live.get('doh'),
                    'priority_bucket': bucket,
                    'priority_reason': reason,
                })
        urgent_no_po.sort(key=lambda x: float(x.get('doh') if x.get('doh') is not None else 9999))

        breakdown = {}
        for item in loaded:
            b = item.get('priority_bucket', 'LOW')
            breakdown[b] = breakdown.get(b, 0) + 1

        # The FC label actually loaded on the truck (matches one entry in fc_options
        # if the candidate pool had items). Use the first loaded item if available
        # so even after best-effort spillover the label reflects reality.
        fc_used = None
        if loaded:
            fc_used = loaded[0].get('destination_fc')
        elif primary_fc and fc_options:
            for opt in fc_options:
                if opt['fc'].strip().upper() == primary_fc:
                    fc_used = opt['fc']
                    break

        truck_suggestion = _suggest_smaller_truck(planned_liters, capacity, truck_size)

        return Response({
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
            'urgent_no_po': urgent_no_po,
            'priority_requested': priority,
            'priority_actual': priority_actual,
            'priority_strict': priority_strict,
            'maximize_fill': maximize_fill,
            'filler_count': filler_count,
            'load_summary': {
                'truck_size': truck_size,
                'capacity': capacity,
                'planned_liters': planned_liters,
                'load_percentage': load_pct,
            },
            'priority_breakdown': breakdown,
            'stats': {
                'total_candidates': len(actionable),
                'loaded_count': len(loaded),
                'not_loaded_count': len(not_loaded),
                'urgent_no_po_count': len(urgent_no_po),
            },
            'source': {
                'sales': 'amazon_sec_range_master_view',
                'inventory': 'amazon_master_inventory',
            },
            'doh_snapshot': doh_meta,
            'effective_date': effective_date.isoformat() if effective_date else None,
            'month': month_name,
            'year': year,
            'fc_used': fc_used,
            'fc_options': fc_options,
            'primary_fc': fc_used,
            'truck_suggestion': truck_suggestion,
        })


class ShipmentPendingApprovalsView(_SafeAPIView):
    """Returns full detail (including items) for all pending-approval shipments."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        qs = Shipment.objects.prefetch_related('items', 'audit_logs').filter(
            status=Shipment.Status.PENDING_APPROVAL
        )
        return Response(ShipmentSerializer(qs, many=True).data)


class PoShipmentLookupView(_SafeAPIView):
    """
    Live map of (asin, po_number) -> list of shipments that contain that line.

    Used by the planning UI to block re-selection of items already committed to
    another active shipment (draft / pending / approved / dispatched / in_transit /
    delivered). Rejected shipments are excluded — when a plan is rejected its POs
    and SKUs become re-selectable in new shipments. Frontend shows a popup with
    these details when the user tries to select a blocked row.
    """
    permission_classes = [IsAuthenticated]

    STATUS_LABELS = {
        'draft': 'Draft',
        'pending_approval': 'Pending Approval',
        'approved': 'Approved',
        'rejected': 'Rejected',
        'dispatched': 'Dispatched',
        'in_transit': 'In Transit',
        'delivered': 'Delivered',
    }

    def get(self, request):
        items = (
            ShipmentItem.objects
            .filter(not_loaded=False)
            .exclude(shipment__status=Shipment.Status.REJECTED)
            .select_related('shipment', 'shipment__created_by')
            .only(
                'asin', 'po_number', 'destination_fc',
                'planned_qty', 'planned_liters', 'accepted_qty',
                'product_name', 'internal_sku',
                'shipment__id', 'shipment__status', 'shipment__appointment_id',
                'shipment__destination_fc', 'shipment__truck_size',
                'shipment__planned_liters', 'shipment__load_percentage',
                'shipment__created_at', 'shipment__rejection_reason',
                'shipment__dispatch_date_planned', 'shipment__created_by__email',
            )
        )

        # Two keys per line: the FC-scoped `asin__po__fc` AND an FC-agnostic
        # `asin__po`, each listing the holding shipments + summed committed qty.
        # The UI tries the FC key first (same-FC commitments net per-FC), then
        # falls back to the FC-agnostic key so a FLIPPED PO committed at its sister
        # FC (e.g. DED3↔DED5) is still netted/blocked (see CreateShipment.jsx
        # loadData / getBlockReason) — matching the auto candidate SQL, whose
        # `committed` CTE is keyed by (asin, po) only for the same flip-safe reason.
        result = {}
        for it in items:
            asin = (it.asin or '').strip()
            po = (it.po_number or '').strip()
            if not asin or not po:
                continue
            fc_key = (it.destination_fc or '').strip().upper()
            s = it.shipment
            entry = {
                'shipment_id': s.id,
                'status': s.status,
                'status_label': self.STATUS_LABELS.get(s.status, s.status or '—'),
                'appointment_id': s.appointment_id or '',
                'destination_fc': s.destination_fc or '',
                'truck_size': s.truck_size or '',
                'planned_liters_shipment': float(s.planned_liters or 0),
                'load_percentage': float(s.load_percentage or 0),
                'created_at': s.created_at.isoformat() if s.created_at else None,
                'created_by': s.created_by.email if s.created_by else None,
                'dispatch_date_planned': s.dispatch_date_planned.isoformat() if s.dispatch_date_planned else None,
                'rejection_reason': s.rejection_reason or '',
                'item_planned_qty': float(it.planned_qty or 0),
                'item_planned_liters': float(it.planned_liters or 0),
                'item_accepted_qty': float(it.accepted_qty or 0),
                'product_name': it.product_name or '',
                'internal_sku': it.internal_sku or '',
            }
            # Index under BOTH the FC-scoped key and the FC-agnostic (asin, po) key
            # so the picker's legacy fallback actually resolves — before, this
            # endpoint only emitted FC keys, silently breaking flip netting.
            for key in (f"{asin}__{po}__{fc_key}", f"{asin}__{po}"):
                bucket = result.setdefault(key, {'shipments': [], 'committed_qty': 0.0})
                bucket['shipments'].append(entry)
                bucket['committed_qty'] += float(it.planned_qty or 0)

        # Sort each list newest-first
        for k in result:
            result[k]['shipments'].sort(key=lambda x: x.get('created_at') or '', reverse=True)

        return Response(result)


class PoShortSupplyView(_SafeAPIView):
    """
    Short-supply report — PER SHIPMENT EVENT. Every loaded line that shipped fewer
    units than were orderable on its own truck (planned_qty < accepted_qty) is
    listed with its short quantity AND the reason it was short (manual or auto),
    regardless of whether the leftover later ships on another appointment. This
    records each short-supply as an event, not just the net PO shortfall — so a
    manual short-supply stays on the report even after the remainder is shipped.
    Also FC-flip-safe: keyed off the item's own fields, so flipped POs aren't
    dropped by a sheet-FC vs stored-FC mismatch.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        with connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.id                                AS shipment_id,
                    si.po_number, si.asin,
                    si.product_name, si.internal_sku,
                    si.item                             AS item_name,
                    si.destination_fc,
                    -- The appointment this line shipped under (single item), named
                    -- appointment_ids for the report's appointment-wise grouping.
                    NULLIF(TRIM(si.appointment_id), '') AS appointment_ids,
                    s.created_at                        AS last_shipped_at,
                    s.appointment_time,
                    s.status                            AS shipment_status,
                    ch.channel                          AS channel,
                    -- Per-truck: what was orderable on this shipment vs what shipped.
                    COALESCE(si.accepted_qty, 0)        AS ordered_qty,
                    COALESCE(si.planned_qty, 0)         AS shipped_qty,
                    (COALESCE(si.accepted_qty, 0) - COALESCE(si.planned_qty, 0)) AS short_qty,
                    NULLIF(TRIM(si.short_reason), '')   AS short_reason
                FROM sp_items si
                JOIN sp_shipments s ON s.id = si.shipment_id
                LEFT JOIN public.fc_city_state_channel_master ch
                       ON UPPER(TRIM(ch.fc::text)) = UPPER(TRIM(COALESCE(si.destination_fc, '')))
                WHERE si.not_loaded = FALSE
                  AND s.status != 'rejected'
                  AND COALESCE(si.planned_qty, 0) > 0
                  AND (COALESCE(si.accepted_qty, 0) - COALESCE(si.planned_qty, 0)) > 0
                ORDER BY (COALESCE(si.accepted_qty, 0) - COALESCE(si.planned_qty, 0)) DESC
            """)
            rows = _row_to_dict(cur, cur.fetchall())

        results = [_serialize_row(r) for r in rows]
        total_short_units = sum(float(r.get('short_qty') or 0) for r in results)
        return Response({
            'results': results,
            'count': len(results),
            'total_short_units': round(total_short_units, 4),
        })


_FC_CHANNEL_CACHE = {'at': 0.0, 'data': None}


def _fc_channel_map():
    """{FC (upper) → channel} from fc_city_state_channel_master. Tiny + near-static,
    so cache for 5 min to avoid a per-request query."""
    now = time.time()
    c = _FC_CHANNEL_CACHE
    if c['data'] is not None and (now - c['at'] < 300):
        return c['data']
    m = {}
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT UPPER(TRIM(fc::text)), channel FROM public.fc_city_state_channel_master")
            for fc, ch in cur.fetchall():
                if fc:
                    m[fc] = ch
    except Exception:
        pass
    c['at'] = now
    c['data'] = m
    return m


class ShipmentRecordView(_SafeAPIView):
    """Audit 'Record' (Data → Record): every ACTIVE shipment (not rejected) with its
    items grouped by PO, each ASIN tagged shipped / short / not_loaded plus the reason
    it wasn't fully shipped. Two cheap queries (shipments, then their items by
    shipment_id) so it stays fast; newest shipments first."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        limit = _safe_int(request.query_params.get('limit'), 200, lo=1, hi=2000)
        status_filter = request.query_params.get('status', '').strip().lower()

        with connection.cursor() as cur:
            where = ["s.status != 'rejected'"]
            params = []
            if status_filter and status_filter not in ('', 'all', 'active'):
                where.append("LOWER(s.status) = %s")
                params.append(status_filter)
            cur.execute(f"""
                SELECT s.id, s.status, s.planning_mode, s.appointment_id,
                       s.destination_fc, s.truck_size, s.created_at, s.appointment_time
                FROM sp_shipments s
                WHERE {' AND '.join(where)}
                ORDER BY s.created_at DESC NULLS LAST, s.id DESC
                LIMIT %s
            """, params + [limit])
            ship_rows = _row_to_dict(cur, cur.fetchall())
            ship_ids = [r['id'] for r in ship_rows]

            items_by_ship = {}
            if ship_ids:
                cur.execute("""
                    SELECT si.shipment_id, si.po_number, si.asin, si.internal_sku,
                           si.product_name, si.item, si.destination_fc, si.item_head,
                           si.source_warehouse,
                           si.accepted_qty, si.planned_qty, si.planned_liters,
                           si.case_pack, si.not_loaded,
                           si.unfit_reason, si.short_reason,
                           si.priority_bucket, si.priority_reason,
                           si.expiry_date, si.days_to_expiry
                    FROM sp_items si
                    WHERE si.shipment_id = ANY(%s)
                    ORDER BY si.not_loaded ASC, UPPER(TRIM(si.po_number)), si.asin
                """, [ship_ids])
                for r in _row_to_dict(cur, cur.fetchall()):
                    items_by_ship.setdefault(r['shipment_id'], []).append(r)

        # FC → channel for display (tiny, cached map).
        channel_map = _fc_channel_map()

        results = []
        for sh in ship_rows:
            items = items_by_ship.get(sh['id'], [])
            pos = {}
            summ = {'asins': 0, 'shipped': 0, 'short': 0, 'not_loaded': 0,
                    'ordered_units': 0.0, 'shipped_units': 0.0, 'short_units': 0.0}
            for it in items:
                ordered = float(it.get('accepted_qty') or 0)
                planned = 0.0 if it.get('not_loaded') else float(it.get('planned_qty') or 0)
                short = max(0.0, ordered - planned)
                if it.get('not_loaded'):
                    status = 'not_loaded'
                    reason = it.get('unfit_reason') or it.get('priority_reason') or 'Not loaded (reason not recorded)'
                elif short > 1e-6:
                    status = 'short'
                    reason = it.get('short_reason') or (
                        f'Shipped {int(round(planned))} of {int(round(ordered))} '
                        f'— {int(round(short))} short'
                    )
                else:
                    status = 'shipped'
                    reason = ''
                summ['asins'] += 1
                summ[status] += 1
                summ['ordered_units'] += ordered
                summ['shipped_units'] += planned
                summ['short_units'] += short
                po = (it.get('po_number') or '—').strip() or '—'
                pos.setdefault(po, []).append(_serialize_row({
                    'asin': it.get('asin'),
                    'item': it.get('item') or it.get('product_name'),
                    'internal_sku': it.get('internal_sku'),
                    'item_head': it.get('item_head'),
                    'source_warehouse': it.get('source_warehouse'),
                    'source_inventory': _inventory_label(it.get('source_warehouse')),
                    'destination_fc': it.get('destination_fc'),
                    'ordered_qty': ordered,
                    'shipped_qty': planned,
                    'short_qty': short,
                    'planned_liters': float(it.get('planned_liters') or 0),
                    'case_pack': it.get('case_pack'),
                    'priority_bucket': it.get('priority_bucket'),
                    'expiry_date': it.get('expiry_date'),
                    'days_to_expiry': it.get('days_to_expiry'),
                    'status': status,
                    'reason': reason,
                }))

            fc = str(sh.get('destination_fc') or '').strip().upper()
            results.append(_serialize_row({
                'id': sh['id'],
                'status': sh['status'],
                'planning_mode': sh['planning_mode'],
                'appointment_id': sh['appointment_id'],
                'destination_fc': sh['destination_fc'],
                'channel': channel_map.get(fc),
                'truck_size': sh['truck_size'],
                'created_at': sh['created_at'],
                'appointment_time': sh['appointment_time'],
                'summary': {
                    'pos': len(pos),
                    'asins': summ['asins'],
                    'shipped': summ['shipped'],
                    'short': summ['short'],
                    'not_loaded': summ['not_loaded'],
                    'ordered_units': round(summ['ordered_units'], 2),
                    'shipped_units': round(summ['shipped_units'], 2),
                    'short_units': round(summ['short_units'], 2),
                },
                'pos': [
                    {'po_number': po, 'items': its}
                    for po, its in sorted(pos.items())
                ],
            }))

        return Response({'results': results, 'count': len(results)})


class ShipmentKpiView(_SafeAPIView):
    """Live planner KPIs (Data → KPIs), computed from sp_shipments / sp_items over
    ACTIVE shipments (not rejected): truck fill %, unit fill %, short-supply %,
    commitment adherence, line/status/mode breakdowns, and inventory-snapshot
    freshness. A few cheap aggregate queries — no heavy joins."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        import json as _json
        with connection.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                              AS shipments,
                    COUNT(*) FILTER (WHERE status = 'draft')             AS draft,
                    COUNT(*) FILTER (WHERE status = 'pending_approval')  AS pending,
                    COUNT(*) FILTER (WHERE status = 'approved')          AS approved,
                    COUNT(*) FILTER (WHERE status = 'dispatched')        AS dispatched,
                    COUNT(*) FILTER (WHERE planning_mode = 'manual')     AS manual_mode,
                    -- Everything that isn't manual (appointment, doh, and legacy
                    -- NULL/empty modes) is auto, so auto + manual == active.
                    COUNT(*) FILTER (WHERE planning_mode IS DISTINCT FROM 'manual') AS auto_mode,
                    COALESCE(SUM(planned_liters), 0)        AS planned_liters,
                    COALESCE(SUM(truck_capacity_liters), 0) AS capacity_liters
                FROM sp_shipments
                WHERE status != 'rejected'
            """)
            sh = dict(zip([c[0] for c in cur.description], cur.fetchone()))

            cur.execute("""
                SELECT
                    COALESCE(SUM(si.accepted_qty), 0) AS ordered_units,
                    COALESCE(SUM(CASE WHEN si.not_loaded = FALSE THEN si.planned_qty ELSE 0 END), 0) AS shipped_units,
                    COUNT(*)                                                       AS total_lines,
                    COUNT(*) FILTER (WHERE si.not_loaded = TRUE)                   AS not_loaded_lines,
                    COUNT(*) FILTER (WHERE si.not_loaded = FALSE AND si.planned_qty < si.accepted_qty) AS short_lines,
                    COUNT(*) FILTER (WHERE si.not_loaded = FALSE AND si.planned_qty >= si.accepted_qty) AS full_lines
                FROM sp_items si
                JOIN sp_shipments s ON s.id = si.shipment_id
                WHERE s.status != 'rejected'
            """)
            it = dict(zip([c[0] for c in cur.description], cur.fetchone()))

            cur.execute("""
                SELECT commitment_snapshot
                FROM sp_shipments
                WHERE status != 'rejected' AND commitment_snapshot IS NOT NULL
            """)
            snaps = [r[0] for r in cur.fetchall()]

        # Commitment adherence from the frozen per-appointment snapshots.
        appt_total = appt_within = 0
        commit_u = filled_u = commit_c = filled_c = 0.0
        for snap in snaps:
            if isinstance(snap, str):
                try:
                    snap = _json.loads(snap)
                except (ValueError, TypeError):
                    snap = []
            for e in (snap or []):
                cu = float(e.get('committed_units') or 0)
                fu = float(e.get('filled_units') or 0)
                cc = float(e.get('committed_cartons') or 0)
                fc = float(e.get('filled_cartons') or 0)
                if cu <= 0 and cc <= 0:
                    continue
                appt_total += 1
                commit_u += cu; filled_u += fu
                commit_c += cc; filled_c += fc
                within_u = cu <= 0 or fu <= cu * CAP_TOLERANCE + 1e-6
                within_c = cc <= 0 or fc <= cc * CAP_TOLERANCE + 1e-6
                if within_u and within_c:
                    appt_within += 1

        _, meta = _live_doh_by_asin()

        ordered = float(it['ordered_units'] or 0)
        shipped = float(it['shipped_units'] or 0)
        short = max(0.0, ordered - shipped)
        cap = float(sh['capacity_liters'] or 0)
        pl = float(sh['planned_liters'] or 0)
        pct = lambda a, b: round((a / b * 100), 1) if b else 0.0  # noqa: E731

        return Response({
            'shipments': {
                'active': int(sh['shipments'] or 0),
                'draft': int(sh['draft'] or 0),
                'pending_approval': int(sh['pending'] or 0),
                'approved': int(sh['approved'] or 0),
                'dispatched': int(sh['dispatched'] or 0),
                'auto': int(sh['auto_mode'] or 0),
                'manual': int(sh['manual_mode'] or 0),
            },
            'truck_fill_pct': pct(pl, cap),
            'planned_liters': round(pl, 0),
            'capacity_liters': round(cap, 0),
            'unit_fill_pct': pct(shipped, ordered),
            'short_supply_pct': pct(short, ordered),
            'ordered_units': round(ordered, 0),
            'shipped_units': round(shipped, 0),
            'short_units': round(short, 0),
            'lines': {
                'total': int(it['total_lines'] or 0),
                'full': int(it['full_lines'] or 0),
                'short': int(it['short_lines'] or 0),
                'not_loaded': int(it['not_loaded_lines'] or 0),
            },
            'commitment': {
                'appointments': appt_total,
                'within_cap': appt_within,
                'adherence_pct': pct(appt_within, appt_total),
                'unit_fill_of_commit_pct': pct(filled_u, commit_u),
                'carton_fill_of_commit_pct': pct(filled_c, commit_c),
            },
            'snapshot': {
                # _doh_snapshot_meta already returns effective_date as an ISO string.
                'effective_date': meta.get('effective_date'),
                'age_days': meta.get('snapshot_age_days'),
                'is_stale': meta.get('is_stale', False),
            },
        })


_SAP_INV_CACHE = {}   # whs_code -> {'at': float, 'data': payload} — cached per warehouse
_SAP_INV_TTL = 60  # seconds — full-warehouse SAP read; the inventory page
#                    auto-refreshes, so cache to avoid a live HANA hit each time.


class SapInventoryView(_SafeAPIView):
    """Live SAP HANA finished-goods stock for the Shipment Planner inventory page.

    Two modes, chosen by the ``?warehouse=`` query param:
      - a specific code (BH-FGM / GP-FG / BH-EC) → just that warehouse
      - ``ALL`` → every warehouse below, merged into one sheet. Each row already
        carries its own WhsCode / WhsName, so the UI shows a warehouse column.

    The warehouses live in two different company DBs, so each is queried in its
    own schema (see WAREHOUSE_SOURCES):
      - BH-FGM → mart · "Jivo Mart"     (Sonipat finished goods)
      - GP-FG  → oil  · "Gupta Godown Basement Finished Godown"
      - BH-EC  → oil  · "Bhakharpur Finished E-Commerce"
    Read-only; scoped to FINISHED item group + Active items (enforced in SQL).
    Unknown / missing warehouse falls back to the default, BH-FGM."""
    permission_classes = [IsAuthenticated]

    # (warehouse code, company-DB source key). Order here is the display order
    # for the combined ALL view. Add a warehouse in exactly one place: here.
    WAREHOUSE_SOURCES = (
        ('BH-FGM', 'mart'),
        ('GP-FG',  'oil'),
        ('BH-EC',  'oil'),
    )
    ALLOWED_WHS = tuple(code for code, _src in WAREHOUSE_SOURCES)
    _WHS_SOURCE = dict(WAREHOUSE_SOURCES)
    DEFAULT_WHS = 'BH-FGM'

    # One warehouse's finished-goods stock; `?` binds the WhsCode. The same
    # unqualified SQL runs against whichever company schema the warehouse maps to.
    INVENTORY_SQL = '''
        SELECT
            T0."ItemCode",
            T0."ItemName",
            T3."ItmsGrpNam"  AS "GroupName",
            T0."SalUnitMsr"  AS "UOM",
            T0."validFor"    AS "Active",
            T0."LastPurPrc"  AS "LastPurchasePrice",
            T1."WhsCode",
            T2."WhsName",
            T2."City",
            T1."OnHand",
            T1."IsCommited" AS "Committed",
            T1."OnHand" - T1."IsCommited" AS "Available",
            T1."OnOrder",
            T1."MinStock",
            T1."MaxStock",
            T1."OnHand" * T0."LastPurPrc" AS "StockValue"
        FROM OITM T0
        INNER JOIN OITW T1 ON T1."ItemCode"   = T0."ItemCode"
        LEFT  JOIN OWHS T2 ON T2."WhsCode"     = T1."WhsCode"
        LEFT  JOIN OITB T3 ON T3."ItmsGrpCod"  = T0."ItmsGrpCod"
        WHERE T1."WhsCode" = ?
          AND T0."validFor" = 'Y'
          AND T3."ItmsGrpNam" = 'FINISHED'
        ORDER BY T0."ItemName"
    '''

    def _resolve_whs(self, request):
        raw = (request.query_params.get('warehouse') or '').strip().upper()
        return raw if raw in self.ALLOWED_WHS else self.DEFAULT_WHS

    def get(self, request):
        raw = (request.query_params.get('warehouse') or '').strip().upper()
        if raw == 'ALL':
            return self._get_combined()
        return self._get_single(raw if raw in self.ALLOWED_WHS else self.DEFAULT_WHS)

    # ── fetch / enrich / summarize (shared by both modes) ────────────────────
    @classmethod
    def _fetch_rows(cls, whs_code):
        """Run INVENTORY_SQL for one warehouse in its own company DB schema.
        The HANA driver is imported lazily so the app never hard-depends on
        hdbcli; raises on driver-missing / HANA-down so the caller surfaces it."""
        from sap.service import select, resolve_schema
        _src, schema = resolve_schema(cls._WHS_SOURCE.get(whs_code, 'mart'))
        return select(cls.INVENTORY_SQL, [whs_code], schema=schema)

    @staticmethod
    def _enrich(rows):
        """Attach per_unit + format_sku_code (ASIN) from public.master_sheet,
        keyed by SAP ItemCode (master_sheet.sku_sap_code). AMAZON listing only —
        items with no Amazon row map to nothing."""
        codes = list({(r.get('ItemCode') or '').strip().upper() for r in rows if r.get('ItemCode')})
        master = {}
        if codes:
            with connection.cursor() as cur:
                cur.execute("""
                    SELECT UPPER(TRIM(sku_sap_code)) AS code,
                           MAX(per_unit) AS per_unit,
                           MAX(format_sku_code) AS format_sku_code
                    FROM public.master_sheet
                    WHERE UPPER(format) = 'AMAZON'
                      AND UPPER(TRIM(sku_sap_code)) = ANY(%s)
                    GROUP BY UPPER(TRIM(sku_sap_code))
                """, [codes])
                for code, per_unit, fmt_code in cur.fetchall():
                    master[code] = {'per_unit': per_unit, 'format_sku_code': fmt_code}
        for r in rows:
            m = master.get((r.get('ItemCode') or '').strip().upper()) or {}
            r['per_unit'] = m.get('per_unit')
            r['format_sku_code'] = m.get('format_sku_code')
        return rows

    @staticmethod
    def _summarize(rows):
        total_units = sum(float(r.get('OnHand') or 0) for r in rows)
        total_value = sum(float(r.get('StockValue') or 0) for r in rows)
        zero_stock = sum(1 for r in rows if float(r.get('OnHand') or 0) == 0)
        return {
            'total_skus': len(rows),
            'total_units_on_hand': round(total_units, 3),
            'total_stock_value': round(total_value, 2),
            'items_at_zero_stock': zero_stock,
        }

    # ── single-warehouse mode ────────────────────────────────────────────────
    def _get_single(self, whs_code):
        now = time.time()
        entry = _SAP_INV_CACHE.get(whs_code)
        if entry is not None and (now - entry['at'] < _SAP_INV_TTL):
            self._overlay_reserved(entry['data'].get('results') or [])
            return Response(entry['data'])
        try:
            rows = self._fetch_rows(whs_code)
        except Exception as e:  # HANA unreachable / VPN down / driver missing
            return Response(
                {'error': f'Could not reach SAP HANA: {e}', 'results': [], 'summary': {}},
                status=502,
            )
        self._enrich(rows)
        payload = {
            'warehouse': whs_code,
            'source': self._WHS_SOURCE.get(whs_code, 'mart'),
            'results': rows,
            'count': len(rows),
            'summary': self._summarize(rows),
        }
        _SAP_INV_CACHE[whs_code] = {'at': now, 'data': payload}
        self._overlay_reserved(payload['results'])
        return Response(payload)

    # ── combined mode: every warehouse merged into one sheet ─────────────────
    def _get_combined(self):
        now = time.time()
        entry = _SAP_INV_CACHE.get('ALL')
        if entry is not None and (now - entry['at'] < _SAP_INV_TTL):
            self._overlay_reserved(entry['data'].get('results') or [])
            return Response(entry['data'])
        all_rows, warnings = [], []
        for whs_code, _source in self.WAREHOUSE_SOURCES:
            try:
                all_rows.extend(self._fetch_rows(whs_code))
            except Exception as e:  # one warehouse down shouldn't blank the sheet
                warnings.append(f'{whs_code}: {e}')
        # Only 502 if EVERY warehouse failed — otherwise return what we got.
        if not all_rows and warnings:
            return Response(
                {'error': 'Could not reach SAP HANA: ' + '; '.join(warnings),
                 'results': [], 'summary': {}},
                status=502,
            )
        self._enrich(all_rows)
        payload = {
            'warehouse': 'ALL',
            'warehouses': list(self.ALLOWED_WHS),
            'results': all_rows,
            'count': len(all_rows),
            'summary': self._summarize(all_rows),
        }
        if warnings:
            payload['warnings'] = warnings
        _SAP_INV_CACHE['ALL'] = {'at': now, 'data': payload}
        self._overlay_reserved(payload['results'])
        return Response(payload)

    @staticmethod
    def _overlay_reserved(rows):
        """Attach the planner's LIVE reservation to each SAP row: ``planned_reserved``
        = units already spoken for by ACTIVE Amazon plans (draft / pending_approval /
        approved) for that ASIN, and ``free_to_plan`` = On Hand − planned_reserved
        (what the planner actually treats as available). Computed fresh on every
        request — NOT part of the 60s SAP cache — so a load shows here the instant
        it's planned. SAP's own OnHand / Committed / Available are left untouched.
        Keyed by the row's ASIN (format_sku_code); non-Amazon rows reserve nothing."""
        reserved = _reserved_stock_by_asin()
        for r in rows:
            asin = str(r.get('format_sku_code') or '').strip().upper()
            res = reserved.get(asin, 0.0) if asin else 0.0
            r['planned_reserved'] = res
            r['free_to_plan'] = float(r.get('OnHand') or 0) - res
