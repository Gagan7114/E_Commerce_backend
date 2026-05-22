from __future__ import annotations

import math
from decimal import Decimal

from django.db import connection, transaction
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Shipment, ShipmentAuditLog, ShipmentItem
from .serializers import (
    ShipmentAuditLogSerializer,
    ShipmentItemSerializer,
    ShipmentListSerializer,
    ShipmentSerializer,
)

TRUCK_CAPACITIES = {'10_ton': 10000.0, '15_ton': 15000.0}
LOCKED_STATUSES = ('approved', 'dispatched', 'in_transit', 'delivered')


# ---------------------------------------------------------------------------
# Priority helpers
# ---------------------------------------------------------------------------

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
        total_liters = float(item.get('total_accepted_liters') or 0)
        per_liter    = float(item.get('per_liter') or 0)
        case_pack    = float(item.get('case_pack') or 1)
        accepted_qty = float(item.get('accepted_qty') or 0)

        if accepted_qty == 0 or total_liters == 0:
            item['planned_qty'] = 0
            item['planned_liters'] = 0
            not_loaded.append(item)
            continue

        if total_liters <= remaining + 0.001:
            item['planned_qty'] = accepted_qty
            item['planned_liters'] = round(total_liters, 4)
            remaining -= total_liters
            loaded.append(item)
        else:
            if per_liter > 0 and case_pack > 0:
                partial_qty = math.floor((remaining / per_liter) / case_pack) * case_pack
                if partial_qty > 0:
                    partial_liters = round(partial_qty * per_liter, 4)
                    item['planned_qty'] = partial_qty
                    item['planned_liters'] = partial_liters
                    remaining -= partial_liters
                    loaded.append(item)
                else:
                    item['planned_qty'] = 0
                    item['planned_liters'] = 0
                    not_loaded.append(item)
            else:
                item['planned_qty'] = 0
                item['planned_liters'] = 0
                not_loaded.append(item)
    used = float(capacity_lt) - remaining
    return loaded, not_loaded, used


def _auto_plan_truck(items, truck_size, capacity_override=None, priority=None):
    """
    Plan a truck load.

    `priority` (optional): {'PREMIUM': pct, 'COMMODITY': pct, 'OTHER': pct} — each
    percentage 0..100, summing to 100. When provided, the loader allocates
    `pct/100 * capacity` to each bucket and packs ONLY items from that bucket
    into its slice (no cross-bucket spillover — strict adherence). Items that
    don't fit go to not_loaded.

    When `priority` is None, falls back to a flat greedy pack across all items.
    """
    capacity = _resolve_capacity(truck_size, capacity_override)

    if not priority:
        loaded, not_loaded, used = _pack_into_capacity(items, capacity)
        planned = round(used, 4)
        load_pct = round((planned / capacity * 100) if capacity > 0 else 0, 2)
        return loaded, not_loaded, capacity, planned, load_pct, None

    # Strict priority allocation
    buckets = {'PREMIUM': [], 'COMMODITY': [], 'OTHER': []}
    for it in items:
        buckets[_item_head_bucket(it)].append(it)

    bucket_caps = {
        k: round(capacity * (float(priority.get(k, 0) or 0) / 100.0), 4)
        for k in buckets
    }

    loaded_all, not_loaded_all = [], []
    priority_actual = {}
    for k, bucket_items in buckets.items():
        cap_k = bucket_caps.get(k, 0)
        if cap_k <= 0:
            # Bucket not requested — push everything to not_loaded
            for it in bucket_items:
                it['planned_qty'] = 0
                it['planned_liters'] = 0
                not_loaded_all.append(it)
            priority_actual[k] = {'requested_liters': 0, 'used_liters': 0}
            continue
        l, nl, used = _pack_into_capacity(bucket_items, cap_k)
        loaded_all.extend(l)
        not_loaded_all.extend(nl)
        priority_actual[k] = {'requested_liters': cap_k, 'used_liters': round(used, 4)}

    planned = round(sum(p['used_liters'] for p in priority_actual.values()), 4)
    load_pct = round((planned / capacity * 100) if capacity > 0 else 0, 2)

    # Attach priority adherence info to one item-like dict at the end of the list?
    # Better — return as an extra dict via a side channel. We'll stash it on
    # loaded_all[0]._priority_meta if any items loaded; callers can read it.
    # Cleaner approach: return as tuple via separate variable; callers updated
    # to handle 6-tuple when priority is present.
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


def _live_doh_by_asin():
    """
    Returns {asin_upper: {soh_unit, soh_ltr, drr_unit, drr_ltr, doh, units_sold, ltr_sold}}
    sourced from amazon_master_inventory + amazon_sec_range_master_view — exact same logic
    as the SOH/DOH dashboard and DOH Auto-Fill, so numbers match across all 4 surfaces.

    Returns {} if no inventory snapshot is available yet.
    """
    with connection.cursor() as cur:
        cur.execute(
            "SELECT MAX(inventory_date) FROM amazon_master_inventory"
        )
        eff_row = cur.fetchone()
        effective_date = eff_row[0] if eff_row else None
        if not effective_date:
            return {}

        elapsed_day = max(1, effective_date.day)
        month_name = effective_date.strftime('%B').upper()
        year = effective_date.year
        month_day = f"{effective_date.day:02d}-{effective_date.strftime('%b').upper()}"

        cur.execute(
            """
            WITH sales AS (
                SELECT
                    UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                    COALESCE(SUM(shipped_units), 0)::numeric  AS units_sold,
                    COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
                FROM amazon_sec_range_master_view
                WHERE "year" = %s
                  AND UPPER(TRIM("month"::text)) = %s
                  AND UPPER(TRIM(month_day::text)) = %s
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
            [year, month_name, month_day, year, month_name, effective_date],
        )
        rows = cur.fetchall()

    by_asin = {}
    for asin_key, soh_unit, soh_ltr, units_sold, ltr_sold in rows:
        if not asin_key:
            continue
        soh_unit_f = float(soh_unit or 0)
        soh_ltr_f  = float(soh_ltr or 0)
        units_sold_f = float(units_sold or 0)
        ltr_sold_f   = float(ltr_sold or 0)
        drr_unit = units_sold_f / elapsed_day if elapsed_day > 0 else 0.0
        drr_ltr  = ltr_sold_f / elapsed_day if elapsed_day > 0 else 0.0
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
    return by_asin


# ---------------------------------------------------------------------------
# Appointment endpoints
# ---------------------------------------------------------------------------

class AppointmentDatesView(APIView):
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
        return Response({'dates': dates, 'counts': counts, 'cancelled': cancelled})


class AppointmentListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        date_str = request.query_params.get('date')
        if not date_str:
            return Response({'error': 'date parameter required'}, status=400)

        with connection.cursor() as cur:
            cur.execute("""
                SELECT * FROM (
                    SELECT DISTINCT ON (a.appointment_id)
                        a.appointment_id,
                        a.status,
                        a.appointment_time,
                        a.destination_fc,
                        a.pro,
                        (
                            SELECT COUNT(DISTINCT NULLIF(TRIM(pv), ''))
                            FROM unnest(
                                regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
                            ) AS pv
                            WHERE NULLIF(TRIM(pv), '') IS NOT NULL
                        ) AS po_count
                    FROM reporting."appointment" a
                    WHERE DATE(a.appointment_time) = %s
                    ORDER BY a.appointment_id, a.appointment_time DESC NULLS LAST
                ) deduped
                ORDER BY appointment_time, appointment_id
            """, [date_str])
            rows = _row_to_dict(cur, cur.fetchall())

        return Response([_serialize_row(r) for r in rows])


class AppointmentItemsView(APIView):
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

        with connection.cursor() as cur:
            cur.execute("""
                SELECT appointment_id, status, appointment_time, destination_fc, pro
                FROM reporting."appointment"
                WHERE appointment_id = %s
                LIMIT 1
            """, [appointment_id])
            row = cur.fetchone()

        if not row:
            return Response({'error': 'Appointment not found'}, status=404)

        appt = {
            'appointment_id': row[0],
            'status': row[1],
            'appointment_time': row[2].isoformat() if row[2] else None,
            'destination_fc': row[3],
            'pro': row[4],
        }

        if appt['status'] != 'Confirmed':
            return Response({'error': 'Appointment is not Confirmed'}, status=400)

        with connection.cursor() as cur:
            cur.execute("""
                WITH appt_pos AS (
                    SELECT DISTINCT UPPER(TRIM(pv)) AS po_number
                    FROM reporting."appointment" a,
                    LATERAL unnest(
                        regexp_split_to_array(COALESCE(a.pos, ''), '\s*[,;]\s*')
                    ) AS pv
                    WHERE a.appointment_id = %s
                      AND NULLIF(TRIM(pv), '') IS NOT NULL
                ),
                locked_pairs AS (
                    -- Block items committed to ANY shipment, regardless of status,
                    -- so users can't re-pick rows already used in a draft/pending plan.
                    SELECT DISTINCT si.asin, UPPER(TRIM(si.po_number)) AS po_number
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE si.not_loaded = FALSE
                ),
                doh_data AS (
                    -- placeholder; DOH joined in Python via _live_doh_by_asin() below
                    SELECT NULL::text AS asin
                )
                SELECT
                    p.po_number,
                    p.asin,
                    p.merchant_sku        AS internal_sku,
                    p.sku_name            AS product_name,
                    p.accepted_qty,
                    p.case_pack,
                    p.per_liter,
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
                    p.fulfillment_center
                FROM appt_pos ap
                JOIN reporting."Amazon PO" p
                    ON UPPER(TRIM(p.po_number)) = ap.po_number
                    AND p.fulfillment_center = %s
                LEFT JOIN locked_pairs lp
                    ON lp.asin = p.asin
                    AND lp.po_number = UPPER(TRIM(p.po_number))
                WHERE p.status = 'Confirmed'
                  AND p.availability_status = 'AC - Accepted: In stock'
                  AND p.accepted_qty > 0
                  AND p.po_status = 'PENDING'
                  AND lp.asin IS NULL
            """, [appointment_id, appt['destination_fc']])
            raw = _row_to_dict(cur, cur.fetchall())

        # Attach LIVE DOH/DRR/SOH (matches SOH/DOH dashboard exactly)
        doh_by_asin = _live_doh_by_asin()
        for r in raw:
            asin_up = str(r.get('asin') or '').upper().strip()
            live = doh_by_asin.get(asin_up, {})
            r['soh_unit'] = live.get('soh_unit', 0) or 0
            r['soh_ltr']  = live.get('soh_ltr', 0) or 0
            r['drr_unit'] = live.get('drr_unit', 0) or 0
            r['drr_ltr']  = live.get('drr_ltr', 0) or 0
            r['doh']      = live.get('doh', 0) or 0

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
            item['appointment_id'] = appointment_id

        items.sort(key=lambda x: (
            -x['priority_score'],
            x.get('days_to_expiry') or 999,
            -(x.get('accepted_qty') or 0),
        ))

        loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
            items, truck_size, capacity_override, priority=priority,
        )

        return Response({
            'appointment': appt,
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
        })


# ---------------------------------------------------------------------------
# Shipment CRUD
# ---------------------------------------------------------------------------

class ShipmentListCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        qs = Shipment.objects.select_related('created_by', 'approved_by').all()
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

        with transaction.atomic():
            shipment = Shipment.objects.create(
                appointment_id=appointment_id or '',
                appointment_time=appointment.get('appointment_time') if appointment else None,
                destination_fc=destination_fc,
                pro=(appointment or {}).get('pro', ''),
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
                    appointment_id=appointment_id or '',
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
                )

            all_items = (
                [_make_item(i, False) for i in loaded_items]
                + [_make_item(i, True) for i in not_loaded_items]
            )
            ShipmentItem.objects.bulk_create(all_items)

        serializer = ShipmentSerializer(shipment)
        return Response(serializer.data, status=201)


class ShipmentDetailView(APIView):
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
        # Only DRAFT shipments can be deleted; rejected/approved/dispatched are protected.
        if shipment.status != Shipment.Status.DRAFT:
            return Response(
                {'error': f'Only draft shipments can be deleted. This shipment is "{shipment.get_status_display()}".'},
                status=400,
            )
        # Only the creator (or staff) can delete.
        if shipment.created_by_id and shipment.created_by_id != request.user.id and not request.user.is_staff:
            return Response({'error': 'Only the creator or staff can delete this draft.'}, status=403)
        sid = shipment.id
        shipment.delete()  # cascades to items + audit_logs via FK
        return Response({'deleted': True, 'shipment_id': sid}, status=200)


class ShipmentItemUpdateView(APIView):
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
            new_qty = float(data['new_qty'])
            case_pack = float(item.case_pack or 1)
            if case_pack > 0:
                new_qty = math.floor(new_qty / case_pack) * case_pack
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

class ShipmentSubmitView(APIView):
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

        shipment.status = Shipment.Status.PENDING_APPROVAL
        shipment.save(update_fields=['status'])
        return Response(ShipmentListSerializer(shipment).data)


class ShipmentApproveView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        if not (request.user.is_staff or request.user.is_superuser):
            return Response({'error': 'Manager access required'}, status=403)

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


class ShipmentRejectView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        if not (request.user.is_staff or request.user.is_superuser):
            return Response({'error': 'Manager access required'}, status=403)

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


class ShipmentDispatchView(APIView):
    permission_classes = [IsAuthenticated]

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
    loaded_items = shipment.items.filter(not_loaded=False)
    for item in loaded_items:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT s.id, si.planned_qty
                FROM sp_items si
                JOIN sp_shipments s ON s.id = si.shipment_id
                WHERE si.asin = %s
                  AND UPPER(TRIM(si.po_number)) = UPPER(TRIM(%s))
                  AND s.status IN ('approved','dispatched','in_transit','delivered')
                  AND s.id != %s
                  AND si.not_loaded = FALSE
            """, [item.asin, item.po_number, shipment.id])
            locked = cur.fetchall()

        locked_qty = sum(float(r[1] or 0) for r in locked)
        accepted = float(item.accepted_qty or 0)
        available = accepted - locked_qty
        planned = float(item.planned_qty or 0)

        if planned > available:
            conflicts.append({
                'asin': item.asin,
                'po_number': item.po_number,
                'accepted_qty': accepted,
                'locked_qty': locked_qty,
                'available_qty': available,
                'planned_qty': planned,
                'locked_shipment_ids': [r[0] for r in locked],
            })
    return conflicts


class ShipmentStatsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        from django.db.models import Count, Q
        stats = Shipment.objects.aggregate(
            total=Count('id'),
            draft=Count('id', filter=Q(status='draft')),
            pending=Count('id', filter=Q(status='pending_approval')),
            approved=Count('id', filter=Q(status='approved')),
            dispatched=Count('id', filter=Q(status='dispatched')),
            in_transit=Count('id', filter=Q(status='in_transit')),
            delivered=Count('id', filter=Q(status='delivered')),
            rejected=Count('id', filter=Q(status='rejected')),
        )
        return Response(stats)


class AsinCatalogView(APIView):
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
        doh_by_asin = _live_doh_by_asin()

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


class POListView(APIView):
    """Paginated list of POs from reporting."Amazon PO"."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        po_number = request.query_params.get('po_number', '').strip()
        po_status = request.query_params.get('po_status', '').strip()
        status = request.query_params.get('status', '').strip()
        fc = request.query_params.get('fc', '').strip()
        asin = request.query_params.get('asin', '').strip()
        no_paginate = request.query_params.get('no_paginate', '').lower() == 'true'
        page = max(1, int(request.query_params.get('page', 1)))
        page_size = 9999 if no_paginate else min(5000, int(request.query_params.get('page_size', 50)))
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
                SELECT
                    ap.po_number, ap.asin, ap.merchant_sku, ap.sku_code,
                    ap.sku_name        AS product_name,
                    ap.accepted_qty, ap.cancelled_qty, ap.requested_qty, ap.received_qty,
                    ap.fulfillment_center AS destination_fc,
                    ap.availability_status,
                    ap.status, ap.po_status, ap.item_status,
                    ap.case_pack, ap.per_liter,
                    ap.total_accepted_liters, ap.total_order_liters, ap.days_to_expiry,
                    ap.expiry_date, ap.category, ap.sub_category, ap.brand,
                    ap.item_head, ap.item, ap.order_date,
                    ap.fill_rate,
                    COALESCE(NULLIF(ap.city,''), fcm.city)   AS city,
                    COALESCE(NULLIF(ap.state,''), fcm.state) AS state
                FROM reporting."Amazon PO" ap
                LEFT JOIN public.fc_city_state_channel_master fcm
                    ON UPPER(TRIM(fcm.fc::text)) = UPPER(TRIM(ap.fulfillment_center::text))
                WHERE {where_sql}
                ORDER BY ap.order_date DESC NULLS LAST, ap.po_number
                LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            rows = _row_to_dict(cur, cur.fetchall())

        return Response({
            'results': [_serialize_row(r) for r in rows],
            'count': total,
            'page': page,
            'page_size': page_size,
            'total_pages': math.ceil(total / page_size) if page_size else 1,
        })


class AllAppointmentsView(APIView):
    """All appointments from reporting.appointment with filters."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        status = request.query_params.get('status', '').strip()
        fc = request.query_params.get('fc', '').strip()
        appt_id = request.query_params.get('appointment_id', '').strip()
        date_from = request.query_params.get('date_from', '').strip()
        date_to = request.query_params.get('date_to', '').strip()
        no_paginate = request.query_params.get('no_paginate', '').lower() == 'true'
        page = max(1, int(request.query_params.get('page', 1)))
        page_size = 9999 if no_paginate else min(100, int(request.query_params.get('page_size', 50)))
        offset = 0 if no_paginate else (page - 1) * page_size

        where = ["appointment_time IS NOT NULL"]
        params = []
        if status:
            where.append("LOWER(status) LIKE LOWER(%s)")
            params.append(f'%{status}%')
        if fc:
            where.append("LOWER(destination_fc) LIKE LOWER(%s)")
            params.append(f'%{fc}%')
        if appt_id:
            where.append("LOWER(appointment_id) LIKE LOWER(%s)")
            params.append(f'%{appt_id}%')
        if date_from:
            where.append("DATE(appointment_time) >= %s")
            params.append(date_from)
        if date_to:
            where.append("DATE(appointment_time) <= %s")
            params.append(date_to)

        where_sql = ' AND '.join(where)

        with connection.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT appointment_id
                    FROM reporting."appointment"
                    WHERE {where_sql}
                ) _distinct
            """, params)
            total = cur.fetchone()[0]

            cur.execute(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (appointment_id)
                        appointment_id, status, appointment_time,
                        creation_date, destination_fc, pro,
                        array_to_string(
                            ARRAY(
                                SELECT DISTINCT NULLIF(TRIM(pv),'')
                                FROM unnest(regexp_split_to_array(
                                    COALESCE(pos,''), '\s*[,;]\s*'
                                )) pv
                                WHERE NULLIF(TRIM(pv),'') IS NOT NULL
                            ), ', '
                        ) AS pos,
                        (
                            SELECT COUNT(DISTINCT NULLIF(TRIM(pv),''))
                            FROM unnest(regexp_split_to_array(COALESCE(pos,''),'\s*[,;]\s*')) pv
                            WHERE NULLIF(TRIM(pv),'') IS NOT NULL
                        ) AS po_count
                    FROM reporting."appointment"
                    WHERE {where_sql}
                    ORDER BY appointment_id, appointment_time DESC NULLS LAST
                ) deduped
                ORDER BY appointment_time DESC NULLS LAST
                LIMIT %s OFFSET %s
            """, params + params + [page_size, offset])
            rows = _row_to_dict(cur, cur.fetchall())

        return Response({
            'results': [_serialize_row(r) for r in rows],
            'count': total,
            'page': page,
            'page_size': page_size,
            'total_pages': math.ceil(total / page_size) if page_size else 1,
        })


class ManualPlanView(APIView):
    """Preview a plan from manually selected PO items (no DB writes — Save as Draft persists it)."""
    permission_classes = [IsAuthenticated]

    def post(self, request):
        selected_items = request.data.get('items', [])
        truck_size = request.data.get('truck_size', '15_ton')
        capacity_override = request.data.get('truck_capacity_liters')

        if not selected_items:
            return Response({'error': 'No items selected'}, status=400)

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

        loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
            selected_items, truck_size, capacity_override
        )

        return Response({
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
            'priority_actual': priority_actual,
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


class DOHAutoFillView(APIView):
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

        # 1) Resolve the effective inventory snapshot date (latest available)
        with connection.cursor() as cur:
            cur.execute("""
                SELECT MAX(inventory_date) FROM amazon_master_inventory
            """)
            effective_date = cur.fetchone()[0]

        if not effective_date:
            return Response({
                'loaded_items': [],
                'not_loaded_items': [],
                'urgent_no_po': [],
                'load_summary': {'truck_size': truck_size, 'capacity': _resolve_capacity(truck_size, capacity_override), 'planned_liters': 0, 'load_percentage': 0},
                'priority_breakdown': {},
                'stats': {'total_candidates': 0, 'loaded_count': 0, 'not_loaded_count': 0, 'urgent_no_po_count': 0},
                'source': {'sales': 'amazon_sec_range_master_view', 'inventory': 'amazon_master_inventory'},
                'message': 'No inventory snapshots found in amazon_master_inventory.',
            })

        elapsed_day = max(1, effective_date.day)
        month_name = effective_date.strftime('%B').upper()
        year = effective_date.year
        month_day = f"{effective_date.day:02d}-{effective_date.strftime('%b').upper()}"

        # 2) Compute live DOH per ASIN (mirrors SOH/DOH dashboard logic)
        with connection.cursor() as cur:
            cur.execute("""
                WITH sales AS (
                    SELECT
                        UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                        COALESCE(SUM(shipped_units), 0)::numeric  AS units_sold,
                        COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
                    FROM amazon_sec_range_master_view
                    WHERE "year" = %s
                      AND UPPER(TRIM("month"::text)) = %s
                      AND UPPER(TRIM(month_day::text)) = %s
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
            """, [year, month_name, month_day, year, month_name, effective_date])
            doh_rows = _row_to_dict(cur, cur.fetchall())

        # Compute DOH per ASIN: (soh_unit / drr_unit) - 2, drr = units_sold / elapsed_day
        doh_by_asin = {}
        for r in doh_rows:
            row = _serialize_row(r)
            units_sold = float(row.get('units_sold') or 0)
            ltr_sold = float(row.get('ltr_sold') or 0)
            soh_unit = float(row.get('soh_unit') or 0)
            soh_ltr = float(row.get('soh_ltr') or 0)
            drr_unit = units_sold / elapsed_day if elapsed_day > 0 else 0.0
            drr_ltr = ltr_sold / elapsed_day if elapsed_day > 0 else 0.0
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
                WITH locked_pairs AS (
                    -- Block items committed to ANY shipment, regardless of status.
                    SELECT DISTINCT si.asin, UPPER(TRIM(si.po_number)) AS po_number
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE si.not_loaded = FALSE
                )
                SELECT
                    p.po_number, p.asin,
                    p.merchant_sku       AS internal_sku,
                    p.sku_name           AS product_name,
                    p.accepted_qty, p.case_pack, p.per_liter, p.total_accepted_liters,
                    p.days_to_expiry, p.expiry_date,
                    p.fulfillment_center AS destination_fc,
                    p.category, p.sub_category, p.brand,
                    p.item_head, p.item,
                    p.availability_status, p.po_status, p.status
                FROM reporting."Amazon PO" p
                LEFT JOIN locked_pairs lp
                    ON lp.asin = p.asin AND lp.po_number = UPPER(TRIM(p.po_number))
                WHERE {po_where_sql} AND lp.asin IS NULL
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

        # 6) Single-FC constraint: a truck must contain items from one FC only.
        #    Pick the FC of the most urgent (lowest-DOH) item; items from other FCs
        #    are moved to not_loaded with a reason.
        primary_fc = ''
        if actionable:
            primary_fc = (actionable[0].get('destination_fc') or '').strip().upper()

        if primary_fc:
            same_fc = []
            other_fc = []
            for it in actionable:
                if (it.get('destination_fc') or '').strip().upper() == primary_fc:
                    same_fc.append(it)
                else:
                    it_copy = dict(it)
                    it_copy['skipped_reason'] = f'Different FC ({it.get("destination_fc")}); truck is locked to {actionable[0].get("destination_fc")}'
                    other_fc.append(it_copy)
            loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
                same_fc, truck_size, capacity_override, priority=priority,
            )
            not_loaded = not_loaded + other_fc + no_demand
        else:
            loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
                actionable, truck_size, capacity_override, priority=priority,
            )
            not_loaded = not_loaded + no_demand

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

        return Response({
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
            'urgent_no_po': urgent_no_po,
            'priority_requested': priority,
            'priority_actual': priority_actual,
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
            'effective_date': effective_date.isoformat() if effective_date else None,
            'month': month_name,
            'year': year,
            'month_day': month_day,
            'elapsed_day': elapsed_day,
            'primary_fc': actionable[0].get('destination_fc') if actionable else None,
        })


class ShipmentPendingApprovalsView(APIView):
    """Returns full detail (including items) for all pending-approval shipments."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        qs = Shipment.objects.prefetch_related('items', 'audit_logs').filter(
            status=Shipment.Status.PENDING_APPROVAL
        )
        return Response(ShipmentSerializer(qs, many=True).data)


class PoShipmentLookupView(APIView):
    """
    Live map of (asin, po_number) -> list of shipments that contain that line.

    Used by the planning UI to block re-selection of items already committed to
    another shipment (any status: draft / pending / approved / dispatched / ...).
    Frontend shows a popup with these details when the user tries to select a
    blocked row.
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
            .select_related('shipment', 'shipment__created_by')
            .only(
                'asin', 'po_number', 'planned_qty', 'planned_liters', 'accepted_qty',
                'product_name', 'internal_sku',
                'shipment__id', 'shipment__status', 'shipment__appointment_id',
                'shipment__destination_fc', 'shipment__truck_size',
                'shipment__planned_liters', 'shipment__load_percentage',
                'shipment__created_at', 'shipment__rejection_reason',
                'shipment__dispatch_date_planned', 'shipment__created_by__email',
            )
        )

        result = {}
        for it in items:
            asin = (it.asin or '').strip()
            po = (it.po_number or '').strip()
            if not asin or not po:
                continue
            key = f"{asin}__{po}"
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
            result.setdefault(key, []).append(entry)

        # Sort each list newest-first
        for k in result:
            result[k].sort(key=lambda x: x.get('created_at') or '', reverse=True)

        return Response(result)
