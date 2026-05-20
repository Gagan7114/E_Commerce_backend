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


def _auto_plan_truck(items, truck_size):
    capacity = TRUCK_CAPACITIES.get(truck_size, 15000.0)
    remaining = capacity
    loaded, not_loaded = [], []

    for item in items:
        total_liters = float(item.get('total_accepted_liters') or 0)
        per_liter = float(item.get('per_liter') or 0)
        case_pack = float(item.get('case_pack') or 1)
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

    planned = round(capacity - remaining, 4)
    load_pct = round((planned / capacity * 100) if capacity > 0 else 0, 2)
    return loaded, not_loaded, capacity, planned, load_pct


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


# ---------------------------------------------------------------------------
# Appointment endpoints
# ---------------------------------------------------------------------------

class AppointmentDatesView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        with connection.cursor() as cur:
            cur.execute("""
                SELECT DATE(appointment_time) AS appt_date,
                       COUNT(DISTINCT appointment_id) AS appt_count
                FROM reporting."appointment"
                WHERE status = 'Confirmed'
                  AND appointment_time IS NOT NULL
                GROUP BY DATE(appointment_time)
                ORDER BY appt_date
            """)
            rows = cur.fetchall()
        dates = [row[0].isoformat() for row in rows if row[0]]
        counts = {row[0].isoformat(): row[1] for row in rows if row[0]}
        return Response({'dates': dates, 'counts': counts})


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
                    SELECT DISTINCT si.asin, UPPER(TRIM(si.po_number)) AS po_number
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE s.status IN ('approved','dispatched','in_transit','delivered')
                      AND si.not_loaded = FALSE
                ),
                doh_data AS (
                    SELECT asin, soh_unit, soh_ltr, drr_unit, drr_ltr, doh
                    FROM sp_asin_doh_daily
                    WHERE date = (SELECT MAX(date) FROM sp_asin_doh_daily)
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
                    p.fulfillment_center,
                    COALESCE(d.soh_unit, 0)  AS soh_unit,
                    COALESCE(d.soh_ltr, 0)   AS soh_ltr,
                    COALESCE(d.drr_unit, 0)  AS drr_unit,
                    COALESCE(d.drr_ltr, 0)   AS drr_ltr,
                    COALESCE(d.doh, 0)        AS doh
                FROM appt_pos ap
                JOIN reporting."Amazon PO" p
                    ON UPPER(TRIM(p.po_number)) = ap.po_number
                    AND p.fulfillment_center = %s
                LEFT JOIN doh_data d ON d.asin = p.asin
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

        if not raw:
            return Response({
                'appointment': appt,
                'loaded_items': [],
                'not_loaded_items': [],
                'load_summary': {
                    'truck_size': truck_size,
                    'capacity': TRUCK_CAPACITIES.get(truck_size, 15000),
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

        loaded, not_loaded, capacity, planned_liters, load_pct = _auto_plan_truck(items, truck_size)

        return Response({
            'appointment': appt,
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
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

        with transaction.atomic():
            shipment = Shipment.objects.create(
                appointment_id=appointment_id or '',
                appointment_time=appointment.get('appointment_time') if appointment else None,
                destination_fc=(appointment or {}).get('destination_fc', data.get('destination_fc', '')),
                pro=(appointment or {}).get('pro', ''),
                truck_size=truck_size,
                truck_capacity_liters=load_summary.get('capacity'),
                planned_liters=load_summary.get('planned_liters'),
                load_percentage=load_summary.get('load_percentage'),
                auto_planned=bool(appointment_id),
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
                return ShipmentItem(
                    shipment=shipment,
                    appointment_id=appointment_id,
                    po_number=item_data.get('po_number', ''),
                    asin=item_data.get('asin', ''),
                    internal_sku=item_data.get('internal_sku', ''),
                    product_name=item_data.get('product_name', ''),
                    category=item_data.get('category', ''),
                    sub_category=item_data.get('sub_category', ''),
                    brand=item_data.get('brand', ''),
                    item_head=item_data.get('item_head', ''),
                    item=item_data.get('item', ''),
                    availability_status=item_data.get('availability_status', ''),
                    po_status=item_data.get('po_status', ''),
                    status=item_data.get('status', ''),
                    accepted_qty=item_data.get('accepted_qty'),
                    available_qty=item_data.get('accepted_qty'),
                    planned_qty=item_data.get('planned_qty', 0) if not not_loaded else 0,
                    planned_liters=item_data.get('planned_liters', 0) if not not_loaded else 0,
                    per_liter=item_data.get('per_liter'),
                    case_pack=item_data.get('case_pack'),
                    doh=item_data.get('doh'),
                    drr_unit=item_data.get('drr_unit'),
                    soh_unit=item_data.get('soh_unit'),
                    days_to_expiry=item_data.get('days_to_expiry'),
                    priority_bucket=item_data.get('priority_bucket', ''),
                    priority_score=item_data.get('priority_score'),
                    priority_reason=item_data.get('priority_reason', ''),
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

        allowed = ['driver_name', 'driver_phone', 'vehicle_number', 'vehicle_type']
        for field in allowed:
            if field in request.data:
                setattr(shipment, field, request.data[field])
        shipment.save(update_fields=allowed)
        return Response(ShipmentSerializer(shipment).data)


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

            # DOH from latest daily snapshot
            cur.execute("""
                SELECT asin, doh, drr_unit, soh_unit
                FROM sp_asin_doh_daily
                WHERE date = (SELECT MAX(date) FROM sp_asin_doh_daily)
            """)
            doh_rows = {r['asin']: r for r in _row_to_dict(cur, cur.fetchall())}

        catalog = {}
        for r in po_rows:
            entry = _serialize_row(r)
            doh = doh_rows.get(r['asin'], {})
            entry['doh'] = float(doh.get('doh') or 0) if doh.get('doh') else None
            entry['drr_unit'] = float(doh.get('drr_unit') or 0) if doh.get('drr_unit') else None
            entry['soh_unit'] = float(doh.get('soh_unit') or 0) if doh.get('soh_unit') else None
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
    """Create a draft shipment from manually selected PO items."""
    permission_classes = [IsAuthenticated]

    def post(self, request):
        selected_items = request.data.get('items', [])
        truck_size = request.data.get('truck_size', '15_ton')

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

        loaded, not_loaded, capacity, planned_liters, load_pct = _auto_plan_truck(
            selected_items, truck_size
        )

        with transaction.atomic():
            shipment = Shipment.objects.create(
                truck_size=truck_size,
                truck_capacity_liters=capacity,
                planned_liters=planned_liters,
                load_percentage=load_pct,
                auto_planned=False,
                status=Shipment.Status.DRAFT,
                created_by=request.user,
            )

            def _make_item(item_data, not_loaded=False):
                return ShipmentItem(
                    shipment=shipment,
                    po_number=item_data.get('po_number', ''),
                    asin=item_data.get('asin', ''),
                    internal_sku=item_data.get('merchant_sku') or item_data.get('internal_sku', ''),
                    product_name=item_data.get('sku_name') or item_data.get('product_name', ''),
                    category=item_data.get('category', ''),
                    sub_category=item_data.get('sub_category', ''),
                    brand=item_data.get('brand', ''),
                    item_head=item_data.get('item_head', ''),
                    item=item_data.get('item', ''),
                    availability_status=item_data.get('availability_status', ''),
                    po_status=item_data.get('po_status', ''),
                    accepted_qty=item_data.get('accepted_qty'),
                    available_qty=item_data.get('accepted_qty'),
                    planned_qty=item_data.get('planned_qty', 0) if not not_loaded else 0,
                    planned_liters=item_data.get('planned_liters', 0) if not not_loaded else 0,
                    per_liter=item_data.get('per_liter'),
                    case_pack=item_data.get('case_pack'),
                    days_to_expiry=item_data.get('days_to_expiry'),
                    priority_bucket=item_data.get('priority_bucket', ''),
                    priority_score=item_data.get('priority_score'),
                    priority_reason=item_data.get('priority_reason', ''),
                    is_auto_selected=False,
                    not_loaded=not_loaded,
                )

            ShipmentItem.objects.bulk_create(
                [_make_item(i, False) for i in loaded]
                + [_make_item(i, True) for i in not_loaded]
            )

        return Response(ShipmentSerializer(shipment).data, status=201)


class DOHAutoFillView(APIView):
    """
    Auto-fill a truck with the most urgent items by DOH priority.
    Scans ALL pending confirmed POs (optionally filtered by FC),
    scores each by the priority engine, sorts CRITICAL→HOLD with
    FEFO tiebreaker, then fills the truck greedily.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        truck_size = request.query_params.get('truck_size', '15_ton')
        fc = request.query_params.get('fc', '').strip()

        where = [
            "p.status = 'Confirmed'",
            "p.availability_status = 'AC - Accepted: In stock'",
            "p.accepted_qty > 0",
            "p.po_status = 'PENDING'",
            "p.per_liter IS NOT NULL",
            "p.per_liter > 0",
        ]
        params = []
        if fc:
            where.append("LOWER(p.fulfillment_center) LIKE LOWER(%s)")
            params.append(f'%{fc}%')

        where_sql = ' AND '.join(where)

        with connection.cursor() as cur:
            cur.execute(f"""
                WITH locked_pairs AS (
                    SELECT DISTINCT si.asin, UPPER(TRIM(si.po_number)) AS po_number
                    FROM sp_items si
                    JOIN sp_shipments s ON s.id = si.shipment_id
                    WHERE s.status IN ('approved','dispatched','in_transit','delivered')
                      AND si.not_loaded = FALSE
                ),
                doh_data AS (
                    SELECT asin, soh_unit, soh_ltr, drr_unit, drr_ltr, doh
                    FROM sp_asin_doh_daily
                    WHERE date = (SELECT MAX(date) FROM sp_asin_doh_daily)
                )
                SELECT
                    p.po_number,
                    p.asin,
                    p.merchant_sku           AS internal_sku,
                    p.sku_name               AS product_name,
                    p.accepted_qty,
                    p.case_pack,
                    p.per_liter,
                    p.total_accepted_liters,
                    p.days_to_expiry,
                    p.expiry_date,
                    p.fulfillment_center     AS destination_fc,
                    p.category,
                    p.sub_category,
                    p.brand,
                    p.item_head,
                    p.item,
                    p.availability_status,
                    p.po_status,
                    p.status,
                    COALESCE(d.soh_unit, 0)  AS soh_unit,
                    COALESCE(d.soh_ltr,  0)  AS soh_ltr,
                    COALESCE(d.drr_unit, 0)  AS drr_unit,
                    COALESCE(d.drr_ltr,  0)  AS drr_ltr,
                    COALESCE(d.doh,      0)  AS doh
                FROM reporting."Amazon PO" p
                LEFT JOIN doh_data d ON d.asin = p.asin
                LEFT JOIN locked_pairs lp
                    ON lp.asin = p.asin
                    AND lp.po_number = UPPER(TRIM(p.po_number))
                WHERE {where_sql}
                  AND lp.asin IS NULL
            """, params)
            raw = _row_to_dict(cur, cur.fetchall())

        capacity = TRUCK_CAPACITIES.get(truck_size, 15000)
        if not raw:
            return Response({
                'loaded_items': [],
                'not_loaded_items': [],
                'load_summary': {'truck_size': truck_size, 'capacity': capacity, 'planned_liters': 0, 'load_percentage': 0},
                'priority_breakdown': {},
                'stats': {'total_candidates': 0, 'loaded_count': 0, 'not_loaded_count': 0},
                'message': 'No eligible PO items found. All confirmed POs may already be dispatched or locked.',
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

        items.sort(key=lambda x: (
            -x['priority_score'],
            x.get('days_to_expiry') or 999,
            -(float(x.get('accepted_qty') or 0)),
        ))

        loaded, not_loaded, capacity, planned_liters, load_pct = _auto_plan_truck(items, truck_size)

        breakdown = {}
        for item in loaded:
            b = item.get('priority_bucket', 'HOLD')
            breakdown[b] = breakdown.get(b, 0) + 1

        return Response({
            'loaded_items': loaded,
            'not_loaded_items': not_loaded,
            'load_summary': {
                'truck_size': truck_size,
                'capacity': capacity,
                'planned_liters': planned_liters,
                'load_percentage': load_pct,
            },
            'priority_breakdown': breakdown,
            'stats': {
                'total_candidates': len(items),
                'loaded_count': len(loaded),
                'not_loaded_count': len(not_loaded),
            },
        })


class ShipmentPendingApprovalsView(APIView):
    """Returns full detail (including items) for all pending-approval shipments."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        qs = Shipment.objects.prefetch_related('items', 'audit_logs').filter(
            status=Shipment.Status.PENDING_APPROVAL
        )
        return Response(ShipmentSerializer(qs, many=True).data)
