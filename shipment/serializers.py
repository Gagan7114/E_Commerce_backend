from datetime import timedelta

from django.db import connection
from django.utils import timezone
from rest_framework import serializers

from .models import Shipment, ShipmentItem, ShipmentAuditLog


# Drafts older than this many days are flagged stale in list views so planners
# can spot abandoned drafts that are still holding their PO rows hostage.
STALE_DRAFT_DAYS = 3


def _is_stale_draft(obj):
    if obj.status != Shipment.Status.DRAFT or not obj.created_at:
        return False
    return (timezone.now() - obj.created_at) > timedelta(days=STALE_DRAFT_DAYS)


class ShipmentItemSerializer(serializers.ModelSerializer):
    source_inventory = serializers.SerializerMethodField()

    class Meta:
        model = ShipmentItem
        fields = '__all__'

    def get_source_inventory(self, obj):
        """Human inventory name for the saved source_warehouse code (e.g.
        BH-FGM → 'Jivo Mart'). Lazy import to avoid a views↔serializers cycle."""
        try:
            from .views import _inventory_label
            return _inventory_label(obj.source_warehouse)
        except Exception:
            return None


class ShipmentAuditLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShipmentAuditLog
        fields = '__all__'


class ShipmentSerializer(serializers.ModelSerializer):
    items = ShipmentItemSerializer(many=True, read_only=True)
    audit_logs = ShipmentAuditLogSerializer(many=True, read_only=True)
    created_by_email = serializers.SerializerMethodField()
    approved_by_email = serializers.SerializerMethodField()
    is_stale_draft = serializers.SerializerMethodField()
    channel = serializers.SerializerMethodField()

    class Meta:
        model = Shipment
        fields = '__all__'

    def get_created_by_email(self, obj):
        return obj.created_by.email if obj.created_by else None

    def get_approved_by_email(self, obj):
        return obj.approved_by.email if obj.approved_by else None

    def get_is_stale_draft(self, obj):
        return _is_stale_draft(obj)

    def get_channel(self, obj):
        """Channel (CORE/FRESH/NOW) mapped to this shipment's FC.

        Reads the cached {UPPER(TRIM(fc)) -> channel} map (5-min TTL, near-static
        master) instead of querying fc_city_state_channel_master once per object
        — important when this serializer renders a list (pending-approvals)."""
        fc = (obj.destination_fc or '').strip()
        if not fc:
            return None
        try:
            from .views import _fc_channel_map
            return _fc_channel_map().get(fc.upper()) or None
        except Exception:
            return None

    def to_representation(self, instance):
        """Enrich each saved item with `cost_price` (per-unit basic cost) from
        reporting."Amazon PO", so the PDF/print Basic-Rate / Landing columns work
        for saved shipments too (sp_items doesn't store cost). One batched query
        keyed on ASIN; exact PO match preferred, else any PO for that ASIN."""
        data = super().to_representation(instance)
        items = data.get('items') or []
        asins = list({str(it.get('asin') or '').strip().upper() for it in items if it.get('asin')})
        if not asins:
            return data
        ph = ','.join(['%s'] * len(asins))
        pair_cost, asin_cost = {}, {}
        pair_sku, asin_sku = {}, {}
        try:
            with connection.cursor() as cur:
                cur.execute(
                    f'''SELECT UPPER(TRIM(po_number)), UPPER(TRIM(asin)),
                               MAX(cost_price), MAX(sap_sku_code)
                        FROM reporting."Amazon PO"
                        WHERE UPPER(TRIM(asin)) IN ({ph})
                        GROUP BY UPPER(TRIM(po_number)), UPPER(TRIM(asin))''',
                    asins,
                )
                for po, asin, cp, sku in cur.fetchall():
                    if cp is not None:
                        pair_cost[(po, asin)] = cp
                        asin_cost[asin] = cp
                    if sku:
                        pair_sku[(po, asin)] = sku
                        asin_sku[asin] = sku
        except Exception:
            return data
        for it in items:
            asin = str(it.get('asin') or '').strip().upper()
            po = str(it.get('po_number') or '').strip().upper()
            cp = pair_cost.get((po, asin)) or asin_cost.get(asin)
            it['cost_price'] = float(cp) if cp is not None else None
            it['sap_sku_code'] = pair_sku.get((po, asin)) or asin_sku.get(asin)
        return data


class ShipmentListSerializer(serializers.ModelSerializer):
    created_by_email = serializers.SerializerMethodField()
    item_count = serializers.SerializerMethodField()
    is_stale_draft = serializers.SerializerMethodField()
    channel = serializers.SerializerMethodField()
    summary = serializers.SerializerMethodField()
    approved_by_email = serializers.SerializerMethodField()

    class Meta:
        model = Shipment
        fields = [
            'id', 'appointment_id', 'appointment_time', 'destination_fc', 'channel',
            'pro', 'truck_size', 'truck_capacity_liters', 'planned_liters',
            'load_percentage', 'auto_planned', 'planning_mode', 'status',
            'created_by_email', 'item_count', 'created_at', 'updated_at',
            'vehicle_type', 'vehicle_number', 'driver_name', 'driver_phone',
            'dispatch_date_planned', 'notes', 'is_stale_draft', 'summary',
            'approved_by_email',
        ]

    def get_created_by_email(self, obj):
        return obj.created_by.email if obj.created_by else None

    def get_approved_by_email(self, obj):
        # `approved_by` is set at approval and never cleared, so it survives into
        # dispatched/delivered too. The list view already select_relates it.
        return obj.approved_by.email if obj.approved_by_id else None

    def get_item_count(self, obj):
        # Prefer the `loaded_item_count` annotation set by the list view (one
        # grouped query for the whole page). Fall back to a per-object count if
        # the serializer is ever used on an un-annotated queryset.
        count = getattr(obj, 'loaded_item_count', None)
        if count is not None:
            return count
        return obj.items.filter(not_loaded=False).count()

    def get_is_stale_draft(self, obj):
        return _is_stale_draft(obj)

    def get_channel(self, obj):
        """Channel (CORE / FRESH / NOW) mapped to this shipment's FC, via the
        cached FC→channel map (near-static master, no per-object query)."""
        fc = (obj.destination_fc or '').strip()
        if not fc:
            return None
        try:
            from .views import _fc_channel_map
            return _fc_channel_map().get(fc.upper()) or None
        except Exception:
            return None

    def get_summary(self, obj):
        """Compact rollup for the list card: category tonnes (Premium / Commodity /
        Other), distinct PO / SKU counts, total units & cartons (from the prefetched
        loaded items) and the Vendor-Central committed/filled totals (from
        commitment_snapshot). Mirrors the ShipmentDetail summary header."""
        items = getattr(obj, 'loaded_items_pref', None)
        if items is None:
            items = list(obj.items.filter(not_loaded=False))
        prem = comm = other = 0.0
        units = cartons = 0.0
        pos, skus = set(), set()
        for it in items:
            liters = float(it.planned_liters or 0)
            head = (it.item_head or '').lower()
            if 'premium' in head:
                prem += liters
            elif 'commodity' in head:
                comm += liters
            else:
                other += liters
            q = float(it.planned_qty or 0)
            cp = max(float(it.case_pack or 1), 1.0)
            units += q
            cartons += q / cp
            if it.po_number:
                pos.add(it.po_number.strip().upper())
            key = it.asin or it.internal_sku
            if key:
                skus.add(str(key).strip().upper())
        snap = obj.commitment_snapshot if isinstance(obj.commitment_snapshot, list) else []
        cu = sum(float(r.get('committed_units') or 0) for r in snap)
        fu = sum(float(r.get('filled_units') or 0) for r in snap)
        cc = sum(float(r.get('committed_cartons') or 0) for r in snap)
        fcar = sum(float(r.get('filled_cartons') or 0) for r in snap)
        return {
            'po_count': len(pos),
            'sku_count': len(skus),
            'total_units': int(round(units)),
            'total_cartons': int(round(cartons)),
            'total_tonnes': round((prem + comm + other) / 1000, 2),
            'premium_tonnes': round(prem / 1000, 2),
            'commodity_tonnes': round(comm / 1000, 2),
            'other_tonnes': round(other / 1000, 2),
            'committed_units': int(round(cu)),
            'filled_units': int(round(fu)),
            'committed_cartons': int(round(cc)),
            'filled_cartons': int(round(fcar)),
        }
