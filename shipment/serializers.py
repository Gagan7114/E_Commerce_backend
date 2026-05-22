from datetime import timedelta

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
    class Meta:
        model = ShipmentItem
        fields = '__all__'


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

    class Meta:
        model = Shipment
        fields = '__all__'

    def get_created_by_email(self, obj):
        return obj.created_by.email if obj.created_by else None

    def get_approved_by_email(self, obj):
        return obj.approved_by.email if obj.approved_by else None

    def get_is_stale_draft(self, obj):
        return _is_stale_draft(obj)


class ShipmentListSerializer(serializers.ModelSerializer):
    created_by_email = serializers.SerializerMethodField()
    item_count = serializers.SerializerMethodField()
    is_stale_draft = serializers.SerializerMethodField()

    class Meta:
        model = Shipment
        fields = [
            'id', 'appointment_id', 'appointment_time', 'destination_fc',
            'pro', 'truck_size', 'truck_capacity_liters', 'planned_liters',
            'load_percentage', 'auto_planned', 'planning_mode', 'status',
            'created_by_email', 'item_count', 'created_at', 'updated_at',
            'vehicle_type', 'vehicle_number', 'driver_name', 'driver_phone',
            'dispatch_date_planned', 'notes', 'is_stale_draft',
        ]

    def get_created_by_email(self, obj):
        return obj.created_by.email if obj.created_by else None

    def get_item_count(self, obj):
        return obj.items.filter(not_loaded=False).count()

    def get_is_stale_draft(self, obj):
        return _is_stale_draft(obj)
