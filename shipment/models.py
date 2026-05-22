from django.conf import settings
from django.db import models


class AsinDohDaily(models.Model):
    date = models.DateField()
    month = models.TextField(blank=True)
    year = models.IntegerField(null=True)
    asin = models.TextField()
    units_sold = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    ltr_sold = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    soh_unit = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    soh_ltr = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    drr_unit = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    drr_ltr = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    doh = models.DecimalField(max_digits=10, decimal_places=4, null=True)

    class Meta:
        unique_together = ('date', 'asin')
        db_table = 'sp_asin_doh_daily'


class Shipment(models.Model):
    class Status(models.TextChoices):
        DRAFT = 'draft', 'Draft'
        PENDING_APPROVAL = 'pending_approval', 'Pending Approval'
        APPROVED = 'approved', 'Approved'
        REJECTED = 'rejected', 'Rejected'
        DISPATCHED = 'dispatched', 'Dispatched'
        IN_TRANSIT = 'in_transit', 'In Transit'
        DELIVERED = 'delivered', 'Delivered'

    class PlanningMode(models.TextChoices):
        MANUAL = 'manual', 'Manual'
        APPOINTMENT = 'appointment', 'With Appointment'
        DOH = 'doh', 'With DOH'

    appointment_id = models.TextField(blank=True)
    appointment_time = models.DateTimeField(null=True)
    destination_fc = models.TextField(blank=True)
    pro = models.TextField(blank=True)
    truck_size = models.CharField(max_length=16, blank=True)
    truck_capacity_liters = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    planned_liters = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    load_percentage = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    auto_planned = models.BooleanField(default=False)
    planning_mode = models.CharField(
        max_length=16, choices=PlanningMode.choices, default=PlanningMode.MANUAL, blank=True
    )
    vehicle_type = models.TextField(blank=True)
    driver_name = models.TextField(blank=True)
    driver_phone = models.TextField(blank=True)
    vehicle_number = models.TextField(blank=True)
    dispatch_date_planned = models.DateField(null=True, blank=True)
    notes = models.TextField(blank=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.DRAFT)
    rejection_reason = models.TextField(blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='created_shipments',
    )
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='approved_shipments',
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'sp_shipments'
        ordering = ['-created_at']


class ShipmentItem(models.Model):
    shipment = models.ForeignKey(Shipment, on_delete=models.CASCADE, related_name='items')
    appointment_id = models.TextField(blank=True)
    po_number = models.TextField(blank=True)
    asin = models.TextField(blank=True)
    internal_sku = models.TextField(blank=True)
    product_name = models.TextField(blank=True)
    destination_fc = models.TextField(blank=True)
    category = models.TextField(blank=True)
    sub_category = models.TextField(blank=True)
    brand = models.TextField(blank=True)
    item_head = models.TextField(blank=True)
    item = models.TextField(blank=True)
    availability_status = models.TextField(blank=True)
    po_status = models.TextField(blank=True)
    status = models.TextField(blank=True)
    accepted_qty = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    available_qty = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    planned_qty = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    planned_liters = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    per_liter = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    case_pack = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    doh = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    drr_unit = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    soh_unit = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    days_to_expiry = models.IntegerField(null=True)
    expiry_date = models.DateField(null=True, blank=True)
    priority_bucket = models.TextField(blank=True)
    priority_score = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    priority_reason = models.TextField(blank=True)
    is_auto_selected = models.BooleanField(default=True)
    is_changed = models.BooleanField(default=False)
    change_reason = models.TextField(blank=True)
    not_loaded = models.BooleanField(default=False)

    class Meta:
        db_table = 'sp_items'


class ShipmentAuditLog(models.Model):
    shipment = models.ForeignKey(Shipment, on_delete=models.CASCADE, related_name='audit_logs')
    changed_by = models.TextField()
    change_type = models.TextField()
    old_asin = models.TextField(blank=True)
    new_asin = models.TextField(blank=True)
    old_sku = models.TextField(blank=True)
    new_sku = models.TextField(blank=True)
    old_qty = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    new_qty = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    reason = models.TextField()
    reason_note = models.TextField(blank=True)
    changed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'sp_audit_log'
        ordering = ['-changed_at']
