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
    # Multi-appointment support: when a single truck services multiple
    # appointments at the same FC, `appointment_id` holds the PRIMARY
    # (majority by loaded liters) and these two fields carry the rest.
    additional_appointment_ids = models.TextField(blank=True, default='')
    appointments_meta = models.JSONField(default=list, blank=True)
    # Frozen snapshot of the Amazon Vendor Central commitment vs what was loaded,
    # per appointment, captured at save time so Review/Print/Draft always show the
    # same numbers. Shape: [{appointment_id, destination_fc, committed_units,
    # committed_cartons, filled_units, filled_cartons}]
    commitment_snapshot = models.JSONField(default=list, blank=True)
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
        indexes = [
            # Status is filtered/counted on nearly every shipment read — the list
            # (status filter), stats/KPI conditional counts, pending-approvals,
            # and the `status != 'rejected'` join guard used by the plan-review /
            # appointment / PO-list aggregates. Without this it was a sequential
            # scan of sp_shipments every time.
            models.Index(fields=['status'], name='sp_shipments_status_idx'),
        ]


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
    # Inventory/warehouse this line's stock was auto-pooled from at plan time
    # (BH-FGM = Jivo Mart, DL-EC = Jivo Wellness). Empty for legacy rows planned
    # before pooling, or when the ASIN mapped to no planner warehouse.
    source_warehouse = models.TextField(blank=True)
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
    # Why an item wasn't fully shipped — captured from the planner at draft time so
    # the Record/audit view can show it. unfit_reason: why a NOT-loaded item couldn't
    # ship; short_reason: why a loaded item shipped partial (short-supplied).
    unfit_reason = models.TextField(blank=True)
    short_reason = models.TextField(blank=True)

    class Meta:
        db_table = 'sp_items'
        indexes = [
            # Almost every planner/dashboard query scans loaded items grouped/joined
            # by (asin, po_number) with not_loaded=FALSE (availability checks,
            # committed/locked lookups, short-supply, the advisory-lock save guard).
            # A partial composite index keeps that to an index scan over only the
            # loaded rows instead of a full table scan.
            models.Index(
                fields=['asin', 'po_number'],
                condition=models.Q(not_loaded=False),
                name='sp_items_loaded_asin_po',
            ),
        ]


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


class ShipmentPoDocument(models.Model):
    """One uploaded PO document (PDF) per (shipment, PO).

    Stored inline in the DB as bytes — the project has no external file storage.
    Replaceable: re-uploading a PO overwrites its row (unique on shipment + po)."""
    shipment = models.ForeignKey(Shipment, on_delete=models.CASCADE, related_name='po_documents')
    po_number = models.CharField(max_length=64)
    file_name = models.CharField(max_length=255)
    content_type = models.CharField(max_length=100, default='application/pdf')
    size = models.IntegerField(default=0)
    data = models.BinaryField()
    uploaded_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    uploaded_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'sp_po_document'
        unique_together = ('shipment', 'po_number')
        ordering = ['po_number']


class ShipmentInvoice(models.Model):
    """Invoice PDF(s) for a shipment — one OR MORE per shipment, each tagged with
    the PO it belongs to, stored inline in the DB as bytes (no external file
    storage). A shipment can carry several invoices (e.g. one per PO); each is an
    independent row (ForeignKey, not OneToOne)."""
    shipment = models.ForeignKey(Shipment, on_delete=models.CASCADE, related_name='invoices')
    po_number = models.CharField(max_length=255, blank=True, default='')
    file_name = models.CharField(max_length=255)
    content_type = models.CharField(max_length=100, default='application/pdf')
    size = models.IntegerField(default=0)
    data = models.BinaryField()
    uploaded_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    uploaded_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'sp_invoice'
        ordering = ['id']


class ShipmentDeletionLog(models.Model):
    """Append-only record of a deleted shipment. The shipment (and its cascade
    of items + audit logs) is gone after a delete, so this is the only durable,
    in-app trace of who deleted what, when, and the shipment's state at the time.
    shipment_id is a plain int (not a FK) because the row it referenced no longer
    exists; emails are snapshotted so they survive later user deletions."""
    shipment_id = models.IntegerField()
    status = models.CharField(max_length=32, blank=True)
    planning_mode = models.CharField(max_length=16, blank=True)
    appointment_id = models.TextField(blank=True)
    destination_fc = models.TextField(blank=True)
    loaded_item_count = models.IntegerField(default=0)
    planned_liters = models.DecimalField(max_digits=14, decimal_places=4, null=True)
    created_by_email = models.TextField(blank=True)
    deleted_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    deleted_by_email = models.TextField(blank=True)
    deleted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'sp_deletion_log'
        ordering = ['-deleted_at']
