from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone

from .managers import UserManager


class User(AbstractUser):
    username = None
    email = models.EmailField(unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []

    objects = UserManager()

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return self.email


class InventoryDohNotification(models.Model):
    ALERT_TYPE = "INVENTORY_DOH_LOW"

    class Severity(models.TextChoices):
        WARNING = "warning", "Warning"
        CRITICAL = "critical", "Critical"

    alert_type = models.CharField(max_length=64, default=ALERT_TYPE)
    format = models.CharField(max_length=64, db_index=True)
    platform_slug = models.CharField(max_length=64, blank=True, db_index=True)
    sku_code = models.CharField(max_length=128, db_index=True)
    sku_name = models.TextField(blank=True)
    item = models.CharField(max_length=255, blank=True, db_index=True)
    item_head = models.CharField(max_length=128, blank=True, db_index=True)
    category = models.CharField(max_length=255, blank=True)
    sub_category = models.CharField(max_length=255, blank=True)
    brand = models.CharField(max_length=255, blank=True)
    inventory_date = models.DateField(db_index=True)
    sales_max_date = models.DateField(null=True, blank=True)
    month_start = models.DateField(null=True, blank=True)
    units_sold = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    ltr_sold = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    soh_units = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    soh_ltr = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    drr_units = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    drr_ltr = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    doh = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    threshold = models.DecimalField(max_digits=10, decimal_places=4, default=10)
    severity = models.CharField(
        max_length=16,
        choices=Severity.choices,
        default=Severity.WARNING,
        db_index=True,
    )
    title = models.CharField(max_length=255)
    message = models.TextField(blank=True)
    payload = models.JSONField(default=dict, blank=True)
    is_read = models.BooleanField(default=False, db_index=True)
    resolved_at = models.DateTimeField(null=True, blank=True, db_index=True)
    first_seen_at = models.DateTimeField(default=timezone.now)
    last_seen_at = models.DateTimeField(default=timezone.now, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-last_seen_at", "severity", "format", "sku_code"]
        constraints = [
            models.UniqueConstraint(
                fields=["alert_type", "format", "sku_code", "inventory_date"],
                name="uniq_inventory_doh_notification_snapshot",
            )
        ]
        indexes = [
            models.Index(fields=["format", "inventory_date", "doh"]),
            models.Index(fields=["platform_slug", "resolved_at", "is_read"]),
        ]

    @property
    def is_active(self) -> bool:
        return self.resolved_at is None

    def __str__(self) -> str:
        return f"{self.format} {self.sku_code} DOH {self.doh}"
