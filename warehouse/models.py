"""Reflected read-only warehouse tables.

These mirror the physical Postgres tables managed by the external uploader
tool. They are declared with `managed = False` so Django will never issue DDL
against them — migrations only register them for the admin.

Column sets below are PLACEHOLDERS. Replace them with the real schemas using:

    python manage.py inspectdb master_po blinkit_inventory ... > /tmp/out.py

Then copy the generated field definitions in here and keep `managed = False`.
"""

from django.db import models


class _ReadOnlyBase(models.Model):
    """Adds the shared `managed = False` + no-write guard."""

    class Meta:
        abstract = True
        managed = False

    def save(self, *args, **kwargs):
        raise RuntimeError(f"{self.__class__.__name__} is read-only")

    def delete(self, *args, **kwargs):
        raise RuntimeError(f"{self.__class__.__name__} is read-only")


class MasterPO(_ReadOnlyBase):
    platform = models.CharField(max_length=40)
    po_number = models.CharField(max_length=80, primary_key=True)
    created_at = models.DateTimeField(null=True, blank=True)
    expected_delivery = models.DateField(null=True, blank=True)
    status = models.CharField(max_length=40, blank=True)
    total_value = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    class Meta(_ReadOnlyBase.Meta):
        db_table = "master_po"
        verbose_name = "Master PO"
        verbose_name_plural = "Master POs"

    def __str__(self) -> str:
        return f"{self.platform}:{self.po_number}"


def _inventory_model(platform_slug: str):
    class Meta(_ReadOnlyBase.Meta):
        db_table = f"{platform_slug}_inventory"
        verbose_name = f"{platform_slug.title()} Inventory"
        verbose_name_plural = f"{platform_slug.title()} Inventory"

    attrs = {
        "sku": models.CharField(max_length=80, primary_key=True),
        "product_name": models.CharField(max_length=240, blank=True),
        "quantity": models.IntegerField(null=True, blank=True),
        "expiry_date": models.DateField(null=True, blank=True),
        "updated_at": models.DateTimeField(null=True, blank=True),
        "__module__": __name__,
        "Meta": Meta,
    }
    return type(f"{platform_slug.title()}Inventory", (_ReadOnlyBase,), attrs)


def _secondary_model(platform_slug: str):
    class Meta(_ReadOnlyBase.Meta):
        db_table = f"{platform_slug}_secondary"
        verbose_name = f"{platform_slug.title()} Secondary Sales"
        verbose_name_plural = f"{platform_slug.title()} Secondary Sales"

    attrs = {
        "id": models.BigIntegerField(primary_key=True),
        "sku": models.CharField(max_length=80, blank=True),
        "quantity_sold": models.IntegerField(null=True, blank=True),
        "sale_date": models.DateField(null=True, blank=True),
        "__module__": __name__,
        "Meta": Meta,
    }
    return type(f"{platform_slug.title()}Secondary", (_ReadOnlyBase,), attrs)


_PLATFORMS = ["blinkit", "zepto", "jiomart", "amazon", "bigbasket", "swiggy", "flipkart"]

for _slug in _PLATFORMS:
    _inv = _inventory_model(_slug)
    _sec = _secondary_model(_slug)
    globals()[_inv.__name__] = _inv
    globals()[_sec.__name__] = _sec
