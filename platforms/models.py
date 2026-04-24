from django.core.validators import RegexValidator
from django.db import models

SLUG_REGEX = RegexValidator(
    regex=r"^[a-z][a-z0-9_]*$",
    message="Slug must be lowercase letters/digits/underscores, starting with a letter.",
)


class PlatformConfig(models.Model):
    slug = models.SlugField(max_length=40, unique=True, validators=[SLUG_REGEX])
    name = models.CharField(max_length=80)
    inventory_table = models.CharField(max_length=80, blank=True)
    secondary_table = models.CharField(max_length=80, blank=True)
    master_po_table = models.CharField(max_length=80, default="master_po")
    po_filter_column = models.CharField(max_length=80, blank=True, default="platform")
    po_filter_value = models.CharField(max_length=80, blank=True, default="")
    match_column = models.CharField(max_length=80, blank=True)
    # B2B or B2C — drives the TYPE column on the monthly targets dashboard.
    sales_type = models.CharField(max_length=8, blank=True, default="B2B")
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ["slug"]

    def __str__(self) -> str:
        return self.name
