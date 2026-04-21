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
    match_column = models.CharField(max_length=80, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ["slug"]

    def __str__(self) -> str:
        return self.name
