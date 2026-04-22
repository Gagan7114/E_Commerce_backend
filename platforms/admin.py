from django.contrib import admin

from .models import PlatformConfig


@admin.register(PlatformConfig)
class PlatformConfigAdmin(admin.ModelAdmin):
    list_display = ("slug", "name", "inventory_table", "secondary_table", "master_po_table", "po_filter_column", "po_filter_value", "is_active")
    list_editable = ("is_active",)
    list_filter = ("is_active",)
    search_fields = ("slug", "name")
    ordering = ("slug",)
