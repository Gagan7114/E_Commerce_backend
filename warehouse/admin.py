from django.apps import apps
from django.contrib import admin
from django.db.models import CharField

from accounts.permissions import has_permission_code


class ReadOnlyAdmin(admin.ModelAdmin):
    """Generic read-only admin for reflected warehouse tables."""

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_view_permission(self, request, obj=None):
        return has_permission_code(request.user, "admin.warehouse.view")

    def get_readonly_fields(self, request, obj=None):
        return [f.name for f in self.model._meta.fields]


def _register_all():
    for model in apps.get_app_config("warehouse").get_models():
        fields = tuple(f.name for f in model._meta.fields)
        search = tuple(
            f.name for f in model._meta.fields if isinstance(f, CharField)
        )
        admin_cls = type(
            f"{model.__name__}Admin",
            (ReadOnlyAdmin,),
            {
                "list_display": fields[:6],
                "search_fields": search,
            },
        )
        admin.site.register(model, admin_cls)


# Warehouse admin registration is OFF until the model column definitions in
# warehouse/models.py match the real physical schemas. Enable with:
#   1. python manage.py inspectdb master_po blinkit_inventory ... > /tmp/cols.py
#   2. Paste real fields into warehouse/models.py, keeping `managed = False`
#   3. Uncomment the line below
# _register_all()
