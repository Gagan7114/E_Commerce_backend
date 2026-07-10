from django.db import migrations


# Make the Amazon Shipment Planner permission truly opt-in. 0007 granted it to
# the Super Admin + Platform Admin GROUPS, but in this org effectively everyone
# is in those groups, so it leaked to everyone. Remove it from ALL groups here.
# The permission row is kept so it can be assigned DIRECTLY to selected users
# (Django admin → user → "user permissions"); real superusers (is_superuser flag)
# still bypass every gate. It is also no longer in accounts.catalog, so the
# Super Admin "*" (all-catalog-permissions) seed can never re-add it.
PERMISSION_CODE = "amazon.shipment_planning.view"


def strip_from_groups(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Group = apps.get_model("auth", "Group")
    perm = Permission.objects.filter(codename=PERMISSION_CODE).first()
    if not perm:
        return
    for group in Group.objects.filter(permissions=perm):
        group.permissions.remove(perm)


def regrant_admin_groups(apps, schema_editor):
    # Reverse: restore the 0007 state (perm on Super Admin + Platform Admin).
    Permission = apps.get_model("auth", "Permission")
    Group = apps.get_model("auth", "Group")
    perm = Permission.objects.filter(codename=PERMISSION_CODE).first()
    if not perm:
        return
    for group in Group.objects.filter(name__in=("Super Admin", "Platform Admin")):
        group.permissions.add(perm)


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0007_seed_shipment_planning_permission"),
    ]

    operations = [migrations.RunPython(strip_from_groups, regrant_admin_groups)]
