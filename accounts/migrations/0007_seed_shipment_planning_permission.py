from django.db import migrations


# New restricted permission that gates the Amazon Shipment Planner section (UI +
# backend endpoints). By default only admin roles get it; the Super Admin group
# holds every permission via its "*" seed. Everyone else must be granted it
# explicitly (directly on the user or via a group) to see the section.
PERMISSION_CODE = "amazon.shipment_planning.view"
PERMISSION_NAME = "View Amazon Shipment Planner"

DEFAULT_GROUPS = (
    "Super Admin",
    "Platform Admin",
)


def seed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Group = apps.get_model("auth", "Group")
    ContentType = apps.get_model("contenttypes", "ContentType")

    user_ct, _ = ContentType.objects.get_or_create(
        app_label="accounts",
        model="user",
    )
    permission, _ = Permission.objects.update_or_create(
        codename=PERMISSION_CODE,
        content_type=user_ct,
        defaults={"name": PERMISSION_NAME},
    )

    for group in Group.objects.filter(name__in=DEFAULT_GROUPS):
        group.permissions.add(permission)


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0006_seed_month_targets_view_permission"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
