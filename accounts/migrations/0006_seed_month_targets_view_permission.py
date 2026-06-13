from django.db import migrations


PERMISSION_CODE = "platform.month_targets.view"
PERMISSION_NAME = "View primary and secondary target sheets"

TARGET_VIEW_GROUPS = (
    "Super Admin",
    "Platform Admin",
    "Operations Manager",
    "Finance Analyst",
    "Viewer",
    "Blinkit User",
    "Zepto User",
    "JioMart User",
    "Amazon User",
    "BigBasket User",
    "Swiggy User",
    "Flipkart User",
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

    for group in Group.objects.filter(name__in=TARGET_VIEW_GROUPS):
        group.permissions.add(permission)


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0005_seed_target_sheet_edit_permission"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
