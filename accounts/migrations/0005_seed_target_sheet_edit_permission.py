from django.db import migrations


PERMISSION_CODE = "target_sheet.edit"
PERMISSION_NAME = "Edit primary and secondary target sheets"


def seed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    ContentType = apps.get_model("contenttypes", "ContentType")

    user_ct, _ = ContentType.objects.get_or_create(
        app_label="accounts",
        model="user",
    )
    Permission.objects.update_or_create(
        codename=PERMISSION_CODE,
        content_type=user_ct,
        defaults={"name": PERMISSION_NAME},
    )


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0004_inventory_doh_notification"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
