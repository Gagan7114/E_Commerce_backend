from django.db import migrations


# Per-user DISPLAY LENS permission: when a user holds this code, the Home
# dashboard shows the JM Primary card's figures as positive (absolute) numbers
# instead of negatives. Deliberately:
#   - NOT in the catalog (never part of the "*" all-catalog expansion), and
#   - granted to NO groups here — assign it DIRECTLY to the specific user(s)
#     who should see the flipped view (Django admin → user → user permissions).
# The frontend checks the raw permissions list (not the superuser-bypassing
# hasPermission), so superusers do NOT inherit this lens implicitly.

PERMISSION_CODE = "jm.primary.abs_view"
PERMISSION_NAME = "Show JM Primary figures as positive"


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
        ("accounts", "0009_seed_business_mode_and_jm_permissions"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
