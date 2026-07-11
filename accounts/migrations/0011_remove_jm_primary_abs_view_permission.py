from django.db import migrations


# The jm.primary.abs_view display lens (0010) is retired: the frontend no
# longer reads it, and the user it was created for should see the same JM
# Primary figures as everyone else. Deleting the Permission row also cascades
# away every direct user grant, so no account keeps a dangling code. A new
# migration (rather than deleting 0010) keeps migration history consistent on
# databases where 0010 already ran — fresh installs run 0010 then 0011, net
# zero.

PERMISSION_CODE = "jm.primary.abs_view"
PERMISSION_NAME = "Show JM Primary figures as positive"


def remove(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


def restore(apps, schema_editor):
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


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0010_seed_jm_primary_abs_view_permission"),
    ]

    operations = [migrations.RunPython(remove, restore)]
