from django.db import migrations


# Restricted permission that gates the JivoBot bounce mini-game. Users must be
# granted it (directly or via a group) to play; everyone else can only move the
# chatbot launcher. Real superusers pass automatically. Kept OUT of the catalog
# so the Super Admin "*" all-catalog expansion can't silently hand it to every
# admin — it is added to the Super Admin group explicitly here.
PERMISSION_CODE = "chatbot_game.play"
PERMISSION_NAME = "Play JivoBot mini-game"

DEFAULT_GROUPS = ("Super Admin",)


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
        ("accounts", "0011_remove_jm_primary_abs_view_permission"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
