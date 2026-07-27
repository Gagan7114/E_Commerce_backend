from django.db import migrations


# Consolidation: the JivoBot mini-game is now gated on `profile_controls.manage`
# (0014) — the one owner-only code that also reveals the Settings → Profile
# "Feature Controls" card holding the game's on/off switch. The game's own
# panel-header toggle is gone, so `chatbot_game.play` gates nothing and would
# only be a second code to keep in sync. Delete the row; the m2m cascade takes
# its group and direct-user grants with it.
PERMISSION_CODE = "chatbot_game.play"
# Name as of 0013 — restored on reverse so the state matches 0012 + 0013.
PERMISSION_NAME = "Play JivoBot mini-game (chatbot_game.play)"
DEFAULT_GROUPS = ("Super Admin",)


def drop(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


def restore(apps, schema_editor):
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
    # Only the group grant is restorable — which individual users had it
    # directly isn't recorded anywhere once the row is gone.
    for group in Group.objects.filter(name__in=DEFAULT_GROUPS):
        group.permissions.add(permission)


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0014_seed_profile_controls_permission"),
    ]

    operations = [migrations.RunPython(drop, restore)]
