from django.db import migrations


# The Django admin permission picker filters by the permission's display NAME,
# not its codename — so admins searching "chatbot_game.play" find nothing.
# Embed the codename in the name so either search term matches.
PERMISSION_CODE = "chatbot_game.play"
NEW_NAME = "Play JivoBot mini-game (chatbot_game.play)"
OLD_NAME = "Play JivoBot mini-game"


def rename(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).update(name=NEW_NAME)


def unrename(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).update(name=OLD_NAME)


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0012_seed_chatbot_game_permission"),
    ]

    operations = [migrations.RunPython(rename, unrename)]
