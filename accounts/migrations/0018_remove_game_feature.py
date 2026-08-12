from django.db import migrations


# Permanent removal of the JivoBot bounce mini-game.
#
# Two leftovers to clear from every existing database:
#
#   1. The `game_play` accounts_featureflag row — the ON/OFF switch that used to
#      sit next to Uploader in Settings → Profile → "Feature Controls". The
#      switch, its API payload key and FeatureFlag.GAME_PLAY are all gone, so the
#      row is unreadable data that would only reappear in an admin listing.
#   2. The `chatbot_game.play` auth_permission row. The migration that dropped it
#      has itself been deleted along with the two that created it, so this is the
#      defensive sweep for any database that still holds the row (a deployment
#      that never reached the old 0015, or one where it was re-granted by hand).
#      Deleting the Permission cascades its group and direct-user grants away.
#
# Irreversible on purpose: re-creating a permission for a feature that no longer
# has any code behind it would gate nothing. `uploader` is untouched — it is the
# Upload Hub kill switch and has nothing to do with the game.
GAME_FLAG_KEY = "game_play"
GAME_PERMISSION_CODE = "chatbot_game.play"


def drop_game_leftovers(apps, schema_editor):
    FeatureFlag = apps.get_model("accounts", "FeatureFlag")
    Permission = apps.get_model("auth", "Permission")

    FeatureFlag.objects.filter(key=GAME_FLAG_KEY).delete()
    Permission.objects.filter(codename=GAME_PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0017_seed_pricing_view_permission"),
    ]

    operations = [
        migrations.RunPython(drop_game_leftovers, migrations.RunPython.noop),
    ]
