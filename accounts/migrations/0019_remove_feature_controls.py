from django.db import migrations


# Permanent removal of the Settings → Profile "Feature Controls" card and
# everything behind it.
#
# What went with it:
#   * the Uploader ON/OFF button (the card's only remaining switch),
#   * the accounts_featureflag table that stored its state,
#   * /api/auth/feature-flags and /api/auth/feature-flags/update,
#   * uploads.middleware.UploaderKillSwitchMiddleware, the enforcement behind
#     the switch — with nothing able to write the flag it could only ever pass
#     traffic through, so it was dead weight.
#
# Two rows to clear from existing databases:
#   1. `profile_controls.manage` (auth_permission) — the owner-only code that
#      revealed the card. It gates nothing now; deleting it cascades its direct
#      user grants away. The migration that seeded it (the old 0014) is deleted,
#      so a fresh database never creates it in the first place.
#   2. the accounts_featureflag table itself (DeleteModel below).
#
# Irreversible on purpose: re-creating a permission and a table for a card that
# no longer has any code behind it would restore nothing usable.
PERMISSION_CODE = "profile_controls.manage"


def drop_permission(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0018_remove_game_feature"),
    ]

    operations = [
        migrations.RunPython(drop_permission, migrations.RunPython.noop),
        migrations.DeleteModel(name="FeatureFlag"),
    ]
