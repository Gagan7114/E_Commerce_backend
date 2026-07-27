from django.db import migrations


# ONE permission gates BOTH switches in the Settings → Profile "Feature Controls"
# card (Uploader on/off and Game Play on/off): hold it and you see the pair,
# without it the whole card is hidden.
#
# It is deliberately kept OUT of accounts.catalog so the Super Admin "*"
# (all-catalog-permissions) seed can never hand it to every admin, and OUT of
# every GROUP so it can't leak that way either — in this org effectively everyone
# is in the admin groups, which is exactly how 0007 leaked and why 0008 had to
# strip it back. Instead it is granted DIRECTLY to the owner account(s) below.
#
# Caveat by design: real superusers (is_superuser) bypass every permission gate,
# so any superuser account also sees the card.
PERMISSION_CODE = "profile_controls.manage"
# The Django admin permission picker filters on the display NAME, not the
# codename (the 0013 lesson) — embed the code so either search term finds it.
PERMISSION_NAME = "Manage profile feature switches (profile_controls.manage)"

# The only account granted the permission at seed time. Adding an email here and
# re-running `migrate` will NOT re-grant (this migration is applied once); grant
# further users via Django admin → user → "user permissions".
OWNER_EMAILS = ("ecom@jivo.in",)


def seed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    ContentType = apps.get_model("contenttypes", "ContentType")
    User = apps.get_model("accounts", "User")

    user_ct, _ = ContentType.objects.get_or_create(
        app_label="accounts",
        model="user",
    )
    permission, _ = Permission.objects.update_or_create(
        codename=PERMISSION_CODE,
        content_type=user_ct,
        defaults={"name": PERMISSION_NAME},
    )

    # Direct per-user grant. Missing accounts are skipped silently so a fresh
    # database (or a deployment without the owner seeded yet) still migrates.
    for user in User.objects.filter(email__in=OWNER_EMAILS):
        user.user_permissions.add(permission)


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    # Deleting the row cascades the direct user grants away with it.
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0013_chatbot_game_permission_searchable_name"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
