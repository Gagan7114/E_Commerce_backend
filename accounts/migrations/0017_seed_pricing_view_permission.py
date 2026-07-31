from django.db import migrations
from django.db.models import Q


# `pricing.view` gates the Pricing section (Insights → Pricing in the sidebar,
# the /pricing route, and the /api/reports/live/* endpoints behind it).
#
# Modelled exactly on `amazon.shipment_planning.view` (0007 + 0008):
#   * kept OUT of accounts.catalog, so the Super Admin "*" (all-catalog
#     permissions) seed can never hand it to every admin;
#   * kept OUT of every GROUP — in this org effectively everyone is in the admin
#     groups, which is how 0007 leaked and why 0008 had to strip it back;
#   * granted DIRECTLY to selected users instead.
#
# Seed audience: exactly the users who can already see the Shipment Planner —
# whoever holds `amazon.shipment_planning.view` directly or through a group.
# Real superusers (is_superuser) bypass every gate, so they see Pricing too.
# Adding users later is a Django admin → user → "user permissions" grant; this
# migration only runs once.
PERMISSION_CODE = "pricing.view"
# The Django admin permission picker filters on the display NAME, not the
# codename (the 0013 lesson) — embed the code so either search term finds it.
PERMISSION_NAME = "View Pricing section (pricing.view)"

SOURCE_CODE = "amazon.shipment_planning.view"


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

    source = Permission.objects.filter(codename=SOURCE_CODE).first()
    if source is None:
        # Fresh database where 0007 never ran (or the row was deleted): create
        # the permission and leave it ungranted rather than guessing.
        return

    # Direct grants AND group-held ones — 0008 stripped the code from every
    # group, but a group grant may have been re-added by hand since.
    holders = (
        User.objects
        .filter(Q(user_permissions=source) | Q(groups__permissions=source))
        .distinct()
    )
    for user in holders:
        user.user_permissions.add(permission)


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    # Deleting the row cascades the direct user grants away with it.
    Permission.objects.filter(codename=PERMISSION_CODE).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0016_featureflag"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
