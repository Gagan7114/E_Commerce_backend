from django.db import migrations


# Three new permission codes (mirrors 0007's pattern):
#
#   business_mode.view — gates the Business Mode toggle (the +N% data lens).
#       Restricted: granted ONLY to the Super Admin group by default; everyone
#       else needs an explicit direct/group grant. Kept OUT of the catalog so
#       the "*" all-catalog expansion never hands it out silently.
#
#   jm.primary.view / jm.inventory.view — gate the JM Primary and JM Inventory
#       sections (sidebar links + /sap-* pages). Granted to EVERY existing
#       group at seed time so nothing changes for current users; hide the
#       sections from someone by revoking the code from their group (or moving
#       them to a group without it).

BUSINESS_MODE = ("business_mode.view", "Use Business Mode")
JM_PERMISSIONS = (
    ("jm.primary.view", "View JM Primary section"),
    ("jm.inventory.view", "View JM Inventory section"),
)


def seed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Group = apps.get_model("auth", "Group")
    ContentType = apps.get_model("contenttypes", "ContentType")

    user_ct, _ = ContentType.objects.get_or_create(
        app_label="accounts",
        model="user",
    )

    code, name = BUSINESS_MODE
    business_mode, _ = Permission.objects.update_or_create(
        codename=code,
        content_type=user_ct,
        defaults={"name": name},
    )
    for group in Group.objects.filter(name="Super Admin"):
        group.permissions.add(business_mode)

    jm_permissions = []
    for code, name in JM_PERMISSIONS:
        permission, _ = Permission.objects.update_or_create(
            codename=code,
            content_type=user_ct,
            defaults={"name": name},
        )
        jm_permissions.append(permission)
    # Every existing group keeps seeing the JM sections (status quo); hiding is
    # a deliberate revoke afterwards.
    for group in Group.objects.all():
        group.permissions.add(*jm_permissions)


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    codes = [BUSINESS_MODE[0], *(code for code, _ in JM_PERMISSIONS)]
    Permission.objects.filter(codename__in=codes).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0008_shipment_planning_direct_only"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
