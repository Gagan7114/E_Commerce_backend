from django.db import migrations

from accounts.catalog import GROUP_CATALOG, PERMISSION_CATALOG


def seed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Group = apps.get_model("auth", "Group")
    ContentType = apps.get_model("contenttypes", "ContentType")

    user_ct, _ = ContentType.objects.get_or_create(app_label="accounts", model="user")

    perms_by_code = {}
    for codename, label in PERMISSION_CATALOG:
        perm, _ = Permission.objects.update_or_create(
            codename=codename,
            content_type=user_ct,
            defaults={"name": label},
        )
        perms_by_code[codename] = perm

    for name, codes in GROUP_CATALOG.items():
        group, _ = Group.objects.get_or_create(name=name)
        if codes == ["*"]:
            resolved = list(perms_by_code.values())
        else:
            resolved = [perms_by_code[c] for c in codes if c in perms_by_code]
        group.permissions.set(resolved)


def unseed(apps, schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Group = apps.get_model("auth", "Group")
    new_codes = [
        "platform.*.access",
        "platform.blinkit.access", "platform.zepto.access", "platform.jiomart.access",
        "platform.amazon.access", "platform.bigbasket.access",
        "platform.swiggy.access", "platform.flipkart.access",
    ]
    new_groups = [
        "Blinkit User", "Zepto User", "JioMart User", "Amazon User",
        "BigBasket User", "Swiggy User", "Flipkart User",
    ]
    Permission.objects.filter(codename__in=new_codes).delete()
    Group.objects.filter(name__in=new_groups).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0002_seed_permissions_and_groups"),
        ("platforms", "0002_seed_platforms"),
    ]

    operations = [migrations.RunPython(seed, unseed)]
