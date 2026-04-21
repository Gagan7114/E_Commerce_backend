from django.db import migrations

from platforms.management.commands.sync_platforms import DEFAULT_PLATFORMS


def seed(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")
    for slug, name, inv, sec in DEFAULT_PLATFORMS:
        PlatformConfig.objects.update_or_create(
            slug=slug,
            defaults={
                "name": name,
                "inventory_table": inv,
                "secondary_table": sec,
                "master_po_table": "master_po",
                "is_active": True,
            },
        )


def unseed(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")
    PlatformConfig.objects.filter(slug__in=[s for s, *_ in DEFAULT_PLATFORMS]).delete()


class Migration(migrations.Migration):
    dependencies = [("platforms", "0001_initial")]
    operations = [migrations.RunPython(seed, unseed)]
