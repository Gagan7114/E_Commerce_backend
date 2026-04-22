from django.db import migrations, models


PLATFORM_FILTERS = {
    "blinkit":   ("platform", "blinkit"),
    "zepto":     ("platform", "zepto"),
    "jiomart":   ("platform", "jiomart"),
    "amazon":    ("platform", "amazon"),
    "bigbasket": ("platform", "bigbasket"),
    "swiggy":    ("platform", "swiggy"),
    "flipkart":  ("platform", "flipkart"),
}


def seed_filters(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")
    for slug, (col, val) in PLATFORM_FILTERS.items():
        PlatformConfig.objects.filter(slug=slug).update(
            po_filter_column=col, po_filter_value=val
        )


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [("platforms", "0002_seed_platforms")]

    operations = [
        migrations.AddField(
            model_name="platformconfig",
            name="po_filter_column",
            field=models.CharField(blank=True, default="platform", max_length=80),
        ),
        migrations.AddField(
            model_name="platformconfig",
            name="po_filter_value",
            field=models.CharField(default="", blank=True, max_length=80),
        ),
        migrations.RunPython(seed_filters, noop),
    ]
