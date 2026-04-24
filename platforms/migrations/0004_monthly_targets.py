from django.db import migrations, models


# (slug, name, po_filter_value, sales_type)
# B2B: storefronts we serve through an aggregator.
# B2C: marketplaces we sell directly on.
SALES_TYPES = {
    "blinkit":          "B2B",
    "swiggy":           "B2B",
    "zepto":            "B2B",
    "bigbasket":        "B2B",
    "flipkart_grocery": "B2B",
    "zomato":           "B2B",
    "citymall":         "B2B",
    "amazon":           "B2C",
    "flipkart":         "B2C",
    "jiomart":          "B2C",
}


NEW_PLATFORMS = [
    # slug,      name,        po_filter_value
    ("zomato",   "Zomato",    "zomato"),
    ("citymall", "CityMall",  "city mall"),
]


# Flipkart Grocery is referenced by the sheet but isn't in the initial seed.
# Add it here too so the combined dashboard can render a row for it.
OPTIONAL_PLATFORMS = [
    ("flipkart_grocery", "Flipkart Grocery", "flipkart grocery"),
]


def seed_platforms_and_types(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")

    for slug, name, filter_value in NEW_PLATFORMS + OPTIONAL_PLATFORMS:
        PlatformConfig.objects.update_or_create(
            slug=slug,
            defaults={
                "name": name,
                "inventory_table": "",
                "secondary_table": "",
                "master_po_table": "master_po",
                "po_filter_column": "format",
                "po_filter_value": filter_value,
                "is_active": True,
                "sales_type": SALES_TYPES.get(slug, "B2B"),
            },
        )

    for slug, sales_type in SALES_TYPES.items():
        PlatformConfig.objects.filter(slug=slug).update(sales_type=sales_type)


def unseed_platforms(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")
    PlatformConfig.objects.filter(
        slug__in=[s for s, *_ in NEW_PLATFORMS + OPTIONAL_PLATFORMS]
    ).delete()


class Migration(migrations.Migration):
    dependencies = [("platforms", "0003_po_filter_fields")]

    operations = [
        migrations.AddField(
            model_name="platformconfig",
            name="sales_type",
            field=models.CharField(blank=True, default="B2B", max_length=8),
        ),
        migrations.RunPython(seed_platforms_and_types, unseed_platforms),
    ]
