from django.core.management.base import BaseCommand

from platforms.models import PlatformConfig

DEFAULT_PLATFORMS = [
    # slug,       name,        inventory_table,         secondary_table
    ("blinkit",   "Blinkit",   "blinkit_inventory",     "blinkit_secondary"),
    ("zepto",     "Zepto",     "zepto_inventory",       "zepto_secondary"),
    ("jiomart",   "JioMart",   "jiomart_inventory",     "jiomart_secondary"),
    ("amazon",    "Amazon",    "amazon_inventory",      "amazon_secondary"),
    ("bigbasket", "BigBasket", "bigbasket_inventory",   "bigbasket_secondary"),
    ("swiggy",    "Swiggy",    "swiggy_inventory",      "swiggy_secondary"),
    ("flipkart",  "Flipkart",  "flipkart_inventory",    "flipkart_secondary"),
]


class Command(BaseCommand):
    help = "Seed or update the 7 PlatformConfig rows."

    def handle(self, *args, **options):
        for slug, name, inv, sec in DEFAULT_PLATFORMS:
            obj, created = PlatformConfig.objects.update_or_create(
                slug=slug,
                defaults={
                    "name": name,
                    "inventory_table": inv,
                    "secondary_table": sec,
                    "master_po_table": "master_po",
                    "is_active": True,
                },
            )
            verb = "created" if created else "updated"
            self.stdout.write(f"  {slug}: {verb}")
        self.stdout.write(self.style.SUCCESS(f"Synced {len(DEFAULT_PLATFORMS)} platforms."))
