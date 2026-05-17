from django.core.management.base import BaseCommand

from platforms.services.inventory_doh_alerts import (
    DEFAULT_THRESHOLD,
    find_low_doh_rows,
    upsert_low_doh_notifications,
)


class Command(BaseCommand):
    help = "Generate inventory DOH notifications for SKUs with DOH below threshold."

    def add_arguments(self, parser):
        parser.add_argument(
            "--platform",
            dest="platform",
            default=None,
            help="Optional platform slug: blinkit, zepto, swiggy, bigbasket, amazon.",
        )
        parser.add_argument(
            "--date",
            dest="date_value",
            default=None,
            help="Optional inventory snapshot date in YYYY-MM-DD or DD-MM-YYYY.",
        )
        parser.add_argument(
            "--threshold",
            type=float,
            default=DEFAULT_THRESHOLD,
            help="DOH threshold. Default: 10.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Only count matching rows; do not create or update notifications.",
        )
        parser.add_argument(
            "--skip-firebase",
            action="store_true",
            help="Create database notifications without sending Firebase Cloud Messaging.",
        )

    def handle(self, *args, **options):
        platform = options["platform"]
        threshold = options["threshold"]
        date_value = options["date_value"]
        if options["dry_run"]:
            rows = find_low_doh_rows(
                threshold=threshold,
                platform_slug=platform,
                date_value=date_value,
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"Dry run found {len(rows)} low-DOH rows below {threshold:g}."
                )
            )
            for row in rows[:20]:
                self.stdout.write(
                    f"{row['format']} | {row['sku_code']} | {row.get('item') or '-'} | DOH {row['doh']:.2f}"
                )
            return

        result = upsert_low_doh_notifications(
            threshold=threshold,
            platform_slug=platform,
            date_value=date_value,
            send_firebase=not options["skip_firebase"],
        )
        self.stdout.write(self.style.SUCCESS(f"Inventory DOH notification result: {result}"))
