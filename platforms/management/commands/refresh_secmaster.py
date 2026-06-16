"""Manually/scheduled refresh of the secmaster materialized view.

Usage:
    python manage.py refresh_secmaster
"""

from django.core.management.base import BaseCommand

from platforms.master_po_refresh import refresh_secmaster_mv


class Command(BaseCommand):
    help = "Refresh the secmaster materialized view (secmaster_mv)."

    def handle(self, *args, **options):
        if refresh_secmaster_mv():
            self.stdout.write(self.style.SUCCESS("secmaster_mv refreshed."))
        else:
            self.stdout.write(
                self.style.WARNING(
                    "secmaster_mv not refreshed "
                    "(matview missing - apply migration 0042 - or refresh failed)."
                )
            )
