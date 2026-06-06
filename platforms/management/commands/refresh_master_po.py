"""Manually/scheduled refresh of the master_po materialized view.

Usage:
    python manage.py refresh_master_po
"""

from django.core.management.base import BaseCommand

from platforms.master_po_refresh import refresh_master_po_mv


class Command(BaseCommand):
    help = "Refresh the master_po materialized view (master_po_mv)."

    def handle(self, *args, **options):
        if refresh_master_po_mv():
            self.stdout.write(self.style.SUCCESS("master_po_mv refreshed."))
        else:
            self.stdout.write(
                self.style.WARNING(
                    "master_po_mv not refreshed "
                    "(matview missing - apply migration 0040 - or refresh failed)."
                )
            )
