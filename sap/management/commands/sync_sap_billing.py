from django.core.management.base import BaseCommand

from sap.billing import sync_rk_billing


class Command(BaseCommand):
    help = "Sync R K Worldinfocom SAP billing into the sap_billing table (net billed qty per PO × SAP item). Wire to cron/Celery for a fixed cadence, or rely on the planner's lazy refresh."

    def add_arguments(self, parser):
        parser.add_argument("--months", type=int, default=6, help="Look-back window in months (default 6).")

    def handle(self, *args, **opts):
        summary = sync_rk_billing(months=opts["months"], force=True)
        self.stdout.write(self.style.SUCCESS(f"sap_billing synced: {summary}"))
