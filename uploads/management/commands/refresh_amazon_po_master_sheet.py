"""
Re-derive every master_sheet-sourced column on reporting."Amazon PO"
(category, sub_category, item_head, brand, per_liter, etc.) plus the litre/box
columns that depend on per_liter / case_pack.

Amazon PO is a materialized table: its master_sheet attributes are frozen at
upload time, so later master_sheet edits never reach existing rows. The save
endpoints now propagate edits automatically (scoped to the changed SKUs); this
command does the same for the WHOLE table - use it to backfill historical rows
or after a bulk master_sheet correction.

Usage:
    python manage.py refresh_amazon_po_master_sheet
    python manage.py refresh_amazon_po_master_sheet --sku ASIN1 --sku ASIN2
"""

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from uploads.amazon_uploads import refresh_amazon_po_from_master_sheet


class Command(BaseCommand):
    help = "Re-sync reporting.\"Amazon PO\" master_sheet-derived columns from master_sheet."

    def add_arguments(self, parser):
        parser.add_argument(
            "--sku",
            action="append",
            dest="skus",
            help="Limit to these master_sheet format_sku_code(s)/asin(s). Repeatable. Omit for the whole table.",
        )

    def handle(self, *args, **options):
        skus = options.get("skus") or None
        with transaction.atomic(), connection.cursor() as cur:
            updated = refresh_amazon_po_from_master_sheet(cur, format_sku_codes=skus)
        scope = f"{len(skus)} SKU(s)" if skus else "all rows"
        self.stdout.write(
            self.style.SUCCESS(f"Refreshed {updated} Amazon PO row(s) from master_sheet ({scope}).")
        )
