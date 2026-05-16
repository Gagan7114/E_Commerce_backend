"""
Zero out all liter columns in reporting."Amazon PO" for ASINs that have
no per_unit_value in public.master_sheet.

These were incorrectly auto-calculated from PER UNIT / UOM text.
"""

from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    help = "Zero liter columns for ASINs with no per_unit_value in master_sheet."

    def handle(self, *args, **options):
        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute("""
                    UPDATE reporting."Amazon PO" po
                    SET
                        per_liter             = 0,
                        total_order_liters    = 0,
                        total_accepted_liters = 0,
                        total_delivered_liters= 0,
                        order_ltrs_cl         = 0,
                        filled_ltrs           = 0,
                        missed_ltrs           = CASE
                                                    WHEN po_status IN ('MOV','CANCELLED') THEN NULL
                                                    ELSE 0
                                                END
                    WHERE po.sku_code IN (
                        SELECT format_sku_code
                        FROM public.master_sheet
                        WHERE per_unit_value IS NULL
                           OR per_unit_value::numeric = 0
                    )
                """)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Zeroed liter columns for {cur.rowcount} rows."
                    )
                )
