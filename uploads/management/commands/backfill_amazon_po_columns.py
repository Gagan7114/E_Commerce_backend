"""Backfill the computed Amazon-PO columns on existing reporting."Amazon PO"
rows, with NO statement timeout — the full-fat version of the best-effort
backfills in migrations 0074 / 0075.

Run this once after deploying the revised PO Status / Item Status rules and the
new Remaining QTY / Remaining LTR columns, if the in-migration backfills were
skipped (they run under a short timeout so they can never block a deploy):

    python manage.py backfill_amazon_po_columns

Idempotent and safe to re-run. New uploads already write every column under the
new rules; this only refreshes rows that pre-date the change.
"""
from django.core.management.base import BaseCommand
from django.db import connection

_AMAZON_PO = 'reporting."Amazon PO"'

# 1) Flip Confirmed + In-stock partial receipts the old rule stored as COMPLETED
#    to PENDING, and refresh the PENDING-derived columns (mirror of migration 0074).
FLIP_PARTIAL_TO_PENDING = f"""
UPDATE {_AMAZON_PO}
   SET po_status   = 'PENDING',
       missed_ltrs = 0,
       missed_unit = 0,
       miss_rate   = 0,
       helper = CASE
           WHEN LOWER(COALESCE(status, '')) = 'confirmed'
                AND COALESCE(days_to_expiry, 0) BETWEEN 1 AND 18 THEN 'INCLUDE'
           ELSE 'EXCLUDE'
       END,
       updated_at  = now()
 WHERE UPPER(TRIM(COALESCE(po_status, ''))) = 'COMPLETED'
   AND TRIM(COALESCE(status, '')) = 'Confirmed'
   AND TRIM(COALESCE(availability_status, '')) = 'AC - Accepted: In stock'
   AND COALESCE(accepted_qty, 0) > 0
   AND COALESCE(received_qty, 0) > 0
   AND COALESCE(received_qty, 0) < COALESCE(accepted_qty, 0);
"""

_ITEM_STATUS_CASE = """
    CASE
        WHEN COALESCE(NULLIF(TRIM(po_status), ''), '') = '' THEN ''
        WHEN UPPER(TRIM(po_status)) IN ('CANCELLED', 'MOV') THEN 'NOT SUPPLIED'
        WHEN COALESCE(received_qty, 0) = 0 THEN 'NOT SUPPLIED'
        WHEN COALESCE(received_qty, 0) >= COALESCE(requested_qty, 0) THEN 'FULL SUPPLIED'
        ELSE 'SHORT SUPPLIED'
    END
"""

# 2) Recompute Item Status for every row from the corrected PO Status (0074).
RECOMPUTE_ITEM_STATUS = f"""
WITH calc AS (
    SELECT source_line_key, {_ITEM_STATUS_CASE} AS new_item_status
      FROM {_AMAZON_PO}
)
UPDATE {_AMAZON_PO} a
   SET item_status = c.new_item_status
  FROM calc c
 WHERE a.source_line_key = c.source_line_key
   AND a.item_status IS DISTINCT FROM c.new_item_status;
"""

# 3) Backfill Remaining QTY / Remaining LTR (mirror of migration 0075).
REMAINING_BACKFILL = f"""
UPDATE {_AMAZON_PO}
   SET remaining_qty = CASE
           WHEN COALESCE(NULLIF(TRIM(po_status), ''), '') = '' THEN NULL
           WHEN UPPER(TRIM(po_status)) = 'PENDING'
                AND COALESCE(received_qty, 0) > 0
                AND COALESCE(received_qty, 0) < COALESCE(requested_qty, 0)
               THEN accepted_qty - received_qty
           ELSE 0
       END,
       remaining_ltrs = CASE
           WHEN COALESCE(NULLIF(TRIM(po_status), ''), '') = '' THEN NULL
           WHEN UPPER(TRIM(po_status)) = 'PENDING'
                AND COALESCE(received_qty, 0) > 0
                AND COALESCE(received_qty, 0) < COALESCE(requested_qty, 0)
               THEN COALESCE(total_accepted_liters, 0) - COALESCE(total_delivered_liters, 0)
           ELSE 0
       END
 -- Only fill historical rows that have no uploaded value; never clobber the
 -- Amazon-sourced remaining_qty the upload transform now writes.
 WHERE remaining_qty IS NULL;
"""


class Command(BaseCommand):
    help = "Backfill Amazon PO computed columns (PO/Item Status + Remaining QTY/LTR) on existing rows, no timeout. Idempotent."

    def handle(self, *args, **options):
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", ['reporting."Amazon PO"'])
            if cur.fetchone()[0] is None:
                self.stdout.write('reporting."Amazon PO" not found; nothing to do.')
                return
            # Make sure the columns exist before backfilling them.
            cur.execute(
                f'ALTER TABLE IF EXISTS {_AMAZON_PO} '
                'ADD COLUMN IF NOT EXISTS remaining_qty NUMERIC, '
                'ADD COLUMN IF NOT EXISTS remaining_ltrs NUMERIC;'
            )
            cur.execute("SET statement_timeout = 0")
            for label, sql in (
                ("flip partial COMPLETED -> PENDING", FLIP_PARTIAL_TO_PENDING),
                ("recompute item_status", RECOMPUTE_ITEM_STATUS),
                ("backfill remaining_qty / remaining_ltrs", REMAINING_BACKFILL),
            ):
                cur.execute(sql)
                self.stdout.write(f"  {label}: {cur.rowcount} row(s)")
        self.stdout.write(self.style.SUCCESS("Amazon PO column backfill complete."))
