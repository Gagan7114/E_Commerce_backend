from django.db import migrations

# Two brand-new Amazon-PO columns: Remaining QTY and Remaining LTR.
# They carry the outstanding balance on a still-open, partially-received line,
# i.e. Item Status = SHORT SUPPLIED AND PO Status = PENDING (equivalently:
# PO Status = PENDING with 0 < received < requested):
#   Remaining QTY = accepted_qty - received_qty
#   Remaining LTR = total_accepted_liters - total_delivered_liters
#                 = (accepted_qty - received_qty) * per_liter
# Blank (NULL) when PO Status is blank; 0 for every other classified PO.
#
# Going-forward computation lives in uploads/amazon_uploads.py
# (_transform_amazon_po). This migration adds the columns and backfills them on
# existing reporting."Amazon PO" rows so the Amazon-Primary views and the
# Shipment Planner show them immediately, without a re-upload. Depends on 0074
# so PO Status / Item Status are already on the revised rules before we read
# them here (the backfill mirrors the uploader's quantity condition, so it is
# correct regardless).

_AMAZON_PO = 'reporting."Amazon PO"'

ADD_COLUMNS = f"""
ALTER TABLE IF EXISTS {_AMAZON_PO}
    ADD COLUMN IF NOT EXISTS remaining_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS remaining_ltrs NUMERIC;
"""

BACKFILL = f"""
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
       END;
"""


def add_and_backfill(apps, schema_editor):
    conn = schema_editor.connection
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", ['reporting."Amazon PO"'])
        if cur.fetchone()[0] is None:
            return
        cur.execute(ADD_COLUMNS)
        cur.execute(BACKFILL)


def drop_columns(apps, schema_editor):
    conn = schema_editor.connection
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", ['reporting."Amazon PO"'])
        if cur.fetchone()[0] is None:
            return
        cur.execute(
            f'ALTER TABLE IF EXISTS {_AMAZON_PO} '
            'DROP COLUMN IF EXISTS remaining_qty, '
            'DROP COLUMN IF EXISTS remaining_ltrs;'
        )


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0074_amazon_po_status_partial_pending"),
    ]

    operations = [
        migrations.RunPython(add_and_backfill, drop_columns),
    ]
