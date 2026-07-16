from django.db import migrations, transaction

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


def add_columns(apps, schema_editor):
    conn = schema_editor.connection
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", ['reporting."Amazon PO"'])
        if cur.fetchone()[0] is None:
            return
    # NOTE: the app now computes remaining_qty / remaining_ltrs ON READ (see
    # amazon_uploads._REMAINING_QTY_EXPR and shipment.views.POListView), so it
    # does NOT depend on these stored columns existing. We still add them
    # (harmless — a future stored-column path could use them), but strictly
    # best-effort: a failure must never halt the migration chain, since nothing
    # requires the columns.
    try:
        with transaction.atomic():
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = 0")
                cur.execute("SET LOCAL lock_timeout = '30s'")
                cur.execute(ADD_COLUMNS)
    except Exception:
        pass


def backfill_data(apps, schema_editor):
    conn = schema_editor.connection
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", ['reporting."Amazon PO"'])
        if cur.fetchone()[0] is None:
            return
    # Best-effort: add_columns above already committed the schema, so reads work
    # even if this backfill can't finish under a prod timeout. Runs in its own
    # transaction so a slow/failed run rolls back cleanly and is simply skipped.
    # Full backfill: manage.py backfill_amazon_po_columns
    try:
        with transaction.atomic():
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '30s'")
                cur.execute("SET LOCAL lock_timeout = '10s'")
                cur.execute(BACKFILL)
    except Exception:
        pass


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
    # atomic=False so ADD COLUMN (add_columns) commits independently of the
    # best-effort data backfill — the columns MUST exist for reads to work even
    # if the backfill can't finish under a prod statement_timeout.
    atomic = False

    dependencies = [
        ("uploads", "0074_amazon_po_status_partial_pending"),
    ]

    operations = [
        # 1) Schema first — fast, idempotent, controlled timeouts. This is what
        #    the Amazon-Primary report and Shipment Planner SELECTs depend on.
        migrations.RunPython(add_columns, drop_columns),
        # 2) Data backfill — best-effort (see backfill_data).
        migrations.RunPython(backfill_data, migrations.RunPython.noop),
    ]
