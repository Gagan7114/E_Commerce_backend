from django.db import migrations, transaction

# One-time backfill so the revised PO Status / Item Status rules take effect on
# EXISTING reporting."Amazon PO" rows immediately — in the Amazon-Primary views
# and the Shipment Planner — instead of only on the next re-upload of each PO.
# The going-forward computation lives in uploads/amazon_uploads.py
# (_transform_amazon_po); this migration mirrors that change for stored data.
#
# What changed vs the previous ruleset:
#   * PO Status  — a Confirmed + In-stock PO with a PARTIAL receipt
#     (0 < received < accepted) is now PENDING instead of COMPLETED. COMPLETED
#     now requires received >= accepted. Every other PO Status rule is unchanged,
#     so we flip ONLY the rows the old rule mislabelled (stored COMPLETED) — we
#     deliberately do NOT re-run the whole CASE, which would also re-evaluate the
#     time-relative EXPIRED rules against today's date.
#   * Item Status — rewritten and now applies to every row (no longer gated on
#     COMPLETED): '' when PO Status is blank, NOT SUPPLIED for CANCELLED / MOV or
#     when nothing was received, else FULL / SHORT SUPPLIED (received >= requested
#     = FULL).
#
# For the handful of rows that flip COMPLETED -> PENDING we also refresh the four
# PO-Status-derived columns (missed_ltrs, missed_unit, miss_rate, helper) with
# their existing, unchanged rules so the stored row stays internally consistent
# — exactly what a fresh upload of that PO would write.

_AMAZON_PO = 'reporting."Amazon PO"'

# 1) Flip Confirmed + In-stock partial receipts that the old rule stored as
#    COMPLETED. Under the new rule these are PENDING; refresh the PENDING-derived
#    columns to match (PENDING => missed_* / miss_rate = 0; helper by expiry band).
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

# 2) Recompute Item Status for every row using the (now corrected) PO Status.
#    Only rows whose value actually changes are written.
_ITEM_STATUS_CASE = """
    CASE
        WHEN COALESCE(NULLIF(TRIM(po_status), ''), '') = '' THEN ''
        WHEN UPPER(TRIM(po_status)) IN ('CANCELLED', 'MOV') THEN 'NOT SUPPLIED'
        WHEN COALESCE(received_qty, 0) = 0 THEN 'NOT SUPPLIED'
        WHEN COALESCE(received_qty, 0) >= COALESCE(requested_qty, 0) THEN 'FULL SUPPLIED'
        ELSE 'SHORT SUPPLIED'
    END
"""

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


def backfill(apps, schema_editor):
    conn = schema_editor.connection
    # reporting."Amazon PO" is a raw (non-ORM) table; skip cleanly on any DB
    # where it does not exist yet (fresh installs / test DBs).
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", ['reporting."Amazon PO"'])
        if cur.fetchone()[0] is None:
            return
    # This is a data-only backfill (Item Status changed for most rows, so the
    # UPDATE can touch many rows). It must NEVER block the migration chain: if it
    # did, the schema migration right after this one (0075, which ADDS the
    # remaining_qty/remaining_ltrs columns the app now SELECTs) would stay
    # unapplied and every read of the table would error. So each statement runs
    # in its OWN transaction under a bounded timeout: a slow/failed one rolls
    # back cleanly (no aborted-transaction state can reach Django's migration
    # recorder) and is simply skipped. New uploads already apply the new rules,
    # and the full backfill can be completed later with:
    #     manage.py backfill_amazon_po_columns
    for statement in (FLIP_PARTIAL_TO_PENDING, RECOMPUTE_ITEM_STATUS):
        try:
            with transaction.atomic():
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '30s'")
                    cur.execute("SET LOCAL lock_timeout = '10s'")
                    cur.execute(statement)
        except Exception:
            pass


class Migration(migrations.Migration):
    # atomic=False so this data-only backfill is not wrapped in one big
    # transaction; each statement above manages its own (see backfill), so a
    # slow/failed one can never halt migrate and block the schema change in 0075.
    atomic = False

    dependencies = [
        ("uploads", "0073_ads_daily_master_total_sale_basic_rate"),
    ]

    operations = [
        migrations.RunPython(backfill, migrations.RunPython.noop),
    ]
