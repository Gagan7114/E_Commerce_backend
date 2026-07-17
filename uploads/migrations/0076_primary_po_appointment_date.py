"""Add appointment_date to the primary PO source tables (total_po / total_po_zbs).

WHY
---
Primary platform PO files (Blinkit, Zepto, Swiggy, Big Basket, Flipkart Grocery,
Zomato, City Mall, ...) can carry an appointment (delivery-slot) date that is
distinct from the GRN/delivery date. The user wants it captured on upload and
shown as its own "Appointment Date" column on the Master PO sheet.

Amazon primary POs use a separate uploader/table (amazon_po) and never land in
total_po / total_po_zbs, so this change automatically covers "all primary
platforms except Amazon".

WHAT
----
Add a nullable ``appointment_date date`` column to both raw primary PO tables.
It is optional: PO files that don't include an appointment date simply leave it
NULL (the uploader never requires it). The generic upload path
(uploads.views._batch_upload) accepts it the moment the column exists, and
normalizes DD/MM/YYYY -> ISO automatically because it is a ``date`` column.

The master_po view chain surfaces this column (platforms migration 0057, which
depends on this one).

SAFETY
------
  * Additive, nullable column -> existing rows and every ``SELECT col, ...``
    consumer are unaffected.
  * IF NOT EXISTS guards make it safe to re-run.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0075_amazon_po_remaining_qty_ltrs"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE public.total_po
                ADD COLUMN IF NOT EXISTS appointment_date date;
            ALTER TABLE public.total_po_zbs
                ADD COLUMN IF NOT EXISTS appointment_date date;
            """,
            reverse_sql="""
            ALTER TABLE public.total_po DROP COLUMN IF EXISTS appointment_date;
            ALTER TABLE public.total_po_zbs DROP COLUMN IF EXISTS appointment_date;
            """,
        ),
    ]
