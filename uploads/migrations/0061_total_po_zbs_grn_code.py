"""Add a per-GRN identifier column to total_po_zbs (Zepto GRN multi-receipt).

WHY
---
Zepto's GRN sheet can list the SAME po_number + sku_code more than once, one
line per physical GRN receipt, each with its OWN unique GRN id. The lean GRN
uploader used to collapse those lines into the single matching PO+SKU row, so
the second (and later) receipts were merged away. The user wants each unique GRN
id preserved as its own row.

WHAT
----
Add a nullable ``grn_code`` text column. It is populated ONLY for Zepto GRN rows
(format = 'ZEPTO') by the GRN uploader; every other platform / existing row
keeps it NULL, so nothing else changes. A btree index on
(lower(po_number), lower(sku_code), grn_code) speeds the uploader's
"does this exact GRN already exist?" lookup.

SAFETY
------
  * Additive, nullable column -> existing rows and all `SELECT col, ...` views
    (master_po, etc.) are unaffected.
  * IF NOT EXISTS guards make it safe to re-run.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0060_ads_master_materialized"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE public.total_po_zbs
                ADD COLUMN IF NOT EXISTS grn_code text;
            CREATE INDEX IF NOT EXISTS idx_total_po_zbs_grn_lookup
                ON public.total_po_zbs
                (LOWER(TRIM(po_number)), LOWER(TRIM(sku_code)), grn_code);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.idx_total_po_zbs_grn_lookup;
            ALTER TABLE public.total_po_zbs DROP COLUMN IF EXISTS grn_code;
            """,
        ),
    ]
