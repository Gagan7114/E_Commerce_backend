"""
Restore ``prim_master_po`` as a live UNION ALL view over the per-platform
``<slug>_prim`` tables — the way migration 0027 originally intended.

After 0027 was applied somebody manually replaced the view body with
``SELECT * FROM prim_master_po_csv_source``, turning the dashboard into a
stale snapshot of an externally-imported CSV. This migration:

1. Drops that manually-overridden view.
2. Re-runs migration 0027's ``_FORWARD`` SQL so the view is rebuilt as a
   live join over the platform PO tables, including the Zepto ``sku`` fix
   committed alongside this migration (Zepto's product UUID lives in
   ``sku``, not ``sku_code``, so the master_sheet join now works).

The ``prim_master_po_csv_source`` table is *not* dropped — it stays as a
backup snapshot. Drop it manually if you no longer want it around.

Because the view body lives in 0027, any future correction only needs to
happen there; this migration simply replays it.
"""

import importlib

from django.db import migrations


_ZERO_TWENTY_SEVEN = importlib.import_module(
    "platforms.migrations.0027_prim_master_po_view"
)
_FORWARD = _ZERO_TWENTY_SEVEN._FORWARD


_REVERSE = """
-- Rolling this back restores the previous behaviour: the view returns
-- whatever lives in prim_master_po_csv_source. The table must still exist
-- on the target DB for this rollback to succeed.
DROP VIEW IF EXISTS public.prim_master_po;
CREATE VIEW public.prim_master_po AS
    SELECT * FROM public.prim_master_po_csv_source;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0027_prim_master_po_view"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
