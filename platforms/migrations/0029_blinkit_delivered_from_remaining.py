"""
Recreate ``prim_master_po`` so the Blinkit branch computes ``delivered_qty``
from ``units_ordered - remaining_quantity`` instead of the unreliable
``blinkit_prim.delivered_qty`` column.

Audit found that Blinkit's upload pipeline does not populate ``delivered_qty``
for most rows (even fully-received POs marked Expired had delivered_qty = 0
or NULL), while ``remaining_quantity`` is filled in accurately. For Blinkit
MAY 2026 the dashboard previously showed 3,051 units delivered; with this
fix it shows the true ~61,589 units.

Like migration 0028, this re-runs migration 0027's corrected ``_FORWARD``
SQL — the view body lives there so it stays a single source of truth.
"""

import importlib

from django.db import migrations


_ZERO_TWENTY_SEVEN = importlib.import_module(
    "platforms.migrations.0027_prim_master_po_view"
)
_FORWARD = _ZERO_TWENTY_SEVEN._FORWARD


_REVERSE = """
-- Rolling this back leaves Blinkit's broken delivered_qty data in place,
-- so this rollback only restores the previous view shape (which used
-- b.delivered_qty directly). The view body is identical apart from one
-- line in the BLINKIT branch.
DROP VIEW IF EXISTS public.prim_master_po;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0028_restore_live_prim_master_po"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
