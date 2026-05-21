"""
Recreate ``prim_master_po`` so the ``item`` column returns the SHORT
``master_sheet.item`` label (e.g. "JIVO POMACE 1L") instead of the long
``product_name`` (e.g. "Jivo Pomace Olive Oil 1.0 LITER"). The dashboard's
Top-SKU / vendor / item-head cards display ``row.item`` directly, so this
fix gives compact, readable labels without any frontend change.

Pack-size regex parsing (in ``_pack_text``) still has access to the long
``product_name`` via the new ``master_full_name`` column on master_lookup,
so per_liter detection keeps working even when the short item label doesn't
contain a unit.

Same pattern as 0028/0029: re-runs migration 0027's ``_FORWARD`` SQL so the
single source of truth stays in 0027.
"""

import importlib

from django.db import migrations


_ZERO_TWENTY_SEVEN = importlib.import_module(
    "platforms.migrations.0027_prim_master_po_view"
)
_FORWARD = _ZERO_TWENTY_SEVEN._FORWARD


_REVERSE = """
DROP VIEW IF EXISTS public.prim_master_po;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0029_blinkit_delivered_from_remaining"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
