"""
Recreate ``prim_master_po`` so Zepto rows with blank ``grn_date`` still get a
delivery date.

The Zepto branch previously used ``z.grn_date`` directly as ``delivery_date``.
Many pending Zepto rows do not have a GRN date yet, so delivery-month filters
only saw the small subset that had GRN data. Falling back to ``po_date`` keeps
those rows visible in delivery-month reporting while still using GRN date when
it exists.
"""

import importlib

from django.db import migrations


_ZERO_TWENTY_SEVEN = importlib.import_module(
    "platforms.migrations.0027_prim_master_po_view"
)

_FORWARD = _ZERO_TWENTY_SEVEN._FORWARD.replace(
    "public._pm_dmy_text(z.grn_date)",
    "COALESCE(public._pm_dmy_text(z.grn_date), public._pm_dmy_text(z.po_date))",
    1,
)

_REVERSE = _ZERO_TWENTY_SEVEN._FORWARD


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0030_short_item_name_in_view"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
