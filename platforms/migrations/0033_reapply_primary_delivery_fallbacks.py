"""
Reapply the current ``prim_master_po`` delivery-date fallback view.

Some local databases may have migrations 0031/0032 recorded as applied while
the migration files were missing from disk, leaving the live view on the old
Zepto ``grn_date`` expression. This migration reapplies the final view body so
Zepto DEL MONTH totals include rows whose GRN date is absent or unparseable by
falling back to PO date.
"""

import importlib

from django.db import migrations


_PREVIOUS = importlib.import_module(
    "platforms.migrations.0032_primary_delivery_date_fallbacks"
)

_FORWARD = _PREVIOUS._FORWARD
_REVERSE = _PREVIOUS._REVERSE


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0032_primary_delivery_date_fallbacks"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
