"""
Recreate ``prim_master_po`` so every primary platform has a delivery-month
fallback.

Some platform PO rows do not have a delivery / appointment / GRN date yet,
especially pending or expired rows. When ``delivery_date`` is blank or cannot
be parsed, those rows disappear from DEL MONTH filters even though they are
present in the source table. This keeps the preferred platform delivery date
when valid and falls back to PO date only when needed.
"""

import importlib

from django.db import migrations


_PREVIOUS = importlib.import_module(
    "platforms.migrations.0031_zepto_delivery_date_fallback"
)

_FORWARD = _PREVIOUS._FORWARD

_REPLACEMENTS = [
    (
        "public._pm_dmy_text(b.appointment_date)        AS delivery_date",
        "COALESCE(public._pm_dmy_text(b.appointment_date), public._pm_dmy_text(b.order_date)) AS delivery_date",
    ),
    (
        "public._pm_dmy_text(s.expected_delivery_date)",
        "COALESCE(public._pm_dmy_text(s.expected_delivery_date), public._pm_dmy_text(s.po_created_at))",
    ),
    (
        "public._pm_dmy_text(bb.delivery_date)",
        "COALESCE(public._pm_dmy_text(bb.delivery_date), public._pm_dmy_text(bb.po_date))",
    ),
    (
        "public._pm_dmy_text(fg.delivery_date)",
        "COALESCE(public._pm_dmy_text(fg.delivery_date), public._pm_dmy_text(fg.po_date))",
    ),
    (
        "public._pm_dmy_text(COALESCE(NULLIF(TRIM(zm.delivery_date), ''), zm.appointment_date))",
        "COALESCE(public._pm_dmy_text(zm.delivery_date), public._pm_dmy_text(zm.appointment_date), public._pm_dmy_text(zm.po_date))",
    ),
    (
        "public._pm_dmy_text(cm.delivery_date)",
        "COALESCE(public._pm_dmy_text(cm.delivery_date), public._pm_dmy_text(cm.po_date))",
    ),
]

for old, new in _REPLACEMENTS:
    _FORWARD = _FORWARD.replace(old, new, 1)

_REVERSE = _PREVIOUS._FORWARD


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0031_zepto_delivery_date_fallback"),
    ]

    operations = [
        migrations.RunSQL(sql=_FORWARD, reverse_sql=_REVERSE),
    ]
