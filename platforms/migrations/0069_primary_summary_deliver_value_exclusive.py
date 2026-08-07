"""Deliver Value reads total_delivered_amt_exclusive (user decision 2026-08-07).

primary_summary_mv's stored body (0057 FWD_SUMMARY) computed
metric_delivered_value from total_deliver_amt_inclusive; the Primary Summary
endpoint prefers the matview over the Python-side CTE, so the code change in
platforms/views.py alone would leave the Summary dashboard on the old column.
Recreate the matview with metric_delivered_value = total_delivered_amt_exclusive
(the exclusive column already flows through every CTE level of the body).

Only the summary matview changes — master_po_base/raw/mv/master are untouched.
"""
from importlib import import_module

from django.db import migrations

_OLD_METRIC = (
    "COALESCE(metric_base.total_deliver_amt_inclusive, 0::numeric)"
    " AS metric_delivered_value"
)
_NEW_METRIC = (
    "COALESCE(metric_base.total_delivered_amt_exclusive, 0::numeric)"
    " AS metric_delivered_value"
)


def _m57():
    return import_module("platforms.migrations.0057_master_po_appointment_date")


def _rebuild_summary(schema_editor, summary_sql):
    m57 = _m57()

    def ex(stmt):
        # params=None: the body contains literal '%' LIKE patterns.
        schema_editor.execute(stmt, params=None)

    ex("DROP MATERIALIZED VIEW IF EXISTS public.primary_summary_mv")
    ex(
        "CREATE MATERIALIZED VIEW public.primary_summary_mv AS "
        + summary_sql
        + " WITH DATA"
    )
    for stmt in m57.PSMV_INDEXES:
        ex(stmt)


def forwards(apps, schema_editor):
    fwd = _m57().FWD_SUMMARY
    if fwd.count(_OLD_METRIC) != 1:
        raise RuntimeError(
            "0057 FWD_SUMMARY no longer contains exactly one "
            "metric_delivered_value line - refusing to guess."
        )
    _rebuild_summary(schema_editor, fwd.replace(_OLD_METRIC, _NEW_METRIC))


def backwards(apps, schema_editor):
    _rebuild_summary(schema_editor, _m57().FWD_SUMMARY)


class Migration(migrations.Migration):

    atomic = True

    dependencies = [
        ("platforms", "0068_landing_rate_backfill_from_may_2026"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
