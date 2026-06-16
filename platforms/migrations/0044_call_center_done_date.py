"""Call Center targets — add the editable Done Ltrs + Date columns.

Extends the isolated `call_center_targets` store (migration 0043) so the
frontend can persist a Call Center row's `done_ltrs` and source `date` in
addition to its `targets`. Still fully self-contained: no existing target table
or platform logic is touched. Idempotent: ADD COLUMN IF NOT EXISTS, reverse
DROP COLUMN IF EXISTS.
"""

from django.db import migrations


ADD_SQL = r"""
ALTER TABLE public.call_center_targets
    ADD COLUMN IF NOT EXISTS done_ltrs NUMERIC NULL;
ALTER TABLE public.call_center_targets
    ADD COLUMN IF NOT EXISTS data_date DATE NULL;
"""

DROP_SQL = r"""
ALTER TABLE public.call_center_targets DROP COLUMN IF EXISTS done_ltrs;
ALTER TABLE public.call_center_targets DROP COLUMN IF EXISTS data_date;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0043_call_center_targets"),
    ]

    operations = [
        migrations.RunSQL(ADD_SQL, reverse_sql=DROP_SQL),
    ]
