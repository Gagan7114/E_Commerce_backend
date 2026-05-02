from django.db import migrations


# Add `new_targets` to the audit log so each row captures BOTH the
# pre-edit (`targets`) and post-edit (`new_targets`) values in one record.
# Backfill is intentionally left NULL — historical rows pre-dating this
# migration only carry the pre-edit snapshot.
CREATE_SQL = """
ALTER TABLE month_target_logs
    ADD COLUMN IF NOT EXISTS new_targets NUMERIC(14, 2);
"""

REVERSE_SQL = """
ALTER TABLE month_target_logs
    DROP COLUMN IF EXISTS new_targets;
"""


class Migration(migrations.Migration):
    dependencies = [("platforms", "0008_month_target_logs")]

    operations = [migrations.RunSQL(sql=CREATE_SQL, reverse_sql=REVERSE_SQL)]
