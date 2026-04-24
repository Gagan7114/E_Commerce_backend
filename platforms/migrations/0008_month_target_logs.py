from django.db import migrations


# Audit-log table for month_targets target edits.
#
# Policy (updated spec §4): originally `targets` was INSERT-only — once set
# for a month, the value was locked. That caused a support problem: if the
# company team typed a wrong number, there was no way to correct it from
# the UI. We now allow updating `targets` while keeping full history: the
# PRE-EDIT row snapshot is INSERTed into `month_target_logs`, then the
# live row in `month_targets` is UPDATEd with the corrected value and
# re-derived columns.
#
# One log row per edit. `month_target_id` is a soft FK (no constraint) so
# the log survives if the main row is ever deleted.
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS month_target_logs (
    id                BIGSERIAL PRIMARY KEY,
    month_target_id   BIGINT,
    "format"          TEXT,
    "type"            TEXT,
    item_head         TEXT,
    month             INTEGER,
    year              INTEGER,
    "date"            DATE,
    targets           NUMERIC(14, 2),
    done_ltrs         NUMERIC(14, 2),
    done_value        NUMERIC(18, 2),
    achieved_pct      NUMERIC(10, 4),
    est_ltr           NUMERIC(14, 2),
    est_value         NUMERIC(18, 2),
    est_ltr_pct       NUMERIC(10, 4),
    last_month        NUMERIC(14, 2),
    growth            NUMERIC(14, 2),
    growth_pct        NUMERIC(10, 4),
    change_type       TEXT DEFAULT 'UPDATE',
    reason            TEXT,
    changed_by_id     BIGINT,
    changed_by_email  TEXT,
    changed_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS month_target_logs_mt_id_idx
    ON month_target_logs (month_target_id);

CREATE INDEX IF NOT EXISTS month_target_logs_period_idx
    ON month_target_logs (
        LOWER(TRIM("format")),
        UPPER(TRIM(item_head)),
        month,
        year
    );
"""

REVERSE_SQL = """
DROP INDEX IF EXISTS month_target_logs_period_idx;
DROP INDEX IF EXISTS month_target_logs_mt_id_idx;
DROP TABLE IF EXISTS month_target_logs;
"""


class Migration(migrations.Migration):
    dependencies = [("platforms", "0007_bigbasket_format_fix")]

    operations = [migrations.RunSQL(sql=CREATE_SQL, reverse_sql=REVERSE_SQL)]
