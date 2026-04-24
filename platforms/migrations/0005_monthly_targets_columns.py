from django.db import migrations


# Adds the 13 sheet columns (format, type, date, targets, done_ltrs,
# done_value, achieved_pct, est_ltr, est_value, est_ltr_pct, last_month,
# growth, growth_pct) to monthly_targets.
#
# Idempotent: IF NOT EXISTS on every column, so re-running against a DB
# where some columns already exist is safe. The table itself is created if
# missing so a fresh environment also works without a manual CREATE TABLE.
ADD_COLUMNS_SQL = """
CREATE TABLE IF NOT EXISTS monthly_targets (
    id BIGSERIAL PRIMARY KEY
);

ALTER TABLE monthly_targets
    ADD COLUMN IF NOT EXISTS format        TEXT,
    ADD COLUMN IF NOT EXISTS type          TEXT,
    ADD COLUMN IF NOT EXISTS date          DATE,
    ADD COLUMN IF NOT EXISTS targets       NUMERIC(14, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS done_ltrs     NUMERIC(14, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS done_value    NUMERIC(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS achieved_pct  NUMERIC(10, 4) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS est_ltr       NUMERIC(14, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS est_value     NUMERIC(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS est_ltr_pct   NUMERIC(10, 4) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_month    NUMERIC(14, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS growth        NUMERIC(14, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS growth_pct    NUMERIC(10, 4) DEFAULT 0;
"""


# Reverse drops only the columns this migration added. The table itself is
# left in place — if it pre-existed the migration we don't own it.
REVERSE_SQL = """
ALTER TABLE monthly_targets
    DROP COLUMN IF EXISTS format,
    DROP COLUMN IF EXISTS type,
    DROP COLUMN IF EXISTS date,
    DROP COLUMN IF EXISTS targets,
    DROP COLUMN IF EXISTS done_ltrs,
    DROP COLUMN IF EXISTS done_value,
    DROP COLUMN IF EXISTS achieved_pct,
    DROP COLUMN IF EXISTS est_ltr,
    DROP COLUMN IF EXISTS est_value,
    DROP COLUMN IF EXISTS est_ltr_pct,
    DROP COLUMN IF EXISTS last_month,
    DROP COLUMN IF EXISTS growth,
    DROP COLUMN IF EXISTS growth_pct;
"""


class Migration(migrations.Migration):
    dependencies = [("platforms", "0004_monthly_targets")]

    operations = [
        migrations.RunSQL(sql=ADD_COLUMNS_SQL, reverse_sql=REVERSE_SQL),
    ]
