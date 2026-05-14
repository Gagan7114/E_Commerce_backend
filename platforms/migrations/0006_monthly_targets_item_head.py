from django.db import migrations


# The `month_targets` table (singular) already exists in the live database.
# This migration adds the tracking columns (`id`, `month`, `year`, timestamps)
# and the unique constraint that the Monthly Targets feature relies on.
#
# Every statement uses IF NOT EXISTS so re-running is safe.
#
# Note: `item_head`, `format`, `type`, `date`, and the 11 numeric columns
# already exist on the table — we do not touch them here.
ADD_SQL = """
-- Fresh test/dev databases may not have the legacy singular table yet.
-- The live database already had it before this migration was introduced.
CREATE TABLE IF NOT EXISTS month_targets (
    id        BIGSERIAL PRIMARY KEY,
    "format" TEXT,
    item_head TEXT,
    "date"   DATE
);

-- Primary key. Uses BIGSERIAL so new rows get an auto-incrementing id.
-- If some existing rows have NULL ids they get backfilled by DEFAULT.
ALTER TABLE month_targets
    ADD COLUMN IF NOT EXISTS id         BIGSERIAL,
    ADD COLUMN IF NOT EXISTS month      INTEGER,
    ADD COLUMN IF NOT EXISTS year       INTEGER,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Promote `id` to PRIMARY KEY if it isn't already.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'month_targets'::regclass
           AND contype = 'p'
    ) THEN
        ALTER TABLE month_targets ADD PRIMARY KEY (id);
    END IF;
END $$;

-- Back-fill month/year from any rows that had `date` set before the columns
-- existed. Harmless on a fresh install.
UPDATE month_targets
   SET month = EXTRACT(MONTH FROM "date")::INTEGER,
       year  = EXTRACT(YEAR  FROM "date")::INTEGER
 WHERE "date" IS NOT NULL
   AND (month IS NULL OR year IS NULL);

-- One row per (platform, SKU-group, month, year). Comparison is
-- case-insensitive / trimmed so "Blinkit", "blinkit ", "BLINKIT" collapse
-- to the same logical row.
CREATE UNIQUE INDEX IF NOT EXISTS month_targets_uq
    ON month_targets (
        LOWER(TRIM("format")),
        UPPER(TRIM(item_head)),
        month,
        year
    );
"""


REVERSE_SQL = """
DROP INDEX IF EXISTS month_targets_uq;

ALTER TABLE month_targets
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS month,
    DROP COLUMN IF EXISTS year,
    DROP COLUMN IF EXISTS created_at,
    DROP COLUMN IF EXISTS updated_at;
"""


class Migration(migrations.Migration):
    dependencies = [("platforms", "0005_monthly_targets_columns")]

    operations = [migrations.RunSQL(sql=ADD_SQL, reverse_sql=REVERSE_SQL)]
