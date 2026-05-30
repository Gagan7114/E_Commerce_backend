from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0033_reapply_primary_delivery_fallbacks"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS primary_month_targets (
                id BIGSERIAL PRIMARY KEY,
                "format" TEXT NOT NULL,
                "type" TEXT NOT NULL DEFAULT 'prim',
                item_head TEXT NOT NULL,
                month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
                year INTEGER NOT NULL CHECK (year BETWEEN 2000 AND 2100),
                "date" DATE,
                targets NUMERIC(14, 2) NOT NULL DEFAULT 0,
                done_ltrs NUMERIC(14, 2) NOT NULL DEFAULT 0,
                achieved_pct NUMERIC(12, 6),
                est_ltr NUMERIC(14, 2) NOT NULL DEFAULT 0,
                est_ltr_pct NUMERIC(12, 6),
                drr NUMERIC(14, 2) NOT NULL DEFAULT 0,
                require_drr NUMERIC(14, 2) NOT NULL DEFAULT 0,
                pending_ltr NUMERIC(14, 2) NOT NULL DEFAULT 0,
                dp_ltrs NUMERIC(14, 2) NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS primary_month_targets_unique_month
                ON primary_month_targets (
                    LOWER(TRIM("format")),
                    UPPER(TRIM(item_head)),
                    month,
                    year
                );

            CREATE INDEX IF NOT EXISTS primary_month_targets_month_idx
                ON primary_month_targets (year, month);

            CREATE TABLE IF NOT EXISTS primary_month_target_logs (
                id BIGSERIAL PRIMARY KEY,
                primary_month_target_id BIGINT,
                "format" TEXT,
                "type" TEXT,
                item_head TEXT,
                month INTEGER,
                year INTEGER,
                "date" DATE,
                targets NUMERIC(14, 2),
                new_targets NUMERIC(14, 2),
                done_ltrs NUMERIC(14, 2),
                achieved_pct NUMERIC(12, 6),
                est_ltr NUMERIC(14, 2),
                est_ltr_pct NUMERIC(12, 6),
                drr NUMERIC(14, 2),
                require_drr NUMERIC(14, 2),
                pending_ltr NUMERIC(14, 2),
                dp_ltrs NUMERIC(14, 2),
                change_type TEXT NOT NULL DEFAULT 'UPDATE',
                reason TEXT,
                changed_by_id BIGINT,
                changed_by_email TEXT,
                changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS primary_month_target_logs_target_idx
                ON primary_month_target_logs (primary_month_target_id, changed_at DESC);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS primary_month_target_logs;
            DROP TABLE IF EXISTS primary_month_targets;
            """,
        ),
    ]
