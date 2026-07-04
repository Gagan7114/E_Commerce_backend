from django.db import migrations


class Migration(migrations.Migration):
    """Amazon Secondary state-wise table now stores the City export.

    The Amazon "Sales by State" report was replaced by the city-wise variant
    (View By=[City]); the `state` column is renamed to `city` and the table is
    cleared so only city-level uploads live in it. Guarded so environments where
    the rename was already applied by hand (or fresh installs re-running it) are
    a no-op. Table name stays amazon_sec_state.
    """

    dependencies = [
        ("uploads", "0068_consolidated_fsn_report_date"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'amazon_sec_state'
                      AND column_name = 'state'
                ) THEN
                    TRUNCATE TABLE public.amazon_sec_state;
                    ALTER TABLE public.amazon_sec_state RENAME COLUMN state TO city;
                END IF;
            END $$;
            """,
            reverse_sql=r"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'amazon_sec_state'
                      AND column_name = 'city'
                ) THEN
                    ALTER TABLE public.amazon_sec_state RENAME COLUMN city TO state;
                END IF;
            END $$;
            """,
        ),
    ]
