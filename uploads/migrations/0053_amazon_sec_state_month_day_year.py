from django.db import migrations


class Migration(migrations.Migration):
    """Add derived `month_day` + `year` columns to amazon_sec_state.

    Both are derived from `to_date` (e.g. 2026-06-16):
      month_day = lower full month name + '-' + day-of-month  -> 'june-16'
      year      = the 4-digit year                            -> 2026

    The frontend State uploader computes these at parse time so they show in the
    preview and are posted with each row; this migration adds the columns and
    backfills any rows already in the table.
    """

    dependencies = [
        ("uploads", "0052_city_state_mapping"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            ALTER TABLE public.amazon_sec_state
                ADD COLUMN IF NOT EXISTS month_day text,
                ADD COLUMN IF NOT EXISTS year integer;

            UPDATE public.amazon_sec_state
               SET month_day = lower(to_char("to_date", 'FMMonth')) || '-'
                               || EXTRACT(DAY FROM "to_date")::int,
                   year = EXTRACT(YEAR FROM "to_date")::int
             WHERE "to_date" IS NOT NULL
               AND (month_day IS NULL OR year IS NULL);
            """,
            reverse_sql=r"""
            ALTER TABLE public.amazon_sec_state
                DROP COLUMN IF EXISTS month_day,
                DROP COLUMN IF EXISTS year;
            """,
        ),
    ]
