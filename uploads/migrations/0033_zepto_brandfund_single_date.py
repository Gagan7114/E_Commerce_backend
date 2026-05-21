from django.db import migrations


class Migration(migrations.Migration):
    """Switch `zepto_brandfund` from (date_start, date_end) to a single `date`
    column. The Daily/Range toggle was removed from the uploader UI, so the
    range-mode columns are dead weight.

    New unique key: (date, promo_unique_id).

    Safe to drop the columns — the table has no rows yet (or any rows had
    `date_start = date_end` after the Daily-only revert).
    """

    dependencies = [
        ("uploads", "0032_zepto_brandfund"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.zepto_brandfund_dedup_idx;
            DROP INDEX IF EXISTS public.zepto_brandfund_date_start_idx;

            ALTER TABLE public.zepto_brandfund
                DROP COLUMN IF EXISTS date_start,
                DROP COLUMN IF EXISTS date_end,
                ADD COLUMN IF NOT EXISTS date DATE NOT NULL DEFAULT '1970-01-01';

            CREATE UNIQUE INDEX IF NOT EXISTS zepto_brandfund_dedup_idx
                ON public.zepto_brandfund (date, promo_unique_id);

            CREATE INDEX IF NOT EXISTS zepto_brandfund_date_idx
                ON public.zepto_brandfund (date);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.zepto_brandfund_dedup_idx;
            DROP INDEX IF EXISTS public.zepto_brandfund_date_idx;

            ALTER TABLE public.zepto_brandfund
                DROP COLUMN IF EXISTS date,
                ADD COLUMN IF NOT EXISTS date_start DATE NOT NULL DEFAULT '1970-01-01',
                ADD COLUMN IF NOT EXISTS date_end   DATE NOT NULL DEFAULT '1970-01-01';

            CREATE UNIQUE INDEX IF NOT EXISTS zepto_brandfund_dedup_idx
                ON public.zepto_brandfund (date_start, date_end, promo_unique_id);
            CREATE INDEX IF NOT EXISTS zepto_brandfund_date_start_idx
                ON public.zepto_brandfund (date_start);
            """,
        ),
    ]
