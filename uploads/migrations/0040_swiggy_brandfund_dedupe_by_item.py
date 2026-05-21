from django.db import migrations


class Migration(migrations.Migration):
    """Switch swiggy_brandfund dedup key from (date, city, item_code,
    combo_item_code) to just (date, item_code).

    The Swiggy Brand Fund export emits one row per (city × combo) breakdown,
    but for downstream reporting we want a single per-item-per-day row with
    all numeric metrics summed. The uploader will merge raw rows by
    (date, item_code) before insert, so the unique index must match.

    Safe to drop the previous index — any rows currently in the table are
    a small dev-test dataset (the bug fixed in 0039 prevented production
    Swiggy brand-fund uploads from succeeding until now).
    """

    dependencies = [
        ("uploads", "0039_swiggy_brandfund_fix_schema"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.swiggy_brandfund_dedup_idx;

            -- Collapse any pre-existing rows so the new tighter unique index
            -- can be created without conflict. Keeps the lowest id per
            -- (date, item_code) group.
            DELETE FROM public.swiggy_brandfund a
            USING public.swiggy_brandfund b
            WHERE a.date = b.date
              AND a.item_code = b.item_code
              AND a.id > b.id;

            CREATE UNIQUE INDEX IF NOT EXISTS swiggy_brandfund_dedup_idx
                ON public.swiggy_brandfund (date, item_code);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.swiggy_brandfund_dedup_idx;
            CREATE UNIQUE INDEX IF NOT EXISTS swiggy_brandfund_dedup_idx
                ON public.swiggy_brandfund (date, city, item_code, combo_item_code);
            """,
        ),
    ]
