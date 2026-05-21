from django.db import migrations


class Migration(migrations.Migration):
    """Switch blinkit_brandfund dedup key from (date, city, item_id, p_type)
    to just (date, item_id).

    Same rationale as 0040 for Swiggy: the Blinkit Brand Fund export emits
    one row per (city × p_type × offer) breakdown, but reporting wants a
    single per-item-per-day row with all numeric metrics summed. The
    uploader will merge raw rows by (date, item_id) before insert.
    """

    dependencies = [
        ("uploads", "0040_swiggy_brandfund_dedupe_by_item"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.blinkit_brandfund_dedup_idx;

            -- Collapse any pre-existing rows so the new tighter unique index
            -- can be created without conflict. Keeps the lowest id per
            -- (date, item_id) group.
            DELETE FROM public.blinkit_brandfund a
            USING public.blinkit_brandfund b
            WHERE a.date = b.date
              AND a.item_id = b.item_id
              AND a.id > b.id;

            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_brandfund_dedup_idx
                ON public.blinkit_brandfund (date, item_id);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.blinkit_brandfund_dedup_idx;
            CREATE UNIQUE INDEX IF NOT EXISTS blinkit_brandfund_dedup_idx
                ON public.blinkit_brandfund (date, city, item_id, p_type);
            """,
        ),
    ]
