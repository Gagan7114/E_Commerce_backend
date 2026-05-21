from django.db import migrations


class Migration(migrations.Migration):
    """Switch zepto_brandfund dedup key from (date, promo_unique_id) to
    (date, zepto_sku_code).

    The Zepto Brand Fund export emits one row per (city × promo) split for
    the same SKU on the same day. PromoUniqueID is globally unique per raw
    row (so the old key never collided), but for reporting we want a
    single per-SKU-per-day row with all claim/qty/promo metrics summed —
    same pattern as the Swiggy/Blinkit brand-fund tables.

    Enforces `zepto_sku_code` NOT NULL DEFAULT '' (was nullable in 0032) so
    the new unique index dedupes cleanly without NULL-distinct surprises.
    """

    dependencies = [
        ("uploads", "0041_blinkit_brandfund_dedupe_by_item"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.zepto_brandfund_dedup_idx;

            UPDATE public.zepto_brandfund
               SET zepto_sku_code = ''
             WHERE zepto_sku_code IS NULL;

            ALTER TABLE public.zepto_brandfund
                ALTER COLUMN zepto_sku_code SET NOT NULL,
                ALTER COLUMN zepto_sku_code SET DEFAULT '';

            -- Collapse any pre-existing rows so the new tighter unique index
            -- can be created without conflict. Keeps the lowest id per
            -- (date, zepto_sku_code) group.
            DELETE FROM public.zepto_brandfund a
            USING public.zepto_brandfund b
            WHERE a.date = b.date
              AND a.zepto_sku_code = b.zepto_sku_code
              AND a.id > b.id;

            CREATE UNIQUE INDEX IF NOT EXISTS zepto_brandfund_dedup_idx
                ON public.zepto_brandfund (date, zepto_sku_code);

            CREATE INDEX IF NOT EXISTS zepto_brandfund_zepto_sku_code_idx
                ON public.zepto_brandfund (zepto_sku_code);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.zepto_brandfund_dedup_idx;
            DROP INDEX IF EXISTS public.zepto_brandfund_zepto_sku_code_idx;

            ALTER TABLE public.zepto_brandfund
                ALTER COLUMN zepto_sku_code DROP NOT NULL,
                ALTER COLUMN zepto_sku_code DROP DEFAULT;

            CREATE UNIQUE INDEX IF NOT EXISTS zepto_brandfund_dedup_idx
                ON public.zepto_brandfund (date, promo_unique_id);
            """,
        ),
    ]
