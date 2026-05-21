from django.db import migrations


class Migration(migrations.Migration):
    """Drop `store_id` + `area_name` from swiggy_brandfund and switch the
    unique key to (date, city, item_code, combo_item_code).

    The actual Swiggy Brand Fund export ('Discounts Report' sheet) doesn't
    contain STORE_ID or AREA_NAME — only BRAND / CITY / item-level columns.
    Without STORE_ID the original unique key was unusable and the uploader's
    REQUIRED_DB_FIELDS rejected every row at the header-detection step.

    Safe to drop without data preservation — the table is empty (no Swiggy
    brand-fund upload has succeeded yet because of this very bug).
    """

    dependencies = [
        ("uploads", "0038_blinkit_brandfund_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.swiggy_brandfund_dedup_idx;
            DROP INDEX IF EXISTS public.swiggy_brandfund_store_id_idx;

            ALTER TABLE public.swiggy_brandfund
                DROP COLUMN IF EXISTS store_id,
                DROP COLUMN IF EXISTS area_name,
                ALTER COLUMN city SET NOT NULL,
                ALTER COLUMN city SET DEFAULT '';

            CREATE UNIQUE INDEX IF NOT EXISTS swiggy_brandfund_dedup_idx
                ON public.swiggy_brandfund (date, city, item_code, combo_item_code);

            CREATE INDEX IF NOT EXISTS swiggy_brandfund_city_idx
                ON public.swiggy_brandfund (city);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.swiggy_brandfund_dedup_idx;
            DROP INDEX IF EXISTS public.swiggy_brandfund_city_idx;

            ALTER TABLE public.swiggy_brandfund
                ALTER COLUMN city DROP NOT NULL,
                ALTER COLUMN city DROP DEFAULT,
                ADD COLUMN IF NOT EXISTS area_name TEXT,
                ADD COLUMN IF NOT EXISTS store_id  TEXT NOT NULL DEFAULT '';

            CREATE UNIQUE INDEX IF NOT EXISTS swiggy_brandfund_dedup_idx
                ON public.swiggy_brandfund (date, store_id, item_code, combo_item_code);
            CREATE INDEX IF NOT EXISTS swiggy_brandfund_store_id_idx
                ON public.swiggy_brandfund (store_id);
            """,
        ),
    ]
