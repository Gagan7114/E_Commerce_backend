from django.db import migrations


class Migration(migrations.Migration):
    """Create amazon_coupon — Amazon coupon metrics export (one row per coupon).

    Mirrors the columns of the AMP coupon metrics XLSX:
      Coupon name, Start date, End date, Clips, Redemptions, Total discount,
      Budget spent, Budget remaining, Budget used (fraction 0-1), Total budget.

    Plus two columns the uploader fills:
      date    — user-picked at upload time (one date per upload batch).
      format  — auto-filled with 'AMAZON' on the frontend.

    Unique key: (date, coupon_name). Re-uploading the same date+coupon UPSERTS
    so users can refresh a day's metrics without creating duplicates.
    """

    dependencies = [
        ("uploads", "0042_zepto_brandfund_dedupe_by_sku"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.amazon_coupon (
                id BIGSERIAL PRIMARY KEY,

                date              DATE NOT NULL,
                coupon_name       TEXT NOT NULL DEFAULT '',
                start_date        DATE,
                end_date          DATE,
                clips             INTEGER,
                redemptions       INTEGER,
                total_discount    NUMERIC(14, 2),
                budget_spent      NUMERIC(14, 2),
                budget_remaining  NUMERIC(14, 2),
                budget_used       NUMERIC(10, 4),
                total_budget      NUMERIC(14, 2),
                format            TEXT NOT NULL DEFAULT 'AMAZON',

                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS amazon_coupon_dedup_idx
                ON public.amazon_coupon (date, coupon_name);

            CREATE INDEX IF NOT EXISTS amazon_coupon_date_idx
                ON public.amazon_coupon (date);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.amazon_coupon;
            """,
        ),
    ]
