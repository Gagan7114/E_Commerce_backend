from django.db import migrations


class Migration(migrations.Migration):
    """Rename amazon_sec_state -> amazon_sec_city and keep only the latest
    cumulative range per month.

    The table has stored the city-wise Amazon Secondary export since uploads
    migration 0069; the name now says so. Indexes are renamed to match.

    Amazon's export is cumulative month-to-date (1-28, then 1-29, then 1-30
    ...), so re-uploads used to pile up overlapping snapshots and every sum
    over the month multi-counted. The DELETE keeps, for each business + month,
    only the rows carrying that month's MAX to_date — the freshest snapshot,
    which already contains everything the older ones did. From here on the
    uploader prunes older ranges on every upload (_batch_upload), so this
    cleanup is one-time.
    """

    dependencies = [
        ("uploads", "0069_amazon_sec_state_city"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = 'amazon_sec_state'
                ) THEN
                    ALTER TABLE public.amazon_sec_state RENAME TO amazon_sec_city;
                END IF;
            END $$;

            ALTER INDEX IF EXISTS amazon_sec_state_business_state_asin_from_to_key
                RENAME TO amazon_sec_city_business_city_asin_from_to_key;
            ALTER INDEX IF EXISTS idx_amazon_sec_state_dates
                RENAME TO idx_amazon_sec_city_dates;
            ALTER INDEX IF EXISTS idx_amazon_sec_state_asin
                RENAME TO idx_amazon_sec_city_asin;
            ALTER INDEX IF EXISTS idx_amazon_sec_state_state
                RENAME TO idx_amazon_sec_city_city;
            ALTER INDEX IF EXISTS amazon_sec_state_pkey
                RENAME TO amazon_sec_city_pkey;
            ALTER SEQUENCE IF EXISTS amazon_sec_state_id_seq
                RENAME TO amazon_sec_city_id_seq;

            -- One-time dedupe: keep only the freshest (max to_date) snapshot
            -- per business + month of from_date.
            DELETE FROM public.amazon_sec_city a
            WHERE EXISTS (
                SELECT 1 FROM public.amazon_sec_city b
                WHERE COALESCE(b.business, '') = COALESCE(a.business, '')
                  AND date_trunc('month', b.from_date) = date_trunc('month', a.from_date)
                  AND b.to_date > a.to_date
            );
            """,
            reverse_sql=r"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = 'amazon_sec_city'
                ) THEN
                    ALTER TABLE public.amazon_sec_city RENAME TO amazon_sec_state;
                END IF;
            END $$;

            ALTER INDEX IF EXISTS amazon_sec_city_business_city_asin_from_to_key
                RENAME TO amazon_sec_state_business_state_asin_from_to_key;
            ALTER INDEX IF EXISTS idx_amazon_sec_city_dates
                RENAME TO idx_amazon_sec_state_dates;
            ALTER INDEX IF EXISTS idx_amazon_sec_city_asin
                RENAME TO idx_amazon_sec_state_asin;
            ALTER INDEX IF EXISTS idx_amazon_sec_city_city
                RENAME TO idx_amazon_sec_state_state;
            ALTER INDEX IF EXISTS amazon_sec_city_pkey
                RENAME TO amazon_sec_state_pkey;
            ALTER SEQUENCE IF EXISTS amazon_sec_city_id_seq
                RENAME TO amazon_sec_state_id_seq;
            """,
        ),
    ]
