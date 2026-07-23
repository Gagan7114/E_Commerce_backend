from django.db import migrations


class Migration(migrations.Migration):
    """Platform city universe — the coverage denominator for the Penetration
    Report.

    One row per (platform, city): the official list of cities a quick-commerce
    platform actually operates in (dark stores / serviceable cities), uploaded
    from each platform's seller portal via the Upload Hub "City Universe"
    dataset. For BIG BASKET the rows are its regional CLUSTERS (its reporting
    unit — "Chandigarh Tricity", "Lucknow-Kanpur"), not cities.

    Why: the report's coverage %% used to divide every platform's covered
    cities by INDIA_TOTAL_CITIES (7,935 census towns) — sensible for Amazon
    parcel delivery, meaningless for dark-store platforms (Blinkit operating
    in ~200 cities read as "2.3%% covered"). When this table has rows for a
    platform they become its denominator; platforms without rows fall back to
    a universe derived from their own all-time secondary + inventory history.
    Amazon / Amazon MP never use this table.

    Upsert dedup key matches the frontend cityUniverse config and the forced
    server-side key: (platform, city).
    """

    dependencies = [
        ("uploads", "0083_fkg_binola_casing"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.platform_city_universe (
                id         bigserial PRIMARY KEY,
                platform   text NOT NULL,
                city       text NOT NULL,
                state      text,
                active     boolean NOT NULL DEFAULT true,
                created_at timestamp without time zone DEFAULT now()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS platform_city_universe_platform_city_key
                ON public.platform_city_universe (platform, city);
            """,
            reverse_sql=r"""
            DROP TABLE IF EXISTS public.platform_city_universe;
            """,
        ),
    ]
