"""City -> State -> PIN code mapping table (one row per city).

Three columns only — city, state, pincode (plus the id PK). Seeded (city, state)
from city_state_mapping (uploads.0052) — the aggregated city->state universe for
the QC platforms — with `pincode` left blank for ops to fill in via the
Master-Sheet-style manager. A UNIQUE functional index on the normalised city
(UPPER, every non-alphanumeric run collapsed to one space) keeps it one row per
city so re-uploads update instead of duplicating, without storing a separate key
column. Not consumed by any view yet — pure reference data managed through
/api/upload/pincode-mapping.
"""
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0065_amazon_coupon_item_head_by_item"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.pincode_mapping (
                id       bigserial PRIMARY KEY,
                city     text NOT NULL,
                state    text NOT NULL,
                pincode  text
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_pincode_mapping_city
                ON public.pincode_mapping
                (btrim(regexp_replace(upper(city), '[^A-Z0-9]+', ' ', 'g')));

            INSERT INTO public.pincode_mapping (city, state)
            SELECT DISTINCT ON (btrim(regexp_replace(upper(city), '[^A-Z0-9]+', ' ', 'g')))
                   city, state
            FROM public.city_state_mapping
            ORDER BY btrim(regexp_replace(upper(city), '[^A-Z0-9]+', ' ', 'g')), city;
            """,
            reverse_sql="DROP TABLE IF EXISTS public.pincode_mapping;",
        ),
    ]
