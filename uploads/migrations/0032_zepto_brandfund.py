from django.db import migrations


class Migration(migrations.Migration):
    """Create zepto_brandfund table for Zepto Brand Fund report ingestion.

    Source: Zepto Brand Fund CSV export (43 columns). Per the spec, we drop
    4 source columns:
      - GeneratedFrom, GeneratedTill   (file-level metadata; replaced by
                                        user-picked date_start / date_end)
      - StartDate, EndDate             (duplicates of ActiveFrom/Till in
                                        prose form — redundant)

    The user picks a date (Daily mode → one date stored as both date_start
    and date_end; Range mode → start + end). Format is auto-tagged 'ZEPTO'.

    Unique key: (date_start, date_end, promo_unique_id). PromoUniqueID is
    the per-(promo × city × product) identifier; combined with the
    user-picked dates it dedupes cleanly on re-uploads.
    """

    dependencies = [
        ("uploads", "0031_flipkart_ads_master_view"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS public.zepto_brandfund (
                id BIGSERIAL PRIMARY KEY,

                -- Unique key
                date_start       DATE NOT NULL DEFAULT '1970-01-01',
                date_end         DATE NOT NULL DEFAULT '1970-01-01',
                promo_unique_id  TEXT NOT NULL DEFAULT '',

                -- Text descriptors
                product_type             TEXT,
                bundle_product_variant_id TEXT,
                zepto_sku_code           TEXT,
                sku_name                 TEXT,
                campaign_id              TEXT,
                campaign_name            TEXT,
                category_name            TEXT,
                subcategory_name         TEXT,
                brand_id                 TEXT,
                brand_name               TEXT,
                unit_of_measure          TEXT,
                event_type               TEXT,
                tag                      TEXT,
                mfg_id                   TEXT,
                mfg                      TEXT,
                city                     TEXT,
                active_from              TEXT,
                active_till              TEXT,
                type                     TEXT,
                promo_id                 TEXT,
                promo_input_id           TEXT,
                is_gst_adjusted          TEXT,
                is_cess_adjusted         TEXT,
                is_margin_adjusted       TEXT,
                is_return                TEXT,
                vendor_code              TEXT,
                comments                 TEXT,

                -- Numeric fields
                weight              NUMERIC,
                quantity_in_bundle  NUMERIC,
                mrp                 NUMERIC,
                promo_percentage    NUMERIC,
                promo_inr           NUMERIC,
                gst                 NUMERIC,
                cess                NUMERIC,
                margin              NUMERIC,
                qty                 NUMERIC,
                claim               NUMERIC,
                promo_claim_amt     NUMERIC,

                -- Bookkeeping
                format       TEXT NOT NULL DEFAULT 'ZEPTO',
                uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS zepto_brandfund_dedup_idx
                ON public.zepto_brandfund (date_start, date_end, promo_unique_id);

            CREATE INDEX IF NOT EXISTS zepto_brandfund_brand_id_idx
                ON public.zepto_brandfund (brand_id);

            CREATE INDEX IF NOT EXISTS zepto_brandfund_date_start_idx
                ON public.zepto_brandfund (date_start);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS public.zepto_brandfund;
            """,
        ),
    ]
