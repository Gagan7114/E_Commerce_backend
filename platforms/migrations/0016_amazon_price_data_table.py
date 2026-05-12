from django.db import migrations


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS amazon_price_data (
    id                  BIGSERIAL PRIMARY KEY,
    upload_date         DATE NOT NULL,
    url                 TEXT,
    asin                TEXT NOT NULL,
    product             TEXT,
    margin_basis        TEXT,
    mrp                 NUMERIC(14, 2),
    asp                 NUMERIC(14, 2),
    margin_pct          NUMERIC(10, 4),
    tax_pct             NUMERIC(10, 4),
    cost_without_tax    NUMERIC(14, 2),
    url_price           NUMERIC(14, 2),
    stock_status        TEXT,
    seller              TEXT,
    rk_price            NUMERIC(14, 2),
    jm_price            NUMERIC(14, 2),
    svd_price           NUMERIC(14, 2),
    bau_price           NUMERIC(14, 2),
    art_price           NUMERIC(14, 2),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT amazon_price_data_unique UNIQUE (upload_date, asin)
);

CREATE INDEX IF NOT EXISTS amazon_price_data_upload_date_idx
    ON amazon_price_data (upload_date);

CREATE INDEX IF NOT EXISTS amazon_price_data_asin_idx
    ON amazon_price_data (asin);
"""

REVERSE_SQL = """
DROP INDEX IF EXISTS amazon_price_data_asin_idx;
DROP INDEX IF EXISTS amazon_price_data_upload_date_idx;
DROP TABLE IF EXISTS amazon_price_data;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0015_flipkart_secondary_all_view"),
        ("platforms", "0009_month_target_logs_new_targets"),
    ]

    operations = [
        migrations.RunSQL(sql=CREATE_SQL, reverse_sql=REVERSE_SQL),
    ]
