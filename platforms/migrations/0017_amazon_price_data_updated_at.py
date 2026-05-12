from django.db import migrations


CREATE_SQL = """
CREATE OR REPLACE FUNCTION set_amazon_price_data_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS amazon_price_data_set_updated_at ON amazon_price_data;

CREATE TRIGGER amazon_price_data_set_updated_at
BEFORE UPDATE ON amazon_price_data
FOR EACH ROW
EXECUTE FUNCTION set_amazon_price_data_updated_at();
"""

REVERSE_SQL = """
DROP TRIGGER IF EXISTS amazon_price_data_set_updated_at ON amazon_price_data;
DROP FUNCTION IF EXISTS set_amazon_price_data_updated_at();
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0016_amazon_price_data_table"),
    ]

    operations = [
        migrations.RunSQL(sql=CREATE_SQL, reverse_sql=REVERSE_SQL),
    ]
