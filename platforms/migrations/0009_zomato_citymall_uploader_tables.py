from django.db import migrations


# Zomato and CityMall already exist as PlatformConfig rows (added in
# 0004_monthly_targets) but with empty inventory_table / secondary_table.
# This migration:
#   1. Creates the physical Postgres tables that the batch uploader writes to,
#      mirroring the BigBasket-style layout (city + sku + qty + value, with
#      a date dimension on secondary and an inventory_date dimension on
#      inventory).
#   2. Backfills the inventory_table / secondary_table on PlatformConfig so
#      `/api/platform/{slug}/stats` and the uploader UI can resolve them.
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS "zomatoSec" (
    id            BIGSERIAL PRIMARY KEY,
    "date"        DATE,
    city          TEXT,
    brand         TEXT,
    category      TEXT,
    sku_code      TEXT,
    sku_name      TEXT,
    units         INTEGER,
    mrp           NUMERIC(14, 2),
    sales_value   NUMERIC(18, 2),
    CONSTRAINT zomatosec_unique UNIQUE (sku_code, city, "date")
);

CREATE TABLE IF NOT EXISTS "citymallSec" (
    id            BIGSERIAL PRIMARY KEY,
    "date"        DATE,
    city          TEXT,
    brand         TEXT,
    category      TEXT,
    sku_code      TEXT,
    sku_name      TEXT,
    units         INTEGER,
    mrp           NUMERIC(14, 2),
    sales_value   NUMERIC(18, 2),
    CONSTRAINT citymallsec_unique UNIQUE (sku_code, city, "date")
);

CREATE TABLE IF NOT EXISTS zomato_inventory (
    id              BIGSERIAL PRIMARY KEY,
    inventory_date  DATE,
    city            TEXT,
    sku_code        TEXT,
    sku_name        TEXT,
    brand           TEXT,
    category        TEXT,
    soh             INTEGER,
    soh_value       NUMERIC(18, 2),
    CONSTRAINT zomato_inventory_unique UNIQUE (inventory_date, city, sku_code)
);

CREATE TABLE IF NOT EXISTS citymall_inventory (
    id              BIGSERIAL PRIMARY KEY,
    inventory_date  DATE,
    city            TEXT,
    sku_code        TEXT,
    sku_name        TEXT,
    brand           TEXT,
    category        TEXT,
    soh             INTEGER,
    soh_value       NUMERIC(18, 2),
    CONSTRAINT citymall_inventory_unique UNIQUE (inventory_date, city, sku_code)
);
"""

REVERSE_SQL = """
DROP TABLE IF EXISTS citymall_inventory;
DROP TABLE IF EXISTS zomato_inventory;
DROP TABLE IF EXISTS "citymallSec";
DROP TABLE IF EXISTS "zomatoSec";
"""


_TABLE_BINDINGS = [
    ("zomato",   "zomato_inventory",   "zomatoSec"),
    ("citymall", "citymall_inventory", "citymallSec"),
]


def bind_tables(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")
    for slug, inv, sec in _TABLE_BINDINGS:
        PlatformConfig.objects.filter(slug=slug).update(
            inventory_table=inv,
            secondary_table=sec,
        )


def unbind_tables(apps, schema_editor):
    PlatformConfig = apps.get_model("platforms", "PlatformConfig")
    PlatformConfig.objects.filter(
        slug__in=[s for s, *_ in _TABLE_BINDINGS]
    ).update(inventory_table="", secondary_table="")


class Migration(migrations.Migration):
    dependencies = [("platforms", "0008_month_target_logs")]

    operations = [
        migrations.RunSQL(sql=CREATE_SQL, reverse_sql=REVERSE_SQL),
        migrations.RunPython(bind_tables, unbind_tables),
    ]
