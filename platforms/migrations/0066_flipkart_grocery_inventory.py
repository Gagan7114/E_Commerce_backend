"""flipkart_grocery_inventory — daily SOH snapshot from the Vendor Hub INVENTORY report.

Joins the existing per-platform inventory family (blinkit/zepto/swiggy/bigbasket/
jiomart/amazon/zomato/citymall_inventory); Flipkart Grocery was the only platform
without one. Fed by the flipkart-grocery-cli `pull --type inventory` lane (flow
16-INV, dev-PC daily) via jivo-ecom-upload `upload flipkart-grocery-inventory`.

Grain: one row per (inventory_date, warehouse, sku_code) — the report is per-FSN
per-warehouse (~22 Jivo FSNs x ~760 warehouses per day), snapshot-dated from the
report's "Inventory Data Valid as of" column. Upserts on that key make re-uploads
idempotent. Numeric ROS/DOH columns are unconstrained NUMERIC because the report
ships full-precision doubles (same policy as total_po.basic_rate).

Reversible; creates one empty table and its unique constraint, touches no data.
"""

from django.db import migrations


FORWARD = r"""
CREATE TABLE IF NOT EXISTS flipkart_grocery_inventory (
    id                BIGSERIAL PRIMARY KEY,
    inventory_date    DATE NOT NULL,
    warehouse         TEXT NOT NULL,
    sku_code          TEXT NOT NULL,
    sku_name          TEXT,
    brand             TEXT,
    category          TEXT,
    vertical          TEXT,
    soh               INTEGER,
    soh_value         NUMERIC(18, 2),
    ros_30_units      NUMERIC,
    ros_60_units      NUMERIC,
    ros_90_units      NUMERIC,
    ros_30_value      NUMERIC,
    ros_60_value      NUMERIC,
    ros_90_value      NUMERIC,
    estimated_doh     NUMERIC,
    ros_used          TEXT,
    inventory_health  TEXT,
    sell_through_rate TEXT,
    stock_level       TEXT,
    aging             TEXT,
    age_days          NUMERIC,
    hsn               TEXT,
    CONSTRAINT flipkart_grocery_inventory_unique
        UNIQUE (inventory_date, warehouse, sku_code)
);
"""

REVERSE = r"""
DROP TABLE IF EXISTS flipkart_grocery_inventory;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("platforms", "0065_secmaster_ads_summary_agg_mv"),
    ]

    operations = [
        migrations.RunSQL(FORWARD, REVERSE),
    ]
