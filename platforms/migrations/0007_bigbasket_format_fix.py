from django.db import migrations


# BigBasket's SecMaster rows are stored with the format `BIG BASKET` (two
# words, space-separated) but PlatformConfig historically used the slug
# `bigbasket` as its po_filter_value. This caused the Monthly Targets
# lookup to miss all BigBasket data (LOWER-TRIM comparison of 'bigbasket'
# ≠ 'big basket'). Bring bigbasket in line with the two other
# two-word platforms (`city mall`, `flipkart grocery`) by fixing the
# stored filter value, and back-fill any `month_targets` rows that were
# inserted under the wrong format string.
FIX_SQL = """
UPDATE platforms_platformconfig
   SET po_filter_value = 'big basket'
 WHERE slug = 'bigbasket'
   AND (po_filter_value IS NULL OR LOWER(TRIM(po_filter_value)) = 'bigbasket');

UPDATE month_targets
   SET "format" = 'big basket'
 WHERE LOWER(TRIM("format")) = 'bigbasket';
"""


REVERSE_SQL = """
UPDATE platforms_platformconfig
   SET po_filter_value = 'bigbasket'
 WHERE slug = 'bigbasket'
   AND LOWER(TRIM(po_filter_value)) = 'big basket';

UPDATE month_targets
   SET "format" = 'bigbasket'
 WHERE LOWER(TRIM("format")) = 'big basket';
"""


class Migration(migrations.Migration):
    dependencies = [("platforms", "0006_monthly_targets_item_head")]

    operations = [migrations.RunSQL(sql=FIX_SQL, reverse_sql=REVERSE_SQL)]
