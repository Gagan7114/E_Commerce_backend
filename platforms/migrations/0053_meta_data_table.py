from django.db import migrations


# Meta (Facebook/Instagram) ads campaign data. Mirrors the MASTER sheet in
# META SHEETS.xlsx. The 5 computed columns are STORED generated columns so the
# formulas run automatically on every insert/update (no real_date column is kept
# -- month/year are derived straight from the DD-MM-YYYY `date` text):
#   cost_per_click       = amount_spent / unique_clicks                 (J: =F/I)
#   cost_per_1000_imp    = amount_spent / impressions * 1000            (K: =(F/G)*1000)
#   cost_per_1000_reach  = amount_spent / reach * 1000                  (L: =(F/H)*1000)
#   month                = UPPER full month name from date              (N)
#   year                 = 4-digit year from date                       (O)
CREATE_SQL = r"""
CREATE TABLE IF NOT EXISTS meta_data (
    id                  bigserial PRIMARY KEY,
    "date"              text,
    start_date          text,
    end_date            text,
    campaign_name       text,
    campaign_status     text,
    amount_spent        numeric,
    impressions         numeric,
    reach               numeric,
    unique_clicks       numeric,
    cost_per_click      numeric GENERATED ALWAYS AS
                            (amount_spent / NULLIF(unique_clicks, 0)) STORED,
    cost_per_1000_imp   numeric GENERATED ALWAYS AS
                            ((amount_spent / NULLIF(impressions, 0)) * 1000) STORED,
    cost_per_1000_reach numeric GENERATED ALWAYS AS
                            ((amount_spent / NULLIF(reach, 0)) * 1000) STORED,
    month               text GENERATED ALWAYS AS (
                            CASE WHEN "date" ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                                 THEN (ARRAY['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE',
                                             'JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER'])
                                      [substring("date" from 4 for 2)::int]
                                 ELSE NULL END
                        ) STORED,
    year                text GENERATED ALWAYS AS (
                            CASE WHEN "date" ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                                 THEN right("date", 4)
                                 ELSE NULL END
                        ) STORED
);
"""

DROP_SQL = "DROP TABLE IF EXISTS meta_data;"


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0052_inventory_month_column"),
    ]

    operations = [migrations.RunSQL(sql=CREATE_SQL, reverse_sql=DROP_SQL)]
