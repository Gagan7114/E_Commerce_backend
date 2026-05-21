from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `swiggy_brandfund_master`.

    Mirrors the Swiggy slice of the 'BF MASTER' sheet of ADs SPENT (1).xlsx.
    Joins `swiggy_brandfund` to `master_sheet` via item_code = format_sku_code
    (filtered to master_sheet.format = 'SWIGGY' to avoid cross-platform leakage)
    and adds the derived MONTH / YEAR / MONTH-DAY columns.

    Column mappings (verified against the Excel formulas):
      DATE              ← swiggy_brandfund.date
      SKU ID            ← swiggy_brandfund.item_code
      SKU NAME          ← swiggy_brandfund.product_name
      FORMAT            ← swiggy_brandfund.format          (always 'SWIGGY')
      BRAND FUND SPENT  ← swiggy_brandfund.discount_spend
      CATEGORY          ← master_sheet.category            (joined)
      SUB-CATEGORY      ← master_sheet.sub_category        (joined)
      ITEM              ← master_sheet.item                (joined)
      ITEM HEAD         ← master_sheet.item_head           (joined)
      MONTH             ← UPPER(TO_CHAR(date, 'FMMonth'))
      YEAR              ← EXTRACT(YEAR FROM date)
      MONTH-DAY         ← LPAD(DAY,2,'0') || '-' || MONTH

    Defensive REPLACE(UPPER(TRIM(format))) on the master_sheet filter — same
    pattern that fixed the BigBasket 'BIG BASKET' space-variance bug.
    """

    dependencies = [
        ("uploads", "0035_swiggy_brandfund"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.swiggy_brandfund_master AS
            SELECT
                -- Source from swiggy_brandfund
                s.date                                          AS date,
                s.item_code                                     AS sku_id,
                s.product_name                                  AS sku_name,
                s.format                                        AS format,
                s.discount_spend                                AS brand_fund_spent,

                -- Joined from master_sheet (SWIGGY rows only)
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,

                -- Derived
                UPPER(TO_CHAR(s.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM s.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM s.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(s.date, 'FMMonth'))) AS month_day

            FROM public.swiggy_brandfund s

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(s.item_code))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'SWIGGY';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.swiggy_brandfund_master;
            """,
        ),
    ]
