from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `blinkit_brandfund_master`.

    Mirrors the Blinkit slice of the 'BF MASTER' sheet of ADs SPENT (1).xlsx.
    Joins `blinkit_brandfund` to `master_sheet` via item_id = format_sku_code
    (filtered to master_sheet.format = 'BLINKIT') and adds derived MONTH /
    YEAR / MONTH-DAY columns.

    Column mappings (verified against the Excel formulas):
      DATE              ← blinkit_brandfund.date
      SKU ID            ← blinkit_brandfund.item_id
      SKU NAME          ← blinkit_brandfund.product_name
      FORMAT            ← blinkit_brandfund.format        (always 'BLINKIT')
      BRAND FUND SPENT  ← blinkit_brandfund.total_brand_fund
      CATEGORY          ← master_sheet.category           (joined)
      SUB-CATEGORY      ← master_sheet.sub_category       (joined)
      ITEM              ← master_sheet.item               (joined)
      ITEM HEAD         ← master_sheet.item_head          (joined)
      MONTH             ← UPPER(TO_CHAR(date, 'FMMonth'))
      YEAR              ← EXTRACT(YEAR FROM date)
      MONTH-DAY         ← LPAD(DAY,2,'0') || '-' || MONTH

    Defensive REPLACE(UPPER(TRIM(format))) filter on master_sheet — same
    pattern that fixed the BigBasket 'BIG BASKET' space-variance case.

    Note: blinkit_brandfund granularity is (date, city, item_id, p_type), so
    the view emits one row per (date × city × item × p_type). The LEFT JOIN
    to master_sheet does not inflate the row count because
    master_sheet_format_sku_unique_idx (migration 0018) guarantees at most
    one BLINKIT row per format_sku_code.
    """

    dependencies = [
        ("uploads", "0037_blinkit_brandfund"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.blinkit_brandfund_master AS
            SELECT
                -- Source from blinkit_brandfund
                b.date                                          AS date,
                b.item_id                                       AS sku_id,
                b.product_name                                  AS sku_name,
                b.format                                        AS format,
                b.total_brand_fund                              AS brand_fund_spent,

                -- Joined from master_sheet (BLINKIT rows only)
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,

                -- Derived
                UPPER(TO_CHAR(b.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM b.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM b.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(b.date, 'FMMonth'))) AS month_day

            FROM public.blinkit_brandfund b

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(b.item_id))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'BLINKIT';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.blinkit_brandfund_master;
            """,
        ),
    ]
