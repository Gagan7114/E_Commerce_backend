from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `zepto_brandfund_master`.

    Mirrors the 'ZEPTO BF' sheet of ADs SPENT (1).xlsx — joins
    `zepto_brandfund` (the raw brand-fund upload) to `master_sheet` (SKU
    metadata) via zepto_sku_code = master_sheet.format_sku_code, and adds
    derived MONTH / YEAR / MONTH-DAY columns from the user-picked `date`.

    Column mappings (verified against the Excel formulas):
      DATE              ← zepto_brandfund.date
      SKU ID            ← zepto_brandfund.zepto_sku_code
      SKU NAME          ← zepto_brandfund.sku_name
      FORMAT            ← zepto_brandfund.format          (always 'ZEPTO')
      BRAND FUND SPENT  ← zepto_brandfund.promo_claim_amt
      CATEGORY          ← master_sheet.category           (joined)
      SUB-CATEGORY      ← master_sheet.sub_category       (joined)
      ITEM              ← master_sheet.item               (joined)
      ITEM HEAD         ← master_sheet.item_head          (joined)
      MONTH             ← UPPER(TO_CHAR(date, 'FMMonth'))
      YEAR              ← EXTRACT(YEAR FROM date)
      MONTH-DAY         ← LPAD(DAY,2,'0') || '-' || MONTH

    Join uses the same defensive REPLACE(UPPER(TRIM(format))) normalization
    used by `bigbasket_ads_master` so any future space-variance in
    master_sheet.format (e.g. 'zepto ' vs 'ZEPTO') still matches.
    """

    dependencies = [
        ("uploads", "0033_zepto_brandfund_single_date"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.zepto_brandfund_master AS
            SELECT
                -- Source from zepto_brandfund
                z.date                                          AS date,
                z.zepto_sku_code                                AS sku_id,
                z.sku_name                                      AS sku_name,
                z.format                                        AS format,
                z.promo_claim_amt                               AS brand_fund_spent,

                -- Joined from master_sheet (ZEPTO rows only)
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,
                ms.item                                         AS item,
                ms.item_head                                    AS item_head,

                -- Derived
                UPPER(TO_CHAR(z.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM z.date)::integer              AS year,
                (LPAD(EXTRACT(DAY FROM z.date)::text, 2, '0')
                   || '-' || UPPER(TO_CHAR(z.date, 'FMMonth'))) AS month_day

            FROM public.zepto_brandfund z

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(z.zepto_sku_code))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'ZEPTO';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.zepto_brandfund_master;
            """,
        ),
    ]
