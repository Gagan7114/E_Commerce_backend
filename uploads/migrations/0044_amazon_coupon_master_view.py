from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `amazon_coupon_master`.

    Joins `amazon_coupon` to `master_sheet` via the `ads_master_bs` bridge so
    coupon rows pick up the SKU metadata (ASIN, item head, brand, category,
    sub-category) that lives on master_sheet.

    Column mappings:
      DATE              ← amazon_coupon.date
      COUPON NAME       ← amazon_coupon.coupon_name
      START DATE        ← amazon_coupon.start_date
      END DATE          ← amazon_coupon.end_date
      CLIPS             ← amazon_coupon.clips
      REDEMPTIONS       ← amazon_coupon.redemptions
      TOTAL DISCOUNT    ← amazon_coupon.total_discount
      BUDGET SPENT      ← amazon_coupon.budget_spent
      BUDGET REMAINING  ← amazon_coupon.budget_remaining
      BUDGET USED       ← amazon_coupon.budget_used
      TOTAL BUDGET      ← amazon_coupon.total_budget

      ASIN              ← master_sheet.format_sku_code (joined via ads_master_bs)
      ITEM HEAD         ← master_sheet.item_head
      BRAND             ← master_sheet.brand
      CATEGORY          ← master_sheet.category
      SUB CATEGORY      ← master_sheet.sub_category

      MONTH             ← UPPER(TO_CHAR(date, 'FMMonth'))
      YEAR              ← EXTRACT(YEAR FROM date)

    Join chain:
      amazon_coupon (c) ⟕ ads_master_bs (amb)
          ON amb.campaign_id = c.coupon_name
         AND amb.format       = 'AMAZON'
      ⟕ master_sheet (ms)
          ON ms.format_sku_code = amb.sku_id
         AND ms.format          = 'AMAZON'

    Defensive UPPER/TRIM normalization matches the swiggy/blinkit ads_master
    views — same pattern that fixed the BigBasket 'BIG BASKET' space-variance
    bug, so future whitespace/casing drift in either table doesn't silently
    drop matches.
    """

    dependencies = [
        ("uploads", "0043_amazon_coupon"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.amazon_coupon_master AS
            SELECT
                -- Source from amazon_coupon
                c.date                                          AS date,
                c.coupon_name                                   AS coupon_name,
                c.start_date                                    AS start_date,
                c.end_date                                      AS end_date,
                c.clips                                         AS clips,
                c.redemptions                                   AS redemptions,
                c.total_discount                                AS total_discount,
                c.budget_spent                                  AS budget_spent,
                c.budget_remaining                              AS budget_remaining,
                c.budget_used                                   AS budget_used,
                c.total_budget                                  AS total_budget,

                -- Joined from ads_master_bs (bridge) -> master_sheet
                ms.format_sku_code                              AS asin,
                ms.item_head                                    AS item_head,
                ms.brand                                        AS brand,
                ms.category                                     AS category,
                ms.sub_category                                 AS sub_category,

                -- Derived from c.date
                UPPER(TO_CHAR(c.date, 'FMMonth'))               AS month,
                EXTRACT(YEAR FROM c.date)::integer              AS year

            FROM public.amazon_coupon c

            LEFT JOIN public.ads_master_bs amb
                   ON UPPER(TRIM(amb.campaign_id::text))
                    = UPPER(TRIM(c.coupon_name))
                  AND REPLACE(UPPER(TRIM(amb.format::text)), ' ', '') = 'AMAZON'

            LEFT JOIN public.master_sheet ms
                   ON UPPER(TRIM(ms.format_sku_code::text))
                    = UPPER(TRIM(amb.sku_id))
                  AND REPLACE(UPPER(TRIM(ms.format::text)), ' ', '') = 'AMAZON';
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.amazon_coupon_master;
            """,
        ),
    ]
