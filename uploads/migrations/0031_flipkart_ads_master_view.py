from django.db import migrations


class Migration(migrations.Migration):
    """Reporting view `flipkart_ads_master`.

    Unlike the other ads_master views, Flipkart's source is campaign-level
    (no SKU dimension), so there's no master_sheet join. The view exists
    purely to add the derived `year` / `month` columns the dashboard
    filtering layer expects.
    """

    dependencies = [
        ("uploads", "0030_flipkart_ads"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE VIEW public.flipkart_ads_master AS
            SELECT
                f.*,
                EXTRACT(YEAR FROM f.date)::integer  AS year,
                UPPER(TO_CHAR(f.date, 'FMMonth'))   AS month
            FROM public.flipkart_ads f;
            """,
            reverse_sql="""
            DROP VIEW IF EXISTS public.flipkart_ads_master;
            """,
        ),
    ]
