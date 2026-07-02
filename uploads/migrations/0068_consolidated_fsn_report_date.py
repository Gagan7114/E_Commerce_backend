from django.db import migrations


class Migration(migrations.Migration):
    """Add a user-entered `date` to the Flipkart Consolidated FSN Report.

    The FSN file itself carries no date, so the uploader now captures one
    manually (config.hasUploadDate + injectDateColumn='date') and stamps every
    row with it on insert. This lets the FSN dashboard show which period the
    report is for (MONTH / YEAR / MAX DATE) instead of borrowing the Flipkart
    ads period. Nullable so existing snapshots stay valid until re-uploaded.
    """

    dependencies = [
        ("uploads", "0067_amazon_ads_master_join_perf"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE public.consolidated_fsn_report
                ADD COLUMN IF NOT EXISTS date DATE;
            CREATE INDEX IF NOT EXISTS consolidated_fsn_report_date_idx
                ON public.consolidated_fsn_report (date);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS consolidated_fsn_report_date_idx;
            ALTER TABLE public.consolidated_fsn_report DROP COLUMN IF EXISTS date;
            """,
        ),
    ]
