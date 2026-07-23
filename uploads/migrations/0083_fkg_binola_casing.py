from django.db import migrations


def _refresh_master_po_mvs(apps, schema_editor):
    """Same best-effort matview rebuild as 0082."""
    with schema_editor.connection.cursor() as cur:
        for mv in ("master_po_mv", "primary_summary_mv"):
            cur.execute("SELECT to_regclass('public.%s')" % mv)
            if cur.fetchone()[0]:
                cur.execute("REFRESH MATERIALIZED VIEW public.%s" % mv)


class Migration(migrations.Migration):
    """Fold the Flipkart Grocery 'binola'/'BINOLA' casings into 'Binola'.

    Follow-up to 0082 (Manesar -> Binola): the pendency city grouping is
    case-sensitive (GROUP BY TRIM(city)), and total_po held three casings of
    the same warehouse — 'Binola' (146 after 0082), 'binola' (128), 'BINOLA'
    (32), all format FLIPKART GROCERY — splitting one city across three rows.
    Canonical is 'Binola', the label the uploaders emit.

    Reversible: original casing is backed up per (po_number, sku_code).
    """

    dependencies = [
        ("uploads", "0082_fkg_manesar_to_binola"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP TABLE IF EXISTS public.total_po_binola_case_backup_0083;
            CREATE TABLE public.total_po_binola_case_backup_0083 AS
            SELECT po_number, sku_code, location
            FROM public.total_po
            WHERE upper(btrim(location::text)) = 'BINOLA'
              AND btrim(location::text) <> 'Binola'
              AND upper(btrim(format::text)) = 'FLIPKART GROCERY';

            UPDATE public.total_po
            SET location = 'Binola'
            WHERE upper(btrim(location::text)) = 'BINOLA'
              AND btrim(location::text) <> 'Binola'
              AND upper(btrim(format::text)) = 'FLIPKART GROCERY';
            """,
            reverse_sql="""
            UPDATE public.total_po t
            SET location = b.location
            FROM public.total_po_binola_case_backup_0083 b
            WHERE t.po_number = b.po_number
              AND t.sku_code = b.sku_code
              AND btrim(t.location::text) = 'Binola';
            DROP TABLE IF EXISTS public.total_po_binola_case_backup_0083;
            """,
        ),
        migrations.RunPython(
            _refresh_master_po_mvs,
            reverse_code=_refresh_master_po_mvs,
        ),
    ]
