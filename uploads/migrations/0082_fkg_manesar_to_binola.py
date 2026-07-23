from django.db import migrations


def _refresh_master_po_mvs(apps, schema_editor):
    """Rebuild the matviews so the renamed city shows up immediately.

    Best-effort, mirrors platforms.master_po_refresh: master_po_mv first, then
    the derived primary_summary_mv. No-ops when a matview doesn't exist yet
    (fresh environments).
    """
    with schema_editor.connection.cursor() as cur:
        for mv in ("master_po_mv", "primary_summary_mv"):
            cur.execute("SELECT to_regclass('public.%s')" % mv)
            if cur.fetchone()[0]:
                cur.execute("REFRESH MATERIALIZED VIEW public.%s" % mv)


class Migration(migrations.Migration):
    """Rename the Flipkart Grocery 'Manesar' location to 'Binola'.

    Business rule (2026-07-23): Manesar POs are booked under Binola. Verified
    on live before writing this: 'Manesar' exists ONLY in total_po and only
    with format FLIPKART GROCERY (40 rows, exact spelling 'Manesar'; none in
    total_po_zbs, no other format). 'Binola' already exists for this format,
    so this is a merge into the existing label.

    The uploaders are changed in the same drop (Hub PrimaryUploader.jsx
    FLIPKART_GROCERY_LOCATION_STATES + the flow-35 Go parser
    fkgrocery_primary_parse.go): the \\bmanesar\\b pattern now resolves to
    'Binola', so re-uploads / daily pulls won't reintroduce 'Manesar'.

    Reversible: the touched (po_number, sku_code) pairs are backed up first so
    the reverse restores exactly these rows and leaves the pre-existing Binola
    rows alone.
    """

    dependencies = [
        ("uploads", "0081_blinkit_ads_dedup_index_no_name"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- Remember exactly which rows carried 'Manesar' (for the reverse).
            DROP TABLE IF EXISTS public.total_po_manesar_backup_0082;
            CREATE TABLE public.total_po_manesar_backup_0082 AS
            SELECT po_number, sku_code
            FROM public.total_po
            WHERE btrim(location::text) = 'Manesar'
              AND upper(btrim(format::text)) = 'FLIPKART GROCERY';

            UPDATE public.total_po
            SET location = 'Binola'
            WHERE btrim(location::text) = 'Manesar'
              AND upper(btrim(format::text)) = 'FLIPKART GROCERY';
            """,
            reverse_sql="""
            UPDATE public.total_po t
            SET location = 'Manesar'
            FROM public.total_po_manesar_backup_0082 b
            WHERE t.po_number = b.po_number
              AND t.sku_code = b.sku_code
              AND btrim(t.location::text) = 'Binola';
            DROP TABLE IF EXISTS public.total_po_manesar_backup_0082;
            """,
        ),
        migrations.RunPython(
            _refresh_master_po_mvs,
            reverse_code=_refresh_master_po_mvs,
        ),
    ]
