from django.db import migrations


class Migration(migrations.Migration):
    """Per-appointment Vendor Central commit numbers.

    Amazon's appointment LIST export omits Carton Count / Unit Count — those
    live only on each appointment's detail page. We scrape them into a CSV and
    upsert here via the generic /api/upload path (table added to
    UPLOAD_ALLOWED_TABLES), keyed on appointment_id. The shipment planner then
    prefills its commitment card from this table instead of manual entry.

    Kept as a standalone public table (not a column on reporting."appointment",
    which is per-(appointment_id, PO) and managed by the external uploader).
    """

    dependencies = [
        ("uploads", "0045_primary_po_rate_precision"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r'''
            CREATE TABLE IF NOT EXISTS public.appointment_commit (
                appointment_id  text PRIMARY KEY,
                destination_fc  text,
                carton_count    integer,
                unit_count      integer,
                scac            text,
                pos             text,
                source          text DEFAULT 'amazon',
                updated_at      timestamptz DEFAULT now()
            );
            ''',
            reverse_sql='DROP TABLE IF EXISTS public.appointment_commit;',
        ),
    ]
