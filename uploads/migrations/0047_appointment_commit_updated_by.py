from django.db import migrations


class Migration(migrations.Migration):
    """Track who last imported each Vendor Central carton/unit row.

    Set by the logged-in manual-import endpoint so the Cartons/Unit Count VC
    page can show "Last updated <when> by <who>" and guard against repeat
    same-day scrapes (Amazon ToS exposure).
    """

    dependencies = [
        ("uploads", "0046_appointment_commit"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE public.appointment_commit ADD COLUMN IF NOT EXISTS updated_by text;",
            reverse_sql="ALTER TABLE public.appointment_commit DROP COLUMN IF EXISTS updated_by;",
        ),
    ]
