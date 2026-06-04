from django.db import migrations


class Migration(migrations.Migration):
    """Audit log of FC 'flips' — POs the team intentionally moved from one
    fulfillment center to its sister FC. Detected live when an appointment lists
    a PO whose Amazon-PO-sheet FC differs from the appointment's FC.
    (from_fc = PO-sheet FC, to_fc = appointment FC.)
    """

    dependencies = [
        ("shipment", "0006_shipment_commitment_snapshot"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.po_fc_flip (
                id          bigserial PRIMARY KEY,
                po_number   text NOT NULL,
                from_fc     text NOT NULL,
                to_fc       text NOT NULL,
                first_seen  timestamptz NOT NULL DEFAULT now(),
                last_seen   timestamptz NOT NULL DEFAULT now(),
                UNIQUE (po_number, from_fc, to_fc)
            );
            """,
            reverse_sql="DROP TABLE IF EXISTS public.po_fc_flip;",
        ),
    ]
