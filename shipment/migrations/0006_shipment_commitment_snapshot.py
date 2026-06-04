from django.db import migrations, models


class Migration(migrations.Migration):
    """Frozen per-appointment commitment vs loaded snapshot on the shipment."""

    dependencies = [
        ("shipment", "0005_shipment_additional_appointment_ids_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="shipment",
            name="commitment_snapshot",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
