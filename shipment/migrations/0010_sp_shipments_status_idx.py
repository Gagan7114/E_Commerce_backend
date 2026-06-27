from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("shipment", "0009_sp_items_record_reasons"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="shipment",
            index=models.Index(fields=["status"], name="sp_shipments_status_idx"),
        ),
    ]
