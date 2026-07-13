from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('shipment', '0010_sp_shipments_status_idx'),
    ]

    operations = [
        migrations.AddField(
            model_name='shipmentitem',
            name='source_warehouse',
            field=models.TextField(blank=True),
        ),
    ]
