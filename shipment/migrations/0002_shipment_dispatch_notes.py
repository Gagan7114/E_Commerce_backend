from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('shipment', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='shipment',
            name='dispatch_date_planned',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='shipment',
            name='notes',
            field=models.TextField(blank=True, default=''),
            preserve_default=False,
        ),
    ]
