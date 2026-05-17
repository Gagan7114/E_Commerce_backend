import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0003_seed_platform_scoped_permissions"),
    ]

    operations = [
        migrations.CreateModel(
            name="InventoryDohNotification",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("alert_type", models.CharField(default="INVENTORY_DOH_LOW", max_length=64)),
                ("format", models.CharField(db_index=True, max_length=64)),
                ("platform_slug", models.CharField(blank=True, db_index=True, max_length=64)),
                ("sku_code", models.CharField(db_index=True, max_length=128)),
                ("sku_name", models.TextField(blank=True)),
                ("item", models.CharField(blank=True, db_index=True, max_length=255)),
                ("item_head", models.CharField(blank=True, db_index=True, max_length=128)),
                ("category", models.CharField(blank=True, max_length=255)),
                ("sub_category", models.CharField(blank=True, max_length=255)),
                ("brand", models.CharField(blank=True, max_length=255)),
                ("inventory_date", models.DateField(db_index=True)),
                ("sales_max_date", models.DateField(blank=True, null=True)),
                ("month_start", models.DateField(blank=True, null=True)),
                ("units_sold", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("ltr_sold", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("soh_units", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("soh_ltr", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("drr_units", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("drr_ltr", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("doh", models.DecimalField(decimal_places=4, default=0, max_digits=18)),
                ("threshold", models.DecimalField(decimal_places=4, default=10, max_digits=10)),
                (
                    "severity",
                    models.CharField(
                        choices=[("warning", "Warning"), ("critical", "Critical")],
                        db_index=True,
                        default="warning",
                        max_length=16,
                    ),
                ),
                ("title", models.CharField(max_length=255)),
                ("message", models.TextField(blank=True)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("is_read", models.BooleanField(db_index=True, default=False)),
                ("resolved_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("first_seen_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("last_seen_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-last_seen_at", "severity", "format", "sku_code"],
            },
        ),
        migrations.AddConstraint(
            model_name="inventorydohnotification",
            constraint=models.UniqueConstraint(
                fields=("alert_type", "format", "sku_code", "inventory_date"),
                name="uniq_inventory_doh_notification_snapshot",
            ),
        ),
        migrations.AddIndex(
            model_name="inventorydohnotification",
            index=models.Index(fields=["format", "inventory_date", "doh"], name="accounts_in_format_d01ed1_idx"),
        ),
        migrations.AddIndex(
            model_name="inventorydohnotification",
            index=models.Index(fields=["platform_slug", "resolved_at", "is_read"], name="accounts_in_platfor_bddcb6_idx"),
        ),
    ]
