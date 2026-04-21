import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="PlatformConfig",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("slug", models.SlugField(max_length=40, unique=True, validators=[django.core.validators.RegexValidator(message="Slug must be lowercase letters/digits/underscores, starting with a letter.", regex="^[a-z][a-z0-9_]*$")])),
                ("name", models.CharField(max_length=80)),
                ("inventory_table", models.CharField(blank=True, max_length=80)),
                ("secondary_table", models.CharField(blank=True, max_length=80)),
                ("master_po_table", models.CharField(default="master_po", max_length=80)),
                ("match_column", models.CharField(blank=True, max_length=80)),
                ("is_active", models.BooleanField(default=True)),
            ],
            options={"ordering": ["slug"]},
        ),
    ]
