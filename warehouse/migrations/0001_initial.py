"""No-op initial migration for the warehouse app.

All warehouse models are `managed = False` — the physical tables are owned by
the external uploader/ETL and already exist. We still create an initial
migration so the app registers cleanly and `makemigrations` stops asking.

If you later want admin-facing ContentTypes for permission grants on these
models, add `CreateModel` operations with `options={'managed': False}` via
`python manage.py makemigrations warehouse`.
"""

from django.db import migrations


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = []
