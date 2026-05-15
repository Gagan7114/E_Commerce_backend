from django.db import migrations


STATUS_VALUES = (
    "uploaded",
    "validating",
    "staged",
    "completed",
    "failed",
    "duplicate",
    "partially_successful",
    "queued",
    "processing",
)

REVERSE_STATUS_VALUES = (
    "uploaded",
    "validating",
    "staged",
    "completed",
    "failed",
    "duplicate",
    "partially_successful",
)


def _status_check_sql(values: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"""
    ALTER TABLE raw.upload_file
        DROP CONSTRAINT IF EXISTS upload_file_status_check;
    ALTER TABLE raw.upload_file
        ADD CONSTRAINT upload_file_status_check
        CHECK (status::text = ANY (ARRAY[{quoted}]::text[]));
    """


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0010_upload_duplicate_index_processing_statuses"),
    ]

    operations = [
        migrations.RunSQL(
            sql=_status_check_sql(STATUS_VALUES),
            reverse_sql=_status_check_sql(REVERSE_STATUS_VALUES),
        ),
    ]
