"""
Management command to purge all Amazon PO data in the correct FK-safe order.

Deletion order (all child tables of raw.upload_file first, then the parent):
  1. quality.upload_validation_summary  (FK → raw.upload_file)
  2. quality.validation_error           (FK → raw.upload_file)
  3. staging."amazon data"              (FK → raw.upload_file)
  4. raw.upload_file                    (WHERE main_table_name = 'Amazon PO')
  5. TRUNCATE reporting."Amazon PO"     (leaf reporting table)

Note: staging."appointment data", staging."price data", and reporting.appointment
are NOT touched because they belong to different upload types.
"""

from django.core.management.base import BaseCommand
from django.db import connection, transaction

_AMAZON_PO_FILTER = "WHERE upload_id IN (SELECT upload_id FROM raw.upload_file WHERE main_table_name = 'Amazon PO')"


class Command(BaseCommand):
    help = "Delete ALL Amazon PO data across all months and re-set the reporting table."

    def add_arguments(self, parser):
        parser.add_argument(
            "--yes",
            action="store_true",
            help="Skip confirmation prompt",
        )

    def handle(self, *args, **options):
        if not options["yes"]:
            confirm = input(
                "This will permanently delete ALL Amazon PO data from every month. "
                "Type 'yes' to continue: "
            )
            if confirm.strip().lower() != "yes":
                self.stdout.write("Aborted.")
                return

        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.upload_file WHERE main_table_name = 'Amazon PO'")
            upload_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM quality.upload_validation_summary {_AMAZON_PO_FILTER}")
            validation_summary_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM quality.validation_error {_AMAZON_PO_FILTER}")
            validation_error_count = cur.fetchone()[0]
            cur.execute(f'SELECT COUNT(*) FROM staging."amazon data" {_AMAZON_PO_FILTER}')
            staging_count = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM reporting."Amazon PO"')
            reporting_count = cur.fetchone()[0]

        self.stdout.write(
            f"\nAbout to delete:\n"
            f"  {validation_summary_count} rows from quality.upload_validation_summary\n"
            f"  {validation_error_count} rows from quality.validation_error\n"
            f"  {staging_count} rows from staging.\"amazon data\"\n"
            f"  {upload_count} rows from raw.upload_file\n"
            f"  {reporting_count} rows from reporting.\"Amazon PO\" (TRUNCATE)\n"
        )

        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute(f"DELETE FROM quality.upload_validation_summary {_AMAZON_PO_FILTER}")
                self.stdout.write(f"  Deleted {cur.rowcount} rows from quality.upload_validation_summary")

                cur.execute(f"DELETE FROM quality.validation_error {_AMAZON_PO_FILTER}")
                self.stdout.write(f"  Deleted {cur.rowcount} rows from quality.validation_error")

                cur.execute(f'DELETE FROM staging."amazon data" {_AMAZON_PO_FILTER}')
                self.stdout.write(f"  Deleted {cur.rowcount} rows from staging.\"amazon data\"")

                cur.execute("DELETE FROM raw.upload_file WHERE main_table_name = 'Amazon PO'")
                self.stdout.write(f"  Deleted {cur.rowcount} rows from raw.upload_file")

                cur.execute('TRUNCATE TABLE reporting."Amazon PO" RESTART IDENTITY')
                self.stdout.write('  Truncated reporting."Amazon PO"')

        self.stdout.write(self.style.SUCCESS("\nAll Amazon PO data deleted successfully."))
