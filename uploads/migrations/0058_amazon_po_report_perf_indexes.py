from django.db import migrations

# Performance indexes on the externally-populated reporting tables
# (reporting."Amazon PO" and reporting."appointment").
#
# These tables previously had ONLY their dedup unique keys, so every Amazon-PO /
# appointment report endpoint did a full sequential scan of the whole table:
#   * amazon_po_report      — ORDER BY order_date / range filters
#   * amazon_po_new_po_dash — order_date BETWEEN ... (≈9 scans/request) + MAX(order_date)
#   * amazon_po_matrix      — WHERE po_month = ? AND year = ?
#   * appointment_summary   — JOIN reporting."Amazon PO" ON UPPER(TRIM(po_number)) = ...
#   * amazon_po_summary     — expiry_date >= CURRENT_DATE ...
#   * appointment_report    — ORDER BY appointment_time DESC
#
# Column types were confirmed from the upsert SQL in uploads/amazon_uploads.py:
# order_date / expiry_date are DATE, appointment_time is a timestamp, po_month /
# year are INTEGER, po_number is TEXT (joined via UPPER(TRIM(po_number))). All
# index expressions match the exact predicates the views use so the planner can
# use them.
#
# The reporting.* tables are created OUTSIDE Django (the initial migration uses
# ALTER TABLE IF EXISTS), so each CREATE INDEX is guarded by a to_regclass()
# existence check — the migration is a no-op in environments where the tables
# aren't present, and IF NOT EXISTS makes it safe to re-run.


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0057_consolidated_fsn_report_inline_enrich"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            DO $$
            BEGIN
                IF to_regclass('reporting."Amazon PO"') IS NOT NULL THEN
                    CREATE INDEX IF NOT EXISTS idx_amazon_po_order_date
                        ON reporting."Amazon PO" (order_date);

                    CREATE INDEX IF NOT EXISTS idx_amazon_po_month_year
                        ON reporting."Amazon PO" (po_month, year);

                    CREATE INDEX IF NOT EXISTS idx_amazon_po_po_number_norm
                        ON reporting."Amazon PO" (UPPER(TRIM(po_number)));

                    CREATE INDEX IF NOT EXISTS idx_amazon_po_expiry_date
                        ON reporting."Amazon PO" (expiry_date);
                END IF;

                IF to_regclass('reporting."appointment"') IS NOT NULL THEN
                    CREATE INDEX IF NOT EXISTS idx_appointment_time
                        ON reporting."appointment" (appointment_time);
                END IF;
            END $$;
            """,
            reverse_sql=r"""
            DROP INDEX IF EXISTS reporting.idx_amazon_po_order_date;
            DROP INDEX IF EXISTS reporting.idx_amazon_po_month_year;
            DROP INDEX IF EXISTS reporting.idx_amazon_po_po_number_norm;
            DROP INDEX IF EXISTS reporting.idx_amazon_po_expiry_date;
            DROP INDEX IF EXISTS reporting.idx_appointment_time;
            """,
        ),
    ]
