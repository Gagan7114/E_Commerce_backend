"""Capture the status_mapping lookup table (structure + seed rows) in a migration.

WHY
---
`status_mapping` maps each platform's RAW PO status word to the canonical PO Status
(PENDING / COMPLETED / EXPIRED / CANCELLED / APPOINTMENT DONE). It is JOINed by the
`master_po_raw` view (created in 0057) to compute `po_status`:

    LEFT JOIN status_mapping sm
      ON upper(TRIM(b.status)) = upper(TRIM(sm.status))
    ... ELSE sm.status_new ...

Until now the table existed ONLY in the live database - created and seeded by hand,
never in the repo. So the mapping rows were not under version control, and a from-zero
`migrate` had nothing for `master_po_raw` to join to. This migration brings the table
and its rows into the repo (Phase 0 of the status-mapping plan).

The seed includes the 2026-07-22 additions `Cancelled post Creation` and `INVALID`
(both -> CANCELLED) that cleared the last 9 blank `po_status` rows.

SAFETY / IDEMPOTENT
-------------------
  * `CREATE TABLE IF NOT EXISTS` + a per-row NOT EXISTS guard on the exact
    (status, status_new) pair => a pure NO-OP on every database that already has these
    rows (all real deployments). On a fresh DB it creates the table and seeds all 21.
  * No UNIQUE constraint is added: `Expired` and `EXPIRED` both exist (both -> EXPIRED),
    which is harmless but would block a unique index. Constraint hardening is deferred.
  * Reverse is a NO-OP: dropping the table would break `master_po_raw`, and the table
    pre-existed this migration, so a rollback must leave it in place.

ORDERING CAVEAT (only affects a true from-scratch rebuild)
----------------------------------------------------------
`master_po_raw` (0057) references `status_mapping`, so on a genuine from-zero `migrate`
this table must exist before 0057 runs. 0057 is already applied on every real database,
so its dependency graph is intentionally NOT edited here (editing an applied migration's
deps risks InconsistentMigrationHistory). On any existing DB the table is already present
and this ordering is irrelevant. For a real from-zero rebuild, apply this migration's
CREATE TABLE before 0057.
"""
from django.db import migrations

# The exact live rows as of 2026-07-22 (SELECT status, status_new FROM status_mapping).
SEED = [
    ("APPOINTMENT DONE", "APPOINTMENT DONE"),
    ("Scheduled", "APPOINTMENT DONE"),
    ("CANCELLED", "CANCELLED"),
    ("Cancelled post Creation", "CANCELLED"),
    ("INVALID", "CANCELLED"),
    ("COMPLETED", "COMPLETED"),
    ("Fulfilled", "COMPLETED"),
    ("GRN DONE", "COMPLETED"),
    ("GRN_DONE", "COMPLETED"),
    ("Expired", "EXPIRED"),
    ("EXPIRED", "EXPIRED"),
    ("ASN_CREATED", "PENDING"),
    ("CONFIRMED", "PENDING"),
    ("Created", "PENDING"),
    ("PENDING", "PENDING"),
    ("PENDING_ACKNOWLEDGEMENT", "PENDING"),
    ("PENDING_ASN_CREATION", "PENDING"),
    ("PENDING_GRN", "PENDING"),
    ("PO_ACKNOWLEDGED", "PENDING"),
    ("Rescheduled", "PENDING"),
    ("Unscheduled", "PENDING"),
]


def seed(apps, schema_editor):
    conn = schema_editor.connection
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS status_mapping "
            "(status varchar, status_new varchar)"
        )
        for raw, canonical in SEED:
            # Insert only if this exact pair is absent -> no duplicates on the live DB
            # (which already holds all 21), full seed on a fresh DB.
            cur.execute(
                "INSERT INTO status_mapping (status, status_new) "
                "SELECT %s, %s WHERE NOT EXISTS ("
                "  SELECT 1 FROM status_mapping x "
                "  WHERE x.status = %s AND x.status_new = %s)",
                [raw, canonical, raw, canonical],
            )


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0059_perf_indexes_penetration_sec_ads"),
    ]

    operations = [
        migrations.RunPython(seed, migrations.RunPython.noop),
    ]
