"""Back-fill precise landing_rate on existing primary-PO rows.

Some platform PO files shipped landing_rate pre-rounded to a whole rupee, so the
derived inclusive amount (order_qty x landing_rate) drifts a few rupees from the
source sheet, which keeps the exact basic_rate x GST value. This command restores
the precise value on rows ALREADY stored in total_po / total_po_zbs, using the
same provable rule as the ingest-time fix (uploads.views._restore_precise_landing_rate):
override only when landing_rate is a whole number and exactly one standard GST
slab, applied to basic_rate and rounded, reproduces it. Margin ratios (x1.40) and
already-decimal rates are left untouched.

Safe by default: DRY RUN unless --apply is passed.

Examples:
  python manage.py backfill_landing_rate                      # dry run, both tables
  python manage.py backfill_landing_rate --apply              # write
  python manage.py backfill_landing_rate --format SWIGGY --apply
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from uploads.views import PRIMARY_PO_GST_MULTIPLIERS

TABLES = ("total_po", "total_po_zbs")


def _precise(basic: Decimal, landing: Decimal):
    """Return the precise landing_rate if a single GST slab reproduces the rounded
    value, else None (leave the row untouched)."""
    if basic is None or landing is None or basic <= 0:
        return None
    if landing != landing.to_integral_value():
        return None  # already has decimals — precise already
    matches = [
        basic * slab
        for slab in PRIMARY_PO_GST_MULTIPLIERS
        if (basic * slab).to_integral_value(rounding=ROUND_HALF_UP) == landing
    ]
    if len(matches) != 1:
        return None
    precise = matches[0].quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return None if precise == landing else precise


class Command(BaseCommand):
    help = "Restore precise basic_rate x GST landing_rate on existing primary-PO rows."

    def add_arguments(self, parser):
        parser.add_argument("--format", action="append", help="Limit to a platform format (repeatable).")
        parser.add_argument("--apply", action="store_true", help="Actually write. Without it, dry run only.")

    def handle(self, *args, **opts):
        fmts = [f.strip().upper() for f in (opts["format"] or [])]
        total_changed = 0
        per_table = {}

        for table in TABLES:
            where = ""
            params: list = []
            if fmts:
                where = "WHERE UPPER(TRIM(format::text)) = ANY(%s)"
                params = [fmts]
            with connection.cursor() as cur:
                cur.execute(
                    f"SELECT id, basic_rate, landing_rate FROM {table} {where}", params
                )
                rows = cur.fetchall()
            updates = []
            for _id, basic, landing in rows:
                precise = _precise(basic, landing)
                if precise is not None:
                    updates.append((precise, _id))
            per_table[table] = len(updates)
            total_changed += len(updates)

            if opts["apply"] and updates:
                with transaction.atomic():
                    with connection.cursor() as cur:
                        for precise, _id in updates:
                            cur.execute(
                                f"UPDATE {table} SET landing_rate = %s WHERE id = %s",
                                [precise, _id],
                            )

        for table in TABLES:
            self.stdout.write(f"  {table}: {per_table[table]} row(s) {'updated' if opts['apply'] else 'would change'}")

        if not opts["apply"]:
            self.stdout.write(self.style.WARNING(f"\nDRY RUN — {total_changed} row(s) would change. Re-run with --apply."))
            return

        if total_changed:
            try:
                from platforms.master_po_refresh import refresh_master_po_mv

                refresh_master_po_mv()
                self.stdout.write("  master_po_mv refreshed.")
            except Exception as exc:  # noqa: BLE001
                self.stdout.write(self.style.WARNING(f"  matview refresh skipped: {exc}"))
        self.stdout.write(self.style.SUCCESS(f"\nDone — {total_changed} row(s) updated."))
