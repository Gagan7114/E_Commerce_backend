"""Swiggy cross-month delivery fix — attribute each PO to its FIRST delivery month.

A Swiggy PO is usually received in several GRN lots on different dates (the
`SWIGGY GRN` tab, column `CreatedAtDate`). When a small tail lot lands in a LATER
month than the first lot, the ingest pipeline stamps the WHOLE PO with the LATEST
receipt date, so the entire PO's delivered qty jumps into the later month. The
`MASTER PO` sheet instead dates each PO by its EARLIEST receipt, so the DB and the
sheet disagree on the delivery MONTH (e.g. JCNPO269264: 96% received 16/25-Jun +
a 55-unit tail on 14-Jul -> sheet says JUNE, DB says JULY).

This command applies the user's rule: a Swiggy PO's delivery belongs to its FIRST
delivery month. For each Swiggy PO whose DB delivery month is LATER than its
earliest GRN receipt month, it moves the whole PO's grn_date back to that earliest
receipt date (`MIN(CreatedAtDate)`), exactly reproducing how the sheet dates it.

Authoritative source: the `SWIGGY GRN` sheet (the per-receipt dates live nowhere
in the DB). Idempotent + self-healing: safe to re-run after every Swiggy pull, or
wire it into the cron right after the Swiggy automation.

Safe by default: DRY RUN unless --apply is passed.

Examples:
  python manage.py fix_swiggy_crossmonth_delivery                # dry run
  python manage.py fix_swiggy_crossmonth_delivery --apply        # write
  python manage.py fix_swiggy_crossmonth_delivery --po JCNPO269264
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db import connection

from accounts import google_sheets as gs

GRN_TAB = "SWIGGY GRN"
PO_COL = "PurchaseOrderNumber"
DATE_COL = "CreatedAtDate"  # the receipt date the MASTER PO sheet dates deliveries by


def _parse_date(value: str) -> dt.date | None:
    """Parse a SWIGGY GRN CreatedAtDate like '16-06-2026 10:24' or '14-07-2026 16.40'.

    Only the date part (DD-MM-YYYY, before the space) matters; the time uses ':' or
    '.' inconsistently and is ignored. Returns None if unparseable.
    """
    text = str(value or "").strip()
    if not text:
        return None
    date_part = text.split(" ", 1)[0].strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(date_part, fmt).date()
        except ValueError:
            continue
    return None


class Command(BaseCommand):
    help = "Move cross-month Swiggy POs to their first delivery month (earliest GRN receipt)."

    def add_arguments(self, parser):
        parser.add_argument("--po", action="append", help="Limit to specific PO number(s) (repeatable).")
        parser.add_argument("--apply", action="store_true", help="Actually write. Without it, dry run only.")

    def handle(self, *args, **opts):
        only = {p.strip() for p in (opts["po"] or []) if p.strip()}

        # 1) earliest receipt date per PO from the authoritative SWIGGY GRN sheet
        self.stdout.write(f"Reading {GRN_TAB} sheet …")
        ws = gs.open_spreadsheet().worksheet(GRN_TAB)
        rows = ws.get_all_values()
        idx = {h: i for i, h in enumerate(rows[0])}
        if PO_COL not in idx or DATE_COL not in idx:
            self.stderr.write(f"'{GRN_TAB}' missing '{PO_COL}' or '{DATE_COL}' column.")
            return
        earliest: dict[str, dt.date] = {}
        for r in rows[1:]:
            po = r[idx[PO_COL]].strip()
            if not po or (only and po not in only):
                continue
            d = _parse_date(r[idx[DATE_COL]])
            if d is None:
                continue
            if po not in earliest or d < earliest[po]:
                earliest[po] = d

        # 2) current DB delivery date per Swiggy PO (delivered rows)
        db_grn: dict[str, dt.date] = {}
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT po_number, MAX(grn_date)
                FROM total_po_zbs
                WHERE UPPER(TRIM(format::text)) = 'SWIGGY'
                  AND grn_date IS NOT NULL AND COALESCE(delivered_qty, 0) > 0
                GROUP BY po_number
                """
            )
            for po, g in cur.fetchall():
                po = (po or "").strip()
                if po and (not only or po in only):
                    db_grn[po] = g

        # 3) targets: DB delivery month is LATER than the earliest receipt month
        def ym(d: dt.date) -> tuple[int, int]:
            return (d.year, d.month)

        targets: list[tuple[str, dt.date, dt.date]] = []
        for po, cur_g in db_grn.items():
            first = earliest.get(po)
            if first is None:
                continue
            if ym(cur_g) > ym(first):  # DB sits in a later month than the first delivery
                targets.append((po, cur_g, first))
        targets.sort(key=lambda t: t[0])

        self.stdout.write(
            f"\nSwiggy POs in a later month than their first receipt: {len(targets)}"
        )
        for po, cur_g, first in targets:
            self.stdout.write(
                f"  {po:18} DB {cur_g} ({cur_g:%B}) -> first receipt {first} ({first:%B})"
            )

        if not opts["apply"]:
            self.stdout.write(self.style.WARNING("\nDRY RUN — nothing written. Re-run with --apply to commit."))
            return
        if not targets:
            self.stdout.write(self.style.SUCCESS("Nothing to fix."))
            return

        # 4) apply: move the whole PO's delivered rows back to the earliest receipt date
        moved_pos = 0
        moved_rows = 0
        with connection.cursor() as cur:
            for po, _cur_g, first in targets:
                cur.execute(
                    """
                    UPDATE total_po_zbs
                    SET grn_date = %s
                    WHERE UPPER(TRIM(format::text)) = 'SWIGGY'
                      AND po_number = %s
                      AND grn_date IS NOT NULL
                    """,
                    [first, po],
                )
                if cur.rowcount:
                    moved_pos += 1
                    moved_rows += cur.rowcount
        self.stdout.write(f"\nUpdated {moved_rows} row(s) across {moved_pos} PO(s).")

        try:
            from platforms.master_po_refresh import refresh_master_po_mv

            refresh_master_po_mv()
            self.stdout.write("  master_po_mv refreshed.")
        except Exception as exc:  # noqa: BLE001
            self.stdout.write(self.style.WARNING(f"  matview refresh skipped: {exc}"))

        self.stdout.write(self.style.SUCCESS("\nDone."))
