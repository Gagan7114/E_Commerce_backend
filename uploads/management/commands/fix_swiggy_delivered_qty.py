"""Swiggy delivered-qty reconciler — align DB delivered qty to the MASTER PO sheet.

When a Swiggy SKU is received in several lots on different dates (the SWIGGY GRN
tab), the ingest OVERWRITES delivered_qty with the latest lot instead of ADDING
the lots, so the DB ends up holding only one lot (e.g. CHCPO361749 / 685793: sheet
88 = lot 80 + lot 8, but DB kept only 8). The MASTER PO sheet carries the correct,
net delivered figure per SKU (it accounts for debit-notes / order caps, so a blind
re-sum of the raw GRN would be wrong — the sheet is the source of truth).

This command sets each Swiggy (PO, SKU)'s DB delivered_qty to the sheet's figure
wherever they differ. Scoped to SKUs the sheet marks delivered (>0) so it never
zeroes a delivery from a blank cell. Idempotent + self-healing: re-run after each
Swiggy pull (or cron it) to undo multi-lot drops the automation re-introduces.

Date/month attribution is handled separately by `fix_swiggy_crossmonth_delivery`;
this command only touches delivered_qty, so the two compose cleanly.

Safe by default: DRY RUN unless --apply is passed.

Examples:
  python manage.py fix_swiggy_delivered_qty                       # dry run
  python manage.py fix_swiggy_delivered_qty --apply               # write
  python manage.py fix_swiggy_delivered_qty --po CHCPO361749 --po CMMPO12149 --apply
"""
from __future__ import annotations

from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db import connection

from accounts import google_sheets as gs

MASTER_TAB = "MASTER PO"


def _num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


class Command(BaseCommand):
    help = "Set Swiggy delivered_qty in the DB to the MASTER PO sheet value (fixes multi-lot under-counts)."

    def add_arguments(self, parser):
        parser.add_argument("--po", action="append", help="Limit to specific PO number(s) (repeatable).")
        parser.add_argument("--tol", type=float, default=0.5, help="Qty tolerance for 'differs' (default 0.5).")
        parser.add_argument("--apply", action="store_true", help="Actually write. Without it, dry run only.")

    def handle(self, *args, **opts):
        only = {p.strip().lower() for p in (opts["po"] or []) if p.strip()}
        tol = opts["tol"]

        # 1) sheet delivered qty per (po, sku) — the source of truth
        self.stdout.write(f"Reading {MASTER_TAB} sheet …")
        ws = gs.open_spreadsheet().worksheet(MASTER_TAB)
        rows = ws.get_all_values()
        i = {h: k for k, h in enumerate(rows[0])}
        sheet_del: dict[tuple[str, str], float] = defaultdict(float)
        disp: dict[tuple[str, str], tuple[str, str]] = {}
        for r in rows[1:]:
            if r[i["Format"]].strip() != "SWIGGY":
                continue
            po = r[i["PO Number"]].strip()
            sku = r[i["SKU Code"]].strip()
            if not po or not sku or (only and po.lower() not in only):
                continue
            key = (po.lower(), sku.lower())
            sheet_del[key] += _num(r[i["Delivered Qty"]])
            disp.setdefault(key, (po, sku))

        # 2) DB delivered qty per (po, sku), plus how many rows carry that pair
        db_del: dict[tuple[str, str], float] = defaultdict(float)
        db_rows: dict[tuple[str, str], int] = defaultdict(int)
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT LOWER(TRIM(po_number::text)), LOWER(TRIM(sku_code::text)),
                       SUM(COALESCE(delivered_qty, 0)), COUNT(*)
                FROM total_po_zbs
                WHERE UPPER(TRIM(format::text)) = 'SWIGGY'
                GROUP BY 1, 2
                """
            )
            for po, sku, q, c in cur.fetchall():
                key = ((po or ""), (sku or ""))
                db_del[key] = _num(q)
                db_rows[key] = c

        # 3) targets: sheet marks it delivered, DB row exists, and they differ
        targets = []
        for key, sval in sheet_del.items():
            if sval <= 0 or key not in db_rows:
                continue
            if abs(sval - db_del.get(key, 0.0)) > tol:
                targets.append((key, sval, db_del.get(key, 0.0), db_rows[key]))
        targets.sort(key=lambda t: -abs(t[1] - t[2]))

        self.stdout.write(f"\nSwiggy (PO, SKU) with delivered qty differing from sheet: {len(targets)}")
        for (key, sval, dval, nrows) in targets:
            po, sku = disp[key]
            flag = "  [!! %d DB rows — skipped on apply]" % nrows if nrows != 1 else ""
            self.stdout.write(
                f"  {po:16} {sku:12} sheet={sval:8.0f} db={dval:8.0f} (set +{sval - dval:.0f}){flag}"
            )

        if not opts["apply"]:
            self.stdout.write(self.style.WARNING("\nDRY RUN — nothing written. Re-run with --apply to commit."))
            return
        if not targets:
            self.stdout.write(self.style.SUCCESS("Nothing to fix."))
            return

        # 4) apply: set delivered_qty to the sheet value. Only for pairs that map to
        # exactly one DB row (Swiggy is one row per PO+SKU) so we never split a total.
        updated = skipped = 0
        with connection.cursor() as cur:
            for (key, sval, _dval, nrows) in targets:
                po, sku = disp[key]
                if nrows != 1:
                    skipped += 1
                    continue
                cur.execute(
                    """
                    UPDATE total_po_zbs
                    SET delivered_qty = %s
                    WHERE UPPER(TRIM(format::text)) = 'SWIGGY'
                      AND LOWER(TRIM(po_number::text)) = %s
                      AND LOWER(TRIM(sku_code::text)) = %s
                    """,
                    [sval, key[0], key[1]],
                )
                updated += cur.rowcount or 0
        self.stdout.write(f"\nUpdated {updated} row(s); skipped {skipped} multi-row pair(s).")

        try:
            from platforms.master_po_refresh import refresh_master_po_mv

            refresh_master_po_mv()
            self.stdout.write("  master_po_mv refreshed.")
        except Exception as exc:  # noqa: BLE001
            self.stdout.write(self.style.WARNING(f"  matview refresh skipped: {exc}"))

        self.stdout.write(self.style.SUCCESS("\nDone."))
