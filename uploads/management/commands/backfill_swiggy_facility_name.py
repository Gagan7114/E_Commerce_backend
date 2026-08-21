"""Backfill total_po_zbs.facility_name for OPEN Swiggy POs from the sheet.

WHY
---
`location` for Swiggy now shows FacilityName when one is stored (see
platforms/0073). New uploads carry it automatically, but rows uploaded BEFORE
that change never stored a facility - FacilityName only ever existed in the
source Google Sheet, so it cannot be recovered with SQL alone.

The user's decision: do not rewrite all history, only the POs that are still
OPEN (the live pendency backlog). Everything already closed keeps
location = city, which is exactly what it has always shown.

WHAT
----
  1. Read the raw `SWIGGY` tab (PoNumber / SkuCode / FacilityName).
  2. Ask master_po which Swiggy PO+SKU rows are currently OPEN
     (open_close = 'OPEN', i.e. po_status PENDING or APPOINTMENT DONE).
  3. For those rows only, write the sheet's FacilityName verbatim (mixed case,
     as Swiggy writes it) into total_po_zbs.facility_name.
  4. Refresh master_po_mv so the dashboards show it.

Rows already holding the same facility are skipped, so re-running is cheap and
idempotent. Closed / cancelled / completed POs are never touched.

Safe by default: DRY RUN unless --apply is passed.

Examples:
  python manage.py backfill_swiggy_facility_name                     # dry run
  python manage.py backfill_swiggy_facility_name --apply             # write
  python manage.py backfill_swiggy_facility_name --tab SWIGGY --apply
  python manage.py backfill_swiggy_facility_name --all-statuses --apply   # not just open
"""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from accounts import google_sheets as gs

PO_HEADER = "PoNumber"
SKU_HEADER = "SkuCode"
FACILITY_HEADER = "FacilityName"


def _key(po: str, sku: str) -> tuple[str, str]:
    return (po or "").strip().upper(), (sku or "").strip().upper()


class Command(BaseCommand):
    help = "Backfill total_po_zbs.facility_name for open Swiggy POs from the SWIGGY sheet tab."

    def add_arguments(self, parser):
        parser.add_argument("--tab", default="SWIGGY", help="Worksheet/tab name (default SWIGGY).")
        parser.add_argument("--apply", action="store_true", help="Actually write. Without it, dry run only.")
        parser.add_argument(
            "--all-statuses",
            action="store_true",
            help="Also update closed/completed/expired POs. Default is OPEN POs only.",
        )
        parser.add_argument("--limit", type=int, default=0, help="Cap rows written (0 = no cap). For a cautious first run.")

    def handle(self, *args, **opts):
        tab = opts["tab"]
        open_only = not opts["all_statuses"]

        # ---- 1. sheet: (po, sku) -> FacilityName -------------------------------
        self.stdout.write(f"Reading '{tab}' tab from the Master PO spreadsheet…")
        try:
            ws = gs.open_spreadsheet().worksheet(tab)
            rows = ws.get_all_values()
        except Exception as exc:  # noqa: BLE001
            raise CommandError(f"Could not read tab '{tab}': {exc}") from exc
        if not rows:
            raise CommandError(f"Tab '{tab}' is empty.")

        hdr = [h.strip() for h in rows[0]]
        missing = [h for h in (PO_HEADER, SKU_HEADER, FACILITY_HEADER) if h not in hdr]
        if missing:
            raise CommandError(f"Tab '{tab}' is missing column(s): {missing}. Found: {hdr}")
        idx = {h: i for i, h in enumerate(hdr)}

        sheet_facility: dict[tuple[str, str], str] = {}
        conflicts = 0
        blank_facility = 0
        for r in rows[1:]:
            if len(r) <= max(idx[PO_HEADER], idx[SKU_HEADER], idx[FACILITY_HEADER]):
                continue
            po = r[idx[PO_HEADER]].strip()
            sku = r[idx[SKU_HEADER]].strip()
            fac = r[idx[FACILITY_HEADER]].strip()
            if not po or not sku:
                continue
            if not fac:
                blank_facility += 1
                continue
            k = _key(po, sku)
            prev = sheet_facility.get(k)
            if prev is None:
                sheet_facility[k] = fac
            elif prev != fac:
                conflicts += 1

        self.stdout.write(
            f"  sheet rows: {len(rows) - 1} | usable PO+SKU keys: {len(sheet_facility)}"
            f" | blank FacilityName: {blank_facility} | conflicting duplicates: {conflicts}"
        )
        if conflicts:
            self.stdout.write(
                self.style.WARNING(
                    f"  {conflicts} PO+SKU key(s) appear twice with DIFFERENT facilities;"
                    " the first value seen wins."
                )
            )
        if not sheet_facility:
            self.stdout.write(self.style.WARNING("Nothing usable in the sheet. Stopping."))
            return

        # ---- 2. DB: which Swiggy rows are in scope, and what do they hold now? --
        scope = "OPEN only" if open_only else "ALL statuses"
        self.stdout.write(f"Reading Swiggy rows from the DB ({scope})…")
        with connection.cursor() as cur:
            if open_only:
                # master_po.open_close is the pendency flag the dashboards use.
                cur.execute(
                    """
                    SELECT DISTINCT UPPER(TRIM(t.po_number)), UPPER(TRIM(t.sku_code)),
                           COALESCE(t.facility_name, '')
                      FROM public.total_po_zbs t
                      JOIN public.master_po m
                        ON UPPER(TRIM(m.po_number)) = UPPER(TRIM(t.po_number))
                       AND UPPER(TRIM(m.sku_code))  = UPPER(TRIM(t.sku_code))
                       AND UPPER(TRIM(m.format))    = 'SWIGGY'
                     WHERE UPPER(TRIM(t.format)) = 'SWIGGY'
                       AND UPPER(TRIM(COALESCE(m.open_close, ''))) = 'OPEN'
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT UPPER(TRIM(po_number)), UPPER(TRIM(sku_code)),
                           COALESCE(facility_name, '')
                      FROM public.total_po_zbs
                     WHERE UPPER(TRIM(format)) = 'SWIGGY'
                    """
                )
            db_rows = cur.fetchall()

        self.stdout.write(f"  Swiggy rows in scope: {len(db_rows)}")

        # ---- 3. decide what actually needs writing -----------------------------
        to_write: list[tuple[str, str, str]] = []   # (facility, po, sku)
        already_ok = 0
        not_in_sheet = 0
        for po_u, sku_u, current in db_rows:
            fac = sheet_facility.get((po_u, sku_u))
            if fac is None:
                not_in_sheet += 1
                continue
            if current.strip() == fac:
                already_ok += 1
                continue
            to_write.append((fac, po_u, sku_u))

        if opts["limit"] and len(to_write) > opts["limit"]:
            self.stdout.write(
                self.style.WARNING(
                    f"  --limit {opts['limit']} applied: {len(to_write) - opts['limit']} row(s) LEFT OUT of this run."
                )
            )
            to_write = to_write[: opts["limit"]]

        self.stdout.write(
            f"\n  to update      : {len(to_write)}"
            f"\n  already correct: {already_ok}"
            f"\n  not in sheet   : {not_in_sheet}"
        )
        for fac, po_u, sku_u in to_write[:10]:
            self.stdout.write(f"    {po_u:16} {sku_u:10} -> {fac}")
        if len(to_write) > 10:
            self.stdout.write(f"    … and {len(to_write) - 10} more")

        if not opts["apply"]:
            self.stdout.write(self.style.WARNING("\nDRY RUN — nothing written. Re-run with --apply to commit."))
            return
        if not to_write:
            self.stdout.write(self.style.SUCCESS("\nNothing to write."))
            return

        # ---- 4. write, via a temp table so it is one set-based UPDATE ----------
        self.stdout.write("\nWriting…")
        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE _swiggy_fac (facility text, po text, sku text) ON COMMIT DROP"
                )
                cur.executemany(
                    "INSERT INTO _swiggy_fac (facility, po, sku) VALUES (%s, %s, %s)",
                    to_write,
                )
                cur.execute(
                    """
                    UPDATE public.total_po_zbs t
                       SET facility_name = s.facility
                      FROM _swiggy_fac s
                     WHERE UPPER(TRIM(t.po_number)) = s.po
                       AND UPPER(TRIM(t.sku_code))  = s.sku
                       AND UPPER(TRIM(t.format))    = 'SWIGGY'
                       AND COALESCE(t.facility_name, '') <> s.facility
                    """
                )
                updated = cur.rowcount
        self.stdout.write(f"  total_po_zbs rows updated: {updated}")

        try:
            from platforms.master_po_refresh import refresh_master_po_mv

            refresh_master_po_mv()
            self.stdout.write("  master_po_mv refreshed.")
        except Exception as exc:  # noqa: BLE001
            self.stdout.write(self.style.WARNING(f"  matview refresh skipped: {exc}"))

        self.stdout.write(self.style.SUCCESS("\nBackfill complete."))
