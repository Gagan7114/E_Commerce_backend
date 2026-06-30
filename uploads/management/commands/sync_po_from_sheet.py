"""Sync missing / out-of-date primary PO rows from the Master PO Google Sheet
into the DB via the SAME ingestion path the platform PO uploaders use.

The Google Sheet (`MASTER PO` tab) is the user's working source of truth: POs and
their GRN/delivery updates are entered there, and the DB app sometimes lags
behind (POs never uploaded, deliveries not recorded). This command finds, for a
given month/year, the POs whose order or delivered qty in the sheet differs from
the DB (or are missing entirely), rebuilds their raw rows, and feeds them through
`uploads.views._batch_upload` — the exact upsert pipeline the uploaders call
(Jivo filter, blank-status -> PENDING, precise landing-rate restore, upsert on
po_number + sku_code). It then refreshes master_po_mv.

Per-platform target table:
  * total_po_zbs : SWIGGY, BLINKIT, ZEPTO
  * total_po     : BIG BASKET, FLIPKART GROCERY, CITY MALL, ZOMATO

Safe by default: runs as a DRY RUN (reports what would change) unless --apply is
passed. Use --platform to scope; ZOMATO is intentionally excluded by default
because its DB delivery is sometimes ahead of the sheet (overwriting would lose
data) — pass it explicitly only if you really mean to.

Examples:
  python manage.py sync_po_from_sheet --month JUNE --year 2026                # dry run, default platforms
  python manage.py sync_po_from_sheet --month JUNE --year 2026 --apply        # write
  python manage.py sync_po_from_sheet --month JUNE --year 2026 --platform SWIGGY --apply
"""
from __future__ import annotations

from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from accounts import google_sheets as gs
from uploads.views import _batch_upload

# Which raw table each platform's PO rows belong to.
PLATFORM_TABLE = {
    "SWIGGY": "total_po_zbs",
    "BLINKIT": "total_po_zbs",
    "ZEPTO": "total_po_zbs",
    "BIG BASKET": "total_po",
    "FLIPKART GROCERY": "total_po",
    "CITY MALL": "total_po",
    "ZOMATO": "total_po",
}

# ZOMATO excluded by default: its DB delivery can be ahead of the sheet.
DEFAULT_PLATFORMS = ["SWIGGY", "BLINKIT", "ZEPTO", "BIG BASKET", "FLIPKART GROCERY", "CITY MALL"]

# Master PO sheet header -> raw table column.
COLMAP = [
    ("PO Number", "po_number"),
    ("PO Date", "po_date"),
    ("PO Expiry Date", "po_expiry_date"),
    ("Delivery Date", "grn_date"),
    ("Vendor Name", "vendor_name"),
    ("Status", "status"),
    ("SKU Code", "sku_code"),
    ("SKU Name", "sku_name"),
    ("Order Qty", "order_qty"),
    ("Delivered Qty", "delivered_qty"),
    ("Basic Rate", "basic_rate"),
    ("landing Rate", "landing_rate"),
    ("Location", "location"),
    ("Format", "format"),
    ("Remarks", "remark"),
]


def _num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


# Numeric raw columns: the sheet may render these with thousands separators
# (e.g. "1,280"), which Postgres rejects on a numeric cast — strip the commas.
NUMERIC_COLS = {"order_qty", "delivered_qty", "basic_rate", "landing_rate"}


def _clean(col: str, value: str) -> str:
    text = (value or "").strip()
    if col in NUMERIC_COLS:
        return text.replace(",", "")
    return text


class Command(BaseCommand):
    help = "Sync missing/out-of-date primary PO rows from the Master PO sheet via the uploader pipeline."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, help="PO month, e.g. JUNE (matches the sheet's PO Month).")
        parser.add_argument("--year", required=True, help="PO year, e.g. 2026.")
        parser.add_argument(
            "--platform",
            action="append",
            help="Limit to a platform (repeatable). Default: all except ZOMATO.",
        )
        parser.add_argument("--apply", action="store_true", help="Actually write. Without it, dry run only.")
        parser.add_argument("--tol", type=float, default=0.5, help="Qty tolerance for 'differs' (default 0.5).")

    def handle(self, *args, **opts):
        month = opts["month"].strip().upper()
        year = str(opts["year"]).strip()
        tol = opts["tol"]
        platforms = [p.strip().upper() for p in (opts["platform"] or DEFAULT_PLATFORMS)]
        unknown = [p for p in platforms if p not in PLATFORM_TABLE]
        if unknown:
            raise CommandError(f"Unknown platform(s): {unknown}. Known: {sorted(PLATFORM_TABLE)}")

        self.stdout.write(f"Reading Master PO sheet for PO month {month} {year} | platforms={platforms}")
        ws = gs.open_spreadsheet().worksheet("MASTER PO")
        rows = ws.get_all_values()
        hdr = rows[0]
        idx = {h: i for i, h in enumerate(hdr)}

        # Group sheet rows by (platform, po) and accumulate qty.
        sheet_rows: dict = defaultdict(list)
        sheet_qty: dict = defaultdict(lambda: [0.0, 0.0])
        for r in rows[1:]:
            if r[idx["PO Month"]].strip() != month or r[idx["PO YEAR"]].strip() != year:
                continue
            fmt = r[idx["Format"]].strip()
            if fmt not in platforms:
                continue
            po = r[idx["PO Number"]].strip()
            sheet_rows[(fmt, po)].append(r)
            q = sheet_qty[(fmt, po)]
            q[0] += _num(r[idx["Order Qty"]])
            q[1] += _num(r[idx["Delivered Qty"]])

        # DB qty per (platform, po) for the same PO month.
        db_qty: dict = defaultdict(lambda: [0.0, 0.0])
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT format, po_number, SUM(order_qty), SUM(delivered_qty)
                FROM master_po
                WHERE UPPER(po_month) = %s AND po_year::text = %s AND format = ANY(%s)
                GROUP BY format, po_number
                """,
                [month, year, platforms],
            )
            for fm, po, oq, dq in cur.fetchall():
                db_qty[((fm or "").strip(), (po or "").strip())] = [_num(oq), _num(dq)]

        # Decide targets: missing, order-diff, or delivery-diff.
        data_by_table: dict = defaultdict(list)
        summary: dict = defaultdict(lambda: {"missing": 0, "order": 0, "delivery": 0, "rows": 0})
        for (fmt, po), sq in sheet_qty.items():
            dq = db_qty.get((fmt, po))
            if dq is None:
                kind = "missing"
            elif abs(sq[1] - dq[1]) > tol:
                kind = "delivery"
            elif abs(sq[0] - dq[0]) > tol:
                kind = "order"
            else:
                continue
            summary[fmt][kind] += 1
            table = PLATFORM_TABLE[fmt]
            for r in sheet_rows[(fmt, po)]:
                data_by_table[table].append(
                    {col: _clean(col, r[idx[h]]) for h, col in COLMAP}
                )
                summary[fmt]["rows"] += 1

        # Report.
        total_rows = sum(len(v) for v in data_by_table.values())
        self.stdout.write("\nPlanned sync (sheet -> DB):")
        for fmt in platforms:
            s = summary.get(fmt)
            if not s:
                self.stdout.write(f"  {fmt:18} up to date")
                continue
            self.stdout.write(
                f"  {fmt:18} missing={s['missing']:3}  order-diff={s['order']:3}  "
                f"delivery-diff={s['delivery']:3}  ({s['rows']} rows -> {PLATFORM_TABLE[fmt]})"
            )
        self.stdout.write(f"\nTotal rows to upsert: {total_rows}")

        if not opts["apply"]:
            self.stdout.write(self.style.WARNING("\nDRY RUN — nothing written. Re-run with --apply to commit."))
            return
        if total_rows == 0:
            self.stdout.write(self.style.SUCCESS("Nothing to sync."))
            return

        # Apply via the real uploader pipeline.
        self.stdout.write("\nApplying via uploader pipeline (_batch_upload)…")
        for table, data in data_by_table.items():
            resp = _batch_upload({"table": table, "data": data, "upsert": True})
            self.stdout.write(f"  {table}: {resp.data}")

        # Refresh the materialized view so master_po reflects the new rows.
        try:
            from platforms.master_po_refresh import refresh_master_po_mv

            refresh_master_po_mv()
            self.stdout.write("  master_po_mv refreshed.")
        except Exception as exc:  # noqa: BLE001
            self.stdout.write(self.style.WARNING(f"  matview refresh skipped: {exc}"))

        self.stdout.write(self.style.SUCCESS("\nSync complete."))
