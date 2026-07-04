"""Sync missing / out-of-date primary PO data from the Master PO Google Sheet
into the DB via the SAME two uploaders the Uploader Hub exposes per platform:

  * PO uploader  -> order data     (batch upsert into total_po / total_po_zbs)
  * GRN uploader -> delivery data  (total_po_grn_update / total_po_zbs_grn_update)

The Google Sheet (`MASTER PO` tab) is the user's working source of truth; the DB
app lags behind (POs never uploaded, deliveries/GRNs not recorded). For a given
month/year this command:

  1. PO phase  — for POs that are MISSING or whose ORDER qty differs, uploads the
     order rows (no delivered_qty / grn_date) through the PO uploader path. Runs
     first so missing POs exist before their GRN update.
  2. GRN phase — for POs whose DELIVERED qty differs, uploads (po, sku, grn_date,
     delivered_qty, status) through the GRN uploader path. Only rows the sheet
     marks as Delivery Month = <MONTH> <YEAR> are sent, so a delivery the sheet
     dates in a PREVIOUS month (e.g. some Zomato POs) is intentionally LEFT alone
     — it must not be forced into this month's bucket.

Per-platform target table:
  * total_po_zbs : SWIGGY, BLINKIT, ZEPTO
  * total_po     : BIG BASKET, FLIPKART GROCERY, CITY MALL, ZOMATO

Safe by default: DRY RUN unless --apply is passed.

Examples:
  python manage.py sync_po_from_sheet --month JUNE --year 2026            # dry run, all platforms
  python manage.py sync_po_from_sheet --month JUNE --year 2026 --apply    # write
  python manage.py sync_po_from_sheet --month JUNE --year 2026 --platform SWIGGY --apply
"""
from __future__ import annotations

from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from accounts import google_sheets as gs
from uploads.views import _batch_upload

# Which raw table each platform's PO rows belong to, and its GRN-update alias.
PLATFORM_TABLE = {
    "SWIGGY": "total_po_zbs",
    "BLINKIT": "total_po_zbs",
    "ZEPTO": "total_po_zbs",
    "BIG BASKET": "total_po",
    "FLIPKART GROCERY": "total_po",
    "CITY MALL": "total_po",
    "ZOMATO": "total_po",
}
GRN_TABLE = {"total_po": "total_po_grn_update", "total_po_zbs": "total_po_zbs_grn_update"}

# All ad/PO platforms present in the sheet. ZOMATO is included, but its
# cross-month deliveries are guarded by the Delivery Month filter below.
DEFAULT_PLATFORMS = list(PLATFORM_TABLE)

# Order columns for the PO uploader (no delivered_qty / grn_date — delivery is
# handled by the GRN uploader).
PO_COLMAP = [
    ("PO Number", "po_number"),
    ("PO Date", "po_date"),
    ("PO Expiry Date", "po_expiry_date"),
    ("Vendor Name", "vendor_name"),
    ("Status", "status"),
    ("SKU Code", "sku_code"),
    ("SKU Name", "sku_name"),
    ("Order Qty", "order_qty"),
    ("Basic Rate", "basic_rate"),
    ("landing Rate", "landing_rate"),
    ("Location", "location"),
    ("Format", "format"),
    ("Remarks", "remark"),
]

NUMERIC_COLS = {"order_qty", "delivered_qty", "basic_rate", "landing_rate"}


def _num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _clean(col: str, value: str) -> str:
    text = (value or "").strip()
    return text.replace(",", "") if col in NUMERIC_COLS else text


class Command(BaseCommand):
    help = "Sync missing/out-of-date primary PO orders (PO uploader) and deliveries (GRN uploader) from the sheet."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, help="Month, e.g. JUNE.")
        parser.add_argument("--year", required=True, help="Year, e.g. 2026.")
        parser.add_argument("--platform", action="append", help="Limit to a platform (repeatable).")
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

        self.stdout.write(f"Reading Master PO sheet | month {month} {year} | platforms={platforms}")
        ws = gs.open_spreadsheet().worksheet("MASTER PO")
        rows = ws.get_all_values()
        hdr = rows[0]
        idx = {h: i for i, h in enumerate(hdr)}

        # ---- gather sheet rows ----
        po_sheet_rows = defaultdict(list)      # (fmt, po) -> [raw rows]  (PO month)
        po_sheet_qty = defaultdict(lambda: [0.0, 0.0])   # (fmt, po) -> [order, deliv]  (PO month)
        del_sheet_rows = defaultdict(list)     # (fmt, po) -> [raw rows]  (Delivery month)
        del_sheet_qty = defaultdict(float)     # (fmt, po) -> delivered   (Delivery month)
        for r in rows[1:]:
            fmt = r[idx["Format"]].strip()
            if fmt not in platforms:
                continue
            if r[idx["PO Month"]].strip() == month and r[idx["PO YEAR"]].strip() == year:
                po = r[idx["PO Number"]].strip()
                po_sheet_rows[(fmt, po)].append(r)
                q = po_sheet_qty[(fmt, po)]
                q[0] += _num(r[idx["Order Qty"]])
                q[1] += _num(r[idx["Delivered Qty"]])
            if r[idx["Delivery Month"]].strip() == month and r[idx["DEL YEAR"]].strip() == year:
                po = r[idx["PO Number"]].strip()
                del_sheet_rows[(fmt, po)].append(r)
                del_sheet_qty[(fmt, po)] += _num(r[idx["Delivered Qty"]])

        # ---- DB qty per (fmt, po) ----
        db_po_qty = defaultdict(lambda: [0.0, 0.0])
        db_del_qty = defaultdict(float)
        with connection.cursor() as cur:
            cur.execute(
                "SELECT format, po_number, SUM(order_qty), SUM(delivered_qty) FROM master_po "
                "WHERE UPPER(po_month)=%s AND po_year::text=%s AND format=ANY(%s) GROUP BY format, po_number",
                [month, year, platforms],
            )
            for fm, po, oq, dq in cur.fetchall():
                db_po_qty[((fm or "").strip(), (po or "").strip())] = [_num(oq), _num(dq)]
            cur.execute(
                "SELECT format, po_number, SUM(delivered_qty) FROM master_po "
                "WHERE UPPER(delivery_month)=%s AND delivered_year::text=%s AND format=ANY(%s) GROUP BY format, po_number",
                [month, year, platforms],
            )
            for fm, po, dq in cur.fetchall():
                db_del_qty[((fm or "").strip(), (po or "").strip())] = _num(dq)

        # ---- PO phase targets: missing or order-diff ----
        po_data = defaultdict(list)
        po_summary = defaultdict(lambda: {"missing": 0, "order": 0})
        for (fmt, po), sq in po_sheet_qty.items():
            dq = db_po_qty.get((fmt, po))
            if dq is None:
                kind = "missing"
            elif abs(sq[0] - dq[0]) > tol:
                kind = "order"
            else:
                continue
            po_summary[fmt][kind] += 1
            table = PLATFORM_TABLE[fmt]
            for r in po_sheet_rows[(fmt, po)]:
                po_data[table].append({col: _clean(col, r[idx[h]]) for h, col in PO_COLMAP})

        # ---- GRN phase targets: delivery-diff, Delivery Month = this month only ----
        grn_data = defaultdict(list)
        grn_summary = defaultdict(lambda: {"pos": 0, "rows": 0})
        for (fmt, po), sdq in del_sheet_qty.items():
            if abs(sdq - db_del_qty.get((fmt, po), 0.0)) <= tol:
                continue
            grn_summary[fmt]["pos"] += 1
            grn_tbl = GRN_TABLE[PLATFORM_TABLE[fmt]]
            for r in del_sheet_rows[(fmt, po)]:
                if _num(r[idx["Delivered Qty"]]) == 0 and not r[idx["Delivery Date"]].strip():
                    continue
                grn_data[grn_tbl].append(
                    {
                        "po_number": r[idx["PO Number"]].strip(),
                        "sku_code": r[idx["SKU Code"]].strip(),
                        "grn_date": r[idx["Delivery Date"]].strip(),
                        "delivered_qty": _clean("delivered_qty", r[idx["Delivered Qty"]]),
                        "status": r[idx["Status"]].strip(),
                        "format": fmt,
                    }
                )
                grn_summary[fmt]["rows"] += 1

        # ---- report ----
        self.stdout.write("\nPO phase (order data -> PO uploader):")
        for fmt in platforms:
            s = po_summary.get(fmt)
            self.stdout.write(f"  {fmt:18} " + (f"missing={s['missing']:3}  order-diff={s['order']:3}" if s else "up to date"))
        self.stdout.write("\nGRN phase (delivery data -> GRN uploader; Delivery Month = %s %s only):" % (month, year))
        for fmt in platforms:
            s = grn_summary.get(fmt)
            self.stdout.write(f"  {fmt:18} " + (f"{s['pos']:3} PO(s), {s['rows']:3} delivery row(s)" if s else "up to date"))
        po_rows = sum(len(v) for v in po_data.values())
        grn_rows = sum(len(v) for v in grn_data.values())
        self.stdout.write(f"\nPO rows to upsert: {po_rows} | GRN rows to update: {grn_rows}")

        if not opts["apply"]:
            self.stdout.write(self.style.WARNING("\nDRY RUN — nothing written. Re-run with --apply to commit."))
            return
        if po_rows == 0 and grn_rows == 0:
            self.stdout.write(self.style.SUCCESS("Nothing to sync."))
            return

        # ---- apply: PO phase first, then GRN phase ----
        self.stdout.write("\nApplying PO phase (PO uploader)…")
        for table, data in po_data.items():
            resp = _batch_upload({"table": table, "data": data, "upsert": True})
            self.stdout.write(f"  {table}: created={resp.data.get('created')} updated={resp.data.get('updated')} failed={resp.data.get('failed')} err={resp.data.get('error')}")

        self.stdout.write("\nApplying GRN phase (GRN uploader)…")
        for grn_tbl, data in grn_data.items():
            resp = _batch_upload({"table": grn_tbl, "data": data})
            self.stdout.write(f"  {grn_tbl}: updated={resp.data.get('updated')} created={resp.data.get('created')} skipped={resp.data.get('skipped')} failed={resp.data.get('failed')} err={resp.data.get('error')}")

        try:
            from platforms.master_po_refresh import refresh_master_po_mv

            refresh_master_po_mv()
            self.stdout.write("  master_po_mv refreshed.")
        except Exception as exc:  # noqa: BLE001
            self.stdout.write(self.style.WARNING(f"  matview refresh skipped: {exc}"))

        self.stdout.write(self.style.SUCCESS("\nSync complete."))
