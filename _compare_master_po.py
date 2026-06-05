"""Compare MASTER PO Google Sheet (CSV side) vs DB master_po (DB side).

Usage: python _compare_master_po.py [MONTH] [YEAR]   (defaults: MAY 2026)
"""
import os, re, sys, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.dev')
django.setup()
from django.db import connection
from accounts import google_sheets as gs

MONTH = (sys.argv[1] if len(sys.argv) > 1 else 'MAY').strip().upper()
YEAR = int(sys.argv[2]) if len(sys.argv) > 2 else 2026

PLATFORMS = [
    ("BIG BASKET", "bigbasket"),
    ("BLINKIT", "blinkit"),
    ("CITY MALL", "citymall"),
    ("DEAL SHARE", "dealshare"),
    ("FLIPKART GROCERY", "flipkartgrocery"),
    ("SWIGGY", "swiggy"),
    ("ZEPTO", "zepto"),
    ("ZOMATO", "zomato"),
]

def norm(s):
    return re.sub(r'[^a-z0-9]+', '', str(s or '').lower().strip())

def to_num(v):
    if v is None or v == '':
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(',', '').replace('%', '')
    try:
        return float(s)
    except ValueError:
        return 0.0

def indian_group(s):
    if len(s) <= 3:
        return s
    last3, rest = s[-3:], s[:-3]
    parts = []
    while len(rest) > 2:
        parts.insert(0, rest[-2:]); rest = rest[:-2]
    if rest:
        parts.insert(0, rest)
    return ",".join(parts) + "," + last3

def fmt(v):
    v = round(float(v or 0), 2)
    s = f"{abs(v):.2f}"
    int_part, frac = s.split(".")
    frac = frac.rstrip("0")
    out = indian_group(int_part) + (("." + frac) if frac else "")
    return ("-" + out) if v < 0 else out

# ---- CSV side: read the sheet once ----
rows = gs.read_worksheet("MASTER PO")

CSV_COLS = {
    'order_ltrs': 'Total Order Liters',
    'deliver_ltrs': 'Total Delivered Liters',
    'pending_ltrs': 'MISSED LTRS',
    'order_val': 'Total Order Amt (INCLUSIVE)',
    'deliver_val': 'Total Deliver Amt (INCLUSIVE)',
    'pending_val': 'MISSED AMT',
    'order_qty': 'Order Qty',
    'deliver_qty': 'Delivered Qty',
    'pending_qty': 'MISSED QTY',
}

def csv_agg(fmt_key, month_col):
    acc = {k: 0.0 for k in CSV_COLS}
    acc['rows'] = 0
    for r in rows:
        if norm(r.get('Format')) != fmt_key:
            continue
        if str(r.get(month_col) or '').strip().upper() != MONTH:
            continue
        if to_num(r.get('Year')) != YEAR:
            continue
        acc['rows'] += 1
        for k, col in CSV_COLS.items():
            acc[k] += to_num(r.get(col))
    return acc

# ---- DB side ----
BASE = "REGEXP_REPLACE(LOWER(TRIM(format::text)), '[^a-z0-9]+', '', 'g') = %s"

def db_agg(fmt_key, month_col, year_col):
    sql = f"""
      SELECT
        COUNT(*)                                       rows,
        COALESCE(SUM(total_order_liters),0)            order_ltrs,
        COALESCE(SUM(total_delivered_liters),0)        deliver_ltrs,
        COALESCE(SUM(missed_ltrs),0)                   pending_ltrs,
        COALESCE(SUM(total_order_amt_inclusive),0)     order_val,
        COALESCE(SUM(total_deliver_amt_inclusive),0)   deliver_val,
        COALESCE(SUM(missed_qty * basic_rate),0)       pending_val,
        COALESCE(SUM(order_qty),0)                     order_qty,
        COALESCE(SUM(delivered_qty),0)                 deliver_qty,
        COALESCE(SUM(missed_qty),0)                    pending_qty
      FROM master_po
      WHERE {BASE} AND UPPER(TRIM({month_col}::text))=%s AND {year_col}=%s
    """
    with connection.cursor() as c:
        c.execute(sql, [fmt_key, MONTH, YEAR])
        cols = [d[0] for d in c.description]
        return dict(zip(cols, c.fetchone()))

LINE = "{:<16}CSV {:>16}   DB {:>16}   Diff {:>16}"
ROW_DEFS = [
    ("Rows:", 'rows'), ("Order LTRS:", 'order_ltrs'),
    ("Deliver LTRS:", 'deliver_ltrs'), ("Pending LTRS:", 'pending_ltrs'),
    None,
    ("Order Value:", 'order_val'), ("Deliver Value:", 'deliver_val'),
    ("Pending Value:", 'pending_val'),
    None,
    ("Order Qty:", 'order_qty'), ("Deliver Qty:", 'deliver_qty'),
    ("Pending Qty:", 'pending_qty'),
]

def section(title, csv, db):
    print(f"For {title}:\n")
    for d in ROW_DEFS:
        if d is None:
            print()
            continue
        label, key = d
        cv, dv = csv[key], float(db[key])
        print(LINE.format(label, fmt(cv), fmt(dv), fmt(abs(cv - dv))))
    print()

out = []
for disp, key in PLATFORMS:
    print(f"\n######## {disp} ########\n")
    section(f"DEL MONTH = {MONTH} {YEAR}",
            csv_agg(key, 'Delivery Month'),
            db_agg(key, 'delivery_month', 'delivered_year'))
    section(f"PO MONTH = {MONTH} {YEAR}",
            csv_agg(key, 'PO Month'),
            db_agg(key, 'po_month', 'po_year'))
