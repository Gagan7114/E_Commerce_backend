"""Add total_po_zbs.facility_name — Swiggy's warehouse label, for `location`.

WHY
---
Swiggy's PO export carries three geo columns: FacilityId ('KOC'), FacilityName
('Koc IM1') and City ('KOCHI'). Until now the uploader mapped City -> location
and threw FacilityId/FacilityName away, so the dashboards could only ever show
the city, never which Instamart facility the PO was raised for. The user wants
`location` to show the facility ('Koc IM1') while `city` keeps showing 'KOCHI'.

WHAT
----
Add a nullable `facility_name` text column. It is populated ONLY by the Swiggy
Primary PO uploader (SWIGGY_PRIM maps FacilityName -> facility_name); every
other platform and every pre-existing row keeps it NULL.

The companion platforms migration teaches master_po_base to emit
    location = COALESCE(NULLIF(TRIM(facility_name), ''), location)
so a row only changes once it actually has a facility name. `city` and `state`
keep deriving from the untouched stored City, so they do not move at all.

SAFETY
------
  * Additive, nullable column -> existing rows unaffected; every
    `SELECT <explicit cols>` view keeps working, so this migration alone is a
    no-op for all reads.
  * No backfill. Historical Swiggy rows never stored FacilityName (it only
    exists in the source sheet), so they keep location = city name until they
    are re-uploaded. This was the explicit choice: leave history alone.
  * IF NOT EXISTS guard makes it safe to re-run.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0088_blinkit_ads_qty_litres_to_packs"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE public.total_po_zbs
                ADD COLUMN IF NOT EXISTS facility_name text;
            """,
            reverse_sql="""
            ALTER TABLE public.total_po_zbs
                DROP COLUMN IF EXISTS facility_name;
            """,
        ),
    ]
