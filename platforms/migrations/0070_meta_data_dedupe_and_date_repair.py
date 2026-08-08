"""Repair the two data defects that made the Meta dashboard over-report spend.

Found 2026-08-08: July-2026 read Rs 76,692 against a true Rs 36,225 (2.12x).
Three things combined; the third (SUMming cumulative snapshots) is a code bug and
is fixed in platforms/views.py + uploads/views.py. This migration cleans the two
data defects those fixes cannot reach retroactively.


DEFECT 1 -- MOJIBAKE CAMPAIGN NAMES CREATE DUPLICATE ROWS
---------------------------------------------------------
Meta campaign names carry an en-dash ("Pomace 5L – PD", U+2013). Meta exports
UTF-8, but a file opened and re-saved through Excel on Windows is read as cp1252
and the corruption is baked in ("Pomace 5L â€“ PD"). meta_data's
unique key is (date, campaign_name), so the two spellings never collide -- the
upsert INSERTS a second row instead of updating, and every dashboard counts that
campaign twice.

34 such rows exist (Dec-2025, Jan-2026, Jun-2026, Jul-2026). On the 31-07-2026
snapshot all 17 pairs were verified byte-identical on reach, impressions, clicks
and spend, so the mojibake row is dropped where a clean twin exists on the same
date, and renamed where it is the only spelling. Nothing is summed or averaged.


DEFECT 2 -- DAY/MONTH TRANSPOSED REPORTING-END DATES
----------------------------------------------------
`date` is TEXT 'DD-MM-YYYY' and `month`/`year` are GENERATED from its character
positions, so a row written MM-DD-YYYY lands in the wrong month whenever the day
is <= 12 (a day > 12 is unambiguous and always parsed correctly). That is why the
dashboard offered Sep/Oct/Nov/Dec-2026 spend while it was only August 2026.

Each remap below is proven twice over, not inferred from the date alone:
  * the batch's `start_date` names the month the swapped date lands in; and
  * these exports are cumulative month-to-date, so the corrected date slots into
    its month's running total in strict monotonic order on BOTH spend and
    campaign count. The May-1 series reads, after the remap:
        May  2   1,725.66 ( 6 campaigns)   May 13   41,590.65 (10)
        May  4   6,248.11 ( 6)             May 15   64,094.58 (10)
        May  8  15,889.44 ( 9)             May 16   82,535.64 (13)
        May  9  20,661.00 (10)             May 18  105,090.15 (13)
        May 11  24,863.51 (10)             May 23  105,577.10 (13)
        May 12  37,133.71 (10)             May 25  111,916.03 (13)

`start_date` is left alone: it is MM-DD-YYYY on most historical rows but nothing
reads it -- month/year come from `date` -- and its convention varies per batch.

NOT REMAPPED: date '06-02-2026'. It swaps to 2 June 2026 but its start_date is
1 May 2026, so it is a May-1 -> Jun-2 cumulative spanning two months. A one-month
-per-row table cannot represent it and any bucket would be part wrong, so it is
left exactly as found and reported to the user for a re-pull.


DEFECT 2b -- THE JULY 1-6 SNAPSHOT STORED TWICE
------------------------------------------------
date '07-06-2026' (bucketed JUNE) is byte-for-byte the same snapshot as
'06-07-2026' (bucketed JULY) -- identical reach 1,755,650 / impressions 2,326,268
/ clicks 45,078 / spend 26,573.17. The July 1-6 pull was loaded twice with day and
month transposed, so June carried a full copy of July's first week. July has since
been reloaded from a clean 1-31 Jul export, which supersedes 1-6 Jul entirely, so
these rows are deleted rather than remapped onto the July snapshot they precede.

Every step is idempotent: re-running selects nothing.
"""

from django.db import migrations

# Reverse map for the cp1252 bytes 0x80-0x9F -- the only bytes whose cp1252
# meaning differs from Latin-1. 0x81/0x8D/0x8F/0x90/0x9D are unassigned.
_CP1252_HIGH = {
    "€": 0x80, "‚": 0x82, "ƒ": 0x83, "„": 0x84,
    "…": 0x85, "†": 0x86, "‡": 0x87, "ˆ": 0x88,
    "‰": 0x89, "Š": 0x8A, "‹": 0x8B, "Œ": 0x8C,
    "Ž": 0x8E, "‘": 0x91, "’": 0x92, "“": 0x93,
    "”": 0x94, "•": 0x95, "–": 0x96, "—": 0x97,
    "˜": 0x98, "™": 0x99, "š": 0x9A, "›": 0x9B,
    "œ": 0x9C, "ž": 0x9E, "Ÿ": 0x9F,
}
_MOJIBAKE_MARKERS = ("â€", "â‚", "Ã", "Â")

# Transposed reporting-end date -> its true DD-MM-YYYY value (see docstring).
_DATE_REMAP = {
    "05-02-2026": "02-05-2026",   # -> 2 May 2026  (was FEBRUARY)
    "05-04-2026": "04-05-2026",   # -> 4 May 2026  (was APRIL)
    "05-08-2026": "08-05-2026",   # -> 8 May 2026  (was AUGUST)
    "05-09-2026": "09-05-2026",   # -> 9 May 2026  (was SEPTEMBER)
    "05-11-2026": "11-05-2026",   # -> 11 May 2026 (was NOVEMBER)
    "05-12-2026": "12-05-2026",   # -> 12 May 2026 (was DECEMBER)
    "03-10-2026": "10-03-2026",   # -> 10 Mar 2026 (was OCTOBER)
    "04-09-2026": "09-04-2026",   # -> 9 Apr 2026  (was SEPTEMBER)
}

# The July 1-6 snapshot's transposed twin (see DEFECT 2b).
_DUPLICATE_SNAPSHOT_DATE = "07-06-2026"


def _repair_mojibake(text):
    """Undo UTF-8-decoded-as-cp1252 corruption; return text unchanged if it is
    not actually mojibake (the round-trip fails to decode)."""
    for _ in range(3):
        if not any(m in text for m in _MOJIBAKE_MARKERS):
            break
        try:
            raw = bytes(
                _CP1252_HIGH[ch] if ord(ch) > 0xFF else ord(ch) for ch in text
            )
            repaired = raw.decode("utf-8")
        except (KeyError, ValueError, UnicodeDecodeError):
            break
        if repaired == text:
            break
        text = repaired
    return text


def forwards(apps, schema_editor):
    cur = schema_editor.connection.cursor()

    # ---- Defect 1: collapse mojibake campaign names -------------------------
    cur.execute(
        'SELECT id, "date", campaign_name FROM meta_data WHERE campaign_name IS NOT NULL'
    )
    dropped = renamed = 0
    for row_id, date_text, name in cur.fetchall():
        clean = _repair_mojibake(name).strip()
        if clean == name:
            continue
        cur.execute(
            'SELECT 1 FROM meta_data WHERE "date" = %s AND campaign_name = %s LIMIT 1',
            [date_text, clean],
        )
        if cur.fetchone():
            # A correctly-spelled twin already holds this (date, campaign) --
            # the mojibake row is the duplicate that inflated the totals.
            cur.execute("DELETE FROM meta_data WHERE id = %s", [row_id])
            dropped += 1
        else:
            cur.execute(
                "UPDATE meta_data SET campaign_name = %s WHERE id = %s", [clean, row_id]
            )
            renamed += 1
    print(f"  meta_data: dropped {dropped} mojibake duplicate row(s), renamed {renamed}")

    # ---- Defect 2b: the July 1-6 snapshot's transposed twin ------------------
    cur.execute('DELETE FROM meta_data WHERE "date" = %s', [_DUPLICATE_SNAPSHOT_DATE])
    print(f"  meta_data: deleted {cur.rowcount} duplicated 1-6 Jul snapshot row(s)")

    # ---- Defect 2: put transposed snapshots back in their real month ---------
    for wrong, right in _DATE_REMAP.items():
        # Never overwrite a genuine row that already owns the corrected key.
        cur.execute(
            'DELETE FROM meta_data w WHERE w."date" = %s AND EXISTS ('
            '  SELECT 1 FROM meta_data r WHERE r."date" = %s'
            '   AND r.campaign_name IS NOT DISTINCT FROM w.campaign_name)',
            [wrong, right],
        )
        collided = cur.rowcount
        cur.execute(
            'UPDATE meta_data SET "date" = %s, end_date = %s WHERE "date" = %s',
            [right, right, wrong],
        )
        if cur.rowcount or collided:
            print(
                f"  meta_data: {wrong} -> {right}: moved {cur.rowcount} row(s)"
                + (f", dropped {collided} colliding" if collided else "")
            )


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0069_primary_summary_deliver_value_exclusive"),
    ]

    # Irreversible by design: the dropped rows were duplicates carrying no
    # information the surviving row does not already hold.
    operations = [migrations.RunPython(forwards, migrations.RunPython.noop)]
