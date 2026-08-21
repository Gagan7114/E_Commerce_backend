"""master_po.location shows the FACILITY when one is stored (Swiggy), not the city.

WHY
---
Swiggy's PO export carries FacilityId ('KOC'), FacilityName ('Koc IM1') and City
('KOCHI'). The uploader only ever kept City (mapped to `location`), so the
dashboards could not tell which Instamart facility a PO belonged to - every Kochi
facility collapsed into one "KOCHI" line. The user wants `location` to show the
facility while `city` keeps showing the city.

WHAT
----
`uploads/0089` added the nullable `total_po_zbs.facility_name` column. This
migration teaches `master_po_base` two things:

  1. location  = COALESCE(NULLIF(TRIM(facility_name), ''), location)
     -> a row only changes once it actually HAS a facility name.

  2. the city / state CASE blocks stop reading `location` and read a new
     internal passthrough column `geo_src` instead, which always carries the
     ORIGINAL stored location (the city).

WHY THIS CANNOT MOVE ANY OTHER PLATFORM
---------------------------------------
`facility_name` is written ONLY by the Swiggy Primary PO uploader. For every
other platform (and for every Swiggy row uploaded before this change) the column
is NULL, so:

    location = COALESCE(NULL, location) = location          -- unchanged
    geo_src  = location                                     -- unchanged CASE input

i.e. the emitted SQL is semantically identical for all non-Swiggy rows. The
`city` and `state` values do not move for ANYONE, including Swiggy: they still
derive from the stored city, just reached through `geo_src`.

`distributor_margin` reads the DERIVED city ('BENGALURU'), not location, so
Swiggy margins are unaffected too.

HOW
---
The chain is master_po_base (view) -> master_po_raw (view) -> master_po_mv
(matview) -> master_po (view), plus primary_summary_mv (matview). Rather than
re-stating ~500 lines of SQL (which would silently revert 0061/0067/0069), this
migration CAPTURES the live definitions with pg_get_viewdef, applies four
targeted string edits to master_po_base only, and rebuilds the chain from the
captured text. Matview indexes are captured from pg_indexes and recreated.

Every edit is guarded: if an anchor string is missing the migration RAISES
instead of silently doing nothing. `geo_src` is used internally and is NOT
selected downstream (the view has no wildcard selects), so the output column list
of master_po is byte-identical - matview indexes and every dashboard keep working.

SAFETY
------
  * atomic -> either the whole chain is rebuilt or nothing changes.
  * Re-running is blocked by the 'already carries geo_src' guard.
  * Requires ownership of the master_po* objects to DROP/CREATE them. If the
    role is not the owner this fails loudly with 'must be owner of ...' and
    nothing is changed.
"""

from django.db import migrations


# ---------------------------------------------------------------- forward ----
FORWARD = r"""
DO $mig$
DECLARE
    base_def   text;
    raw_def    text;
    mv_def     text;
    master_def text;
    summ_def   text;
    mv_idx     text[];
    ps_idx     text[];
    stmt       text;
BEGIN
    -- 1. capture the LIVE definitions (strip the trailing semicolon so the
    --    matview bodies can take a ' WITH DATA' suffix).
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po_base'::regclass, true)), ';')      INTO base_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po_raw'::regclass, true)), ';')       INTO raw_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po_mv'::regclass, true)), ';')        INTO mv_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po'::regclass, true)), ';')           INTO master_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.primary_summary_mv'::regclass, true)), ';')  INTO summ_def;

    SELECT COALESCE(array_agg(indexdef), '{}'::text[]) INTO mv_idx
      FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'master_po_mv';
    SELECT COALESCE(array_agg(indexdef), '{}'::text[]) INTO ps_idx
      FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'primary_summary_mv';

    -- 2. guards: fail loudly rather than silently no-op.
    IF base_def IS NULL OR raw_def IS NULL OR mv_def IS NULL
       OR master_def IS NULL OR summ_def IS NULL THEN
        RAISE EXCEPTION 'master_po chain incomplete - one of base/raw/mv/master/summary is missing';
    END IF;
    IF position('geo_src' in base_def) > 0 THEN
        RAISE EXCEPTION 'master_po_base already carries geo_src - already applied?';
    END IF;
    IF to_regclass('public.total_po_zbs') IS NULL
       OR NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = 'total_po_zbs'
                         AND column_name = 'facility_name') THEN
        RAISE EXCEPTION 'total_po_zbs.facility_name missing - run uploads/0089 first';
    END IF;
    IF position('total_po_zbs.location,' in base_def) = 0 THEN
        RAISE EXCEPTION 'anchor not found: total_po_zbs.location,';
    END IF;
    IF position('total_po.location,' in base_def) = 0 THEN
        RAISE EXCEPTION 'anchor not found: total_po.location,';
    END IF;
    IF position('b.location,' in base_def) = 0 THEN
        RAISE EXCEPTION 'anchor not found: b.location,';
    END IF;
    IF position('upper(COALESCE(p.location, ''''::text))' in base_def) = 0 THEN
        RAISE EXCEPTION 'anchor not found: upper(COALESCE(p.location, <empty>::text))';
    END IF;
    -- The city CASE ends in "ELSE p.location" (pass the city through when no
    -- rename rule matches). That fallback MUST follow geo_src too, otherwise an
    -- unlisted city like KOCHI or VIZAG would report the FACILITY as its city.
    IF (length(base_def) - length(replace(base_def, 'ELSE p.location', ''))) / length('ELSE p.location') <> 1 THEN
        RAISE EXCEPTION 'expected exactly ONE "ELSE p.location" in master_po_base, found a different count';
    END IF;

    -- 3. four targeted edits, base view only.
    --    (a) total_po_zbs branch: facility overrides location; keep city as geo_src.
    base_def := replace(
        base_def,
        'total_po_zbs.location,',
        'COALESCE(NULLIF(btrim(total_po_zbs.facility_name), ''''::text), total_po_zbs.location) AS location,
            total_po_zbs.location AS geo_src,');
    --    (b) total_po branch: no facility column here, so geo_src IS location.
    base_def := replace(
        base_def,
        'total_po.location,',
        'total_po.location,
            total_po.location AS geo_src,');
    --    (c) carry geo_src through prep so the CASE blocks can see it.
    base_def := replace(
        base_def,
        'b.location,',
        'b.location,
            b.geo_src,');
    --    (d) city + state CASE conditions read the city, not the (possibly facility) location.
    base_def := replace(
        base_def,
        'upper(COALESCE(p.location, ''''::text))',
        'upper(COALESCE(p.geo_src, ''''::text))');
    --    (e) ...and so must the city CASE's ELSE fallback. Without this an
    --        unlisted city (KOCHI, VIZAG, CENTRAL GOA, AHMEDABAD, NAGPUR, ...)
    --        would fall through and report the FACILITY as the city. Only the
    --        city CASE has this fallback; the state CASE ends in ELSE NULL.
    --        The bare `p.location,` in the select list is deliberately left
    --        alone - that IS the displayed location.
    base_def := replace(base_def, 'ELSE p.location', 'ELSE p.geo_src');

    -- 4. rebuild the chain: drop top-down, recreate bottom-up.
    DROP VIEW IF EXISTS public.master_po;
    DROP MATERIALIZED VIEW IF EXISTS public.primary_summary_mv;
    DROP MATERIALIZED VIEW IF EXISTS public.master_po_mv;
    DROP VIEW IF EXISTS public.master_po_raw;
    DROP VIEW IF EXISTS public.master_po_base;

    EXECUTE 'CREATE VIEW public.master_po_base AS ' || base_def;
    EXECUTE 'CREATE VIEW public.master_po_raw AS ' || raw_def;
    EXECUTE 'CREATE MATERIALIZED VIEW public.master_po_mv AS ' || mv_def || ' WITH DATA';
    FOREACH stmt IN ARRAY mv_idx LOOP EXECUTE stmt; END LOOP;
    EXECUTE 'CREATE VIEW public.master_po AS ' || master_def;
    EXECUTE 'CREATE MATERIALIZED VIEW public.primary_summary_mv AS ' || summ_def || ' WITH DATA';
    FOREACH stmt IN ARRAY ps_idx LOOP EXECUTE stmt; END LOOP;
END
$mig$;
"""


# ---------------------------------------------------------------- reverse ----
# pg_get_viewdef re-prints the parse tree, so the whitespace of the inserted
# lines is not preserved verbatim. The reverse therefore matches with \s+
# tolerance instead of plain replace().
REVERSE = r"""
DO $mig$
DECLARE
    base_def   text;
    raw_def    text;
    mv_def     text;
    master_def text;
    summ_def   text;
    mv_idx     text[];
    ps_idx     text[];
    stmt       text;
BEGIN
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po_base'::regclass, true)), ';')      INTO base_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po_raw'::regclass, true)), ';')       INTO raw_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po_mv'::regclass, true)), ';')        INTO mv_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.master_po'::regclass, true)), ';')           INTO master_def;
    SELECT rtrim(btrim(pg_get_viewdef('public.primary_summary_mv'::regclass, true)), ';')  INTO summ_def;

    SELECT COALESCE(array_agg(indexdef), '{}'::text[]) INTO mv_idx
      FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'master_po_mv';
    SELECT COALESCE(array_agg(indexdef), '{}'::text[]) INTO ps_idx
      FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'primary_summary_mv';

    IF position('geo_src' in base_def) = 0 THEN
        RAISE EXCEPTION 'master_po_base has no geo_src - nothing to reverse';
    END IF;

    base_def := regexp_replace(
        base_def,
        'COALESCE\(NULLIF\(btrim\(total_po_zbs\.facility_name\),\s*''''::text\),\s*total_po_zbs\.location\)\s+AS location,\s*total_po_zbs\.location\s+AS geo_src,',
        'total_po_zbs.location,');
    base_def := regexp_replace(
        base_def,
        'total_po\.location,\s*total_po\.location\s+AS geo_src,',
        'total_po.location,');
    base_def := regexp_replace(
        base_def,
        'b\.location,\s*b\.geo_src,',
        'b.location,');
    base_def := replace(base_def, 'p.geo_src', 'p.location');

    IF position('geo_src' in base_def) > 0 THEN
        RAISE EXCEPTION 'reverse incomplete - geo_src still present after rewrite';
    END IF;

    DROP VIEW IF EXISTS public.master_po;
    DROP MATERIALIZED VIEW IF EXISTS public.primary_summary_mv;
    DROP MATERIALIZED VIEW IF EXISTS public.master_po_mv;
    DROP VIEW IF EXISTS public.master_po_raw;
    DROP VIEW IF EXISTS public.master_po_base;

    EXECUTE 'CREATE VIEW public.master_po_base AS ' || base_def;
    EXECUTE 'CREATE VIEW public.master_po_raw AS ' || raw_def;
    EXECUTE 'CREATE MATERIALIZED VIEW public.master_po_mv AS ' || mv_def || ' WITH DATA';
    FOREACH stmt IN ARRAY mv_idx LOOP EXECUTE stmt; END LOOP;
    EXECUTE 'CREATE VIEW public.master_po AS ' || master_def;
    EXECUTE 'CREATE MATERIALIZED VIEW public.primary_summary_mv AS ' || summ_def || ' WITH DATA';
    FOREACH stmt IN ARRAY ps_idx LOOP EXECUTE stmt; END LOOP;
END
$mig$;
"""


def forwards(apps, schema_editor):
    # params=None so psycopg sends the SQL verbatim: the captured view bodies
    # contain literal LIKE patterns ('%BENGALURU%'), which the default params=()
    # would try to parse as placeholders.
    schema_editor.execute(FORWARD, params=None)


def backwards(apps, schema_editor):
    schema_editor.execute(REVERSE, params=None)


class Migration(migrations.Migration):

    atomic = True

    dependencies = [
        ("platforms", "0072_blinkitsec_date_index"),
        ("uploads", "0089_total_po_zbs_facility_name"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
