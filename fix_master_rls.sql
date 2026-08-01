-- Part 2 of the fix: the re-imported tables came back with ROW LEVEL SECURITY
-- enabled but NO policies — so the app role (dev01) sees ZERO rows even though
-- the data is there. Disable RLS on those 8 tables (single-role backend; RLS
-- serves no purpose here).
-- RUN AS: postgres (superuser).

ALTER TABLE public.master_sheet      DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."Location_Master" DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."blinkitSec"      DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."zeptoSec"        DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."swiggySec"       DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."bigbasketSec"    DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."jiomartSec"      DISABLE ROW LEVEL SECURITY;
ALTER TABLE public."flipkartSec"     DISABLE ROW LEVEL SECURITY;

-- Verification 1: rls must be f (false) on all 8
SELECT relname, relrowsecurity AS rls
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND relname IN ('master_sheet','Location_Master','blinkitSec','zeptoSec',
                  'swiggySec','bigbasketSec','jiomartSec','flipkartSec')
ORDER BY relname;

-- Verification 2: the real row counts (as postgres you always saw them;
-- after this, dev01 sees the same numbers)
SELECT 'master_sheet' AS t, COUNT(*) FROM public.master_sheet
UNION ALL SELECT 'blinkitSec',   COUNT(*) FROM public."blinkitSec"
UNION ALL SELECT 'zeptoSec',     COUNT(*) FROM public."zeptoSec"
UNION ALL SELECT 'swiggySec',    COUNT(*) FROM public."swiggySec"
UNION ALL SELECT 'bigbasketSec', COUNT(*) FROM public."bigbasketSec"
UNION ALL SELECT 'jiomartSec',   COUNT(*) FROM public."jiomartSec"
UNION ALL SELECT 'flipkartSec',  COUNT(*) FROM public."flipkartSec"
UNION ALL SELECT 'Location_Master', COUNT(*) FROM public."Location_Master";
