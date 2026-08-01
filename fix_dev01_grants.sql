-- Restore the app role's (dev01) access to the 19 postgres-owned data tables.
-- Cause: re-importing these tables (drop + recreate as the postgres user) wiped
-- the old grants, which broke the Amazon PO upload transform (master_sheet) and
-- the platform Sec/inventory dashboards.
-- RUN AS: postgres (superuser).

GRANT SELECT, INSERT, UPDATE, DELETE ON
  public.master_sheet,
  public.oitm_master,
  public."Location_Master",
  public.flipkart_grocery_master,
  public.monthly_landing_rate,
  public.amazon_inventory,
  public.amazon_sec_daily,
  public.amazon_sec_range,
  public."bigbasketSec",
  public.bigbasket_inventory,
  public."blinkitSec",
  public.blinkit_inventory,
  public."flipkartSec",
  public."jiomartSec",
  public.jiomart_inventory,
  public."swiggySec",
  public.swiggy_inventory,
  public."zeptoSec",
  public.zepto_inventory
TO dev01;

-- Future-proof: any table postgres creates in public from now on is
-- automatically readable/writable by dev01 (so the next re-import
-- doesn't break the app again).
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dev01;

-- Verification: every row must show can_read = t
SELECT tablename,
       has_table_privilege('dev01', 'public.' || quote_ident(tablename), 'SELECT') AS can_read
FROM pg_tables
WHERE schemaname = 'public' AND tableowner = 'postgres'
ORDER BY tablename;
