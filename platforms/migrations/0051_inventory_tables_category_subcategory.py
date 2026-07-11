from django.db import migrations

# Physical category/sub_category on the raw per-platform inventory tables, sourced
# from master_sheet by the platform SKU code. Each table gets a one-time backfill
# plus a BEFORE INSERT/UPDATE trigger so every future upload row is populated
# automatically (path-independent — works for the batch upsert or any other write).
#
# Only tables WITHOUT an existing `category` column are included here. jiomart /
# citymall / zomato already carry a raw platform `category`, so they're left alone
# to avoid clobbering it (the all_platform_inventory view still derives category /
# sub_category for them from master_sheet).
TABLES = [
    # (table, sku_column, format_key)
    ("blinkit_inventory", "item_id", "blinkit"),
    ("zepto_inventory", "sku_code", "zepto"),
    ("swiggy_inventory", "sku_code", "swiggy"),
    ("bigbasket_inventory", "sku_id", "bigbasket"),
    ("amazon_inventory", "asin", "amazon"),
]

TRIGGER_FN = """
CREATE OR REPLACE FUNCTION set_inventory_category() RETURNS trigger AS $fn$
DECLARE
    v_sku text;
    v_cat text;
    v_sub text;
BEGIN
    -- TG_ARGV[0] = SKU column name on this table, TG_ARGV[1] = master_sheet format_key
    v_sku := upper(btrim(to_jsonb(NEW) ->> TG_ARGV[0]));
    IF v_sku IS NULL OR v_sku = '' THEN
        RETURN NEW;
    END IF;
    SELECT ms.category, ms.sub_category
      INTO v_cat, v_sub
      FROM master_sheet ms
     WHERE upper(btrim(ms.format_sku_code::text)) = v_sku
       AND regexp_replace(lower(btrim(coalesce(ms.format, ''))), '[^a-z0-9]+', '', 'g') = TG_ARGV[1]
     LIMIT 1;
    NEW.category := v_cat;
    NEW.sub_category := v_sub;
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;
"""


def _forward():
    parts = [
        "CREATE INDEX IF NOT EXISTS master_sheet_sku_key_idx "
        "ON master_sheet (upper(btrim(format_sku_code::text)));",
        TRIGGER_FN,
    ]
    for tbl, sku, fmt in TABLES:
        parts.append(
            f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS category text, "
            f"ADD COLUMN IF NOT EXISTS sub_category text;"
        )
        # Backfill BEFORE attaching the trigger (clean set-based UPDATE).
        parts.append(
            f"UPDATE {tbl} t SET category = m.category, sub_category = m.sub_category "
            f"FROM (SELECT DISTINCT ON (upper(btrim(format_sku_code::text))) "
            f"upper(btrim(format_sku_code::text)) AS sku_key, category, sub_category "
            f"FROM master_sheet "
            f"WHERE regexp_replace(lower(btrim(coalesce(format, ''))), '[^a-z0-9]+', '', 'g') = '{fmt}' "
            f"ORDER BY upper(btrim(format_sku_code::text)), ctid) m "
            f"WHERE m.sku_key = upper(btrim(t.{sku}::text));"
        )
        parts.append(f"DROP TRIGGER IF EXISTS trg_{tbl}_category ON {tbl};")
        parts.append(
            f"CREATE TRIGGER trg_{tbl}_category BEFORE INSERT OR UPDATE ON {tbl} "
            f"FOR EACH ROW EXECUTE FUNCTION set_inventory_category('{sku}', '{fmt}');"
        )
    return "\n".join(parts)


def _reverse():
    parts = []
    for tbl, _sku, _fmt in TABLES:
        parts.append(f"DROP TRIGGER IF EXISTS trg_{tbl}_category ON {tbl};")
        parts.append(
            f"ALTER TABLE {tbl} DROP COLUMN IF EXISTS category, "
            f"DROP COLUMN IF EXISTS sub_category;"
        )
    parts.append("DROP FUNCTION IF EXISTS set_inventory_category();")
    parts.append("DROP INDEX IF EXISTS master_sheet_sku_key_idx;")
    return "\n".join(parts)


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0050_inventory_view_category_subcategory"),
    ]

    operations = [migrations.RunSQL(sql=_forward(), reverse_sql=_reverse())]
