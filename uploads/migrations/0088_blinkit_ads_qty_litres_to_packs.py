# Blinkit ads: convert the reported quantity from LITRES to PACKS.
#
# THE BUG
# -------
# Blinkit's ads report column "Direct Quantities Sold" (-> blinkit_ads.direct_qty_sold)
# is denominated in BASE UNITS (litres), not in packs. The blinkit_ads_master view
# treated it as packs and multiplied it by `basic_rate`, which is a PER-PACK landing
# rate from monthly_landing_rate. So `total_sale_basic_rate` ("Ads sale") was
# over-counted by each SKU's pack size: 5L SKUs 5x, 2L SKUs 2x, 1L SKUs correct.
# `ads_ltr_sold` had the mirror problem — it multiplied an already-in-litres figure
# by per_ltr AGAIN.
#
# EVIDENCE (Aug 1-16 2026, the reported month)
# --------------------------------------------
# 1. basic_rate / gmv_per_unit tracked pack size exactly — 1L 0.48-0.76, 2L 1.20,
#    5L 2.65-3.44 (a landing rate above the consumer sale value is impossible).
#    Dividing basic_rate by per_ltr collapsed all nine SKUs into one 0.48-0.76 band.
# 2. Against real per-pack MRP in "blinkitSec" (qty_sold there IS packs: 542 x
#    Rs 4,999 = the Pomace 5L row), dividing the ads qty by pack litres made every
#    5L/2L realisation match its 1L sibling.
# 3. ads_ltr_sold reported 75,169 L ad-driven against 62,707 L of TOTAL Blinkit
#    litres sold in the same window — more from ads than was sold in total.
#
# Aug 1-16 2026 effect: Ads sale 1,70,40,731 -> 56,03,970; ads litres 75,169 -> 24,587;
# direct qty 24,587 "units" -> 14,074 real packs.
#
# WHAT CHANGES
# ------------
# Only the blinkit_ads_master VIEW. The raw blinkit_ads table and the
# blinkit_ads_master_mv matview are untouched, so nothing is destroyed and no
# matview refresh is needed — replacing the view takes effect immediately.
#
#   direct_qty_sold        raw / per_ltr   -> real packs (matches every other platform)
#   indirect_qty_sold      raw / per_ltr   -> real packs
#   ads_ltr_sold           raw             -> the litres Blinkit actually reported
#   total_sale_basic_rate  basic_rate x packs
#
# ROAS and ACOS are NOT affected: they derive from direct_gmv / indirect_gmv, which
# this migration does not touch. TACOS (spend / secondary sales) is likewise untouched.
#
# The cross-platform Ads Summary union (platforms/views.py _ADS_SUMMARY_UNION) had a
# SECOND copy of the same multiplication — it reads b.direct_qty_sold from this view
# and multiplies by the same per-pack rate, so it is corrected by this change with no
# code edit, and its `qty` column now means packs like the other platforms.
#
# Divisor guard: per_ltr is 100% populated wherever basic_rate is (verified: 0 rows
# with one set and the other missing; only values 1.0 / 2.0 / 5.0 exist). The
# COALESCE(NULLIF(per_ltr, 0), 1) fallback means a future unmapped SKU passes the raw
# figure through unchanged instead of turning NULL — i.e. it degrades to today's
# behaviour rather than losing the row.
#
# Only Blinkit is changed. Swiggy and Zepto show no pack-size gradient in the same
# test (their qty is genuinely packs). BigBasket DOES show the gradient and is
# recorded as a separate open item — it is deliberately NOT changed here, because it
# needs its own evidence pass first.
from django.db import migrations


# Carry-forward landing rate, identical to 0079's _cf (kept verbatim so this
# migration only changes the quantity handling, nothing about rate lookup).
def _cf(sku_expr, date_expr, platform):
    return f"""(
        SELECT mlr.basic_rate
        FROM monthly_landing_rate mlr
        WHERE upper(btrim(mlr.sku_code)) = upper(btrim({sku_expr}))
          AND replace(upper(btrim(mlr.format)), ' ', '') = '{platform}'
          AND mlr.month::date <= date_trunc('month', {date_expr}::timestamp)::date
        ORDER BY mlr.month::date DESC, mlr.created_at DESC NULLS LAST
        LIMIT 1
    )"""


# Pack size to divide by. Never 0 / NULL: an unmapped SKU falls back to 1 so the
# raw figure passes through untouched (today's behaviour) instead of becoming NULL.
_PACK = "COALESCE(NULLIF(per_ltr, 0)::numeric, 1)"

_RATE = _cf("format_sku_code", "date", "BLINKIT")

# Column order and types are byte-for-byte those of 0079, so CREATE OR REPLACE
# works in both directions without a DROP or a dependency cascade.
FORWARD = f"""
CREATE OR REPLACE VIEW blinkit_ads_master AS
 SELECT date, campaign_id, campaign_name,
        (COALESCE(direct_qty_sold, 0)::numeric / {_PACK})   AS direct_qty_sold,
        (COALESCE(indirect_qty_sold, 0)::numeric / {_PACK}) AS indirect_qty_sold,
        impressions, ad_spent, direct_gmv, indirect_gmv, format, format_sku_code,
        sap_sku_name, category, sub_category, item, item_head, per_unit, per_ltr,
        COALESCE(direct_qty_sold, 0)::double precision      AS ads_ltr_sold,
        month, year, month_day,
        {_RATE} * (COALESCE(direct_qty_sold, 0)::numeric / {_PACK})
            AS total_sale_basic_rate,
        {_RATE} AS basic_rate
   FROM blinkit_ads_master_mv;
"""

# Reverse: the exact 0079 definition (litres treated as packs).
REVERSE = f"""
CREATE OR REPLACE VIEW blinkit_ads_master AS
 SELECT date, campaign_id, campaign_name, direct_qty_sold, indirect_qty_sold,
        impressions, ad_spent, direct_gmv, indirect_gmv, format, format_sku_code,
        sap_sku_name, category, sub_category, item, item_head, per_unit, per_ltr,
        ads_ltr_sold, month, year, month_day,
        {_RATE} * COALESCE(direct_qty_sold, 0)
            AS total_sale_basic_rate,
        {_RATE} AS basic_rate
   FROM blinkit_ads_master_mv;
"""


class Migration(migrations.Migration):

    dependencies = [
        ("uploads", "0087_blinkit_ads_detail_cpm_in_key"),
    ]

    operations = [
        migrations.RunSQL(sql=FORWARD, reverse_sql=REVERSE),
    ]
