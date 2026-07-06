# Chatbot Training Question Bank — Jivo Ecom

Built from a deep read of the full project (backend apps: platforms, warehouse/master_po,
shipment, sap, uploads, accounts, dashboard; frontend: all platform / ads / brand-fund /
coupon / Amazon / SAP / state-sales / distributor pages).

Legend:
- ✅ bot answers this TODAY (builtin engine)
- 🔶 partially works (generic table dump or wrong metric) — improve training
- 🆕 needs a new tool / new NLU training

Train phase by phase. For every question: if the bot replies with the help text → NLU gap
(add keywords in `engine/nlu.py`); if it answers but numbers are wrong → tool gap
(fix/add in `engine/tools.py`).

> Note: if "Business Mode" (demo inflation) is ON in the frontend, other platforms' numbers
> on screen are inflated — the bot reads the real DB, so numbers will differ. That is expected.
> Do NOT train claim/payment questions — those pages are placeholders with no data.

---

## Implementation status (built + verified against live DB)

All phases below are LIVE in the builtin engine (26 intents). Every question was run against the
real database and returns real numbers (not help text). Data sources per phase:

| Phase | Status | Intent(s) / data source |
|-------|--------|-------------------------|
| 1 Basics | ✅ done | greeting / help / list_platforms |
| 2 POs & liters | ✅ done | `liters` (master_po + Amazon PO), amount, fill rate, month-wise, `pos` status counts |
| 3 Rankings | ✅ done | `ranking` (+fill-rate ratio), `movers` (MoM), `split` (premium/commodity) |
| 4 Inventory & alerts | ✅ done | `inventory` (all_platform_inventory: totals/low-stock/by-city), `alerts` (+unread) |
| 5 Secondary & DRR | ✅ done | `sales` (SecMaster + Amazon sec view, returns, top-N), `drr` (daily run rate) |
| 6 Targets/landing/pendency | ✅ done | `targets`, `landing`, `pendency` (month_targets / monthly_landing_rate / master_po) |
| 7 Marketing | ✅ done | `ads`, `brand_fund`, `coupon` (per-platform *_ads_master / *_brandfund / coupon) |
| 8 Amazon suite | ✅ done | `amazon_po`, `expiry`, `appointments`, `amazon_mp`, `lead_time`, `shipments` |
| 9 State/realise | ✅ done (Postgres) | `state_sales` (SecMaster), `realise` (commission); SAP HANA → graceful message |
| 10 Excel | ✅ done | any answer + "excel"/"download"/"export" attaches an .xlsx |
| 11 Page actions | ✅ done | frontend widget (screenshot / calculate table / which page) |
| 12 Cross-domain | ⏳ needs Claude | multi-step reasoning — set ANTHROPIC_API_KEY to enable the Claude tool-use path |

### Known limitations (documented, not bugs)
- **SAP HANA data** (JM primary sales, SAP warehouse stock value / below-min / zero-stock,
  distributor balances & invoices, distributor FIFO inventory) is in a separate HANA DB the chatbot
  doesn't query yet — those questions return a clear "use the SAP dashboards" message.
- **Live region/ASIN DOH>threshold** (needs SOH ÷ live DRR join) — low-DOH items are covered by
  `alerts`; healthy-stock DOH thresholds route to alerts/inventory as the closest answer.
- **JioMart** has no PO rows in master_po and no separate PO table, so its liters/rankings are empty.
- **Phase 12** cross-domain questions need the optional Claude LLM path (multi-step reasoning).

---

## Phase 1 — Basics & platform info ✅
1. hi
2. hello, who are you?
3. what can you do
4. help
5. list all platforms
6. which platforms are active
7. what platforms do we sell on
8. what data can you show me

## Phase 2 — Purchase orders & liters (master_po) ✅ core
1. total order ltrs in blinkit of june month
2. how many liters delivered on zepto this month
3. order vs delivered liters for swiggy in june
4. missed ltrs for big basket last month
5. filled ltrs for blinkit this week
6. fill rate for blinkit last 30 days
7. how many po lines for zepto in june
8. order qty vs delivered qty for blinkit this week
9. total order amount inclusive for blinkit this month
10. delivered liters yesterday all platforms
11. blinkit purchase orders this week
12. show zepto pos today
13. flipkart grocery pos last month
14. show me the master po sheet (google sheet)
15. total delivered liters from 2026-06-01 to 2026-06-15 for swiggy
16. citymall order liters this year
17. zomato delivered ltrs in may
18. how many pos completed for blinkit in june 🔶 (po_status filter)
19. how many pos are cancelled for zepto in june 🔶
20. pending pos for big basket right now 🔶
21. pos expiring in next 5 days 🆕 (days_to_expiry 1–5, PENDING/APPOINTMENT DONE)
22. expiry alerts for zepto pos 🆕
23. qty fill rate % and ltrs miss rate % for swiggy in june 🆕
24. average lead time days for blinkit vendors 🆕

## Phase 3 — Rankings & Top-N ✅
1. give me the top states
2. top 10 states by delivered liters
3. top 5 cities by order liters for zepto
4. top brands by order amount in june
5. top 10 skus by delivered litres this month
6. top vendors by order qty for swiggy
7. which category has highest delivered litres
8. top locations by order liters
9. best platform by order liters in june
10. compare platforms by order liters for june
11. top 20 items by order amount
12. top states for blinkit last month
13. rank cities by delivered liters for swiggy this month
14. top vendors by number of pos
15. top states by missed ltrs 🆕 (add "missed" metric to `_metric_key`/`_metric_sql`)
16. top skus by fill rate 🆕 (ratio metric)
17. top riser and top faller skus this month vs last month 🆕 (top movers)
18. premium vs commodity litres split by platform 🆕 (item_head grouping)

## Phase 4 — Alerts, inventory & DOH/SOH ✅ alerts / 🔶 inventory
1. show zepto alerts
2. critical doh alerts for blinkit
3. warning alerts for swiggy
4. show alerts including resolved ones
5. which skus have the lowest doh right now
6. how many active doh alerts are there
7. doh alerts for amazon today
8. excel of zepto alerts
9. blinkit inventory 🔶
10. zepto stock on hand 🔶
11. swiggy inventory this week 🔶
12. total inventory qty for blinkit and top products 🆕
13. items with soh units less than 10 in blinkit 🆕
14. which items have doh greater than 20 in bigbasket 🆕
15. zepto city wise soh ltr and doh for today 🆕 (region DOH — swiggy/zepto only)
16. cities with doh less than 5 in zepto 🆕
17. amazon asin wise soh doh for june 🆕
18. stock by city top 15 across platforms 🆕
19. how many unread low doh alerts for zepto 🆕 (is_read)

## Phase 5 — Secondary sales, DRR & trends 🔶/🆕
1. blinkit secondary sales this month 🔶
2. zepto sales last week 🔶
3. shipped ltr and shipped value for bigbasket in june 🆕 (SecMaster summary)
4. total secondary litres all platforms in june 🆕
5. return value and return units for amazon last month 🆕
6. per liter shpd for bigbasket in june 🆕
7. top 10 skus by ltr sold on blinkit in june 🆕 (ranking on SecMaster, not master_po)
8. day wise ops and ltr for swiggy june 🆕 (DRR daily grid)
9. drr ltr for zepto this month 🆕
10. drr qty and drr value for blinkit premium 🆕
11. projected ltr for amazon in june 🆕 (projection)
12. estimated liters for blinkit this month 🆕
13. sku wise drr for amazon in shipped mode 🆕
14. month on month sale of sunflower 1l on bigbasket last 5 months 🆕
15. compare amazon shipped litres this june vs last year june 🆕 (YoY)
16. daily sales amount of GOLD 1L on zepto in june 🆕 (SKU analysis)
17. secondary yoy growth for blinkit 🆕
18. premium vs commodity shipped litres for swiggy this month 🆕

## Phase 6 — Targets, landing rates & pendency 🆕
1. blinkit done ltrs vs target this month and achieved %
2. target vs done ltrs for swiggy premium this month
3. achieved % for commodity in zepto
4. est ltr and growth % vs last month for zepto targets
5. drr and require drr for blinkit premium primary target
6. pending ltr for zomato premium this month
7. which platforms are behind on their monthly target
8. how much of the overall monthly target is completed
9. call center targets vs done ltrs this month
10. landing rate of sku 10048294 in blinkit this month
11. basic rate of extra light olive oil 1l in swiggy for june
12. which zepto skus have no landing rate this month
13. zepto pending ltrs and pending units right now (pendency)
14. pendency by city for flipkart grocery
15. which warehouse has highest pending ltrs for blinkit
16. top 10 vendors by pending value in swiggy
17. open pos with pending litres older than 30 days

## Phase 7 — Marketing: ads, brand fund, coupons 🆕
1. total ad spent on blinkit in june
2. zepto ads roas for june
3. top 10 items by ad spent on blinkit this month
4. amazon ads acos portfolio wise for june
5. cpc and ctr of amazon ads in june
6. ntb sales and ntb orders on amazon this month
7. how many impressions did bigbasket ads get in june
8. flipkart campaign wise ad spend and roi
9. ads litres sold on zepto in may
10. indirect gmv on blinkit ads june month
11. which item has highest acos on swiggy ads
12. which platform had highest ad spent in june
13. compare ad spent blinkit vs zepto for june
14. tacos of blinkit items this month
15. ads sale vs total sale category wise for june (ads summary)
16. total brand fund spent on blinkit in june
17. top 5 items by brand fund spent on zepto
18. swiggy brand fund sub category wise
19. blinkit brand fund day wise trend for june
20. amazon coupon budget spent vs remaining on latest date
21. total coupon redemptions and clips on amazon
22. which coupon has highest budget used %
23. premium vs commodity coupon budget split
24. total expense (ads + brand fund) for blinkit this month

## Phase 8 — Amazon suite & shipment planning ✅ basic shipments / 🆕 rest
1. how many shipments are there and total planned liters ✅
2. shipments dispatched last 7 days ✅
3. how many shipments pending approval right now ✅ (status word)
4. how many shipments are in transit ✅
5. total order liters of pending amazon pos 🆕 (reporting."Amazon PO")
6. how many amazon pos are pending right now 🆕
7. show me all MOV status pos 🆕
8. which amazon pos are expiring in the next 7 days 🆕
9. fill rate by fulfillment center for may 🆕
10. amazon po list for fc DEL4 🆕
11. requested vs received qty by sub category for june 🆕
12. how many new pos came yesterday and total order value 🆕
13. premium vs commodity order value on amazon this month 🆕
14. how many appointments today 🆕
15. confirmed vs cancelled appointments this month 🆕
16. which destination fc has the most appointments 🆕
17. carton and unit count for appointment 553554037970 🆕
18. load % and planned liters of shipment 87 🆕
19. committed vs filled units for the DED5 appointment 🆕
20. total short units this week and which appointments 🆕
21. which asins have doh below 7 days 🆕
22. live stock and free to plan for finished goods in BH-FGM 🆕
23. amazon mp delivered litres and top states for june 🆕
24. unmapped asins in amazon mp this month 🆕

## Phase 9 — SAP, distributors, state sales & home KPIs 🆕
1. total premium liters in jm primary for may (mart source)
2. wellness billing litres this month premium vs commodity (oil source)
3. grand total value for mart source in june
4. premium liters of EVARA ENTERPRISES this month
5. sales analysis 1 jun to 30 jun — total liter, quantity, line total and cogs
6. top 10 states by liter in sap sales analysis for june
7. total stock value in mart inventory right now
8. how many items are below min stock
9. how many skus at zero stock in oil inventory
10. warehouse wise stock value comparison for june
11. finished goods on hand grand total by warehouse
12. zepto distributors with balance and credit line
13. which distributors in mumbai have negative balance
14. open ap invoices for distributor C0001 with balance due
15. sustainquest on hand qty and fifo value (distributor inventory)
16. which skus are short flagged at antize foods
17. state wise sales for june — top states by litres
18. maharashtra sales value on zepto last month
19. which region sold more ltrs north or south in june
20. top cities by litres in karnataka
21. jivo vs sano state sales split
22. primary realise per ltr for blinkit this month (₹/L)
23. net realise per litre after distributor commission for blinkit
24. total distributor commission for june

## Phase 10 — Excel / download phrasing ✅ for supported intents
1. excel of zepto alerts
2. download blinkit purchase orders for last week
3. excel of top 10 states by delivered liters
4. export top brands by order amount in june
5. excel of delivered liters for swiggy this month
6. download master po sheet
7. spreadsheet of all platforms
8. excel of blinkit inventory
9. csv of zepto sales last month
10. excel of state wise sales for june 🆕
11. export zepto drr for june 🆕
12. excel of blinkit ads item wise for june 🆕
13. download secmaster data for big basket last month 🆕
14. excel of all confirmed appointments this month 🆕

## Phase 11 — Page actions & chat features ✅ (frontend widget)
1. which page am i on
2. what page is this
3. take a screenshot
4. screenshot this page
5. calculate the total of this table
6. sum the order qty column
7. average of the delivered ltrs column
8. calculate this page's table
9. (history button) reopen my last conversation
10. (chip click) fills the input, does not auto-send

## Phase 12 — Advanced cross-domain (future training) 🆕
1. blinkit fill rate last month vs its primary target achieved % — did the shortfall cause the miss?
2. compare acos and tacos across blinkit, zepto, swiggy, amazon for june — cheapest incremental litre?
3. which platform grew secondary litres most month-over-month, and did ad spend grow faster than sales?
4. for zepto skus with doh below 5, do open pending pos cover the gap, and how many expire in 5 days?
5. gap between primary done ltrs and secondary sell-out for amazon this month — is stock building up?
6. top 5 states for premium litres vs where warehouse soh is sitting
7. at current drr, which platforms will miss their monthly target and what req_drr do they need?
8. per-litre net realise blinkit vs zepto after commission, ads and brand fund
9. biggest faller skus — were they also low on stock (doh under 10)? availability vs demand
10. amazon po fill rate vs shipment truck fill % and short supply for the same appointment window
11. vendors with highest pending litres older than 30 days vs their lead time
12. order litres and value in pos expiring within 5 days — do we have BH-FGM stock to fulfil them?
13. share of secondary sale that came from ads per platform (ads_sale / sec_value)
14. premium vs commodity yoy growth per platform — which sub category drove the swing?
15. did last month's landing rate change on blinkit move acos in the ads summary?
