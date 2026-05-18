# Main Admin Dashboard Plan

## Purpose

When the app starts, the main dashboard should not be only a table-count page. For admin users, it should become a cross-platform business dashboard where they can understand all platform performance in one place:

- Inventory position
- Secondary sales
- Primary sales
- Monthly targets
- Risk and freshness issues
- Quick navigation to platform dashboards

The current Home page can still keep table counts as an admin/debug section, but the first screen should show business KPIs.

## Recommendation

Use one adaptive `Home` dashboard:

```text
/dashboard
```

Behavior:

- Admin or platform admin: show all-platform overview.
- Multi-platform user: show only assigned platforms.
- Single-platform user: show the same layout, filtered to that platform only.
- User without dashboard permission: redirect to their first allowed platform page.

This is better than redirecting admin to one platform dashboard because admin needs a full business picture first.

## Dashboard Name

Recommended screen title:

```text
Business Overview
```

Alternative names:

- Admin Overview
- Platform Control Room
- E-Commerce Performance Dashboard

My suggestion: use `Business Overview`. It feels clear for daily use and does not sound too technical.

## First Screen Layout

The first viewport should answer four questions quickly:

1. How much did we sell?
2. How much inventory do we have?
3. Which platforms are weak or risky?
4. Is the uploaded data fresh?

## Visual Example

Desktop wireframe:

```text
+------------------------------------------------------------------------------+
| Home > Business Overview                                   Month: May 2026   |
|                                               [Month] [Date Range] [Refresh] |
+------------------------------------------------------------------------------+
|                                                                              |
|  +---------------+ +---------------+ +---------------+ +----------------+    |
|  | Secondary Val | | Secondary LTR | | Primary Value | | Inventory SOH  |    |
|  | INR 4.82 Cr   | | 1.18 L LTR    | | INR 6.21 Cr   | | 2.44 L Units   |    |
|  | +8% vs last   | | +4% vs last   | | 72% delivered | | 17 May fresh   |    |
|  +---------------+ +---------------+ +---------------+ +----------------+    |
|                                                                              |
|  +---------------------------------+ +-----------------------------------+    |
|  | Platform Performance            | | Risk & Attention                  |    |
|  | Amazon     INR 1.42Cr  28k L 83%| | 14 Low DOH SKUs                   |    |
|  | Blinkit    INR 0.96Cr  19k L 71%| |  3 stale inventory uploads        |    |
|  | Swiggy     INR 0.88Cr  17k L 68%| |  2 secondary date mismatches      |    |
|  | BigBasket  INR 0.61Cr  12k L 54%| |  9 pending primary PO issues      |    |
|  | Zepto      INR 0.44Cr   9k L 49%| |                                   |    |
|  +---------------------------------+ +-----------------------------------+    |
|                                                                              |
|  +---------------------------------+ +-----------------------------------+    |
|  | Item Head Mix                   | | Primary vs Secondary Gap          |    |
|  | Premium     ########## 46%      | | Amazon     ########.. 72%          |    |
|  | Commodity   #######... 33%      | | Blinkit    ######.... 61%          |    |
|  | Others      ####...... 21%      | | Swiggy     #######... 69%          |    |
|  +---------------------------------+ +-----------------------------------+    |
|                                                                              |
|  +------------------------------------------------------------------------+  |
|  | Data Freshness                                                         |  |
|  | Platform     Inventory Date   Secondary Date   Primary Month   Status |  |
|  | Amazon       17-05-2026       17-05-2026       May 2026        Fresh  |  |
|  | Blinkit      17-05-2026       17-05-2026       May 2026        Fresh  |  |
|  | Swiggy       17-05-2026       16-05-2026       May 2026        Check  |  |
|  +------------------------------------------------------------------------+  |
+------------------------------------------------------------------------------+
```

Mobile wireframe:

```text
+----------------------------+
| Business Overview          |
| [Month May 2026] [Refresh] |
+----------------------------+
| Secondary Value            |
| INR 4.82 Cr                |
+----------------------------+
| Inventory SOH              |
| 2.44 L Units               |
+----------------------------+
| Risk & Attention           |
| 14 Low DOH SKUs            |
| 3 stale uploads            |
+----------------------------+
| Platform Performance       |
| Amazon      INR 1.42Cr 83% |
| Blinkit     INR 0.96Cr 71% |
| Swiggy      INR 0.88Cr 68% |
+----------------------------+
```

## Main Sections

### 1. Global Filters

Filters at the top:

- Month
- Date range
- Platform
- Item Head
- Sales Type: Primary / Secondary / Inventory
- Refresh button

Default:

- Current month
- All allowed platforms
- All item heads

### 2. KPI Cards

Show 4 to 6 cards only. Too many cards will make the dashboard hard to read.

Recommended cards:

| Card | Meaning |
| --- | --- |
| Secondary Value | Current month secondary sale value |
| Secondary LTR | Current month secondary shipped/sold litres |
| Primary Delivered Value | Delivered value from primary/PO data |
| Pending PO Value | Order value still pending |
| Inventory SOH | Latest available stock on hand |
| Target Achievement | Monthly target achieved percentage |

Each card should show:

- Current value
- Previous month comparison
- Latest data date
- Small status indicator: good / warning / bad

### 3. Platform Performance Table

This is the most important admin section.

One row per platform:

| Platform | Secondary Value | Secondary LTR | Primary Delivered | Pending PO | SOH | DOH Risk | Target % | Freshness |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Amazon | INR 1.42Cr | 28k | INR 1.80Cr | INR 22L | 42k | 4 SKUs | 83% | Fresh |
| Blinkit | INR 0.96Cr | 19k | INR 1.10Cr | INR 18L | 31k | 2 SKUs | 71% | Fresh |
| Swiggy | INR 0.88Cr | 17k | INR 0.94Cr | INR 11L | 26k | 5 SKUs | 68% | Check |

Row click should open that platform dashboard.

### 4. Risk & Attention Panel

This panel should show the problems admin must act on first.

Examples:

- Low DOH SKUs
- Stale inventory upload
- Secondary date mismatch
- Primary pending value high
- Missing Master Sheet mapping
- Upload validation failures
- Product/category mismatch

Each alert should have:

- Platform
- Issue
- Impact
- Button/link to details

### 5. Charts

Keep charts operational, not decorative.

Recommended charts:

- Platform-wise secondary value
- Platform-wise secondary litres
- Item head mix: Premium / Commodity / Others
- Primary delivered vs pending
- Inventory SOH by platform
- Low DOH count by platform

Avoid too many chart types on the first screen. Use simple bars and stacked bars.

### 6. Data Freshness

This is very important for this project because dashboard mismatch often happens due to upload date problems.

Show:

| Platform | Inventory Date | Secondary Date | Primary Date/Month | Status |
| --- | --- | --- | --- | --- |
| Amazon | 17-05-2026 | 17-05-2026 | May 2026 | Fresh |
| Swiggy | 17-05-2026 | 16-05-2026 | May 2026 | Check |

Rules:

- Fresh: latest date matches selected reporting date/month.
- Check: one source is older than expected.
- Missing: no data available.

### 7. Existing Table Counts

The current dashboard table counts are useful for admin/developer inspection, but they should move lower on the page or into a tab:

```text
Business Overview | Data Health | Raw Tables
```

Recommended:

- Default tab: `Business Overview`
- Second tab: `Data Health`
- Third tab: `Raw Tables`

## Suggested Backend API

Create one endpoint for the main dashboard:

```text
GET /api/dashboard/business-overview
```

Query params:

```text
month=5
year=2026
from_date=2026-05-01
to_date=2026-05-17
platform=amazon
item_head=Premium
```

If `platform` is blank, return all platforms allowed for the user.

Backend must use:

```python
user_platform_slugs(request.user)
```

so a restricted user only gets their assigned platforms.

Response shape:

```json
{
  "filters": {
    "month": 5,
    "year": 2026,
    "from_date": "2026-05-01",
    "to_date": "2026-05-17",
    "platforms": ["amazon", "blinkit", "swiggy"]
  },
  "summary": {
    "secondary_value": 48200000,
    "secondary_ltr": 118000,
    "primary_delivered_value": 62100000,
    "pending_po_value": 9100000,
    "inventory_soh_units": 244000,
    "target_achievement_pct": 76.4
  },
  "platforms": [
    {
      "slug": "amazon",
      "name": "Amazon",
      "secondary_value": 14200000,
      "secondary_ltr": 28000,
      "primary_delivered_value": 18000000,
      "pending_po_value": 2200000,
      "soh_units": 42000,
      "low_doh_skus": 4,
      "target_achievement_pct": 83,
      "inventory_date": "2026-05-17",
      "secondary_date": "2026-05-17",
      "freshness_status": "fresh"
    }
  ],
  "item_head_mix": [
    { "item_head": "Premium", "value": 22100000, "percent": 46 },
    { "item_head": "Commodity", "value": 15900000, "percent": 33 },
    { "item_head": "Others", "value": 10200000, "percent": 21 }
  ],
  "risks": [
    {
      "platform": "swiggy",
      "severity": "warning",
      "title": "Secondary data is stale",
      "detail": "Latest secondary date is 16-05-2026 while inventory is 17-05-2026."
    }
  ],
  "freshness": []
}
```

## Data Source Plan

### Inventory

Use:

- `all_platform_inventory` for Blinkit, Zepto, Swiggy, BigBasket, JioMart.
- `amazon_master_inventory` or Amazon inventory dashboard source for Amazon.

Metrics:

- Latest inventory date
- SOH units
- SOH litres if available
- DOH risk count

### Secondary

Use the same source logic as platform secondary dashboards so numbers match existing platform screens.

Metrics:

- Sale value
- Sale units
- Sale litres
- Max secondary date
- Item head mix

### Primary

Use the same source logic as platform primary dashboards.

Metrics:

- Order value
- Delivered value
- Pending value
- Delivered percentage
- Open/pending order count

### Monthly Targets

Use existing monthly target dashboard logic.

Metrics:

- Target LTR
- Done LTR
- Estimated LTR
- Achievement percentage

## Permission Behavior

Admin:

- Sees every active platform.
- Sees global business KPIs.
- Sees raw table counts.
- Sees SAP/Master Sheet shortcuts if permissions allow.

Platform user:

- Sees only assigned platforms.
- Summary totals include only assigned platforms.
- Cannot see raw table counts unless given dashboard inspect permission.

Single-platform user:

- Home still opens, but it behaves like a platform overview.
- Optional: show a quick button to open the platform dashboard.

## Frontend Implementation Plan

### 1. Replace Current Home First Screen

File:

```text
src/components/dashboard/OverviewHome.jsx
```

Change the top of the page from table-count first to business KPI first.

Keep current sections lower down:

- alerts
- inventory charts
- table counts

### 2. Add API Client

File:

```text
src/services/api.js
```

Add:

```js
dashboardAPI.getBusinessOverview(opts)
```

### 3. Add Query Key

File:

```text
src/services/queryKeys.js
```

Add:

```js
businessOverview: (opts) => ['dashboard', 'businessOverview', opts]
```

### 4. Add Components

Suggested components:

```text
src/components/dashboard/BusinessKpiStrip.jsx
src/components/dashboard/PlatformPerformanceTable.jsx
src/components/dashboard/RiskAttentionPanel.jsx
src/components/dashboard/DataFreshnessTable.jsx
src/components/dashboard/ItemHeadMixChart.jsx
```

### 5. Responsive UI

Desktop:

- KPI cards in 4-column grid.
- Platform table and risk panel side by side.
- Charts in 2-column grid.

Mobile:

- Cards stacked.
- Platform table becomes compact list.
- Risk panel appears before charts.

## Implementation Phases

### Phase 1: Read-Only Admin Overview

Build:

- Backend `/api/dashboard/business-overview`
- KPI cards
- Platform performance table
- Data freshness table

No complex charts yet.

### Phase 2: Risk Intelligence

Add:

- Low DOH SKUs
- Stale upload detection
- Missing Master Sheet mappings
- Upload validation failures

### Phase 3: Charts And Drilldowns

Add:

- Item head mix
- Primary vs secondary gap
- Platform trend
- Click-through links to exact platform dashboards

### Phase 4: Permission-Adaptive Home

Connect with platform permission system:

- Admin sees all.
- Platform users see scoped home.
- Raw table counts become admin/inspect-only.

## Acceptance Criteria

The dashboard is successful when:

- Admin can understand all-platform performance within 10 seconds.
- Platform mismatch/date freshness issues are visible immediately.
- Admin can compare Inventory, Secondary, and Primary on one screen.
- Platform users only see their assigned platform data.
- Numbers match existing platform dashboards.
- Clicking a platform row opens the correct platform dashboard.

## Final Suggestion

Use this startup experience:

```text
Login -> /dashboard -> Business Overview
```

Do not make the default page a single platform dashboard for admin. Admin should start with a cross-platform view, because their job is to compare Amazon, Blinkit, Swiggy, Zepto, BigBasket, Flipkart, Zomato, and CityMall together.
