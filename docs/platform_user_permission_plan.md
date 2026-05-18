# Platform User Permission System Plan

## Goal

Create a user-based platform access system where each user only sees and uses the platforms assigned to them.

Example:

- Amazon user can see only Amazon.
- Blinkit user can see only Blinkit.
- Multi-platform user can see selected platforms, for example Amazon + Swiggy.
- Admin user can see every platform and management section.

The frontend must hide unavailable platform options, and the backend must block direct API access even if someone manually changes the URL.

## Current State

The backend already has a good base:

- Custom email user model in `accounts.models.User`.
- Django auth groups and permissions are seeded from `accounts/catalog.py`.
- Permission helpers exist in `accounts/permissions.py`.
- Platform scope helper already exists:
  - `user_platform_slugs(user)`
  - `can_access_platform(user, slug)`
- `/api/auth/me` already returns:
  - user details
  - groups
  - permissions
  - allowed `platforms`
- Most platform dashboard APIs already call `_ensure_scope(request.user, slug)`.

Missing or incomplete areas:

- Frontend still lists all platforms from `src/config/platforms.js`.
- Some newer platforms are not in the permission catalog yet.
- Uploaders need platform-aware backend checks, not only `upload.use`.
- SAP platform endpoints need platform-scope checks.
- Global dashboards and notifications need permission-aware filtering.

## Platform Permission Codes

Use one access permission per platform:

```text
platform.amazon.access
platform.blinkit.access
platform.zepto.access
platform.jiomart.access
platform.bigbasket.access
platform.swiggy.access
platform.flipkart.access
platform.flipkart_grocery.access
platform.zomato.access
platform.citymall.access
```

Admin wildcard:

```text
platform.*.access
```

Action permissions should stay separate from platform scope:

```text
platform.view
platform.stats.view
platform.po.view
platform.inventory.view
platform.secondary.view
platform.landing_rate.view
platform.landing_rate.edit
platform.month_targets.view
platform.month_targets.edit
upload.use
sap.view
sap.invoice.view
```

This means a user needs both:

1. A platform scope permission, for example `platform.amazon.access`.
2. A feature/action permission, for example `platform.secondary.view`.

## Default Groups

Create or update groups like this:

```text
Amazon User
Blinkit User
Zepto User
JioMart User
BigBasket User
Swiggy User
Flipkart User
Flipkart Grocery User
Zomato User
CityMall User
```

Each platform user group should include:

```text
platform.view
platform.stats.view
platform.po.view
platform.inventory.view
platform.secondary.view
platform.<slug>.access
```

Optional uploader group:

```text
Amazon Uploader
Blinkit Uploader
...
```

Each uploader group should include:

```text
upload.use
platform.<slug>.access
```

Admin groups:

- `Super Admin`: all permissions.
- `Platform Admin`: platform action permissions + `platform.*.access`.
- `Finance Analyst`: only SAP/dashboard permissions as needed.

## Backend Implementation Plan

### 1. Update Permission Catalog

File:

```text
accounts/catalog.py
```

Add missing platform scope codes:

```text
platform.flipkart_grocery.access
platform.zomato.access
platform.citymall.access
```

Also confirm all action codes used by views exist in the catalog:

```text
platform.landing_rate.view
platform.landing_rate.edit
platform.month_targets.view
platform.month_targets.edit
```

Then update `GROUP_CATALOG` with per-platform groups for all active platforms.

### 2. Seed Permissions And Groups

Run after catalog update:

```bash
python manage.py seed_permissions
python manage.py seed_groups
```

For production, create a migration or run these commands during deployment so new permissions exist in the database.

### 3. Keep Backend As Source Of Truth

Every platform-specific API must do both checks:

```python
@permission_classes([require("platform.secondary.view")])
def some_view(request, slug):
    _ensure_scope(request.user, slug)
```

This prevents direct access like:

```text
/api/platform/swiggy/sec-dashboard
```

from an Amazon-only user.

### 4. Audit Platform APIs

Confirm these APIs call `_ensure_scope`:

- `/api/platform/<slug>/stats`
- `/api/platform/<slug>/pos`
- `/api/platform/<slug>/inventory-match`
- `/api/platform/<slug>/primary-dashboard`
- `/api/platform/<slug>/sec-dashboard`
- `/api/platform/<slug>/sec-monthly-dashboard`
- `/api/platform/<slug>/comparison-dashboard`
- `/api/platform/<slug>/price-dashboard`
- `/api/platform/<slug>/sku-analysis-dashboard`
- `/api/platform/<slug>/drr-dashboard`
- `/api/platform/<slug>/soh-doh-dashboard`
- `/api/platform/<slug>/month-on-month-sale`
- `/api/platform/<slug>/landing-rate`
- `/api/platform/<slug>/month-targets`

### 5. Scope SAP Platform APIs

SAP platform APIs currently identify platform by slug and chain mapping. They should also check user platform access.

Apply this pattern:

```python
@permission_classes([require("sap.view")])
def platform_distributors(request, slug):
    if not can_access_platform(request.user, slug):
        raise PermissionDenied(...)
```

Use this for:

- `/api/sap/platform-distributors/<slug>`
- `/api/sap/platform-distributors/<slug>/<card_code>`
- `/api/sap/platform-sales-invoices/<slug>`

Global SAP Data should stay separate. Only admin/finance users should get `sap.view`.

### 6. Scope Uploaders

Upload APIs currently use `upload.use`. Add platform validation too.

Required behavior:

- User must have `upload.use`.
- User must also have `platform.<slug>.access`.
- Backend should derive slug from upload type/platform parameter.
- Backend must reject uploads for unassigned platforms.

Example:

```text
Amazon user + upload.use -> can upload Amazon only.
Amazon user without upload.use -> cannot upload anything.
Amazon user trying Blinkit upload -> 403.
```

### 7. Scope Notifications

Inventory DOH notifications include `platform_slug`.

For restricted users:

- Only return notifications where `platform_slug` is in `user_platform_slugs(request.user)`.
- Admin wildcard can see all.

Apply this to:

- notifications list
- notification detail
- mark read actions
- generate notification endpoint

### 8. Scope Global Dashboard Counts

The main dashboard currently shows table sections across all platforms.

Options:

1. Hide table count inspection from platform users and keep it admin-only.
2. Or map every table to a platform and filter by allowed platform slugs.

Recommended:

- Platform users should see platform cards only.
- Admin/manager users can see global table counts.

## Frontend Implementation Plan

### 1. Use Auth User Platforms

Frontend already gets user data from `AuthContext` and `/api/auth/me`.

Use:

```js
user.platforms
```

to filter platform lists.

### 2. Filter Home Sidebar Platform List

File:

```text
src/pages/Dashboard.jsx
```

Current behavior:

```js
getAllPlatforms().map(...)
```

Required behavior:

```js
const allowedPlatforms = new Set(user?.platforms || []);
const visiblePlatforms = getAllPlatforms().filter((platform) =>
  allowedPlatforms.has(platform.slug)
);
```

Admin wildcard users will receive all slugs from backend, so the same logic works for admin.

### 3. Block Direct Platform Routes

File:

```text
src/layouts/PlatformLayout.jsx
```

Before rendering platform sidebar/content:

- Check `slug` exists.
- Check `user.platforms` contains the slug.
- If not allowed, show an access denied page or redirect to `/dashboard`.

Frontend check is for user experience only. Backend still remains the real security layer.

### 4. Filter Upload Platform Dropdowns

Files:

```text
src/pages/uploader/InventoryUploader.jsx
src/pages/uploader/SecondaryUploader.jsx
src/pages/uploader/PrimaryUploader.jsx
```

Dropdown options must only include platforms in `user.platforms`.

Also handle URL params:

```text
/platform/blinkit/upload/secondary?platform=blinkitSec
```

If URL asks for a platform the user cannot access, show access denied instead of loading that uploader.

### 5. Filter Monthly Targets

The backend monthly target dashboard already scopes data with `user_platform_slugs`.

Frontend should still:

- Hide per-platform monthly target menu entries for unassigned platforms.
- Keep global monthly targets visible only to users with `platform.month_targets.view`.

### 6. Hide Global Tools By Permission

Use `user.permissions` to show or hide these:

- `Master Sheet`: admin/uploader/allowed manager only.
- `SAP Data`: users with `sap.view`.
- `Distributors`: users with `distributor.view` or `sap.view`.
- `Monthly Targets`: users with `platform.month_targets.view`.

Do not show SAP Data to a normal Amazon platform user unless that user also has `sap.view`.

## Admin Workflow

### Create Amazon-Only User

1. Create user with email and password.
2. Assign group:

```text
Amazon User
```

3. Optional: assign upload permission by either:
   - adding `Uploader` group plus Amazon access, or
   - creating `Amazon Uploader` group.

Expected result:

- User sees Amazon only.
- User cannot open Blinkit/Swiggy/Zepto pages.
- User cannot call other platform APIs directly.

### Create Multi-Platform User

Assign multiple groups:

```text
Amazon User
Swiggy User
```

Expected result:

- User sees Amazon and Swiggy only.

### Create Admin User

Assign:

```text
Platform Admin
```

or make the user `is_superuser=True`.

Expected result:

- User sees all active platforms.

## Access Rules Matrix

| User Type | Platform List | Platform APIs | Uploads | SAP Data | Master Sheet |
| --- | --- | --- | --- | --- | --- |
| Amazon User | Amazon only | Amazon only | No, unless `upload.use` | No | No |
| Amazon Uploader | Amazon only | Amazon only | Amazon only | No | Maybe, if allowed |
| Finance Analyst | All or assigned | View only | No | Yes | No |
| Platform Admin | All | All | Depends on upload permission | Depends on SAP permission | Yes |
| Super Admin | All | All | All | All | All |

## Testing Plan

### Backend Tests

Create users:

- `amazon_user`
- `blinkit_user`
- `multi_platform_user`
- `platform_admin`

Test cases:

- Amazon user gets 200 for `/api/platform/amazon/stats`.
- Amazon user gets 403 for `/api/platform/swiggy/stats`.
- Amazon user gets 403 for Swiggy upload.
- Amazon uploader gets 200 for Amazon upload.
- Amazon uploader gets 403 for Blinkit upload.
- Finance user gets 200 for `/api/sap/sales-analysis`.
- Normal platform user gets 403 for `/api/sap/sales-analysis`.
- `/api/auth/me` returns only assigned platform slugs.

### Frontend Tests

Manual checks:

- Login as Amazon user.
- Home sidebar shows only Amazon.
- Direct URL `/platform/swiggy` shows access denied or redirects.
- Upload dropdowns show only allowed platforms.
- SAP Data does not show for platform-only users.
- Admin login still shows all platforms and tools.

## Rollout Steps

1. Update backend permission catalog.
2. Add missing groups for every platform.
3. Add backend scope checks for SAP, uploads, notifications, and any missing platform APIs.
4. Update frontend to filter platform list from `user.platforms`.
5. Update frontend to hide global tools from users without matching permissions.
6. Run migrations or seed commands on production.
7. Create real user accounts and assign groups.
8. Test with at least one platform-only user and one admin user.

## Acceptance Criteria

The work is complete when:

- A user assigned only `Amazon User` sees only Amazon in the sidebar.
- The same user cannot access other platform pages by typing URLs.
- The same user receives 403 from backend APIs for other platforms.
- Uploaders are limited to their assigned platform.
- SAP Data and Master Sheet are not visible unless the user has permission.
- Admin users continue to access all platforms.

