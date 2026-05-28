# Per-Platform Uploader Permission Plan

> **Status: PLAN ONLY — nothing here is implemented yet.**
> This document is the design/spec for restricting each user to the uploaders
> of the platform(s) they are assigned. It extends the broader
> [`platform_user_permission_plan.md`](./platform_user_permission_plan.md)
> (which covers platform *dashboard* access). This doc focuses only on the
> **uploader** dimension.

## 1. Goal

Give each user access to **only the uploaders of their assigned platform(s)**.

Example (the requested case):

- User **ABC** is given **"Amazon Uploader"** permission only.
- ABC can use **every Amazon-related uploader** — Amazon Primary, Secondary,
  Inventory, Ads, Coupon, Price.
- ABC **cannot** see or use Blinkit / Swiggy / Zepto / Flipkart / … uploaders.
- ABC trying to call another platform's upload API directly (by editing the
  URL or payload) gets **403** from the backend.

The same model applies to every platform: "Blinkit Uploader", "Swiggy
Uploader", etc., and to multi-platform users (e.g. Amazon + Swiggy uploader).

## 2. Current state (what already exists)

The permission plumbing is already strong — we mostly need new **codes**,
new **groups**, and per-request **slug checks** on the upload endpoints.

- **Glob permission engine** — `accounts/permissions.py`
  - `require("code")` → DRF permission class used as `@permission_classes([require(...)])`.
  - `has_permission_code(user, code)` supports globs on either side
    (`upload.*`, `*.use`, exact).
  - Codes come from `user.user_permissions` + `user.groups` (Django auth).
    Superuser → wildcard `*`.
  - Precedent helpers for the *platform* dimension already exist:
    `user_platform_slugs(user)`, `can_access_platform(user, slug)` using
    `platform.<slug>.access`.
- **Permission catalog** — `accounts/catalog.py`
  - Single source of truth: `PERMISSION_CATALOG` + `GROUP_CATALOG`.
  - Today there is exactly **one** upload code: `("upload.use", "Use the bulk upload tool")`.
  - Per-platform groups already exist for dashboards: `"Amazon User"`, etc.
  - Seeded via `python manage.py seed_permissions` / `seed_groups`.
- **Current upload enforcement** — every upload endpoint is gated only by the
  coarse `require("upload.use")` (no platform awareness):
  - `uploads/views.py` — `batch`, `master-sheet/*`, `ads-master/*`,
    `flipkart-grocery/*`, etc. (~20 endpoints).
  - `uploads/amazon_uploads.py` — Amazon `/api/uploads` (report-type driven).
  - `dashboard/views.py` — 2 table-write endpoints.
- **Frontend uploader catalog** — the dataset×platform matrix is the single
  source of truth: `Frontend/src/pages/uploader/hub/datasetPlatformMatrix.js`
  (`PLATFORM_DATASETS`, `getRouting`). The Upload Hub
  (`Frontend/src/pages/uploader/UploadHub.jsx`) renders tiles from it and
  tracks `platformSlug` + `datasetKey`.
- `/api/auth/me` already returns `permissions` and an allowed `platforms` list
  — we will add an `upload_platforms` list the same way.

## 3. Uploader inventory (what "an uploader" is, per platform)

Datasets (uploader types): **secondary, primary, inventory, ads, brand_fund,
coupon, price** (`DATASETS` in the matrix). Each platform supports a subset:

| Platform slug      | Uploaders the user should get with that platform's uploader permission |
| ------------------ | ---------------------------------------------------------------------- |
| `amazon`           | primary, secondary, inventory, ads, **coupon**, **price**             |
| `blinkit`          | primary, secondary, inventory, ads, brand_fund                        |
| `swiggy`           | primary, secondary, inventory, ads, brand_fund                        |
| `zepto`            | primary, secondary, inventory, ads, brand_fund                        |
| `bigbasket`        | primary, secondary, inventory, ads                                    |
| `flipkart`         | secondary, ads                                                        |
| `flipkart_grocery` | primary, secondary                                                     |
| `zomato`           | primary, inventory                                                     |
| `citymall`         | primary, inventory                                                     |
| `jiomart`          | inventory                                                             |

Source of truth: `PLATFORM_DATASETS` in `datasetPlatformMatrix.js`. **When a
platform gains/loses a dataset there, the uploader permission automatically
covers it** — we gate by *platform slug*, not by individual dataset, so the
matrix stays the only list to maintain.

**Cross-platform / non-platform uploaders** (handled separately, see §6.4):
`master-sheet/*` and `ads-master/*` are global reference data, not tied to one
platform.

## 4. Permission design

### Recommended: a dedicated `upload.<slug>.use` scope code

Mirror the existing `platform.<slug>.access` pattern, but for uploaders:

```text
upload.amazon.use
upload.blinkit.use
upload.swiggy.use
upload.zepto.use
upload.bigbasket.use
upload.flipkart.use
upload.flipkart_grocery.use
upload.zomato.use
upload.citymall.use
upload.jiomart.use

upload.*.use          # admin wildcard — all uploaders
upload.master.use     # global master-sheet / ads-master (non-platform)
```

**Why a dedicated code (not reuse `platform.<slug>.access`)?**
The request is "ABC has *only the Amazon uploader* permission." A dedicated
`upload.amazon.use` lets us grant uploader access **without** also granting
Amazon dashboard/PO/inventory viewing. Reusing `platform.amazon.access` would
couple the two (an uploader-only user would also see Amazon dashboards). Keep
them decoupled.

> **Alternative considered:** keep `upload.use` + require `platform.<slug>.access`
> (as sketched in the platform plan §6). Simpler (no new scope codes) but
> couples uploading to dashboard access and can't express "upload-only" users.
> Rejected for this requirement.

### Backward compatibility for the existing `upload.use`

`upload.use` does **not** glob-match `upload.amazon.use`, so existing holders
would lose access under a naive switch. Handle it in the helper: treat legacy
`upload.use` as "all uploaders" (equivalent to `upload.*.use`). The seed/
migration should also upgrade the existing **"Uploader"** group from
`upload.use` → `upload.*.use`.

## 5. New permission codes and groups

### `accounts/catalog.py` — add to `PERMISSION_CATALOG`

```python
# Uploads — scope codes (which platform's uploaders the user may use)
("upload.*.use",                 "Use every platform's uploaders (admin)"),
("upload.amazon.use",            "Use Amazon uploaders"),
("upload.blinkit.use",           "Use Blinkit uploaders"),
("upload.swiggy.use",            "Use Swiggy uploaders"),
("upload.zepto.use",             "Use Zepto uploaders"),
("upload.bigbasket.use",         "Use BigBasket uploaders"),
("upload.flipkart.use",          "Use Flipkart uploaders"),
("upload.flipkart_grocery.use",  "Use Flipkart Grocery uploaders"),
("upload.zomato.use",            "Use Zomato uploaders"),
("upload.citymall.use",          "Use CityMall uploaders"),
("upload.jiomart.use",           "Use JioMart uploaders"),
("upload.master.use",            "Use global master-sheet / ads-master"),
# keep "upload.use" (legacy = all uploaders) for back-compat
```

### `GROUP_CATALOG` — add per-platform uploader groups

```python
"Amazon Uploader":           ["upload.amazon.use"],
"Blinkit Uploader":          ["upload.blinkit.use"],
"Swiggy Uploader":           ["upload.swiggy.use"],
"Zepto Uploader":            ["upload.zepto.use"],
"BigBasket Uploader":        ["upload.bigbasket.use"],
"Flipkart Uploader":         ["upload.flipkart.use"],
"Flipkart Grocery Uploader": ["upload.flipkart_grocery.use"],
"Zomato Uploader":           ["upload.zomato.use"],
"CityMall Uploader":         ["upload.citymall.use"],
"JioMart Uploader":          ["upload.jiomart.use"],
"All Uploader":              ["upload.*.use", "upload.master.use"],
```

A multi-platform uploader = assign several groups (e.g. *Amazon Uploader* +
*Swiggy Uploader*). Seed with `seed_permissions` + `seed_groups`, and ship a
data migration so production gets the new codes.

## 6. Backend plan (source of truth)

### 6.1 New helpers in `accounts/permissions.py`

```python
def can_use_uploader(user, slug) -> bool:
    # superuser → *, admin wildcard, exact per-slug, or legacy "all" code
    return (
        user.is_superuser
        or has_permission_code(user, "upload.*.use")
        or has_permission_code(user, f"upload.{slug}.use")
        or has_permission_code(user, "upload.use")  # legacy = all uploaders
    )

def user_upload_slugs(user) -> list[str]:
    # mirror user_platform_slugs(): every active slug if wildcard/legacy/super,
    # else the slugs whose upload.<slug>.use the user holds.
```

### 6.2 Expose to the frontend via `/api/auth/me`

Add `upload_platforms: user_upload_slugs(user)` to the auth-me payload
(`accounts/views.py` + `serializers.py`), alongside the existing `platforms`.
The frontend filters tiles from this (no client-side perm parsing).

### 6.3 Per-request slug check on every upload endpoint

Keep the coarse `require("upload.use")` replaced by **"holds any upload code"**,
then **derive the platform slug from the request and call
`can_use_uploader`**. Pattern:

```python
slug = _slug_for_upload(request)        # see derivation table below
if not can_use_uploader(request.user, slug):
    raise PermissionDenied("You can't upload for this platform.")
```

### 6.4 How to derive the slug per endpoint group

| Endpoint(s)                                              | How to get the slug                                         | Check                          |
| ------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------ |
| `uploads/views.py` `batch` (`_batch_upload`)            | from payload `table` (and/or `format`/`platform`) → **table→slug map** (see §6.5) | `can_use_uploader(user, slug)` |
| `uploads/amazon_uploads.py` `/api/uploads` (report_type AMAZON_PO / APPOINTMENT / price / ads / coupon) | slug = `amazon` (fixed)                                     | `upload.amazon.use`            |
| `uploads/views.py` `flipkart-grocery/*`                 | slug = `flipkart_grocery` (fixed)                          | `upload.flipkart_grocery.use`  |
| `uploads/views.py` `master-sheet/*`, `ads-master/*`     | not platform-scoped — gate by `upload.master.use` (or admin) | `upload.master.use`            |
| `dashboard/views.py` (2 table-write endpoints)          | from `table` param → table→slug map                        | `can_use_uploader(user, slug)` |

> The platform-specific frontend uploaders (e.g. `AmazonAdsCouponUploader`,
> `AmazonPriceUploader`, `SwiggyAdsUploader`, `ZeptoBrandFundUploader`, …) all
> post to one of the endpoints above, so gating those endpoints covers them.

### 6.5 The table → slug map (the one new piece of data)

`batch` uploads carry a `table` name (validated against `UPLOAD_ALLOWED_TABLES`
in `uploads/views.py`). We need a server-side map from table → platform slug,
e.g. `amazon_*` → `amazon`, `blinkit_*` → `blinkit`, etc.

- The frontend already does longest-prefix slug matching in
  `Frontend/src/components/dashboard/HomeDashboard.jsx` (`buildTablePlatformMap`)
  — reuse that logic/rules on the backend.
- Tables that don't belong to any platform (master/ads-master) → treat as
  `upload.master.use`.
- **Decision needed:** maintain this map explicitly in `accounts/catalog.py`
  (or `uploads/`) so it's reviewable, vs. deriving by prefix. Recommend an
  explicit dict to avoid silent mis-mapping.

### 6.6 Migration / seed

1. Add codes + groups to `accounts/catalog.py`.
2. `python manage.py seed_permissions && python manage.py seed_groups`.
3. Data migration for production; upgrade existing **Uploader** group
   `upload.use` → `upload.*.use`.

## 7. Frontend plan (UX only — backend stays the gate)

1. **AuthContext** — read `user.upload_platforms` from `/api/auth/me`.
2. **Upload Hub** (`Frontend/src/pages/uploader/UploadHub.jsx`) — filter the
   platform tiles to `upload_platforms`; if a dataset has no allowed platform,
   hide it. The dataset×platform matrix already drives the grid, so filter the
   platform list it iterates.
3. **Route guards** — the uploader routes in `Frontend/src/App.jsx`
   (`/upload/*`, `/platform/:slug/upload/*`) and the platform layout
   (`PlatformLayout.jsx`) should redirect / show "access denied" if the user
   lacks `upload.<slug>.use`. (URL like `/platform/swiggy/upload/secondary`
   must not load for an Amazon-only uploader.)
4. **Sidebar / tool nav** — show the "Uploaders" entry only if the user has any
   upload permission; within the Upload Hub show only permitted platforms.
5. **Master Sheet / ADS Master** entries — show only with `upload.master.use`
   (or admin).

## 8. Admin workflow — create user ABC (Amazon uploader only)

1. Create user ABC (email + password) in the admin.
2. Assign group **"Amazon Uploader"** (`upload.amazon.use`). Do **not** assign
   any `platform.*.access` if ABC should not see dashboards.
3. Result:
   - Upload Hub shows **Amazon** only, with Amazon's datasets (primary,
     secondary, inventory, ads, coupon, price).
   - All Amazon upload APIs → **200**.
   - Any other platform's upload API → **403**, even by direct URL/payload.

Multi-platform uploader → assign multiple uploader groups. All uploaders →
assign **"All Uploader"** or set `is_superuser`.

## 9. Access matrix

| User                          | Sees in Upload Hub | Amazon upload | Blinkit upload | Master Sheet |
| ----------------------------- | ------------------ | ------------- | -------------- | ------------ |
| ABC — *Amazon Uploader*       | Amazon only        | ✅            | ❌ (403)        | ❌           |
| *Amazon + Swiggy Uploader*    | Amazon, Swiggy     | ✅            | ❌             | ❌           |
| *All Uploader*                | All platforms      | ✅            | ✅             | ✅           |
| Legacy `upload.use` holder    | All platforms      | ✅            | ✅             | ✅ (treated as all) |
| Super Admin                   | All                | ✅            | ✅             | ✅           |
| Platform-only user (no upload)| nothing            | ❌            | ❌             | ❌           |

## 10. Testing plan

**Backend** — create `amazon_uploader`, `swiggy_uploader`, `multi_uploader`,
`all_uploader`:
- Amazon uploader → 200 on every Amazon upload endpoint (PO, secondary,
  inventory, ads, coupon, price).
- Amazon uploader → 403 on a Blinkit/Swiggy `batch` upload (wrong `table`).
- Amazon uploader → 403 on `flipkart-grocery/*`.
- Amazon uploader → 403 on `master-sheet/*` (lacks `upload.master.use`).
- Legacy `upload.use` user → 200 on all platforms (back-compat).
- `/api/auth/me` returns the correct `upload_platforms`.

**Frontend** (manual):
- Login as ABC → Upload Hub shows only Amazon tiles.
- Direct URL `/platform/swiggy/upload/secondary` → access denied/redirect.
- "Uploaders" hidden for a user with no upload permission.

## 11. Rollout steps

1. Add codes + groups + table→slug map to `accounts/catalog.py`.
2. Add `can_use_uploader` / `user_upload_slugs` to `accounts/permissions.py`.
3. Add `upload_platforms` to `/api/auth/me`.
4. Add the per-request slug check to every upload endpoint
   (`uploads/views.py`, `uploads/amazon_uploads.py`, `dashboard/views.py`).
5. Frontend: filter Upload Hub + add route guards + nav gating.
6. Seed/migrate on production; upgrade the legacy *Uploader* group.
7. Create real uploader accounts and assign groups.
8. Run the test matrix.

## 12. Acceptance criteria

- A user with only **"Amazon Uploader"** sees only Amazon uploaders and can
  upload only Amazon data.
- The same user gets **403** from any other platform's upload API (URL/payload
  tampering included).
- Adding a dataset to a platform in `datasetPlatformMatrix.js` automatically
  becomes available to that platform's uploader (no permission change needed).
- `master-sheet` / `ads-master` are reachable only with `upload.master.use`.
- Existing `upload.use` users keep working (treated as all-uploaders).
- Admin / superuser keep full access.

## 13. Open decisions (confirm before implementing)

1. **Scope code vs. reuse** — go with dedicated `upload.<slug>.use` (recommended)
   or reuse `platform.<slug>.access`? (Recommendation: dedicated.)
2. **Master data** — should `master-sheet` / `ads-master` be admin-only, or a
   shared `upload.master.use` that every uploader group also gets?
3. **table→slug map** — explicit dict (recommended) vs. prefix derivation.
4. **Legacy `upload.use`** — auto-upgrade to `upload.*.use`, or keep as the
   "all" alias indefinitely?
5. **Does an uploader-only user need a landing page?** (They have no dashboards
   — confirm the post-login redirect for upload-only users.)
