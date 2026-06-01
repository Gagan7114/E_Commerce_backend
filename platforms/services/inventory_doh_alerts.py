from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Iterable

from django.db import IntegrityError
from django.utils import timezone

from platforms.views import (
    _amazon_soh_month_name,
    _dict_rows,
    _inventory_dashboard_platform,
    _num,
    _parse_price_upload_date,
    _safe_div,
    _scalar,
    _sec_elapsed_day,
    _secmaster_inventory_date_expr,
)


ALERT_TYPE = "INVENTORY_DOH_LOW"
DEFAULT_THRESHOLD = 5.0


@dataclass(frozen=True)
class PlatformAlertConfig:
    slug: str
    label: str
    inventory_format: str
    sales_format: str


PLATFORM_CONFIGS = {
    "blinkit": PlatformAlertConfig("blinkit", "Blinkit", "BLINKIT", "blinkit"),
    "zepto": PlatformAlertConfig("zepto", "Zepto", "ZEPTO", "zepto"),
    "swiggy": PlatformAlertConfig("swiggy", "Swiggy", "SWIGGY", "swiggy"),
    "bigbasket": PlatformAlertConfig("bigbasket", "BigBasket", "BIG BASKET", "bigbasket"),
}


def _clean(value) -> str:
    return str(value or "").strip()


def _decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def _normalize_slug(raw_value: str | None) -> str | None:
    value = _clean(raw_value).lower().replace("_", "-")
    if not value:
        return None
    if value in {"all", "*"}:
        return None
    value = value.replace("big-basket", "bigbasket")
    return value


def _severity(doh: float) -> str:
    return "critical" if doh < 5 else "warning"


def _title(format_name: str, sku_code: str, item: str, doh: float) -> str:
    label = _clean(item) or f"SKU {_clean(sku_code)}"
    title = f"{format_name} {label} DOH {doh:.2f}"
    return title[:255]


def _message(format_name: str, sku_code: str, item: str, doh: float, threshold: float) -> str:
    label = _clean(item) or f"SKU {_clean(sku_code)}"
    return (
        f"{format_name} {label} has DOH {doh:.2f}, "
        f"below threshold {threshold:g}."
    )


def _date_iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def _notification_payload(row: dict, threshold: float) -> dict:
    return {
        "alert_type": ALERT_TYPE,
        "format": row["format"],
        "platform_slug": row["platform_slug"],
        "sku_code": row["sku_code"],
        "sku_name": row.get("sku_name") or "",
        "item": row.get("item") or "",
        "item_head": row.get("item_head") or "",
        "category": row.get("category") or "",
        "sub_category": row.get("sub_category") or "",
        "brand": row.get("brand") or "",
        "inventory_date": _date_iso(row.get("inventory_date")),
        "sales_max_date": _date_iso(row.get("sales_max_date")),
        "month_start": _date_iso(row.get("month_start")),
        "units_sold": _num(row.get("units_sold")),
        "ltr_sold": _num(row.get("ltr_sold")),
        "soh_units": _num(row.get("soh_units")),
        "soh_ltr": _num(row.get("soh_ltr")),
        "drr_units": _num(row.get("drr_units")),
        "drr_ltr": _num(row.get("drr_ltr")),
        "doh": _num(row.get("doh")),
        "threshold": threshold,
    }


def _active_formats(platform_slug: str | None) -> list[str]:
    slug = _normalize_slug(platform_slug)
    if slug == "amazon":
        return ["AMAZON"]
    if slug:
        config = PLATFORM_CONFIGS.get(slug)
        return [config.inventory_format] if config else []
    return [config.inventory_format for config in PLATFORM_CONFIGS.values()] + ["AMAZON"]


@lru_cache(maxsize=512)
def _amazon_item_for_asin(asin: str) -> str:
    asin = _clean(asin)
    if not asin:
        return ""
    item = _clean(_scalar(
        """
        SELECT NULLIF(TRIM(item::text), '')
        FROM master_sheet
        WHERE UPPER(TRIM(format_sku_code::text)) = UPPER(TRIM(%s))
          AND NULLIF(TRIM(item::text), '') IS NOT NULL
        ORDER BY
            CASE
                WHEN REGEXP_REPLACE(LOWER(TRIM(COALESCE(format, '')::text)), '[^a-z0-9]+', '', 'g') = 'amazon'
                    THEN 0
                ELSE 1
            END,
            COALESCE(item_head, ''),
            COALESCE(category, ''),
            COALESCE(product_name, '')
        LIMIT 1
        """,
        [asin],
    ))
    if item:
        return item
    return _clean(_scalar(
        """
        SELECT NULLIF(TRIM(item::text), '')
        FROM amazon_sec_range_master_view
        WHERE UPPER(TRIM(asin::text)) = UPPER(TRIM(%s))
          AND NULLIF(TRIM(item::text), '') IS NOT NULL
        ORDER BY "to_date" DESC NULLS LAST
        LIMIT 1
        """,
        [asin],
    ))


def _platforms_to_scan(platform_slug: str | None) -> list[PlatformAlertConfig]:
    slug = _normalize_slug(platform_slug)
    if slug:
        config = PLATFORM_CONFIGS.get(slug)
        return [config] if config else []
    return list(PLATFORM_CONFIGS.values())


def _all_platform_low_doh_rows(
    config: PlatformAlertConfig,
    *,
    threshold: float,
    requested_date=None,
) -> list[dict]:
    sale_date_expr = _secmaster_inventory_date_expr(config.slug)
    effective_date = requested_date
    if effective_date is None:
        effective_date = _scalar(
            """
            SELECT MAX(inventory_date)
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
            """,
            [config.inventory_format],
        )
    else:
        effective_date = _scalar(
            """
            SELECT MAX(inventory_date)
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
              AND inventory_date <= %s
            """,
            [config.inventory_format, effective_date],
        )

    if effective_date is None:
        return []

    month_start = effective_date.replace(day=1)
    max_sales_date = _scalar(
        f"""
        SELECT MAX({sale_date_expr})
        FROM "SecMaster"
        WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
          AND ({sale_date_expr}) >= %s
          AND ({sale_date_expr}) <= %s
          AND ({sale_date_expr}) IS NOT NULL
        """,
        [config.sales_format, month_start, effective_date],
    )
    elapsed_day = _sec_elapsed_day(max_sales_date)
    if elapsed_day <= 0:
        return []

    rows = _dict_rows(
        f"""
        WITH sales AS (
            SELECT
                UPPER(TRIM(COALESCE("item"::text, ''))) AS item_key,
                COALESCE(SUM("quantity"), 0)::numeric AS units_sold,
                COALESCE(SUM("ltr_sold"), 0)::numeric AS ltr_sold
            FROM "SecMaster"
            WHERE REGEXP_REPLACE(LOWER(TRIM("format"::text)), '[^a-z0-9]+', '', 'g') = %s
              AND ({sale_date_expr}) >= %s
              AND ({sale_date_expr}) <= %s
            GROUP BY UPPER(TRIM(COALESCE("item"::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(item::text, ''))) AS item_key,
                MIN(NULLIF(TRIM(sku_code::text), '')) AS sku_code,
                MIN(NULLIF(TRIM(item::text), '')) AS item,
                MIN(NULLIF(TRIM(item_head::text), '')) AS item_head,
                MIN(NULLIF(TRIM(brand::text), '')) AS brand,
                COALESCE(SUM(soh_unit), 0)::numeric AS soh_units,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM all_platform_inventory
            WHERE UPPER(TRIM(format::text)) = %s
              AND inventory_date = %s
              AND NULLIF(TRIM(COALESCE(sku_code::text, '')), '') IS NOT NULL
            GROUP BY
                UPPER(TRIM(COALESCE(sku_code::text, ''))),
                UPPER(TRIM(COALESCE(item::text, '')))
        )
        SELECT
            i.sku_code,
            i.item,
            i.item AS sku_name,
            i.item_head,
            i.brand,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            i.soh_units,
            i.soh_ltr
        FROM inventory i
        LEFT JOIN sales s ON s.item_key = i.item_key
        WHERE i.sku_code IS NOT NULL
        """,
        [
            config.sales_format,
            month_start,
            max_sales_date,
            config.inventory_format,
            effective_date,
        ],
    )

    low_rows = []
    for row in rows:
        units_sold = _num(row.get("units_sold"))
        ltr_sold = _num(row.get("ltr_sold"))
        soh_units = _num(row.get("soh_units"))
        soh_ltr = _num(row.get("soh_ltr"))
        drr_units = _safe_div(units_sold, elapsed_day)
        if drr_units <= 0:
            continue
        drr_ltr = _safe_div(ltr_sold, elapsed_day)
        doh = _safe_div(soh_units, drr_units)
        if doh >= threshold:
            continue
        low_rows.append({
            "format": config.inventory_format,
            "platform_slug": config.slug,
            "platform_label": config.label,
            "sku_code": _clean(row.get("sku_code")),
            "sku_name": _clean(row.get("sku_name") or row.get("item")),
            "item": _clean(row.get("item")),
            "item_head": _clean(row.get("item_head")),
            "category": "",
            "sub_category": "",
            "brand": _clean(row.get("brand")),
            "inventory_date": effective_date,
            "sales_max_date": max_sales_date,
            "month_start": month_start,
            "units_sold": units_sold,
            "ltr_sold": ltr_sold,
            "soh_units": soh_units,
            "soh_ltr": soh_ltr,
            "drr_units": drr_units,
            "drr_ltr": drr_ltr,
            "doh": doh,
        })
    return low_rows


def _amazon_low_doh_rows(*, threshold: float, requested_date=None) -> list[dict]:
    effective_date = requested_date
    if effective_date is None:
        effective_date = _scalar(
            """
            SELECT MAX(inventory_date)
            FROM amazon_master_inventory
            WHERE inventory_date IS NOT NULL
            """,
            [],
        )
    else:
        effective_date = _scalar(
            """
            SELECT MAX(inventory_date)
            FROM amazon_master_inventory
            WHERE inventory_date <= %s
            """,
            [effective_date],
        )
    if effective_date is None:
        return []

    month_name = _amazon_soh_month_name(effective_date.month)
    year = effective_date.year
    month_day = f"{effective_date.day:02d}-{month_name}"
    month_start = effective_date.replace(day=1)
    elapsed_day = max(1, effective_date.day)

    rows = _dict_rows(
        """
        WITH row_list AS (
            SELECT
                UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                MIN(NULLIF(TRIM(asin::text), '')) AS asin,
                MIN(NULLIF(TRIM(item_head::text), '')) AS item_head,
                MIN(NULLIF(TRIM(category::text), '')) AS category,
                MIN(NULLIF(TRIM(sub_category::text), '')) AS sub_category,
                MIN(NULLIF(TRIM(brand_2::text), '')) AS brand
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND inventory_date = %s
              AND NULLIF(TRIM(COALESCE(asin::text, '')), '') IS NOT NULL
            GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
        ),
        sales AS (
            SELECT
                UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                MIN(NULLIF(TRIM(item::text), '')) AS item,
                COALESCE(SUM(shipped_units), 0)::numeric AS units_sold,
                COALESCE(SUM(shipped_litres), 0)::numeric AS ltr_sold
            FROM amazon_sec_range_master_view
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND UPPER(TRIM(month_day::text)) = %s
            GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
        ),
        inventory AS (
            SELECT
                UPPER(TRIM(COALESCE(asin::text, ''))) AS asin_key,
                COALESCE(SUM(sellable_on_hand_units), 0)::numeric AS soh_unit,
                COALESCE(SUM(soh_ltr), 0)::numeric AS soh_ltr
            FROM amazon_master_inventory
            WHERE "year" = %s
              AND UPPER(TRIM("month"::text)) = %s
              AND inventory_date = %s
            GROUP BY UPPER(TRIM(COALESCE(asin::text, '')))
        )
        SELECT
            r.asin AS sku_code,
            COALESCE(s.item, '') AS sku_name,
            COALESCE(s.item, '') AS item,
            r.item_head,
            r.category,
            r.sub_category,
            r.brand,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.ltr_sold, 0) AS ltr_sold,
            COALESCE(i.soh_unit, 0) AS soh_units,
            COALESCE(i.soh_ltr, 0) AS soh_ltr
        FROM row_list r
        LEFT JOIN sales s ON s.asin_key = r.asin_key
        LEFT JOIN inventory i ON i.asin_key = r.asin_key
        WHERE r.asin IS NOT NULL
        """,
        [
            year,
            month_name,
            effective_date,
            year,
            month_name,
            month_day,
            year,
            month_name,
            effective_date,
        ],
    )

    low_rows = []
    for row in rows:
        units_sold = _num(row.get("units_sold"))
        ltr_sold = _num(row.get("ltr_sold"))
        soh_units = _num(row.get("soh_units"))
        soh_ltr = _num(row.get("soh_ltr"))
        drr_units = _safe_div(units_sold, elapsed_day)
        drr_ltr = _safe_div(ltr_sold, elapsed_day)
        doh = (_safe_div(soh_units, drr_units) - 2) if drr_units else 0.0
        if doh >= threshold:
            continue
        item = _clean(row.get("item")) or _amazon_item_for_asin(row.get("sku_code"))
        low_rows.append({
            "format": "AMAZON",
            "platform_slug": "amazon",
            "platform_label": "Amazon",
            "sku_code": _clean(row.get("sku_code")),
            "sku_name": item,
            "item": item,
            "item_head": _clean(row.get("item_head")),
            "category": _clean(row.get("category")),
            "sub_category": _clean(row.get("sub_category")),
            "brand": _clean(row.get("brand")),
            "inventory_date": effective_date,
            "sales_max_date": effective_date,
            "month_start": month_start,
            "units_sold": units_sold,
            "ltr_sold": ltr_sold,
            "soh_units": soh_units,
            "soh_ltr": soh_ltr,
            "drr_units": drr_units,
            "drr_ltr": drr_ltr,
            "doh": doh,
        })
    return low_rows


def find_low_doh_rows(
    *,
    threshold: float = DEFAULT_THRESHOLD,
    platform_slug: str | None = None,
    date_value=None,
) -> list[dict]:
    requested_date = _parse_price_upload_date(date_value) if date_value else None
    rows: list[dict] = []
    for config in _platforms_to_scan(platform_slug):
        rows.extend(
            _all_platform_low_doh_rows(
                config,
                threshold=threshold,
                requested_date=requested_date,
            )
        )
    slug = _normalize_slug(platform_slug)
    if slug in {None, "amazon"}:
        rows.extend(_amazon_low_doh_rows(threshold=threshold, requested_date=requested_date))
    return rows


def _resolve_missing_notifications(*, formats: Iterable[str], active_keys: set[tuple]) -> int:
    from accounts.models import InventoryDohNotification

    now = timezone.now()
    resolved = 0
    queryset = InventoryDohNotification.objects.filter(
        alert_type=ALERT_TYPE,
        resolved_at__isnull=True,
        format__in=list(formats),
    )
    for notification in queryset.iterator():
        key = (notification.format, notification.sku_code, notification.inventory_date)
        if key in active_keys:
            continue
        notification.resolved_at = now
        notification.save(update_fields=["resolved_at", "updated_at"])
        resolved += 1
    return resolved


def upsert_low_doh_notifications(
    *,
    threshold: float = DEFAULT_THRESHOLD,
    platform_slug: str | None = None,
    date_value=None,
    send_firebase: bool = True,
) -> dict:
    from accounts.models import InventoryDohNotification

    rows = find_low_doh_rows(
        threshold=threshold,
        platform_slug=platform_slug,
        date_value=date_value,
    )
    now = timezone.now()
    created = 0
    updated = 0
    failed = 0
    firebase_results = []
    active_keys = set()

    for row in rows:
        sku_code = _clean(row.get("sku_code"))
        inventory_date = row.get("inventory_date")
        if not sku_code or inventory_date is None:
            continue
        format_name = row["format"]
        active_keys.add((format_name, sku_code, inventory_date))
        doh = _num(row.get("doh"))
        defaults = {
            "platform_slug": row.get("platform_slug") or "",
            "sku_name": row.get("sku_name") or "",
            "item": row.get("item") or "",
            "item_head": row.get("item_head") or "",
            "category": row.get("category") or "",
            "sub_category": row.get("sub_category") or "",
            "brand": row.get("brand") or "",
            "sales_max_date": row.get("sales_max_date"),
            "month_start": row.get("month_start"),
            "units_sold": _decimal(row.get("units_sold")),
            "ltr_sold": _decimal(row.get("ltr_sold")),
            "soh_units": _decimal(row.get("soh_units")),
            "soh_ltr": _decimal(row.get("soh_ltr")),
            "drr_units": _decimal(row.get("drr_units")),
            "drr_ltr": _decimal(row.get("drr_ltr")),
            "doh": _decimal(doh),
            "threshold": _decimal(threshold),
            "severity": _severity(doh),
            "title": _title(format_name, sku_code, row.get("item") or "", doh),
            "message": _message(format_name, sku_code, row.get("item") or "", doh, threshold),
            "payload": _notification_payload(row, threshold),
            "resolved_at": None,
            "last_seen_at": now,
        }
        try:
            notification, was_created = InventoryDohNotification.objects.update_or_create(
                alert_type=ALERT_TYPE,
                format=format_name,
                sku_code=sku_code,
                inventory_date=inventory_date,
                defaults=defaults,
            )
        except IntegrityError:
            failed += 1
            continue
        if was_created:
            created += 1
            if send_firebase:
                try:
                    from accounts.firebase import send_inventory_doh_notification

                    firebase_results.append(send_inventory_doh_notification(notification))
                except Exception as exc:  # pragma: no cover - defensive around optional FCM
                    firebase_results.append({"sent": False, "reason": str(exc)})
        else:
            updated += 1

    formats = _active_formats(platform_slug)
    resolved = _resolve_missing_notifications(formats=formats, active_keys=active_keys)
    return {
        "threshold": threshold,
        "scanned_formats": formats,
        "low_doh_rows": len(rows),
        "created": created,
        "updated": updated,
        "resolved": resolved,
        "failed": failed,
        "firebase": firebase_results,
    }


def notification_to_payload(notification) -> dict:
    item = notification.item
    if _clean(notification.format).upper() == "AMAZON":
        item = _amazon_item_for_asin(notification.sku_code) or item
    return {
        "id": notification.id,
        "type": notification.alert_type,
        "title": _title(
            notification.format,
            notification.sku_code,
            item,
            _num(notification.doh),
        ),
        "message": _message(
            notification.format,
            notification.sku_code,
            item,
            _num(notification.doh),
            _num(notification.threshold),
        ),
        "format": notification.format,
        "platform_slug": notification.platform_slug,
        "sku_code": notification.sku_code,
        "sku_name": notification.sku_name or item,
        "item": item,
        "item_head": notification.item_head,
        "category": notification.category,
        "sub_category": notification.sub_category,
        "brand": notification.brand,
        "inventory_date": _date_iso(notification.inventory_date),
        "sales_max_date": _date_iso(notification.sales_max_date),
        "month_start": _date_iso(notification.month_start),
        "units_sold": _num(notification.units_sold),
        "ltr_sold": _num(notification.ltr_sold),
        "soh_units": _num(notification.soh_units),
        "soh_ltr": _num(notification.soh_ltr),
        "drr_units": _num(notification.drr_units),
        "drr_ltr": _num(notification.drr_ltr),
        "doh": _num(notification.doh),
        "threshold": _num(notification.threshold),
        "severity": notification.severity,
        "read": notification.is_read,
        "is_read": notification.is_read,
        "active": notification.resolved_at is None,
        "resolved_at": _date_iso(notification.resolved_at),
        "first_seen_at": _date_iso(notification.first_seen_at),
        "last_seen_at": _date_iso(notification.last_seen_at),
        "created_at": _date_iso(notification.created_at),
        "link": f"/notifications/inventory-doh/{notification.id}",
        "payload": notification.payload or {},
    }
