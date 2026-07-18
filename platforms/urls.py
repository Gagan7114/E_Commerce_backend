from django.urls import path

from . import views, monthly_targets, primary_monthly_targets, call_center_targets

urlpatterns = [
    # Cross-platform dashboard — registered before the <slug:slug> routes so
    # "month-targets" isn't matched as a platform slug.
    path(
        "ads-summary",
        views.marketing_ads_summary,
        name="marketing-ads-summary",
    ),
    path(
        "meta",
        views.meta_dashboard,
        name="marketing-meta",
    ),
    path(
        "month-targets/dashboard",
        monthly_targets.month_targets_dashboard,
        name="month-targets-dashboard",
    ),
    path(
        "month-targets/refresh",
        monthly_targets.month_targets_refresh_all,
        name="month-targets-refresh-all",
    ),
    path(
        "primary-month-targets/dashboard",
        primary_monthly_targets.primary_month_targets_dashboard,
        name="primary-month-targets-dashboard",
    ),
    path(
        "primary-month-targets/refresh",
        primary_monthly_targets.primary_month_targets_refresh_all,
        name="primary-month-targets-refresh-all",
    ),
    path(
        "primary-overview-total",
        views.primary_overview_total,
        name="platform-primary-overview-total",
    ),
    path(
        "primary-summary",
        views.primary_summary,
        name="platform-primary-summary",
    ),
    path(
        "primary-summary-version",
        views.primary_summary_version,
        name="platform-primary-summary-version",
    ),
    # Call Center monthly target (isolated single-target store). Registered
    # before the <slug:slug> routes so it isn't matched as a platform slug.
    path(
        "call-center-targets",
        call_center_targets.call_center_targets,
        name="call-center-targets",
    ),

    path("<slug:slug>/stats", views.platform_stats, name="platform-stats"),
    path("<slug:slug>/pos", views.platform_pos, name="platform-pos"),
    path("<slug:slug>/inventory-match", views.inventory_match, name="platform-inventory-match"),
    path("<slug:slug>/soh-doh-dashboard", views.blinkit_soh_doh_dashboard, name="platform-blinkit-soh-doh-dashboard"),
    path("swiggy/region-doh-dashboard", views.swiggy_region_doh_dashboard, name="platform-swiggy-region-doh-dashboard"),
    path("zepto/region-doh-dashboard", views.zepto_region_doh_dashboard, name="platform-zepto-region-doh-dashboard"),
    path("<slug:slug>/pendency-dashboard", views.pendency_dashboard, name="platform-pendency-dashboard"),
    path("blinkit/drr-dashboard", views.blinkit_drr_dashboard, name="platform-blinkit-drr-dashboard"),
    path("zepto/drr-dashboard", views.zepto_drr_dashboard, name="platform-zepto-drr-dashboard"),
    path("swiggy/drr-dashboard", views.swiggy_drr_dashboard, name="platform-swiggy-drr-dashboard"),
    path("bigbasket/drr-dashboard", views.bigbasket_drr_dashboard, name="platform-bigbasket-drr-dashboard"),
    path(
        "bigbasket/range-dashboard",
        views.bigbasket_range_dashboard,
        {"slug": "bigbasket"},
        name="platform-bigbasket-range-dashboard",
    ),
    path(
        "zepto/primary-dashboard",
        views.primary_dashboard,
        {"slug": "zepto"},
        name="platform-zepto-primary-dashboard",
    ),
    path(
        "bigbasket/primary-dashboard",
        views.bigbasket_primary_dashboard,
        {"slug": "bigbasket"},
        name="platform-bigbasket-primary-dashboard",
    ),
    path("<slug:slug>/primary-dashboard", views.primary_dashboard, name="platform-primary-dashboard"),
    path("<slug:slug>/price-dashboard", views.amazon_price_dashboard, name="platform-amazon-price-dashboard"),
    path("<slug:slug>/ads-dashboard", views.amazon_ads_dashboard, name="platform-amazon-ads-dashboard"),
    path("<slug:slug>/ads-total-sales", views.amazon_ads_total_sales, name="platform-amazon-ads-total-sales"),
    path("<slug:slug>/swiggy-ads-dashboard", views.swiggy_ads_dashboard, name="platform-swiggy-ads-dashboard"),
    path("<slug:slug>/zepto-ads-dashboard", views.zepto_ads_dashboard, name="platform-zepto-ads-dashboard"),
    path("<slug:slug>/bigbasket-ads-dashboard", views.bigbasket_ads_dashboard, name="platform-bigbasket-ads-dashboard"),
    path("<slug:slug>/swiggy-ads-daily-dashboard", views.swiggy_ads_daily_dashboard, name="platform-swiggy-ads-daily-dashboard"),
    path("<slug:slug>/zepto-ads-daily-dashboard", views.zepto_ads_daily_dashboard, name="platform-zepto-ads-daily-dashboard"),
    path("<slug:slug>/bigbasket-ads-daily-dashboard", views.bigbasket_ads_daily_dashboard, name="platform-bigbasket-ads-daily-dashboard"),
    path("<slug:slug>/blinkit-ads-dashboard", views.blinkit_ads_dashboard, name="platform-blinkit-ads-dashboard"),
    path("<slug:slug>/flipkart-ads-dashboard", views.flipkart_ads_dashboard, name="platform-flipkart-ads-dashboard"),
    path("<slug:slug>/flipkart-fsn-dashboard", views.flipkart_fsn_dashboard, name="platform-flipkart-fsn-dashboard"),
    path("<slug:slug>/blinkit-brandfund-dashboard", views.blinkit_brandfund_dashboard, name="platform-blinkit-brandfund-dashboard"),
    path("<slug:slug>/swiggy-brandfund-dashboard", views.swiggy_brandfund_dashboard, name="platform-swiggy-brandfund-dashboard"),
    path("<slug:slug>/zepto-brandfund-dashboard", views.zepto_brandfund_dashboard, name="platform-zepto-brandfund-dashboard"),
    path("<slug:slug>/comparison-dashboard", views.amazon_comparison_dashboard, name="platform-amazon-comparison-dashboard"),
    path("<slug:slug>/sec-dashboard", views.flipkart_grocery_sec_dashboard, name="platform-sec-dashboard"),
    path("<slug:slug>/sec-dashboard-years", views.sec_dashboard_years, name="platform-sec-dashboard-years"),
    path("<slug:slug>/mp-dashboard", views.amazon_mp_dashboard, name="platform-amazon-mp-dashboard"),
    path("<slug:slug>/mp-dashboard-version", views.amazon_mp_dashboard_version, name="platform-amazon-mp-dashboard-version"),
    path("<slug:slug>/coupon-dashboard", views.amazon_coupon_dashboard, name="platform-amazon-coupon-dashboard"),
    path("<slug:slug>/sec-monthly-dashboard", views.flipkart_secondary_monthly_dashboard, name="platform-sec-monthly-dashboard"),
    path("<slug:slug>/sku-analysis-dashboard", views.sku_analysis_dashboard, name="platform-sku-analysis-dashboard"),
    path("<slug:slug>/drr-dashboard", views.flipkart_grocery_drr_dashboard, name="platform-drr-dashboard"),
    path("<slug:slug>/landing-rate", views.landing_rate_list, name="platform-landing-rate-list"),
    path("<slug:slug>/landing-rate/skus", views.landing_rate_skus, name="platform-landing-rate-skus"),
    path("<slug:slug>/landing-rate/skus/add", views.landing_rate_sku_add, name="platform-landing-rate-sku-add"),
    path("<slug:slug>/landing-rate/add", views.landing_rate_add, name="platform-landing-rate-add"),
    path("<slug:slug>/landing-rate/update", views.landing_rate_update, name="platform-landing-rate-update"),
    path("<slug:slug>/landing-rate/preview", views.landing_rate_bulk_preview, name="platform-landing-rate-bulk-preview"),
    path("<slug:slug>/landing-rate/bulk-upsert", views.landing_rate_bulk_upsert, name="platform-landing-rate-bulk-upsert"),

    # Monthly Targets — per-platform.
    path(
        "<slug:slug>/primary-month-targets",
        primary_monthly_targets.primary_month_targets_list,
        name="platform-primary-month-targets-list",
    ),
    path(
        "<slug:slug>/primary-month-targets/add",
        primary_monthly_targets.primary_month_targets_create,
        name="platform-primary-month-targets-add",
    ),
    path(
        "<slug:slug>/primary-month-targets/refresh",
        primary_monthly_targets.primary_month_targets_refresh_platform,
        name="platform-primary-month-targets-refresh-platform",
    ),
    path(
        "<slug:slug>/primary-month-targets/<int:row_id>/update",
        primary_monthly_targets.primary_month_targets_update,
        name="platform-primary-month-targets-update",
    ),
    path(
        "<slug:slug>/month-targets",
        monthly_targets.month_targets_list,
        name="platform-month-targets-list",
    ),
    path(
        "<slug:slug>/month-targets/add",
        monthly_targets.month_targets_create,
        name="platform-month-targets-add",
    ),
    path(
        "<slug:slug>/month-targets/refresh",
        monthly_targets.month_targets_refresh_platform,
        name="platform-month-targets-refresh-platform",
    ),
    path(
        "<slug:slug>/month-targets/<int:row_id>/refresh",
        monthly_targets.month_targets_refresh,
        name="platform-month-targets-refresh",
    ),
    path(
        "<slug:slug>/month-targets/<int:row_id>/update",
        monthly_targets.month_targets_update,
        name="platform-month-targets-update",
    ),
]
