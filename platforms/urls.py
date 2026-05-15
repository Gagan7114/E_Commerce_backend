from django.urls import path

from . import views, monthly_targets

urlpatterns = [
    # Cross-platform dashboard — registered before the <slug:slug> routes so
    # "month-targets" isn't matched as a platform slug.
    path(
        "month-targets/dashboard",
        monthly_targets.month_targets_dashboard,
        name="month-targets-dashboard",
    ),

    path("<slug:slug>/stats", views.platform_stats, name="platform-stats"),
    path("<slug:slug>/pos", views.platform_pos, name="platform-pos"),
    path("<slug:slug>/inventory-match", views.inventory_match, name="platform-inventory-match"),
    path("<slug:slug>/soh-doh-dashboard", views.blinkit_soh_doh_dashboard, name="platform-blinkit-soh-doh-dashboard"),
    path("blinkit/drr-dashboard", views.blinkit_drr_dashboard, name="platform-blinkit-drr-dashboard"),
    path("zepto/drr-dashboard", views.zepto_drr_dashboard, name="platform-zepto-drr-dashboard"),
    path(
        "zepto/primary-dashboard",
        views.primary_dashboard,
        {"slug": "zepto"},
        name="platform-zepto-primary-dashboard",
    ),
    path("<slug:slug>/primary-dashboard", views.bigbasket_primary_dashboard, name="platform-primary-dashboard"),
    path("<slug:slug>/price-dashboard", views.amazon_price_dashboard, name="platform-amazon-price-dashboard"),
    path("<slug:slug>/comparison-dashboard", views.amazon_comparison_dashboard, name="platform-amazon-comparison-dashboard"),
    path("<slug:slug>/sec-dashboard", views.flipkart_grocery_sec_dashboard, name="platform-sec-dashboard"),
    path("<slug:slug>/sec-monthly-dashboard", views.flipkart_secondary_monthly_dashboard, name="platform-sec-monthly-dashboard"),
    path("<slug:slug>/sku-analysis-dashboard", views.sku_analysis_dashboard, name="platform-sku-analysis-dashboard"),
    path("<slug:slug>/drr-dashboard", views.flipkart_grocery_drr_dashboard, name="platform-drr-dashboard"),
    path("<slug:slug>/month-on-month-sale", views.flipkart_grocery_month_on_month_sale, name="platform-month-on-month-sale"),
    path("<slug:slug>/landing-rate", views.landing_rate_list, name="platform-landing-rate-list"),
    path("<slug:slug>/landing-rate/skus", views.landing_rate_skus, name="platform-landing-rate-skus"),
    path("<slug:slug>/landing-rate/skus/add", views.landing_rate_sku_add, name="platform-landing-rate-sku-add"),
    path("<slug:slug>/landing-rate/add", views.landing_rate_add, name="platform-landing-rate-add"),
    path("<slug:slug>/landing-rate/update", views.landing_rate_update, name="platform-landing-rate-update"),

    # Monthly Targets — per-platform.
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
