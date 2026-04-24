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
    path("<slug:slug>/landing-rate", views.landing_rate_list, name="platform-landing-rate-list"),
    path("<slug:slug>/landing-rate/skus", views.landing_rate_skus, name="platform-landing-rate-skus"),
    path("<slug:slug>/landing-rate/add", views.landing_rate_add, name="platform-landing-rate-add"),

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
