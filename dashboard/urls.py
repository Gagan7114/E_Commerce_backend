from django.urls import path

from . import views

urlpatterns = [
    path("table-counts", views.table_counts, name="dashboard-table-counts"),
    path("latest-month", views.latest_month, name="dashboard-latest-month"),
    path("table-count/<str:table_name>", views.table_count, name="dashboard-table-count"),
    path("table-columns/<str:table_name>", views.table_columns, name="dashboard-table-columns"),
    path("table-distinct/<str:table_name>/<str:column_name>", views.table_distinct_values, name="dashboard-table-distinct-values"),
    path("table-data/<str:table_name>", views.table_data, name="dashboard-table-data"),
    path("table-row/<str:table_name>", views.update_primary_manual_fields, name="dashboard-table-row-update"),
    path("table-rows/<str:table_name>", views.bulk_update_primary_manual_fields, name="dashboard-table-rows-update"),
    path("expiry-alerts/<str:table_name>", views.expiry_alerts, name="dashboard-expiry-alerts"),
    path("inventory-charts", views.inventory_charts, name="dashboard-inventory-charts"),
    path("primary-po-litres", views.primary_po_litres, name="dashboard-primary-po-litres"),
    path("category-litres", views.category_litres, name="dashboard-category-litres"),
    path("category-breakdown", views.category_breakdown, name="dashboard-category-breakdown"),
    path("category-platform-breakdown", views.category_platform_breakdown, name="dashboard-category-platform-breakdown"),
    path("category-sku-breakdown", views.category_sku_breakdown, name="dashboard-category-sku-breakdown"),
    path("category-trend", views.category_trend, name="dashboard-category-trend"),
    path("state-sales", views.state_sales, name="dashboard-state-sales"),
    path("state-sales/detail", views.state_sales_detail, name="dashboard-state-sales-detail"),
    path("secondary-yoy-growth", views.secondary_yoy_growth, name="dashboard-secondary-yoy-growth"),
    path("fulfilment-health", views.fulfilment_health, name="dashboard-fulfilment-health"),
    path("top-skus", views.top_skus, name="dashboard-top-skus"),
    path("platform-expiry-alerts", views.platform_expiry_alerts, name="dashboard-platform-expiry-alerts"),
    path("platform-expiry-alerts/<str:slug>/pos", views.platform_expiry_alert_pos, name="dashboard-platform-expiry-alert-pos"),
    path("platform-expiry-alerts/<str:slug>/pos/<str:po_number>/items", views.platform_expiry_alert_po_items, name="dashboard-platform-expiry-alert-po-items"),
]
