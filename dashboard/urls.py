from django.urls import path

from . import views

urlpatterns = [
    path("table-counts", views.table_counts, name="dashboard-table-counts"),
    path("table-count/<str:table_name>", views.table_count, name="dashboard-table-count"),
    path("table-columns/<str:table_name>", views.table_columns, name="dashboard-table-columns"),
    path("table-distinct/<str:table_name>/<str:column_name>", views.table_distinct_values, name="dashboard-table-distinct-values"),
    path("table-data/<str:table_name>", views.table_data, name="dashboard-table-data"),
    path("table-row/<str:table_name>", views.update_primary_manual_fields, name="dashboard-table-row-update"),
    path("table-rows/<str:table_name>", views.bulk_update_primary_manual_fields, name="dashboard-table-rows-update"),
    path("expiry-alerts/<str:table_name>", views.expiry_alerts, name="dashboard-expiry-alerts"),
    path("inventory-charts", views.inventory_charts, name="dashboard-inventory-charts"),
]
