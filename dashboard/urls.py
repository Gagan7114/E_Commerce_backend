from django.urls import path

from . import views

urlpatterns = [
    path("table-counts", views.table_counts, name="dashboard-table-counts"),
    path("table-count/<str:table_name>", views.table_count, name="dashboard-table-count"),
    path("table-columns/<str:table_name>", views.table_columns, name="dashboard-table-columns"),
    path("table-data/<str:table_name>", views.table_data, name="dashboard-table-data"),
    path("expiry-alerts/<str:table_name>", views.expiry_alerts, name="dashboard-expiry-alerts"),
    path("inventory-charts", views.inventory_charts, name="dashboard-inventory-charts"),
]
