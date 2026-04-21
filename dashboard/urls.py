from django.urls import path

from . import views

urlpatterns = [
    path("table-counts", views.table_counts, name="dashboard-table-counts"),
    path("inventory-chart", views.inventory_chart, name="dashboard-inventory-chart"),
    path("expiry-alerts", views.expiry_alerts, name="dashboard-expiry-alerts"),
]
