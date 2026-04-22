from django.urls import path

from . import views

urlpatterns = [
    path("distributors", views.distributors, name="sap-distributors"),
    path("distributors/<str:card_code>", views.distributor_detail, name="sap-distributor-detail"),
    path("distributor-orders/<str:card_code>", views.distributor_orders, name="sap-distributor-orders"),
    path("distributor-invoices/<str:card_code>", views.distributor_invoices, name="sap-distributor-invoices"),
    path("items", views.items, name="sap-items"),
    path("stock-by-warehouse", views.stock_by_warehouse, name="sap-stock-by-warehouse"),
    path("sales-invoices", views.sales_invoices, name="sap-sales-invoices"),
    path("sales-invoices/<str:card_code>", views.customer_sales_invoices, name="sap-customer-sales-invoices"),
    path("sales-invoice-lines/<int:doc_entry>", views.sales_invoice_lines, name="sap-sales-invoice-lines"),
    path("platform-distributors/<slug:slug>", views.platform_distributors, name="sap-platform-distributors"),
    path("platform-distributors/<slug:slug>/<str:card_code>", views.platform_distributor_detail, name="sap-platform-distributor-detail"),
    path("platform-sales-invoices/<slug:slug>", views.platform_sales_invoices, name="sap-platform-sales-invoices"),
]
