from django.urls import path

from . import views

urlpatterns = [
    path("distributors", views.sap_distributors, name="sap-distributors"),
    path("invoices", views.sap_invoices, name="sap-invoices"),
]
