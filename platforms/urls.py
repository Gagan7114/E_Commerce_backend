from django.urls import path

from . import views

urlpatterns = [
    path("<slug:slug>/stats", views.platform_stats, name="platform-stats"),
    path("<slug:slug>/pos", views.platform_pos, name="platform-pos"),
    path("<slug:slug>/inventory-match", views.inventory_match, name="platform-inventory-match"),
]
