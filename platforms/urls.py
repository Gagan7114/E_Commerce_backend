from django.urls import path

from . import views

urlpatterns = [
    path("<slug:slug>/stats", views.platform_stats, name="platform-stats"),
    path("<slug:slug>/pos", views.platform_pos, name="platform-pos"),
    path("<slug:slug>/inventory", views.platform_inventory, name="platform-inventory"),
    path("<slug:slug>/secondary-sales", views.platform_secondary, name="platform-secondary"),
]
