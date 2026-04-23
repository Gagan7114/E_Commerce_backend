from django.urls import path

from . import views

urlpatterns = [
    path("<slug:slug>/stats", views.platform_stats, name="platform-stats"),
    path("<slug:slug>/pos", views.platform_pos, name="platform-pos"),
    path("<slug:slug>/inventory-match", views.inventory_match, name="platform-inventory-match"),
    path("<slug:slug>/landing-rate", views.landing_rate_list, name="platform-landing-rate-list"),
    path("<slug:slug>/landing-rate/skus", views.landing_rate_skus, name="platform-landing-rate-skus"),
    path("<slug:slug>/landing-rate/add", views.landing_rate_add, name="platform-landing-rate-add"),
]
