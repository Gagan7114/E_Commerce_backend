from django.urls import path

from .views import LoginView, RefreshView, me

urlpatterns = [
    path("login", LoginView.as_view(), name="auth-login"),
    path("refresh", RefreshView.as_view(), name="auth-refresh"),
    path("me", me, name="auth-me"),
]
