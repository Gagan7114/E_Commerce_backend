from django.urls import path

from .views import LoginView, change_password, me, user_permissions

urlpatterns = [
    path("login", LoginView.as_view(), name="auth-login"),
    path("me", me, name="auth-me"),
    path("permissions", user_permissions, name="auth-permissions"),
    path("change-password", change_password, name="auth-change-password"),
]
