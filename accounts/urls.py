from django.urls import path

from .views import LoginView, RegisterView, change_password, me, user_permissions

urlpatterns = [
    path("login", LoginView.as_view(), name="auth-login"),
    path("register", RegisterView.as_view(), name="auth-register"),
    path("me", me, name="auth-me"),
    path("permissions", user_permissions, name="auth-permissions"),
    path("change-password", change_password, name="auth-change-password"),
]
