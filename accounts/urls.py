from django.urls import path

from .views import LoginView, RegisterView, me

urlpatterns = [
    path("login", LoginView.as_view(), name="auth-login"),
    path("register", RegisterView.as_view(), name="auth-register"),
    path("me", me, name="auth-me"),
]
