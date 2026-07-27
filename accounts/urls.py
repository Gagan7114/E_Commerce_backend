from django.urls import path
from rest_framework_simplejwt.views import TokenRefreshView

from .views import (
    LoginView,
    change_password,
    feature_flags,
    me,
    update_feature_flags,
    user_permissions,
)

urlpatterns = [
    path("login", LoginView.as_view(), name="auth-login"),
    # Exchanges a valid refresh token for a fresh access token (and, with
    # rotation enabled, a fresh refresh token) so the frontend can keep the
    # session alive without re-prompting for credentials.
    path("refresh", TokenRefreshView.as_view(), name="auth-refresh"),
    path("me", me, name="auth-me"),
    path("permissions", user_permissions, name="auth-permissions"),
    path("change-password", change_password, name="auth-change-password"),
    # Global feature switches: any signed-in user may READ the state, only
    # `profile_controls.manage` may change it.
    path("feature-flags", feature_flags, name="auth-feature-flags"),
    path(
        "feature-flags/update",
        update_feature_flags,
        name="auth-feature-flags-update",
    ),
]
