from django.contrib import admin
from django.urls import include, path

from accounts.views import mark_all_read, notifications

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/auth/", include("accounts.urls")),
    path("api/dashboard/", include("dashboard.urls")),
    path("api/platform/", include("platforms.urls")),
    path("api/sap/", include("sap.urls")),
    path("api/upload/", include("uploads.urls")),
    path("api/notifications", notifications, name="notifications"),
    path("api/notifications/mark-all-read", mark_all_read, name="notifications-mark-read"),
]

admin.site.site_header = "ECMS Operations"
admin.site.site_title = "ECMS Admin"
admin.site.index_title = "Operations Panel"
