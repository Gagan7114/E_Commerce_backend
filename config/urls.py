from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/auth/", include("accounts.urls")),
    path("api/dashboard/", include("dashboard.urls")),
    path("api/platform/", include("platforms.urls")),
    path("api/sap/", include("sap.urls")),
    path("api/uploads/", include("uploads.urls")),
]

admin.site.site_header = "ECMS Operations"
admin.site.site_title = "ECMS Admin"
admin.site.index_title = "Operations Panel"
