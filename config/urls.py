from django.contrib import admin
from django.urls import include, path

from accounts.views import (
    generate_inventory_doh_notifications,
    inventory_doh_sku_detail,
    mark_all_read,
    mark_notification_read,
    notification_detail,
    notifications,
)
from uploads import amazon_uploads

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/auth/", include("accounts.urls")),
    path("api/dashboard/", include("dashboard.urls")),
    path("api/platform/", include("platforms.urls")),
    path("api/sap/", include("sap.urls")),
    path("api/upload/", include("uploads.urls")),
    path("api/uploads", amazon_uploads.uploads_collection, name="amazon-uploads"),
    path("api/uploads/<int:upload_id>", amazon_uploads.upload_detail, name="amazon-upload-detail"),
    path("api/reports/amazon-po/matrix", amazon_uploads.amazon_po_matrix, name="amazon-po-matrix"),
    path("api/reports/amazon-po", amazon_uploads.amazon_po_report, name="amazon-po-report"),
    path(
        "api/reports/amazon-po/filter-options",
        amazon_uploads.amazon_po_filter_options,
        name="amazon-po-filter-options",
    ),
    path("api/reports/amazon-po/summary", amazon_uploads.amazon_po_summary, name="amazon-po-summary"),
    path("api/reports/appointment", amazon_uploads.appointment_report, name="appointment-report"),
    path("api/reports/appointment/filter-options", amazon_uploads.appointment_filter_options, name="appointment-filter-options"),
    path("api/reports/appointment/summary", amazon_uploads.appointment_summary, name="appointment-summary"),
    path("api/master/products", amazon_uploads.product_master_lookup, name="product-master-lookup"),
    path("api/master/fcs", amazon_uploads.fc_master_lookup, name="fc-master-lookup"),
    path("api/notifications", notifications, name="notifications"),
    path("api/notifications/mark-all-read", mark_all_read, name="notifications-mark-read"),
    path("api/notifications/generate-inventory-doh", generate_inventory_doh_notifications, name="notifications-generate-inventory-doh"),
    path("api/notifications/<int:notification_id>", notification_detail, name="notification-detail"),
    path("api/notifications/<int:notification_id>/mark-read", mark_notification_read, name="notification-mark-read"),
    path("api/notifications/inventory-doh/<int:notification_id>", inventory_doh_sku_detail, name="notification-inventory-doh-detail"),
]

admin.site.site_header = "ECMS Operations"
admin.site.site_title = "ECMS Admin"
admin.site.index_title = "Operations Panel"
