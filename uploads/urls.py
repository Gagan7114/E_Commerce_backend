from django.urls import path

from . import views

urlpatterns = [
    path("batch", views.batch_upload, name="upload-batch"),
    path("master-sheet", views.master_sheet_list, name="master-sheet-list"),
    path("master-sheet/add", views.master_sheet_create, name="master-sheet-add"),
    path("master-sheet/preview", views.master_sheet_bulk_preview, name="master-sheet-preview"),
    path("master-sheet/bulk-upsert", views.master_sheet_bulk_upsert, name="master-sheet-bulk-upsert"),
    path("master-sheet/update", views.master_sheet_update, name="master-sheet-update"),
    path("master-sheet/delete", views.master_sheet_delete, name="master-sheet-delete"),
    path("flipkart-grocery/schema", views.flipkart_grocery_upload_schema, name="flipkart-grocery-upload-schema"),
    path("flipkart-grocery/status", views.flipkart_grocery_upload_status, name="flipkart-grocery-upload-status"),
    path("flipkart-grocery/raw", views.flipkart_grocery_raw_upload, name="flipkart-grocery-raw-upload"),
    path("flipkart-grocery/master", views.fk_grocery_master_upload, name="flipkart-grocery-master-upload"),
    path("fk-grocery-master", views.fk_grocery_master_upload, name="fk-grocery-master-upload"),
]
