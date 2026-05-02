from django.urls import path

from . import views

urlpatterns = [
    path("batch", views.batch_upload, name="upload-batch"),
    path("flipkart-grocery/schema", views.flipkart_grocery_upload_schema, name="flipkart-grocery-upload-schema"),
    path("flipkart-grocery/status", views.flipkart_grocery_upload_status, name="flipkart-grocery-upload-status"),
    path("flipkart-grocery/raw", views.flipkart_grocery_raw_upload, name="flipkart-grocery-raw-upload"),
    path("flipkart-grocery/master", views.fk_grocery_master_upload, name="flipkart-grocery-master-upload"),
    path("fk-grocery-master", views.fk_grocery_master_upload, name="fk-grocery-master-upload"),
]
