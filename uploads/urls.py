from django.urls import path

from . import views

urlpatterns = [
    path("batch", views.batch_upsert, name="uploads-batch"),
]
