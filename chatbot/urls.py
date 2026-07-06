from django.urls import path

from . import views

urlpatterns = [
    path("health", views.health, name="chatbot-health"),
    path("message", views.chat_message, name="chatbot-message"),
    path("conversations", views.conversations, name="chatbot-conversations"),
    path("conversations/<int:conversation_id>", views.conversation_detail, name="chatbot-conversation-detail"),
    path("files/<uuid:token>", views.file_download, name="chatbot-file-download"),
]
