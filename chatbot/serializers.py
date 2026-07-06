from rest_framework import serializers

from .models import ChatConversation, ChatFile, ChatMessage


class ChatFileSerializer(serializers.ModelSerializer):
    download_path = serializers.SerializerMethodField()

    class Meta:
        model = ChatFile
        fields = ["token", "filename", "content_type", "size_bytes", "row_count", "download_path"]

    def get_download_path(self, obj) -> str:
        return f"/api/chatbot/files/{obj.token}"


class ChatMessageSerializer(serializers.ModelSerializer):
    file = ChatFileSerializer(read_only=True)

    class Meta:
        model = ChatMessage
        fields = ["id", "role", "text", "data", "intent", "engine", "is_error", "file", "created_at"]


class ChatConversationSerializer(serializers.ModelSerializer):
    messages = ChatMessageSerializer(many=True, read_only=True)

    class Meta:
        model = ChatConversation
        fields = ["id", "title", "created_at", "updated_at", "messages"]


class ChatConversationListSerializer(serializers.ModelSerializer):
    message_count = serializers.IntegerField(source="messages.count", read_only=True)

    class Meta:
        model = ChatConversation
        fields = ["id", "title", "created_at", "updated_at", "message_count"]
