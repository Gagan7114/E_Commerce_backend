from django.contrib import admin

from .models import ChatConversation, ChatFile, ChatMessage


class ChatMessageInline(admin.TabularInline):
    model = ChatMessage
    extra = 0
    fields = ("role", "text", "intent", "engine", "is_error", "created_at")
    readonly_fields = fields
    can_delete = False
    show_change_link = False


@admin.register(ChatConversation)
class ChatConversationAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "title", "updated_at", "created_at")
    list_filter = ("created_at",)
    search_fields = ("title", "user__email")
    inlines = [ChatMessageInline]


@admin.register(ChatMessage)
class ChatMessageAdmin(admin.ModelAdmin):
    list_display = ("id", "conversation", "role", "intent", "engine", "is_error", "created_at")
    list_filter = ("role", "engine", "is_error", "created_at")
    search_fields = ("text",)


@admin.register(ChatFile)
class ChatFileAdmin(admin.ModelAdmin):
    list_display = ("id", "filename", "user", "size_bytes", "row_count", "created_at")
    search_fields = ("filename", "user__email")
    readonly_fields = ("token", "content", "size_bytes", "row_count", "created_at")
