"""Persistence for the AI chatbot: conversations, messages, generated files.

These are the only tables the chatbot *writes* to. All business data (POs,
alerts, inventory, shipments, liters) is read from the existing app models and
Postgres views — the chatbot never mutates operational data.
"""

from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models


class ChatConversation(models.Model):
    """A single chat thread owned by one user."""

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="chat_conversations",
    )
    title = models.CharField(max_length=200, blank=True, default="New chat")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["user", "-updated_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.title}"

    def touch_title_from(self, text: str) -> None:
        """Set a friendly title from the first user message, once."""
        if self.title and self.title != "New chat":
            return
        clean = " ".join((text or "").split())
        if clean:
            self.title = (clean[:60] + "…") if len(clean) > 60 else clean


class ChatMessage(models.Model):
    """One turn in a conversation (a user question or an assistant reply)."""

    class Role(models.TextChoices):
        USER = "user", "User"
        ASSISTANT = "assistant", "Assistant"

    conversation = models.ForeignKey(
        ChatConversation,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    role = models.CharField(max_length=16, choices=Role.choices)
    text = models.TextField(blank=True)
    # Optional structured payload the frontend renders as a table/preview:
    # {"columns": [...], "rows": [[...], ...], "source": "...", "truncated": bool}
    data = models.JSONField(default=dict, blank=True)
    # Which engine/intent produced this reply (for debugging + analytics).
    intent = models.CharField(max_length=64, blank=True)
    engine = models.CharField(max_length=32, blank=True)  # "builtin" | "claude"
    is_error = models.BooleanField(default=False)
    file = models.ForeignKey(
        "ChatFile",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="messages",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["created_at", "id"]
        indexes = [
            models.Index(fields=["conversation", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.role}: {self.text[:40]}"


class ChatFile(models.Model):
    """A generated download (e.g. an .xlsx export) tied to a user/conversation.

    Bytes are stored in the row so the feature is fully self-contained (no media
    storage to configure). Exports are small; the download endpoint checks
    ownership before streaming the bytes back.
    """

    token = models.UUIDField(default=uuid.uuid4, unique=True, editable=False, db_index=True)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="chat_files",
    )
    conversation = models.ForeignKey(
        ChatConversation,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="files",
    )
    filename = models.CharField(max_length=200)
    content_type = models.CharField(
        max_length=120,
        default="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    content = models.BinaryField(editable=False)
    size_bytes = models.PositiveIntegerField(default=0)
    row_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return self.filename
