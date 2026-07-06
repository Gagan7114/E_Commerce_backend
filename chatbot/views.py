"""Chatbot API.

All endpoints require authentication (the project's global default is
IsAuthenticated). Users only ever see their own conversations and files.
"""

from __future__ import annotations

import logging

from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .engine import answer_question, engine_mode
from .models import ChatConversation, ChatFile, ChatMessage
from .serializers import (
    ChatConversationListSerializer,
    ChatConversationSerializer,
    ChatMessageSerializer,
)

logger = logging.getLogger(__name__)


@api_view(["GET"])
def health(request):
    """Report which engine is active so the UI can show a subtle badge."""
    return Response({"ok": True, "engine": engine_mode()})


@api_view(["POST"])
def chat_message(request):
    message = (request.data.get("message") or "").strip()
    if not message:
        return Response({"detail": "message is required"}, status=status.HTTP_400_BAD_REQUEST)
    if len(message) > 4000:
        message = message[:4000]

    conv_id = request.data.get("conversation_id")
    try:
        conversation = None
        if conv_id:
            conversation = ChatConversation.objects.filter(id=conv_id, user=request.user).first()
        if conversation is None:
            conversation = ChatConversation.objects.create(user=request.user)

        conversation.touch_title_from(message)
        user_msg = ChatMessage.objects.create(
            conversation=conversation, role=ChatMessage.Role.USER, text=message,
        )

        result = answer_question(request.user, conversation, message)
        assistant = ChatMessage.objects.create(
            conversation=conversation, role=ChatMessage.Role.ASSISTANT,
            text=result.text, data=result.data or {}, intent=result.intent,
            engine=result.engine, is_error=result.is_error, file=result.file,
        )
        conversation.save(update_fields=["title", "updated_at"])

        return Response({
            "conversation_id": conversation.id,
            "title": conversation.title,
            "user_message": ChatMessageSerializer(user_msg).data,
            "reply": ChatMessageSerializer(assistant).data,
        })
    except Exception as exc:
        # Never surface an opaque 500 to the chat UI — return a readable reply.
        logger.exception("chatbot message failed")
        return Response({
            "conversation_id": conv_id,
            "reply": {
                "role": "assistant",
                "text": f"Sorry — the assistant hit a server error: {exc}",
                "is_error": True,
                "engine": "builtin",
                "data": {},
                "file": None,
            },
        })


@api_view(["GET"])
def conversations(request):
    qs = ChatConversation.objects.filter(user=request.user)
    return Response(ChatConversationListSerializer(qs, many=True).data)


@api_view(["GET", "DELETE"])
def conversation_detail(request, conversation_id: int):
    conversation = get_object_or_404(
        ChatConversation, id=conversation_id, user=request.user
    )
    if request.method == "DELETE":
        conversation.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
    return Response(ChatConversationSerializer(conversation).data)


@api_view(["GET"])
def file_download(request, token):
    chat_file = get_object_or_404(ChatFile, token=token, user=request.user)
    content = chat_file.content
    if content is None:
        return Response({"detail": "file is empty"}, status=status.HTTP_404_NOT_FOUND)
    resp = HttpResponse(bytes(content), content_type=chat_file.content_type)
    resp["Content-Disposition"] = f'attachment; filename="{chat_file.filename}"'
    resp["Content-Length"] = str(chat_file.size_bytes or len(bytes(content)))
    return resp
