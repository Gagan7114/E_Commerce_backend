from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken

from platforms.services.inventory_doh_alerts import (
    ALERT_TYPE,
    DEFAULT_THRESHOLD,
    notification_to_payload,
    upsert_low_doh_notifications,
)

from .models import InventoryDohNotification
from .permissions import user_permission_codes
from .serializers import MeSerializer

UserModel = get_user_model()


def _issue_tokens(user) -> dict:
    # Hand back BOTH tokens: the short-lived access token used on every request,
    # and the long-lived refresh token the frontend uses to silently renew it so
    # the user is never forced to log in again.
    refresh = RefreshToken.for_user(user)
    return {"token": str(refresh.access_token), "refresh": str(refresh)}


def _user_payload(user) -> dict:
    return MeSerializer(user).data


class LoginView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        email = (request.data.get("email") or "").strip().lower()
        password = request.data.get("password") or ""
        if not email or not password:
            return Response(
                {"detail": "Email and password are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserModel.objects.filter(email=email).first()
        if not user or not user.check_password(password):
            return Response(
                {"detail": "Invalid email or password"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        if not user.is_active:
            return Response(
                {"detail": "Account disabled"}, status=status.HTTP_403_FORBIDDEN
            )
        return Response({"user": _user_payload(user), **_issue_tokens(user)})


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def me(request):
    return Response({"user": _user_payload(request.user)})


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def user_permissions(request):
    codes = sorted(user_permission_codes(request.user))
    grouped: dict[str, list[str]] = {}
    for code in codes:
        module = code.split(".")[0]
        grouped.setdefault(module, []).append(code)
    result = [
        {"module": mod, "count": len(perms), "permissions": perms}
        for mod, perms in sorted(grouped.items())
    ]
    return Response({"permissions": result})


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def change_password(request):
    current = request.data.get("current_password", "")
    new_pwd = request.data.get("new_password", "")
    if not current or not new_pwd:
        return Response(
            {"detail": "Both current_password and new_password are required."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    if len(new_pwd) < 6:
        return Response(
            {"detail": "New password must be at least 6 characters."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    if not request.user.check_password(current):
        return Response(
            {"detail": "Current password is incorrect."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    request.user.set_password(new_pwd)
    request.user.save()
    return Response({"detail": "Password changed successfully."})


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def notifications(request):
    active_only = str(request.query_params.get("active_only", "true")).lower() not in {
        "false",
        "0",
        "no",
    }
    queryset = InventoryDohNotification.objects.filter(alert_type=ALERT_TYPE)
    if active_only:
        queryset = queryset.filter(resolved_at__isnull=True)
    queryset = queryset.filter(doh__lt=DEFAULT_THRESHOLD)
    platform_slug = (request.query_params.get("platform") or "").strip().lower()
    if platform_slug:
        queryset = queryset.filter(platform_slug=platform_slug)
    format_name = (request.query_params.get("format") or "").strip().upper()
    if format_name:
        queryset = queryset.filter(format=format_name)
    try:
        limit = min(max(int(request.query_params.get("limit") or 50), 1), 200)
    except (TypeError, ValueError):
        limit = 50

    # Short-lived cache: this endpoint is polled on every page mount and the
    # underlying snapshot only changes when upserts run. 20s keeps the UI fresh
    # while collapsing burst traffic to a single DB roundtrip.
    cache_key = f"notif:doh:{int(active_only)}:{platform_slug}:{format_name}:{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return Response(cached)

    page = list(queryset.order_by("is_read", "doh", "-last_seen_at")[:limit])
    payload = {
        "notifications": [notification_to_payload(item) for item in page],
        "unread_count": queryset.filter(is_read=False).count(),
        "count": queryset.count(),
    }
    cache.set(cache_key, payload, timeout=20)
    return Response(payload)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def mark_all_read(request):
    updated = InventoryDohNotification.objects.filter(
        alert_type=ALERT_TYPE,
        resolved_at__isnull=True,
        is_read=False,
    ).update(is_read=True)
    # 20s notif cache TTL will refresh shortly; no need to invalidate broadly.
    return Response({"status": "ok", "updated": updated})


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def mark_notification_read(request, notification_id: int):
    notification = get_object_or_404(InventoryDohNotification, id=notification_id)
    if not notification.is_read:
        notification.is_read = True
        notification.save(update_fields=["is_read", "updated_at"])
    return Response({"status": "ok", "notification": notification_to_payload(notification)})


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def notification_detail(request, notification_id: int):
    notification = get_object_or_404(InventoryDohNotification, id=notification_id)
    return Response({"notification": notification_to_payload(notification)})


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def inventory_doh_sku_detail(request, notification_id: int):
    notification = get_object_or_404(InventoryDohNotification, id=notification_id)
    payload = notification_to_payload(notification)
    return Response({
        "notification": payload,
        "sku": {
            "format": payload["format"],
            "platform_slug": payload["platform_slug"],
            "sku_code": payload["sku_code"],
            "sku_name": payload["sku_name"],
            "item": payload["item"],
            "item_head": payload["item_head"],
            "category": payload["category"],
            "sub_category": payload["sub_category"],
            "brand": payload["brand"],
        },
        "metrics": {
            "inventory_date": payload["inventory_date"],
            "sales_max_date": payload["sales_max_date"],
            "month_start": payload["month_start"],
            "units_sold": payload["units_sold"],
            "ltr_sold": payload["ltr_sold"],
            "soh_units": payload["soh_units"],
            "soh_ltr": payload["soh_ltr"],
            "drr_units": payload["drr_units"],
            "drr_ltr": payload["drr_ltr"],
            "doh": payload["doh"],
            "threshold": payload["threshold"],
        },
    })


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def generate_inventory_doh_notifications(request):
    try:
        threshold = float(request.data.get("threshold", DEFAULT_THRESHOLD))
    except (TypeError, ValueError):
        return Response(
            {"detail": "threshold must be a number."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    result = upsert_low_doh_notifications(
        threshold=threshold,
        platform_slug=request.data.get("platform"),
        date_value=request.data.get("date"),
        send_firebase=bool(request.data.get("send_firebase", True)),
    )
    return Response(result)
