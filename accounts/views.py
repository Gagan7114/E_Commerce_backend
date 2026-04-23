from django.contrib.auth import get_user_model
from rest_framework import serializers, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken

from .permissions import user_permission_codes
from .serializers import MeSerializer

UserModel = get_user_model()


def _issue_token(user) -> str:
    return str(RefreshToken.for_user(user).access_token)


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
        return Response({"user": _user_payload(user), "token": _issue_token(user)})


class RegisterView(APIView):
    permission_classes = [AllowAny]

    class _Body(serializers.Serializer):
        email = serializers.EmailField()
        password = serializers.CharField(min_length=1)

    def post(self, request):
        ser = self._Body(data=request.data)
        ser.is_valid(raise_exception=True)
        email = ser.validated_data["email"].strip().lower()
        password = ser.validated_data["password"]
        if UserModel.objects.filter(email=email).exists():
            return Response(
                {"detail": "Email already registered"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserModel.objects.create_user(email=email, password=password)
        return Response({"user": _user_payload(user), "token": _issue_token(user)})


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
    return Response({"notifications": [], "unread_count": 0})


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def mark_all_read(request):
    return Response({"status": "ok"})
