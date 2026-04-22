from django.contrib.auth import get_user_model
from rest_framework import serializers, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken

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
