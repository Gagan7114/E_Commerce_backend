from django.contrib.auth import get_user_model
from rest_framework import serializers
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

from .permissions import user_permission_codes

UserModel = get_user_model()


class EmailTokenObtainPairSerializer(TokenObtainPairSerializer):
    username_field = UserModel.USERNAME_FIELD


class MeSerializer(serializers.ModelSerializer):
    permissions = serializers.SerializerMethodField()
    groups = serializers.SerializerMethodField()

    class Meta:
        model = UserModel
        fields = [
            "id",
            "email",
            "first_name",
            "last_name",
            "is_active",
            "is_superuser",
            "is_staff",
            "groups",
            "permissions",
            "created_at",
        ]

    def get_permissions(self, obj) -> list[str]:
        return sorted(user_permission_codes(obj))

    def get_groups(self, obj) -> list[str]:
        return list(obj.groups.values_list("name", flat=True))
