from django.contrib.auth import get_user_model
from rest_framework import serializers

from .permissions import user_permission_codes, user_platform_slugs

UserModel = get_user_model()


class MeSerializer(serializers.ModelSerializer):
    permissions = serializers.SerializerMethodField()
    groups = serializers.SerializerMethodField()
    platforms = serializers.SerializerMethodField()

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
            "platforms",
            "created_at",
        ]

    def get_permissions(self, obj) -> list[str]:
        return sorted(user_permission_codes(obj))

    def get_groups(self, obj) -> list[str]:
        return list(obj.groups.values_list("name", flat=True))

    def get_platforms(self, obj) -> list[str]:
        return user_platform_slugs(obj)
