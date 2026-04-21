from rest_framework import serializers

from .models import PlatformConfig


class PlatformConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = PlatformConfig
        fields = ["slug", "name", "inventory_table", "secondary_table", "master_po_table", "match_column", "is_active"]
