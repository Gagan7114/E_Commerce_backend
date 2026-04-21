from django.contrib import admin
from django.contrib.auth.admin import GroupAdmin as DjangoGroupAdmin
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin
from django.contrib.auth.forms import UserChangeForm, UserCreationForm
from django.contrib.auth.models import Group, Permission

from .models import User

admin.site.unregister(Group)


class EmailUserCreationForm(UserCreationForm):
    class Meta(UserCreationForm.Meta):
        model = User
        fields = ("email",)


class EmailUserChangeForm(UserChangeForm):
    class Meta(UserChangeForm.Meta):
        model = User
        fields = "__all__"


@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    add_form = EmailUserCreationForm
    form = EmailUserChangeForm
    model = User
    ordering = ("-created_at",)
    list_display = ("email", "first_name", "last_name", "is_active", "is_staff", "is_superuser", "group_list", "created_at")
    list_filter = ("is_active", "is_staff", "is_superuser", "groups")
    search_fields = ("email", "first_name", "last_name")
    readonly_fields = ("created_at", "last_login", "date_joined")
    filter_horizontal = ("groups", "user_permissions")

    fieldsets = (
        (None, {"fields": ("email", "password")}),
        ("Personal info", {"fields": ("first_name", "last_name")}),
        ("Permissions", {"fields": ("is_active", "is_staff", "is_superuser", "groups", "user_permissions")}),
        ("Timestamps", {"fields": ("last_login", "date_joined", "created_at")}),
    )
    add_fieldsets = (
        (None, {
            "classes": ("wide",),
            "fields": ("email", "password1", "password2", "is_active", "is_staff"),
        }),
    )
    actions = ("activate_users", "deactivate_users")

    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related("groups")

    @admin.display(description="Groups")
    def group_list(self, obj) -> str:
        return ", ".join(g.name for g in obj.groups.all()) or "—"

    @admin.action(description="Activate selected users")
    def activate_users(self, request, queryset):
        updated = queryset.update(is_active=True)
        self.message_user(request, f"Activated {updated} users.")

    @admin.action(description="Deactivate selected users")
    def deactivate_users(self, request, queryset):
        updated = queryset.update(is_active=False)
        self.message_user(request, f"Deactivated {updated} users.")


@admin.register(Group)
class GroupAdmin(DjangoGroupAdmin):
    list_display = ("name", "permission_count")
    search_fields = ("name",)
    filter_horizontal = ("permissions",)
    actions = ("clear_permissions",)

    def get_queryset(self, request):
        from django.db.models import Count
        return super().get_queryset(request).annotate(_perm_count=Count("permissions"))

    @admin.display(description="Permissions", ordering="_perm_count")
    def permission_count(self, obj) -> int:
        return obj._perm_count

    @admin.action(description="Remove all permissions from selected groups")
    def clear_permissions(self, request, queryset):
        for group in queryset:
            group.permissions.clear()
        self.message_user(request, f"Cleared permissions on {queryset.count()} groups.")


@admin.register(Permission)
class PermissionAdmin(admin.ModelAdmin):
    list_display = ("codename", "name", "content_type")
    search_fields = ("codename", "name")
    list_filter = ("content_type",)
    list_select_related = ("content_type",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
