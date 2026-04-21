import fnmatch

from rest_framework.permissions import BasePermission


def _compute_codes(user) -> frozenset[str]:
    codes: set[str] = set()
    for perm in user.user_permissions.all():
        codes.add(perm.codename)
    for group in user.groups.all():
        for perm in group.permissions.all():
            codes.add(perm.codename)
    return frozenset(codes)


def user_permission_codes(user) -> frozenset[str]:
    """Return the set of permission codes a user holds.

    Superusers get the wildcard `*`. Result is memoized on the user instance
    for the duration of a single request (users are fetched per-request by
    Django auth), avoiding the staleness of a process-wide cache when admins
    update group assignments.
    """
    if not getattr(user, "is_authenticated", False):
        return frozenset()
    if user.is_superuser:
        return frozenset(["*"])
    cached = getattr(user, "_ecms_perm_codes", None)
    if cached is not None:
        return cached
    codes = _compute_codes(user)
    user._ecms_perm_codes = codes
    return codes


def has_permission_code(user, required: str) -> bool:
    """Return True if `user` holds `required`, supporting glob matches.

    Patterns supported on either side:
      - exact:    dispatch.add
      - wildcard: dashboard.*
      - suffix:   *.view
    Superusers always pass.
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if user.is_superuser:
        return True
    codes = user_permission_codes(user)
    if required in codes:
        return True
    for held in codes:
        if fnmatch.fnmatchcase(required, held) or fnmatch.fnmatchcase(held, required):
            return True
    return False


class HasPermissionCode(BasePermission):
    """DRF permission class gated on a specific permission code.

    Two usage patterns:

        # Bound to a fixed code:
        permission_classes = [require("dispatch.add")]

        # Or let the view expose `required_permission` (str or callable):
        class MyView(APIView):
            required_permission = "dispatch.view"
            permission_classes = [HasPermissionCode]
    """

    code: str | None = None

    def has_permission(self, request, view) -> bool:
        code = self.code or getattr(view, "required_permission", None)
        if code is None:
            return bool(request.user and request.user.is_authenticated)
        if callable(code):
            code = code(request, view)
        return has_permission_code(request.user, code)


def require(code: str):
    """Factory: `permission_classes = [require("dispatch.add")]`."""
    cls = type(
        f"HasPermissionCode_{code.replace('.', '_').replace('*', 'STAR')}",
        (HasPermissionCode,),
        {"code": code},
    )
    return cls
