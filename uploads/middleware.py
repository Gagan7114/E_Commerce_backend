"""Global uploader kill-switch.

When the owner turns the `uploader` switch OFF (Settings → Profile → Feature
Controls) every upload made through the Uploaders UI is refused with 403 —
for every user, including the owner's own browser.

Scheduled CLI uploads must keep running regardless, which is the whole reason
this is enforced here rather than by hiding buttons in the frontend: the block
applies only to requests `is_browser_request()` positively identifies as coming
from a browser, and that check defaults to "not a browser". Go's net/http (the
upload CLI), cron scripts and Apps Script send no Origin/Referer/Mozilla
User-Agent, so they are never blocked.

One choke point covering every upload endpoint present and future beats adding a
permission class to ~25 view decorators, where a newly added view would silently
miss the switch.
"""

from django.http import JsonResponse

from accounts.feature_flags import flag_enabled, is_browser_request
from accounts.models import FeatureFlag

# Write paths that count as "uploading data": the whole Upload Hub API plus the
# Amazon upload collection mounted at the project root.
UPLOAD_PREFIXES = ("/api/upload/", "/api/uploads")
# OPTIONS is deliberately absent so CORS preflight still succeeds.
WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

MESSAGE = (
    "Uploads are currently switched off by the administrator. "
    "Scheduled automated uploads are unaffected."
)


class UploaderKillSwitchMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if self._blocked(request):
            return JsonResponse({"detail": MESSAGE}, status=403)
        return self.get_response(request)

    def _blocked(self, request) -> bool:
        # Method and path first: the flag query never runs for ordinary traffic.
        if request.method not in WRITE_METHODS:
            return False
        if not request.path.startswith(UPLOAD_PREFIXES):
            return False
        if not is_browser_request(request):
            return False  # CLI / script / unknown client → always allowed
        return not flag_enabled(FeatureFlag.UPLOADER)
