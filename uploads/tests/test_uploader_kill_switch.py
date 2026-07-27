"""The uploader kill-switch must stop browsers and NEVER stop the CLI.

`flag_enabled` is patched throughout so these run without a database — the point
under test is the request classification and the path/method scoping, not the
storage.
"""

from __future__ import annotations

from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from accounts.feature_flags import is_browser_request
from uploads.middleware import UploaderKillSwitchMiddleware


def _middleware():
    """Middleware whose inner view returns a 200 sentinel when reached."""
    sentinel = object()

    def get_response(_request):
        return sentinel

    return UploaderKillSwitchMiddleware(get_response), sentinel


class IsBrowserRequestTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()

    def test_origin_marks_a_browser(self):
        request = self.rf.post("/api/upload/batch", HTTP_ORIGIN="https://ecom.jivo.in")
        self.assertTrue(is_browser_request(request))

    def test_referer_marks_a_browser(self):
        request = self.rf.post(
            "/api/upload/batch", HTTP_REFERER="https://ecom.jivo.in/uploaders"
        )
        self.assertTrue(is_browser_request(request))

    def test_mozilla_user_agent_marks_a_browser(self):
        request = self.rf.post(
            "/api/upload/batch",
            HTTP_USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        )
        self.assertTrue(is_browser_request(request))

    def test_go_http_client_is_not_a_browser(self):
        # What jivo-ecom-upload actually sends: Go's default UA, no Origin.
        request = self.rf.post(
            "/api/upload/batch", HTTP_USER_AGENT="Go-http-client/1.1"
        )
        self.assertFalse(is_browser_request(request))

    def test_bare_request_is_not_a_browser(self):
        # curl / Apps Script / a cron script with no identifying headers.
        request = self.rf.post("/api/upload/batch")
        self.assertFalse(is_browser_request(request))


class KillSwitchTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()

    def _run(self, request, uploader_enabled):
        middleware, sentinel = _middleware()
        with patch(
            "uploads.middleware.flag_enabled", return_value=uploader_enabled
        ) as flag:
            response = middleware(request)
        return response, sentinel, flag

    def assertPassedThrough(self, request, uploader_enabled=False):
        response, sentinel, _ = self._run(request, uploader_enabled)
        self.assertIs(response, sentinel)

    def assertBlocked(self, request):
        response, sentinel, _ = self._run(request, uploader_enabled=False)
        self.assertIsNot(response, sentinel)
        self.assertEqual(response.status_code, 403)
        self.assertIn("switched off", response.content.decode())

    # --- the switch OFF ----------------------------------------------------
    def test_browser_upload_is_blocked(self):
        self.assertBlocked(
            self.rf.post("/api/upload/batch", HTTP_ORIGIN="https://ecom.jivo.in")
        )

    def test_browser_amazon_upload_collection_is_blocked(self):
        self.assertBlocked(
            self.rf.post("/api/uploads", HTTP_ORIGIN="https://ecom.jivo.in")
        )

    def test_browser_delete_is_blocked(self):
        self.assertBlocked(
            self.rf.delete(
                "/api/upload/master-sheet/delete", HTTP_ORIGIN="https://ecom.jivo.in"
            )
        )

    def test_cli_upload_still_works_with_the_switch_off(self):
        # THE requirement: automation is never interrupted.
        self.assertPassedThrough(
            self.rf.post("/api/upload/batch", HTTP_USER_AGENT="Go-http-client/1.1")
        )

    def test_unknown_client_upload_still_works(self):
        self.assertPassedThrough(self.rf.post("/api/upload/batch"))

    def test_browser_read_still_works(self):
        # GET stays open so upload history/schema pages still render.
        self.assertPassedThrough(
            self.rf.get(
                "/api/upload/flipkart-grocery/status",
                HTTP_ORIGIN="https://ecom.jivo.in",
            )
        )

    def test_cors_preflight_still_works(self):
        self.assertPassedThrough(
            self.rf.options("/api/upload/batch", HTTP_ORIGIN="https://ecom.jivo.in")
        )

    def test_non_upload_write_is_untouched(self):
        self.assertPassedThrough(
            self.rf.post("/api/auth/login", HTTP_ORIGIN="https://ecom.jivo.in")
        )

    # --- the switch ON -----------------------------------------------------
    def test_browser_upload_works_when_enabled(self):
        self.assertPassedThrough(
            self.rf.post("/api/upload/batch", HTTP_ORIGIN="https://ecom.jivo.in"),
            uploader_enabled=True,
        )

    def test_flag_is_not_read_for_unrelated_paths(self):
        # The DB must not be touched on every request in the app.
        request = self.rf.post("/api/dashboard/anything", HTTP_ORIGIN="https://x.test")
        _, _, flag = self._run(request, uploader_enabled=False)
        flag.assert_not_called()
