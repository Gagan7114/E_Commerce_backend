"""Tiny response cache for read-only GET endpoints.

Purpose
-------
Wrap expensive dashboard / inventory GET views so repeat hits within a short
window (page revisits, tab switches, parallel widget loads) are served from
RAM instead of re-running heavy SQL.

Safety
------
* GET only. Any non-GET (POST/PUT/PATCH/DELETE) bypasses the cache.
* Non-2xx responses are not cached (errors stay live).
* Cache key includes the request path + sorted query string + user id, so
  every distinct request shape is cached independently and no user ever
  sees another user's data.
* `shared=True` drops the user id from the key — use it ONLY for endpoints whose
  response depends on the request params alone, not on the user (e.g. the
  org-wide home dashboards, which do no per-user row filtering). This lets every
  user reuse one cached payload instead of keeping N per-user copies, which is
  what actually makes the cache effective for a team. Permission checks still run
  first (this decorator is innermost, below @permission_classes), so a user
  without access is rejected by DRF before the cache is ever consulted.
* Cached payload is just the serialized response data; auth + permission
  checks always run BEFORE the cache lookup.
* Cache reads are best-effort: if the backend (e.g. Redis) is unavailable, the
  lookup is treated as a miss and the live view runs — a flaky cache can never
  break a response.
* Disable instantly by removing the decorator — no schema/data impact.
"""

from __future__ import annotations

import hashlib
from functools import wraps

from django.core.cache import cache
from rest_framework.response import Response


# A request carrying `?nocache=1` skips the cached copy and recomputes live (used
# by the reload right after a manual "Refresh", so the user always sees current
# data even within the cache window). The param is EXCLUDED from the cache key so
# the bust request maps to the same key as the normal request — it refreshes that
# shared entry instead of writing a separate, useless one.
_BUST_PARAM = "nocache"


def _query_items(request):
    if not hasattr(request, "GET"):
        return []
    return [item for item in sorted(request.GET.lists()) if item[0] != _BUST_PARAM]


def _wants_bypass(request) -> bool:
    return bool(request.GET.get(_BUST_PARAM)) if hasattr(request, "GET") else False


def _make_key(prefix: str, request, args, kwargs, shared: bool = False) -> str:
    if shared:
        user_part = "shared"
    else:
        user_id = getattr(getattr(request, "user", None), "id", "anon") or "anon"
        user_part = f"u={user_id}"
    query = _query_items(request)
    raw = f"{prefix}|{request.path}|{user_part}|q={query}|a={args}|k={sorted(kwargs.items())}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return f"perfcache:{prefix}:{digest}"


def cached_get(timeout: int = 60, prefix: str = "view", shared: bool = False):
    """Cache a DRF GET view's Response for `timeout` seconds.

    Set `shared=True` only on endpoints whose data is identical for every
    authorized user (see module docstring).
    """

    def decorator(view_func):
        @wraps(view_func)
        def wrapper(request, *args, **kwargs):
            method = getattr(request, "method", "GET")
            if method != "GET":
                return view_func(request, *args, **kwargs)

            key = _make_key(prefix, request, args, kwargs, shared=shared)
            if not _wants_bypass(request):
                try:
                    cached_data = cache.get(key)
                except Exception:
                    # A cache-backend failure (e.g. Redis down) must never break
                    # the endpoint — treat it as a miss and serve the live view.
                    cached_data = None
                if cached_data is not None:
                    return Response(cached_data)

            # On a bypass we still fall through to recompute AND re-store below, so
            # the live result also refreshes the shared cache entry for the next
            # normal request (on this worker).
            response = view_func(request, *args, **kwargs)
            try:
                status_code = getattr(response, "status_code", 200)
                if 200 <= status_code < 300 and hasattr(response, "data"):
                    cache.set(key, response.data, timeout=timeout)
            except Exception:
                # Caching is a best-effort optimization; never break the response.
                pass
            return response

        return wrapper

    return decorator
