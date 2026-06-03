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
* Cached payload is just the serialized response data; auth + permission
  checks always run BEFORE the cache lookup.
* Disable instantly by removing the decorator — no schema/data impact.
"""

from __future__ import annotations

import hashlib
from functools import wraps

from django.core.cache import cache
from rest_framework.response import Response


def _make_key(prefix: str, request, args, kwargs) -> str:
    user_id = getattr(getattr(request, "user", None), "id", "anon") or "anon"
    query = sorted(request.GET.lists()) if hasattr(request, "GET") else []
    raw = f"{prefix}|{request.path}|u={user_id}|q={query}|a={args}|k={sorted(kwargs.items())}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return f"perfcache:{prefix}:{digest}"


def cached_get(timeout: int = 60, prefix: str = "view"):
    """Cache a DRF GET view's Response for `timeout` seconds."""

    def decorator(view_func):
        @wraps(view_func)
        def wrapper(request, *args, **kwargs):
            method = getattr(request, "method", "GET")
            if method != "GET":
                return view_func(request, *args, **kwargs)

            key = _make_key(prefix, request, args, kwargs)
            cached_data = cache.get(key)
            if cached_data is not None:
                return Response(cached_data)

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
