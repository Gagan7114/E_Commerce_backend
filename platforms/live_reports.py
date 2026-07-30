"""
Live Reports — proxy + parse + cache for the daily Excel workbooks published to
the public GitHub repo ``daman8271/sending-excel``.

A bot commits ~12 dated ``.xlsx`` files there each day, named
``<Report>-YYYY-MM-DD.xlsx`` (competitor price watch + Jivo live reports across
Amazon / Fresh / Now, BigBasket, Blinkit, Flipkart, Flipkart Minutes, Price
Match). We surface them as a generic, multi-sheet grid viewer:

* ``GET /api/reports/live/reports`` — list reports (latest date per report prefix)
* ``GET /api/reports/live/data``    — one sheet as a raw grid (header + paginated,
  searchable body rows)

No DB tables are involved. The latest workbook is downloaded from GitHub, parsed
with openpyxl, and the parsed grid is cached keyed by the *dated filename* — so a
new day's upload (a new filename) is a cache miss that refreshes automatically,
with no cron. Everything is fully generic: any ``*-YYYY-MM-DD.xlsx`` in the repo
shows up as a report on its own, no per-report code.
"""
from __future__ import annotations

import datetime
import io
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

from django.conf import settings
from django.core.cache import cache
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

try:  # openpyxl is a hard dependency, but guard like the rest of the codebase does.
    from openpyxl import load_workbook
except ImportError:  # pragma: no cover
    load_workbook = None


# --- Source config -----------------------------------------------------------
GITHUB_OWNER = "daman8271"
GITHUB_REPO = "sending-excel"
_CONTENTS_API = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/"

# Cache TTLs (seconds). The parsed-workbook key embeds the dated filename, so a
# new day's file lands under a new key and supersedes the old one on its own.
_LIST_TTL = 15 * 60          # GitHub directory listing — 15 min
_FILE_TTL = 12 * 60 * 60     # parsed workbook — 12 h
_HTTP_TIMEOUT = 120          # seconds

# Response caps.
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 500

# ``<report prefix>-YYYY-MM-DD.xlsx``
_DATED_RE = re.compile(r"^(?P<prefix>.+)-(?P<date>\d{4}-\d{2}-\d{2})\.xlsx$", re.IGNORECASE)


# --- HTTP --------------------------------------------------------------------
def _github_token() -> str:
    """Optional token for when the repo goes private. Empty ⇒ unauthenticated
    (fine while the repo is public). To enable, add ``GITHUB_REPORTS_TOKEN`` to
    the backend ``.env``; django-environ loads it into the process environment."""
    return getattr(settings, "GITHUB_REPORTS_TOKEN", "") or os.environ.get("GITHUB_REPORTS_TOKEN", "")


def _http_get(url: str) -> bytes:
    headers = {
        "User-Agent": "ecms-live-reports",
        "Accept": "application/vnd.github+json",
    }
    token = _github_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
        return resp.read()


# --- Repo listing ------------------------------------------------------------
def _list_repo_files() -> list[dict]:
    """Raw GitHub contents listing for the repo root (cached ~15 min)."""
    cached = cache.get("livereports.contents")
    if cached is not None:
        return cached
    data = json.loads(_http_get(_CONTENTS_API).decode("utf-8"))
    files = [f for f in data if isinstance(f, dict) and f.get("type") == "file"]
    cache.set("livereports.contents", files, timeout=_LIST_TTL)
    return files


def _pretty_label(prefix: str) -> str:
    """``Jivo-AmazonFresh-Live-Report`` → ``Jivo AmazonFresh Live Report``."""
    return re.sub(r"[-_]+", " ", prefix).strip()


def _latest_reports() -> list[dict]:
    """Group dated ``.xlsx`` files by report prefix and keep the newest date each."""
    latest: dict[str, dict] = {}
    for f in _list_repo_files():
        name = f.get("name", "")
        m = _DATED_RE.match(name)
        if not m:
            continue
        prefix, date = m.group("prefix"), m.group("date")
        cur = latest.get(prefix)
        if cur is None or date > cur["date"]:
            latest[prefix] = {
                "key": prefix,                      # stable key = filename prefix
                "label": _pretty_label(prefix),
                "date": date,
                "filename": name,
                "download_url": f.get("download_url"),
                "size": f.get("size"),
            }
    return sorted(latest.values(), key=lambda r: r["label"].lower())


def _find_report(key: str) -> dict | None:
    for r in _latest_reports():
        if r["key"] == key:
            return r
    return None


# --- Parsing -----------------------------------------------------------------
def _cell(v: Any) -> str:
    """Stringify a cell value for display (plain-grid viewer)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:  # NaN
            return ""
        if v.is_integer():
            return str(int(v))
        return f"{v:.4f}".rstrip("0").rstrip(".")
    if isinstance(v, datetime.datetime):
        if (v.hour, v.minute, v.second) == (0, 0, 0):
            return v.strftime("%Y-%m-%d")
        return v.strftime("%Y-%m-%d %H:%M")
    if isinstance(v, datetime.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, datetime.time):
        return v.strftime("%H:%M")
    return str(v)


def _trim(grid: list[list[str]]) -> list[list[str]]:
    """Drop trailing all-empty rows/columns and pad every row to a uniform width
    (openpyxl's max_col/max_row over-report; read-only rows can be ragged)."""
    while grid and all(c == "" for c in grid[-1]):
        grid.pop()
    if not grid:
        return []
    width = 0
    for r in grid:
        last = 0
        for i, c in enumerate(r):
            if c != "":
                last = i + 1
        if last > width:
            width = last
    if width == 0:
        return []
    return [(r + [""] * width)[:width] for r in grid]


def _parse_workbook(report: dict) -> dict:
    """Download + parse the report's latest workbook into
    ``{sheets, grids, date, filename}``, cached by the dated filename."""
    if load_workbook is None:
        raise RuntimeError("openpyxl is required to read Live Reports workbooks.")
    ck = f"livereports.wb:{report['filename']}"
    cached = cache.get(ck)
    if cached is not None:
        return cached
    content = _http_get(report["download_url"])
    wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    try:
        sheets = list(wb.sheetnames)
        grids: dict[str, list[list[str]]] = {}
        for name in sheets:
            ws = wb[name]
            grid = [[_cell(v) for v in row] for row in ws.iter_rows(values_only=True)]
            grids[name] = _trim(grid)
    finally:
        wb.close()
    parsed = {"sheets": sheets, "grids": grids, "date": report["date"], "filename": report["filename"]}
    cache.set(ck, parsed, timeout=_FILE_TTL)
    return parsed


# --- Request helpers ---------------------------------------------------------
def _page_params(request) -> tuple[int, int]:
    def _int(name: str, default: int) -> int:
        try:
            return int(request.query_params.get(name, default))
        except (TypeError, ValueError):
            return default

    page = max(0, _int("page", 0))
    page_size = min(max(1, _int("page_size", _DEFAULT_PAGE_SIZE)), _MAX_PAGE_SIZE)
    return page, page_size


# --- Views -------------------------------------------------------------------
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def live_reports(request):
    """List the available reports (latest dated file per report)."""
    try:
        reports = _latest_reports()
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        return Response({"detail": f"Could not reach the reports source: {e}"}, status=502)
    return Response(
        {
            "reports": [
                {
                    "key": r["key"],
                    "label": r["label"],
                    "date": r["date"],
                    "filename": r["filename"],
                    "download_url": r["download_url"],
                    "size": r["size"],
                }
                for r in reports
            ],
            "count": len(reports),
        }
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def live_data(request):
    """Return one sheet of a report as a raw grid.

    Query params: ``report`` (key, required), ``sheet`` (defaults to first),
    ``q`` (row search), ``page`` (0-indexed), ``page_size``.

    Row 0 of the sheet is returned as ``header``; the remaining rows are the
    searchable, paginated ``rows`` body.
    """
    key = (request.query_params.get("report") or "").strip()
    if not key:
        return Response({"detail": "A 'report' query param is required."}, status=400)

    try:
        report = _find_report(key)
        if not report:
            return Response({"detail": "Unknown or unavailable report."}, status=404)
        parsed = _parse_workbook(report)
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        return Response({"detail": f"Could not reach the reports source: {e}"}, status=502)
    except RuntimeError as e:
        return Response({"detail": str(e)}, status=500)

    sheets = parsed["sheets"]
    if not sheets:
        return Response(
            {
                "report": report["key"], "label": report["label"], "date": report["date"],
                "download_url": report["download_url"], "sheets": [], "sheet": None,
                "header": [], "rows": [], "count": 0, "page": 0, "page_size": _DEFAULT_PAGE_SIZE,
            }
        )

    requested = (request.query_params.get("sheet") or "").strip()
    sheet = requested if requested in parsed["grids"] else sheets[0]
    grid = parsed["grids"].get(sheet, [])

    header = grid[0] if grid else []
    body = grid[1:] if len(grid) > 1 else []

    q = (request.query_params.get("q") or "").strip().lower()
    if q:
        body = [r for r in body if any(q in (c or "").lower() for c in r)]

    total = len(body)
    page, page_size = _page_params(request)
    start = page * page_size
    page_rows = body[start : start + page_size]

    return Response(
        {
            "report": report["key"],
            "label": report["label"],
            "date": report["date"],
            "download_url": report["download_url"],
            "sheets": sheets,
            "sheet": sheet,
            "header": header,
            "rows": page_rows,
            "count": total,
            "page": page,
            "page_size": page_size,
        }
    )
