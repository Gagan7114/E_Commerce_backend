"""
Live Reports (surfaced in the UI as "Pricing") — proxy + parse + cache for the
daily Excel workbooks published to the public GitHub repo
``daman8271/sending-excel``.

A bot commits ~12 dated ``.xlsx`` files there each day, named
``<Report>-YYYY-MM-DD.xlsx`` (competitor price watch + Jivo live reports across
Amazon / Fresh / Now, BigBasket, Blinkit, Flipkart, Flipkart Minutes, Price
Match). We surface them as a generic, multi-sheet grid viewer:

* ``GET /api/reports/live/reports`` — list reports (latest date per report prefix)
* ``GET /api/reports/live/data``    — one sheet as a raw grid (header + paginated,
  searchable body rows)

No DB tables are involved. The latest workbook is downloaded from GitHub, parsed
with openpyxl, and cached **content-addressed by the GitHub blob sha**, so a new
day's file (or a same-day re-commit that changes the sha) refreshes automatically
with no cron. Everything is fully generic: any ``*-YYYY-MM-DD.xlsx`` in the repo
shows up as a report on its own, no per-report code.

Robustness notes:
* Downloads use stdlib ``urllib`` with a short timeout; every network / parse /
  decode failure is funnelled through ``_SourceError`` and returned as a 502 with
  a generic message (details are logged server-side, never echoed to the client).
* A size cap (before parsing) and a per-sheet row cap bound worker memory against
  an oversized or pathological upstream file.
* Parsed workbooks are held in a small per-process LRU so repeated page/search
  requests for one report avoid both a re-download and a re-parse, and memory
  stays bounded regardless of how many reports are browsed.
* KNOWN LIMITATION: the listing uses the GitHub Contents API, which returns at
  most 1000 entries for a directory. These files accumulate by date, so if the
  source repo is never pruned the root will exceed 1000 in a few months and the
  newest files could be truncated from the listing. If that becomes real, switch
  ``_list_repo_files`` to the Git Trees API (``/git/trees/{branch}?recursive=1``).
"""
from __future__ import annotations

import datetime
import http.client
import io
import json
import logging
import os
import re
import threading
import urllib.error
import urllib.request
from collections import OrderedDict
from typing import Any
from urllib.parse import quote

from django.conf import settings
from django.core.cache import cache
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

try:  # openpyxl is a hard dependency, but guard like the rest of the codebase does.
    from openpyxl import load_workbook
except ImportError:  # pragma: no cover
    load_workbook = None

logger = logging.getLogger(__name__)


# --- Source config -----------------------------------------------------------
GITHUB_OWNER = "daman8271"
GITHUB_REPO = "sending-excel"
_CONTENTS_API = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/"

# Cache / limit knobs.
_LIST_TTL = 15 * 60           # GitHub directory listing — 15 min
_FILE_TTL = 12 * 60 * 60      # downloaded workbook bytes — 12 h
_HTTP_TIMEOUT = 30            # seconds (per socket op) — short so a slow GitHub can't pin a worker
_MAX_BYTES = 30 * 1024 * 1024 # reject/parse-guard: workbooks larger than 30 MB
_MAX_ROWS = 50_000            # per-sheet row cap materialised into the grid
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 500
_FULL_GRID_CAP = 2000        # rows returned to the Dashboard view (full=1), un-paginated

# Per-process LRU of parsed workbooks, keyed by blob sha (content-addressed).
# Bounds memory to at most _PARSE_LRU_MAX workbooks per worker and avoids the
# pickle round-trip / re-parse a shared cache would incur on every page request.
_PARSE_LRU_MAX = 3
_PARSE_LRU: "OrderedDict[str, dict]" = OrderedDict()
_LRU_LOCK = threading.Lock()

# ``<report prefix>-YYYY-MM-DD.xlsx``
_DATED_RE = re.compile(r"^(?P<prefix>.+)-(?P<date>\d{4}-\d{2}-\d{2})\.xlsx$", re.IGNORECASE)


class _SourceError(Exception):
    """Any failure reaching, decoding, or parsing the upstream reports source.
    Views translate this into a 502 with a generic client message."""


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
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
            return resp.read()
    except (OSError, http.client.HTTPException) as e:
        # OSError covers URLError/HTTPError (URLError subclasses OSError) and
        # read-phase socket.timeout/TimeoutError; HTTPException covers IncompleteRead.
        raise _SourceError("Could not reach the reports source.") from e


# --- Repo listing ------------------------------------------------------------
def _list_repo_files() -> list[dict]:
    """Raw GitHub contents listing for the repo root (cached ~15 min)."""
    cached = cache.get("livereports.contents")
    if cached is not None:
        return cached
    raw = _http_get(_CONTENTS_API)  # raises _SourceError on network failure
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as e:
        raise _SourceError("The reports source returned an unreadable listing.") from e
    if not isinstance(data, list):
        raise _SourceError("Unexpected response from the reports source.")
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
                "sha": f.get("sha"),                # content id — cache is keyed on this
                "size": f.get("size"),
            }
    return sorted(latest.values(), key=lambda r: r["label"].lower())


def _find_report(key: str) -> dict | None:
    for r in _latest_reports():
        if r["key"] == key:
            return r
    return None


def _last_commit_iso(report: dict) -> str | None:
    """ISO datetime (UTC) of the commit that last published this file — i.e. when
    the report was last updated on GitHub. Cached by sha. Best-effort: any failure
    returns None and the UI simply omits the time.

    NOTE: the source bot publishes all reports together in one daily commit, so
    this value is the same across every report/sheet on a given day; it changes
    day to day, not per sheet."""
    sha = report.get("sha") or report["filename"]
    ck = f"livereports.commit:{sha}"
    cached = cache.get(ck)
    if cached is not None:
        return cached or None
    url = (
        f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits"
        f"?path={quote(report['filename'])}&per_page=1"
    )
    iso = ""
    try:
        data = json.loads(_http_get(url).decode("utf-8"))
        if isinstance(data, list) and data:
            iso = ((data[0].get("commit") or {}).get("committer") or {}).get("date") or ""
    except (_SourceError, ValueError, UnicodeDecodeError, KeyError, IndexError, TypeError):
        iso = ""
    cache.set(ck, iso, timeout=_FILE_TTL)
    return iso or None


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
        # Keep small magnitudes from collapsing to "0": widen precision for them.
        text = f"{v:.4f}".rstrip("0").rstrip(".")
        if text in ("", "0", "-0") and v != 0:
            text = repr(v)
        return text
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


def _lru_get(sha: str) -> dict | None:
    with _LRU_LOCK:
        parsed = _PARSE_LRU.get(sha)
        if parsed is not None:
            _PARSE_LRU.move_to_end(sha)
        return parsed


def _lru_put(sha: str, parsed: dict) -> None:
    with _LRU_LOCK:
        _PARSE_LRU[sha] = parsed
        _PARSE_LRU.move_to_end(sha)
        while len(_PARSE_LRU) > _PARSE_LRU_MAX:
            _PARSE_LRU.popitem(last=False)


def _parse_bytes(content: bytes, report: dict) -> dict:
    if load_workbook is None:
        raise _SourceError("openpyxl is required to read report workbooks.")
    try:
        wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as e:  # BadZipFile / InvalidFileException / KeyError / …
        raise _SourceError("Could not parse the report workbook.") from e
    try:
        sheets = list(wb.sheetnames)
        grids: dict[str, list[list[str]]] = {}
        for name in sheets:
            ws = wb[name]
            grid: list[list[str]] = []
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i >= _MAX_ROWS:  # bound memory against a pathological sheet
                    break
                grid.append([_cell(v) for v in row])
            grids[name] = _trim(grid)
    finally:
        wb.close()
    return {"sheets": sheets, "grids": grids, "date": report["date"], "filename": report["filename"]}


def _parse_workbook(report: dict) -> dict:
    """Return ``{sheets, grids, date, filename}`` for the report's latest workbook.

    Content-addressed by blob sha: a hit in the per-process LRU returns instantly;
    otherwise the raw bytes (cached ~12 h to avoid re-download) are parsed and the
    result is memoised in the LRU."""
    sha = report.get("sha") or report["filename"]

    hit = _lru_get(sha)
    if hit is not None:
        return hit

    size = report.get("size") or 0
    if size and size > _MAX_BYTES:
        raise _SourceError("The report file is too large to display.")

    bkey = f"livereports.bytes:{sha}"
    content = cache.get(bkey)
    if content is None:
        content = _http_get(report["download_url"])
        if len(content) > _MAX_BYTES:
            raise _SourceError("The report file is too large to display.")
        cache.set(bkey, content, timeout=_FILE_TTL)

    parsed = _parse_bytes(content, report)
    _lru_put(sha, parsed)
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
    except _SourceError as e:
        logger.warning("live_reports source error: %s", e.__cause__ or e)
        return Response({"detail": "The reports source is currently unavailable."}, status=502)
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
    except _SourceError as e:
        logger.warning("live_data source error (report=%s): %s", key, e.__cause__ or e)
        return Response({"detail": "The reports source is currently unavailable."}, status=502)

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

    # Dashboard view: return the whole sheet (un-paginated, capped) so the client
    # can interpret its structure into KPI cards / charts / tables. The Table view
    # keeps using the paginated body below.
    if str(request.query_params.get("full") or "").lower() in ("1", "true", "yes"):
        return Response(
            {
                "report": report["key"],
                "label": report["label"],
                "date": report["date"],
                "updated_at": _last_commit_iso(report),
                "download_url": report["download_url"],
                "sheets": sheets,
                "sheet": sheet,
                "grid": grid[:_FULL_GRID_CAP],
                "rows_total": len(grid),
                "truncated": len(grid) > _FULL_GRID_CAP,
            }
        )

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
            "updated_at": _last_commit_iso(report),  # ISO UTC publish time (or null)
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
