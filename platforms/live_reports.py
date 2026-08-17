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

Cell values are rendered the way the workbook renders them: openpyxl is asked for
cell objects (not bare values) so each cell's ``number_format`` survives, and
``_cell`` applies it. Without this a ``0.0%`` cell holding ``-0.164`` reaches the
browser as ``-0.164`` instead of ``-16.4%``, and every ``"Rs"#,##0`` price loses
its currency mark — on a pricing screen that is a wrong number, not a cosmetic
one. Formats are read in READ-ONLY mode, which costs no extra memory (measured:
1.3 MB either way on the largest workbook; a non-read-only load costs 67x more).

Robustness notes:
* Downloads use stdlib ``urllib`` with a short timeout; every network / parse /
  decode failure is funnelled through ``_SourceError`` and returned as a 502 with
  a generic message (details are logged server-side, never echoed to the client).
* A size cap (before parsing) and a per-sheet row cap bound worker memory against
  an oversized or pathological upstream file.
* Parsed workbooks are held in a per-process LRU so repeated page/search requests
  for one report avoid both a re-download and a re-parse. The LRU is bounded by
  RETAINED CELL COUNT (not workbook count), because workbooks differ by two
  orders of magnitude: the nine reports published on 2026-08-17 total 634,972
  cells / 33.8 MB, but a single one of them ranges from 1,774 to 222,843 cells.
* The listing uses the Git Trees API (``/git/trees/{branch}?recursive=1``), which
  has no 1000-entry ceiling, and falls back to the Contents API if the tree call
  fails. NOTE: tree blobs carry no ``download_url``, so the raw URL is built here
  — it is used both to fetch the workbook and as the UI's "Download .xlsx" link.
  (The source repo is rolled daily rather than accumulating, so the old Contents
  API ceiling was never actually reached; the Trees API simply removes the cliff.)
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

from accounts.permissions import require

try:  # openpyxl is a hard dependency, but guard like the rest of the codebase does.
    from openpyxl import load_workbook
except ImportError:  # pragma: no cover
    load_workbook = None

logger = logging.getLogger(__name__)


# --- Source config -----------------------------------------------------------
GITHUB_OWNER = "daman8271"
GITHUB_REPO = "sending-excel"
GITHUB_BRANCH = "main"
_CONTENTS_API = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/"
_TREES_API = (
    f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"
    f"/git/trees/{GITHUB_BRANCH}?recursive=1"
)
_RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}/"

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
# Avoids the pickle round-trip / re-parse a shared cache would incur on every page
# request. Bounded by retained CELLS rather than workbook count: measured at ~53
# bytes per cell, so 750k cells ~= 40 MB per worker, which holds an entire day's
# report set (634,972 cells on 2026-08-17) without evicting on every rail click.
# A workbook ceiling still applies as a belt-and-braces bound.
_PARSE_LRU_MAX = 12
_PARSE_LRU_MAX_CELLS = 750_000
_PARSE_LRU: "OrderedDict[str, dict]" = OrderedDict()
_PARSE_LRU_CELLS = 0
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
def _decode_json(raw: bytes):
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as e:
        raise _SourceError("The reports source returned an unreadable listing.") from e


def _list_via_trees() -> list[dict]:
    """Repo-root files via the Git Trees API — no 1000-entry ceiling.

    Tree blobs carry no ``download_url``, so we build the raw URL ourselves; it is
    used both to fetch the workbook server-side and as the UI's download link.
    Only root-level blobs are returned, matching what the Contents API listed.
    """
    data = _decode_json(_http_get(_TREES_API))
    if not isinstance(data, dict) or not isinstance(data.get("tree"), list):
        raise _SourceError("Unexpected response from the reports source.")
    if data.get("truncated"):
        # Genuinely enormous repo. Log it — this is the one case the Trees API
        # cannot serve in a single call, and it must not fail silently.
        logger.warning("live_reports: git tree response was truncated by GitHub")
    files = []
    for node in data["tree"]:
        if not isinstance(node, dict) or node.get("type") != "blob":
            continue
        path = node.get("path") or ""
        if not path or "/" in path:  # root level only
            continue
        files.append(
            {
                "name": path,
                "type": "file",
                "sha": node.get("sha"),
                "size": node.get("size"),
                "download_url": _RAW_BASE + quote(path),
            }
        )
    return files


def _list_via_contents() -> list[dict]:
    """Fallback listing. Capped at 1000 entries by GitHub — fine as a backstop."""
    data = _decode_json(_http_get(_CONTENTS_API))
    if not isinstance(data, list):
        raise _SourceError("Unexpected response from the reports source.")
    return [f for f in data if isinstance(f, dict) and f.get("type") == "file"]


def _list_repo_files() -> list[dict]:
    """Repo-root file listing (cached ~15 min). Trees API first, Contents as fallback."""
    cached = cache.get("livereports.contents")
    if cached is not None:
        return cached
    try:
        files = _list_via_trees()
    except _SourceError as e:
        logger.warning("live_reports: trees listing failed (%s); falling back to contents",
                       e.__cause__ or e)
        files = _list_via_contents()  # raises _SourceError if this fails too
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
        # The regex only proves the SHAPE is YYYY-MM-DD. Reject impossible dates:
        # "latest" is a string comparison, so `Report-2026-13-45.xlsx` would sort
        # above every real date and hijack the report.
        try:
            datetime.date.fromisoformat(date)
        except ValueError:
            logger.warning("live_reports: ignoring %r — %r is not a real date", name, date)
            continue
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


def _source_updated_iso() -> str | None:
    """Publish time of the newest commit on the source branch — ONE API call.

    The list endpoint needs a timestamp for every report at once. Asking per file
    would be one request per report on a cold cache; because the bot publishes the
    whole set in a single daily commit, the branch head is the same answer for a
    fraction of the cost. The per-file ``_last_commit_iso`` stays authoritative on
    the data endpoint, where only one report is in play.
    """
    ck = "livereports.source_updated"
    cached = cache.get(ck)
    if cached is not None:
        return cached or None
    url = (
        f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits"
        f"?sha={quote(GITHUB_BRANCH)}&per_page=1"
    )
    iso = ""
    try:
        data = json.loads(_http_get(url).decode("utf-8"))
        if isinstance(data, list) and data:
            iso = ((data[0].get("commit") or {}).get("committer") or {}).get("date") or ""
    except (_SourceError, ValueError, UnicodeDecodeError, KeyError, IndexError, TypeError):
        iso = ""
    cache.set(ck, iso, timeout=_LIST_TTL)
    return iso or None


# --- Number formats ----------------------------------------------------------
# Enough of Excel's format grammar to render what these workbooks actually use:
# "General", '"Rs"#,##0', '0.0%', '#,##0', '0.00', and the Indian lakh/crore form
# '[>=10000000]"₹"##\,##\,##\,##0;[>=100000]"₹"##\,##\,##0;"₹"#,##0'.
# Anything unrecognised falls through to the plain rendering — never raises.

_FMT_CONDITION_RE = re.compile(r"\[(>=|<=|<>|>|<|=)\s*(-?[\d.]+)\]")
_FMT_BRACKET_RE = re.compile(r"\[[^\]]*\]")
_FMT_LITERAL_RE = re.compile(r'"([^"]*)"')
_INDIAN_GROUPING_RE = re.compile(r"#,##,#|##\\,##")


def _split_format_sections(fmt: str) -> list[str]:
    """Split on ';' while ignoring separators inside quotes or [] blocks."""
    out, buf, in_quote, depth = [], [], False, 0
    for ch in fmt:
        if ch == '"':
            in_quote = not in_quote
        elif ch == "[" and not in_quote:
            depth += 1
        elif ch == "]" and not in_quote:
            depth = max(0, depth - 1)
        elif ch == ";" and not in_quote and depth == 0:
            out.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    out.append("".join(buf))
    return out


def _pick_format_section(fmt: str, value: float) -> str:
    """Choose the section of a multi-part format that applies to `value`.

    Conditional formats (``[>=100000]...;...``) win by their own test. Otherwise
    Excel's positional rule applies: positive; negative; zero.
    """
    sections = _split_format_sections(fmt)
    conditional = [s for s in sections if _FMT_CONDITION_RE.search(s)]
    if conditional:
        for sec in sections:
            m = _FMT_CONDITION_RE.search(sec)
            if m is None:
                return sec  # the unconditional "everything else" section
            op, raw = m.group(1), m.group(2)
            try:
                threshold = float(raw)
            except ValueError:
                continue
            if (
                (op == ">=" and value >= threshold)
                or (op == ">" and value > threshold)
                or (op == "<=" and value <= threshold)
                or (op == "<" and value < threshold)
                or (op == "=" and value == threshold)
                or (op == "<>" and value != threshold)
            ):
                return sec
        return sections[-1]
    if len(sections) >= 2 and value < 0:
        return sections[1]
    if len(sections) >= 3 and value == 0:
        return sections[2]
    return sections[0]


def _group_indian(digits: str) -> str:
    """1234567 -> 12,34,567 (last three, then pairs)."""
    if len(digits) <= 3:
        return digits
    head, tail = digits[:-3], digits[-3:]
    parts = []
    while len(head) > 2:
        parts.insert(0, head[-2:])
        head = head[:-2]
    if head:
        parts.insert(0, head)
    return ",".join(parts + [tail])


def _decimals_in(pattern: str) -> int:
    """Count the decimal placeholders after the last '.' in a numeric pattern."""
    if "." not in pattern:
        return 0
    tail = pattern.rsplit(".", 1)[1]
    return sum(1 for ch in tail if ch in "0#")


def _format_number(value: float, fmt: str) -> str | None:
    """Render `value` using the Excel number format `fmt`.

    Returns None when the format carries no numeric instruction we understand,
    so the caller can fall back to its plain rendering.
    """
    section = _pick_format_section(fmt, value)

    literals = _FMT_LITERAL_RE.findall(section)
    # Currency can appear as a quoted literal ("Rs") or a locale block ([$₹-en-IN]).
    bracket_currency = ""
    for block in _FMT_BRACKET_RE.findall(section):
        if block.startswith("[$"):
            bracket_currency = block[2:-1].split("-", 1)[0]
    prefix = "".join(literals) + bracket_currency
    for sym in ("₹", "$", "€", "£"):
        if sym in section and sym not in prefix:
            prefix += sym

    # Strip literals / locale blocks / escapes so only the numeric skeleton is left.
    skeleton = _FMT_LITERAL_RE.sub("", section)
    skeleton = _FMT_BRACKET_RE.sub("", skeleton)
    is_percent = "%" in skeleton
    indian = bool(_INDIAN_GROUPING_RE.search(skeleton))
    skeleton = skeleton.replace("\\", "").replace("%", "").replace("_", "").replace("*", "")
    for sym in ("₹", "$", "€", "£"):
        skeleton = skeleton.replace(sym, "")

    if not any(ch in skeleton for ch in "0#?"):
        return None  # e.g. "General", or a pure text/date format

    grouped = "," in skeleton
    decimals = _decimals_in(skeleton)

    n = value * 100 if is_percent else value
    if n != n or n in (float("inf"), float("-inf")):
        return None

    negative = n < 0
    n = abs(n)
    body = f"{n:.{decimals}f}"
    int_part, _, frac_part = body.partition(".")
    if grouped:
        int_part = _group_indian(int_part) if indian else f"{int(int_part):,}"
    text = int_part + ("." + frac_part if frac_part else "")

    out = f"{prefix}{text}"
    if is_percent:
        out += "%"
    if negative:
        # Excel's accounting style wraps negatives in parentheses; everything else
        # takes a leading minus. (A section written as "-#,##0" is spelling that
        # same minus, so it must not suppress it.)
        out = f"({out})" if "(" in section and ")" in section else "-" + out
    return out


# --- Parsing -----------------------------------------------------------------
def _plain_number(v: float | int) -> str:
    if isinstance(v, int):
        return str(v)
    if v != v:  # NaN
        return ""
    if v.is_integer():
        return str(int(v))
    # Keep small magnitudes from collapsing to "0" WITHOUT falling back to
    # scientific notation, which has no place in a price grid.
    text = f"{v:.4f}".rstrip("0").rstrip(".")
    if text in ("", "0", "-0") and v != 0:
        text = f"{v:.12f}".rstrip("0").rstrip(".")
    return text


def _cell(v: Any, number_format: str | None = None) -> str:
    """Stringify a cell for display, honouring the workbook's number format.

    ``number_format`` is what makes a 0.0%-formatted -0.164 read as "-16.4%"
    instead of "-0.164". Formatting never raises: any unknown format falls back
    to the plain rendering.
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)) and number_format and number_format != "General":
        try:
            formatted = _format_number(float(v), number_format)
        except Exception:  # a hostile format string must never break a report
            formatted = None
        if formatted is not None:
            return formatted
    if isinstance(v, (int, float)):
        return _plain_number(v)
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


# A formatted cell that reads as a number: "489", "₹1,234.50", "Rs 489", "-16.4%",
# "12,34,567", "▲ 4.2%". Mirrors the client-side test so alignment agrees.
_NUMERICISH_RE = re.compile(
    r"^[+\-▲▼]?\s*(?:₹|Rs\.?|\$|€|£)?\s*-?\d[\d,]*(?:\.\d+)?\s*(?:%|L|ml|g|kg|x|X)?$",
    re.IGNORECASE,
)


def _numeric_columns(grid: list[list[str]], threshold: float = 0.7) -> list[bool]:
    """Decide, ONCE PER COLUMN over the whole sheet, whether it reads as numeric.

    The client used to infer this from whichever page was on screen, so a column
    could flip between left- and right-aligned as the user paged. Alignment is a
    property of the column, so it is settled here and sent with the response.
    """
    body = grid[1:] if len(grid) > 1 else []
    if not body:
        return []
    width = max((len(r) for r in grid), default=0)
    flags = []
    for c in range(width):
        filled = numeric = 0
        for row in body:
            cell = (row[c] if c < len(row) else "").strip()
            if not cell:
                continue
            filled += 1
            if _NUMERICISH_RE.match(cell):
                numeric += 1
        flags.append(filled > 0 and numeric / filled >= threshold)
    return flags


def _lru_get(sha: str) -> dict | None:
    with _LRU_LOCK:
        parsed = _PARSE_LRU.get(sha)
        if parsed is not None:
            _PARSE_LRU.move_to_end(sha)
        return parsed


def _lru_put(sha: str, parsed: dict) -> None:
    """Insert and evict oldest-first until both bounds hold (cells and count)."""
    global _PARSE_LRU_CELLS
    with _LRU_LOCK:
        prev = _PARSE_LRU.pop(sha, None)
        if prev is not None:
            _PARSE_LRU_CELLS -= prev.get("cells", 0)
        _PARSE_LRU[sha] = parsed
        _PARSE_LRU_CELLS += parsed.get("cells", 0)
        _PARSE_LRU.move_to_end(sha)
        while _PARSE_LRU and (
            len(_PARSE_LRU) > _PARSE_LRU_MAX or _PARSE_LRU_CELLS > _PARSE_LRU_MAX_CELLS
        ):
            if len(_PARSE_LRU) == 1:
                break  # never evict the entry just requested, however large
            _, dropped = _PARSE_LRU.popitem(last=False)
            _PARSE_LRU_CELLS -= dropped.get("cells", 0)


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
        truncated_sheets: list[str] = []
        for name in sheets:
            ws = wb[name]
            grid: list[list[str]] = []
            hit_cap = False
            # Cell objects rather than values_only=True: this is what keeps each
            # cell's number_format, and in read-only mode it costs no extra memory.
            for i, row in enumerate(ws.iter_rows()):
                if i >= _MAX_ROWS:  # bound memory against a pathological sheet
                    hit_cap = True
                    break
                grid.append([_cell(c.value, getattr(c, "number_format", None)) for c in row])
            if hit_cap:
                truncated_sheets.append(name)
            grids[name] = _trim(grid)
    finally:
        wb.close()
    cells = sum(len(r) for g in grids.values() for r in g)
    return {
        "sheets": sheets,
        "grids": grids,
        "date": report["date"],
        "filename": report["filename"],
        "truncated_sheets": truncated_sheets,
        "cells": cells,
    }


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
# Permission code that gates the whole Pricing section. Granted only to selected
# users (plus superusers) — see accounts/migrations/0017. A user who can't see
# the section can't reach its data either.
PRICING_PERMISSION = "pricing.view"
CanViewPricing = require(PRICING_PERMISSION)


@api_view(["GET"])
@permission_classes([IsAuthenticated, CanViewPricing])
def live_reports(request):
    """List the available reports (latest dated file per report)."""
    try:
        reports = _latest_reports()
    except _SourceError as e:
        logger.warning("live_reports source error: %s", e.__cause__ or e)
        return Response({"detail": "The reports source is currently unavailable."}, status=502)
    updated_at = _source_updated_iso()
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
                    "updated_at": updated_at,
                }
                for r in reports
            ],
            "count": len(reports),
            "updated_at": updated_at,
        }
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated, CanViewPricing])
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
                "updated_at": _last_commit_iso(report),
                "download_url": report["download_url"], "sheets": [], "sheet": None,
                "header": [], "rows": [], "count": 0, "page": 0, "page_size": _DEFAULT_PAGE_SIZE,
                "rows_total": 0, "truncated": False,
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
                # Sent so the UI can explain the cap precisely and route oversized
                # sheets to the paginated Table view instead of implying it has
                # shown everything.
                "full_grid_cap": _FULL_GRID_CAP,
                "row_cap_hit": sheet in parsed.get("truncated_sheets", []),
            }
        )

    header = grid[0] if grid else []
    body = grid[1:] if len(grid) > 1 else []
    rows_total = len(body)  # before search, so the UI can say "12 of 4,555"

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
            # Numeric alignment must be a property of the COLUMN, not of whichever
            # page happens to be on screen, so it is decided here over the whole
            # sheet and sent once.
            "numeric_cols": _numeric_columns(grid),
            # `_MAX_ROWS` truncation was previously invisible on this path, so a
            # capped sheet under-reported `count` with no way for the UI to say so.
            "rows_total": rows_total,
            "truncated": sheet in parsed.get("truncated_sheets", []),
            "query": q,
        }
    )
