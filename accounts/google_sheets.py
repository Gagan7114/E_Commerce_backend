from __future__ import annotations

from pathlib import Path
from typing import Any

from django.conf import settings

# Read-only scopes: just enough to open and read a spreadsheet.
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

_CLIENT = None


def _get_client():
    """Return a cached, authenticated gspread client (or raise with a clear reason)."""
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    credentials_file = getattr(settings, "GOOGLE_SHEETS_CREDENTIALS_FILE", "")
    if not credentials_file:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS_FILE is not configured")
    if not Path(credentials_file).exists():
        raise RuntimeError(f"Google Sheets credentials file not found: {credentials_file}")

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "gspread / google-auth are not installed. Run: pip install gspread google-auth"
        ) from exc

    creds = Credentials.from_service_account_file(credentials_file, scopes=_SCOPES)
    _CLIENT = gspread.authorize(creds)
    return _CLIENT


def open_spreadsheet(spreadsheet_id: str | None = None):
    """Open the configured spreadsheet (or an explicit id) and return the gspread object."""
    spreadsheet_id = spreadsheet_id or getattr(settings, "GOOGLE_SHEETS_SPREADSHEET_ID", "")
    if not spreadsheet_id:
        raise RuntimeError("No spreadsheet id provided or configured")
    return _get_client().open_by_key(spreadsheet_id)


def list_worksheets(spreadsheet_id: str | None = None) -> list[str]:
    """Return the titles of all tabs in the spreadsheet."""
    return [ws.title for ws in open_spreadsheet(spreadsheet_id).worksheets()]


def read_worksheet(
    tab_name: str = "MASTER PO",
    spreadsheet_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read a worksheet/tab as a list of row dicts keyed by the header row.

    The first row is treated as the header. Every following row becomes a dict
    mapping column header -> cell value (strings, exactly as they appear).
    """
    worksheet = open_spreadsheet(spreadsheet_id).worksheet(tab_name)
    return worksheet.get_all_records()
