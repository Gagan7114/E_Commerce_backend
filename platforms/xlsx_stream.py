"""Minimal streaming .xlsx writer for large report exports.

openpyxl (even in write-only mode) builds a `Cell` object per value, which costs
~140s for a 428k x 32 export — long enough that the whole response is still being
assembled in memory when the reverse proxy gives up, so the browser only ever
sees "Export failed". This writer emits the SpreadsheetML directly as strings and
pushes them through a ZIP stream, so:

  * bytes start flowing within a second (the proxy sees a live connection instead
    of an idle one, and the browser shows a real download in progress), and
  * the same export finishes ~6x faster with bounded memory - nothing bigger than
    one chunk of rows is ever held.

Only what the reports export needs is implemented: N sheets of flat rows, inline
strings, numbers, and dates/datetimes with an Indian dd-mm-yyyy display format.
No formulas, merges, or styling beyond that.
"""

import re
import zipfile
from datetime import date, datetime, time as _time
from decimal import Decimal

# Flush granularity: accumulate at least this much compressed output before
# yielding a chunk to the WSGI server (fewer, bigger writes down the socket).
_FLUSH_BYTES = 128 * 1024

# Excel's day 0 is 1899-12-30 (it keeps Lotus' fictitious 1900 leap day).
_EPOCH = date(1899, 12, 30).toordinal()

# XML 1.0 forbids most control characters outright; &<> must be escaped. One
# regex search per value is a C-level scan, so clean values (the vast majority)
# cost nothing beyond it.
_XML_UNSAFE = re.compile(r"[&<>\x00-\x08\x0b\x0c\x0e-\x1f]")
_XML_ESCAPES = {"&": "&amp;", "<": "&lt;", ">": "&gt;"}


def _escape(text: str) -> str:
    return _XML_UNSAFE.sub(lambda m: _XML_ESCAPES.get(m.group(), ""), text)


def _datetime_serial(v: datetime) -> float:
    seconds = v.hour * 3600 + v.minute * 60 + v.second + v.microsecond / 1e6
    return v.toordinal() - _EPOCH + seconds / 86400.0


def _cell(v) -> str:
    """One <c> element. Style 1 = dd-mm-yyyy date, style 2 = datetime.

    The optional r="A1" reference is omitted: every cell — empty ones included —
    is emitted in order, so position is unambiguous, and on a 428k x 32 export
    those references alone tripled the finished file (63MB -> 21MB without them).
    Exact class checks come first because they cover ~100% of real rows; the
    isinstance chain below is only reached by subclasses and exotic types.
    """
    if v is None:
        return "<c/>"
    cls = v.__class__
    if cls is str:
        if not v:
            return "<c/>"
        if _XML_UNSAFE.search(v):
            v = _escape(v)
        return f'<c t="inlineStr"><is><t xml:space="preserve">{v}</t></is></c>'
    if cls is int or cls is float or cls is Decimal:
        return f"<c><v>{v}</v></c>"
    if cls is datetime:
        return f'<c s="2"><v>{_datetime_serial(v)}</v></c>'
    if cls is date:
        return f'<c s="1"><v>{v.toordinal() - _EPOCH}</v></c>'
    if cls is bool:
        return f'<c t="b"><v>{1 if v else 0}</v></c>'
    if isinstance(v, bool):
        return f'<c t="b"><v>{1 if v else 0}</v></c>'
    if isinstance(v, (int, float, Decimal)):
        return f"<c><v>{v}</v></c>"
    if isinstance(v, datetime):
        # Excel has no timezone concept; store the wall-clock time as given.
        return f'<c s="2"><v>{_datetime_serial(v.replace(tzinfo=None))}</v></c>'
    if isinstance(v, date):
        return f'<c s="1"><v>{v.toordinal() - _EPOCH}</v></c>'
    if isinstance(v, _time):
        return f'<c s="2"><v>{(v.hour * 3600 + v.minute * 60 + v.second) / 86400.0}</v></c>'
    if isinstance(v, (bytes, bytearray, memoryview)):
        v = bytes(v).decode("utf-8", "replace")
    else:
        v = str(v)
    if not v:
        return "<c/>"
    if _XML_UNSAFE.search(v):
        v = _escape(v)
    return f'<c t="inlineStr"><is><t xml:space="preserve">{v}</t></is></c>'


_SHEET_OPEN = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    "<sheetData>"
)
_SHEET_CLOSE = "</sheetData></worksheet>"

_RELS_ROOT = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/'
    '2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>'
)

# numFmt 164/165 are the custom (>=164 is the user range) date formats the old
# openpyxl path applied per cell; here they are two entries in cellXfs instead.
_STYLES = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    '<numFmts count="2">'
    '<numFmt numFmtId="164" formatCode="DD-MM-YYYY"/>'
    '<numFmt numFmtId="165" formatCode="YYYY-MM-DD HH:MM:SS"/>'
    "</numFmts>"
    '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
    '<fills count="2"><fill><patternFill patternType="none"/></fill>'
    '<fill><patternFill patternType="gray125"/></fill></fills>'
    '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
    '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
    '<cellXfs count="3">'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
    '<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
    '<xf numFmtId="165" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
    "</cellXfs>"
    '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
    "</styleSheet>"
)

_INVALID_SHEET_CHARS = re.compile(r"[\[\]:*?/\\]")


def _safe_sheet_name(name: str, fallback: str) -> str:
    name = _INVALID_SHEET_CHARS.sub(" ", (name or "").strip())[:31]
    return _escape(name) if name else fallback


class _ZipSink:
    """Write target for ZipFile that hands the bytes back instead of storing them.

    Deliberately has no tell()/seek(), which is how zipfile detects a
    non-seekable stream and switches to data-descriptor mode - the only way to
    build a valid archive without ever holding the whole file.
    """

    def __init__(self):
        self._parts = []
        self._size = 0

    def write(self, data):
        self._parts.append(data)
        self._size += len(data)
        return len(data)

    def flush(self):
        pass

    def pending(self) -> int:
        return self._size

    def drain(self) -> bytes:
        if not self._parts:
            return b""
        out = b"".join(self._parts)
        self._parts.clear()
        self._size = 0
        return out


def stream_xlsx(sheets, compresslevel: int = 6):
    """Yield the bytes of an .xlsx built from `sheets`, one chunk at a time.

    `sheets` is a list of (sheet_name, rows) where `rows` is any iterable of row
    sequences - it is consumed lazily, so a database cursor can feed it directly
    and nothing larger than one buffered batch is ever in memory. Compression
    stays at zlib's default 6: on a 428k-row export it costs ~2s more CPU than
    level 1 but ships 14MB less over the wire, which the user waits on for
    longer than the server does.
    """
    sink = _ZipSink()
    zf = zipfile.ZipFile(sink, "w", zipfile.ZIP_DEFLATED, compresslevel=compresslevel)
    names = [
        _safe_sheet_name(name, f"Sheet{i + 1}") for i, (name, _) in enumerate(sheets)
    ]

    content_types = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.'
        'relationships+xml"/><Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.'
        'openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.'
        'openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
    ]
    workbook_sheets, workbook_rels = [], []
    for i, name in enumerate(names, start=1):
        content_types.append(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType='
            '"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
        workbook_sheets.append(f'<sheet name="{name}" sheetId="{i}" r:id="rId{i}"/>')
        workbook_rels.append(
            f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/'
            f'officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
        )
    content_types.append("</Types>")
    style_rel_id = len(names) + 1
    workbook_rels.append(
        f'<Relationship Id="rId{style_rel_id}" Type="http://schemas.openxmlformats.org/'
        'officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    )

    zf.writestr("[Content_Types].xml", "".join(content_types))
    zf.writestr("_rels/.rels", _RELS_ROOT)
    zf.writestr(
        "xl/workbook.xml",
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets>{"".join(workbook_sheets)}</sheets></workbook>',
    )
    zf.writestr(
        "xl/_rels/workbook.xml.rels",
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/'
        f'relationships">{"".join(workbook_rels)}</Relationships>',
    )
    zf.writestr("xl/styles.xml", _STYLES)
    yield sink.drain()

    for i, (_, rows) in enumerate(sheets, start=1):
        with zf.open(f"xl/worksheets/sheet{i}.xml", "w") as part:
            part.write(_SHEET_OPEN.encode())
            buf = []
            for r, row in enumerate(rows, start=1):
                buf.append(f'<row r="{r}">' + "".join(map(_cell, row)) + "</row>")
                if len(buf) >= 500:
                    part.write("".join(buf).encode())
                    buf.clear()
                    if sink.pending() >= _FLUSH_BYTES:
                        yield sink.drain()
            if buf:
                part.write("".join(buf).encode())
            part.write(_SHEET_CLOSE.encode())
        yield sink.drain()

    zf.close()
    tail = sink.drain()
    if tail:
        yield tail
