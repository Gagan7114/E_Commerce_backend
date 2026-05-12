from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
import zipfile
import xml.etree.ElementTree as ET

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction


XML_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
}

CELL_REF_RE = re.compile(r"^([A-Z]+)([0-9]+)$")
REQUIRED_HEADERS = {
    "ASIN": "asin",
    "CATEGORY": "margin_category",
    "MARGIN%": "margin_pct",
}


@dataclass(frozen=True)
class MarginRow:
    asin: str
    margin_category: str | None
    margin_pct: Decimal | None


def _col_to_index(col: str) -> int:
    value = 0
    for char in col:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value - 1


def _clean_header(value) -> str:
    return str(value or "").strip().upper()


def _clean_text(value) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _parse_margin_pct(value) -> Decimal | None:
    if value in (None, ""):
        return None

    text = str(value).strip()
    if not text:
        return None

    has_percent = text.endswith("%")
    if has_percent:
        text = text[:-1].strip()

    number = _parse_decimal(text)
    if number is None:
        return None

    if not has_percent and Decimal("-1") <= number <= Decimal("1"):
        number *= Decimal("100")

    return number.quantize(Decimal("0.0001"))


def _shared_string_text(si: ET.Element) -> str:
    parts: list[str] = []
    for text_node in si.findall(".//main:t", XML_NS):
        parts.append(text_node.text or "")
    return "".join(parts)


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        raw = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []

    root = ET.fromstring(raw)
    return [_shared_string_text(si) for si in root.findall("main:si", XML_NS)]


def _sheet_path_for_name(zf: zipfile.ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rel_targets = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels.findall("pkgrel:Relationship", XML_NS)
    }

    for sheet in workbook.findall(".//main:sheet", XML_NS):
        if sheet.attrib.get("name") != sheet_name:
            continue
        rel_id = sheet.attrib.get(f"{{{XML_NS['rel']}}}id")
        if not rel_id or rel_id not in rel_targets:
            break
        target = rel_targets[rel_id].lstrip("/")
        if not target.startswith("xl/"):
            target = f"xl/{target}"
        return target

    available = [
        sheet.attrib.get("name", "")
        for sheet in workbook.findall(".//main:sheet", XML_NS)
    ]
    raise CommandError(
        f"Sheet {sheet_name!r} was not found. Available sheets: {available}"
    )


def _cell_value(cell: ET.Element, shared_strings: list[str]):
    cell_type = cell.attrib.get("t")
    value_node = cell.find("main:v", XML_NS)

    if cell_type == "inlineStr":
        text_node = cell.find(".//main:t", XML_NS)
        return text_node.text if text_node is not None else None

    if value_node is None:
        return None

    raw = value_node.text
    if raw is None:
        return None

    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (ValueError, IndexError):
            return None

    return raw


def _read_sheet_rows(path: Path, sheet_name: str) -> list[list[object]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings = _load_shared_strings(zf)
        sheet_path = _sheet_path_for_name(zf, sheet_name)
        sheet = ET.fromstring(zf.read(sheet_path))

    rows: list[list[object]] = []
    for row in sheet.findall(".//main:sheetData/main:row", XML_NS):
        values: dict[int, object] = {}
        max_idx = -1
        for cell in row.findall("main:c", XML_NS):
            ref = cell.attrib.get("r", "")
            match = CELL_REF_RE.match(ref)
            if not match:
                continue
            idx = _col_to_index(match.group(1))
            values[idx] = _cell_value(cell, shared_strings)
            max_idx = max(max_idx, idx)
        if max_idx >= 0:
            rows.append([values.get(i) for i in range(max_idx + 1)])
    return rows


def _margin_rows_from_workbook(path: Path, sheet_name: str) -> tuple[list[MarginRow], list[str]]:
    rows = _read_sheet_rows(path, sheet_name)
    if not rows:
        raise CommandError(f"Sheet {sheet_name!r} is empty.")

    headers = [_clean_header(value) for value in rows[0]]
    header_map = {header: idx for idx, header in enumerate(headers) if header}
    missing = [header for header in REQUIRED_HEADERS if header not in header_map]
    if missing:
        raise CommandError(
            f"Sheet {sheet_name!r} is missing required headers: {missing}. "
            f"Found headers: {headers}"
        )

    parsed: list[MarginRow] = []
    warnings: list[str] = []
    seen: dict[str, int] = {}

    asin_idx = header_map["ASIN"]
    category_idx = header_map["CATEGORY"]
    margin_idx = header_map["MARGIN%"]

    for row_number, row in enumerate(rows[1:], start=2):
        asin = _clean_text(row[asin_idx] if asin_idx < len(row) else None)
        if not asin:
            if any(value not in (None, "") for value in row):
                warnings.append(f"Row {row_number}: skipped because ASIN is blank.")
            continue

        margin = _parse_margin_pct(row[margin_idx] if margin_idx < len(row) else None)
        if margin is None:
            warnings.append(f"Row {row_number}: ASIN {asin} has blank/invalid Margin%.")

        if asin in seen:
            warnings.append(
                f"Row {row_number}: duplicate ASIN {asin}; latest row will win."
            )

        seen[asin] = len(parsed)
        parsed.append(
            MarginRow(
                asin=asin,
                margin_category=_clean_text(row[category_idx] if category_idx < len(row) else None),
                margin_pct=margin,
            )
        )

    deduped: dict[str, MarginRow] = {}
    for row in parsed:
        deduped[row.asin] = row

    return list(deduped.values()), warnings


class Command(BaseCommand):
    help = "Import Amazon SEC range margins from the AMAZON SHEET.xlsx MARGINS sheet."

    def add_arguments(self, parser):
        parser.add_argument("workbook", help="Path to AMAZON SHEET.xlsx")
        parser.add_argument(
            "--sheet",
            default="MARGINS",
            help="Workbook sheet to import. Defaults to MARGINS.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse and validate the workbook without writing to the database.",
        )

    def handle(self, *args, **options):
        workbook = Path(options["workbook"]).expanduser()
        sheet_name = options["sheet"]
        dry_run = bool(options["dry_run"])

        if not workbook.exists():
            raise CommandError(f"Workbook does not exist: {workbook}")
        if workbook.suffix.lower() not in {".xlsx", ".xlsm"}:
            raise CommandError("Only .xlsx/.xlsm workbooks are supported.")

        rows, warnings = _margin_rows_from_workbook(workbook, sheet_name)
        for warning in warnings:
            self.stdout.write(self.style.WARNING(warning))

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Parsed {len(rows)} margin rows from {workbook.name}:{sheet_name}."
                )
            )
            return

        with transaction.atomic(), connection.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO amazon_sec_range_margins (
                        asin,
                        margin_category,
                        margin_pct,
                        source_file,
                        source_sheet,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (asin)
                    DO UPDATE SET
                        margin_category = EXCLUDED.margin_category,
                        margin_pct = EXCLUDED.margin_pct,
                        source_file = EXCLUDED.source_file,
                        source_sheet = EXCLUDED.source_sheet,
                        updated_at = NOW()
                    """,
                    [
                        row.asin,
                        row.margin_category,
                        row.margin_pct,
                        str(workbook),
                        sheet_name,
                    ],
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"Imported {len(rows)} margin rows into amazon_sec_range_margins."
            )
        )
