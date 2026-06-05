from __future__ import annotations

from django.test import SimpleTestCase

from uploads.views import (
    PRIMARY_PO_JIVO_ONLY_TABLES,
    _filter_primary_jivo_rows,
    _row_mentions_jivo,
)


class PrimaryJivoFilterTests(SimpleTestCase):
    """Primary PO uploads must keep only Jivo-branded SKUs (Amazon excluded)."""

    def test_only_total_po_tables_are_jivo_filtered(self):
        # The non-Amazon primary PO insert tables, and nothing else.
        self.assertEqual(PRIMARY_PO_JIVO_ONLY_TABLES, frozenset({"total_po", "total_po_zbs"}))

    def test_row_mentions_jivo_is_case_insensitive(self):
        self.assertTrue(_row_mentions_jivo({"sku_name": "Jivo Sunflower Oil 1 L"}))
        self.assertTrue(_row_mentions_jivo({"sku_name": "JIVO Olive Oil"}))
        self.assertTrue(_row_mentions_jivo({"sku_name": "premium jivo cold pressed"}))

    def test_row_without_jivo_is_rejected(self):
        self.assertFalse(_row_mentions_jivo({"sku_name": "Morton Pure Ghee 100 ml"}))
        self.assertFalse(_row_mentions_jivo({"sku_name": ""}))
        self.assertFalse(_row_mentions_jivo({"sku_name": None}))
        self.assertFalse(_row_mentions_jivo({}))

    def test_filter_splits_rows_preserving_order(self):
        rows = [
            {"sku_code": "A", "sku_name": "Jivo Sunflower Oil 1 L"},
            {"sku_code": "B", "sku_name": "Morton Pure Ghee 100 ml"},
            {"sku_code": "C", "sku_name": "JIVO Olive Oil"},
            {"sku_code": "D", "sku_name": ""},
            {"sku_code": "E", "sku_name": None},
        ]
        kept, skipped = _filter_primary_jivo_rows(rows)
        self.assertEqual([r["sku_code"] for r in kept], ["A", "C"])
        self.assertEqual([r["sku_code"] for r in skipped], ["B", "D", "E"])
