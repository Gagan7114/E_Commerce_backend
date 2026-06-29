from __future__ import annotations

from django.test import SimpleTestCase

from uploads.views import (
    PRIMARY_PO_JIVO_ONLY_TABLES,
    _default_blank_status_to_pending,
    _filter_primary_jivo_rows,
    _restore_precise_landing_rate,
    _row_mentions_jivo,
)


class PrimaryJivoFilterTests(SimpleTestCase):
    """Primary PO uploads must keep only own-brand SKUs — Jivo or Sano (Amazon excluded)."""

    def test_only_total_po_tables_are_jivo_filtered(self):
        # The non-Amazon primary PO insert tables, and nothing else.
        self.assertEqual(PRIMARY_PO_JIVO_ONLY_TABLES, frozenset({"total_po", "total_po_zbs"}))

    def test_row_mentions_jivo_is_case_insensitive(self):
        self.assertTrue(_row_mentions_jivo({"sku_name": "Jivo Sunflower Oil 1 L"}))
        self.assertTrue(_row_mentions_jivo({"sku_name": "JIVO Olive Oil"}))
        self.assertTrue(_row_mentions_jivo({"sku_name": "premium jivo cold pressed"}))

    def test_sano_brand_is_accepted(self):
        self.assertTrue(_row_mentions_jivo({"sku_name": "Sano - Pomace Olive Oil, 5 L"}))
        self.assertTrue(_row_mentions_jivo({"sku_name": "SANO Pomace Olive Oil, 1 L"}))
        self.assertTrue(_row_mentions_jivo({"sku_name": "premium sano olive"}))

    def test_row_without_known_brand_is_rejected(self):
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
            {"sku_code": "F", "sku_name": "Sano - Pomace Olive Oil, 5 L"},
        ]
        kept, skipped = _filter_primary_jivo_rows(rows)
        self.assertEqual([r["sku_code"] for r in kept], ["A", "C", "F"])
        self.assertEqual([r["sku_code"] for r in skipped], ["B", "D", "E"])


class BlankStatusToPendingTests(SimpleTestCase):
    """Blank PO status must default to PENDING (not the view's EXPIRED default)."""

    def test_blank_status_becomes_pending(self):
        rows = [
            {"sku_code": "A", "status": None},
            {"sku_code": "B", "status": ""},
            {"sku_code": "C", "status": "   "},
            {"sku_code": "D"},  # status key missing entirely
        ]
        n = _default_blank_status_to_pending(rows)
        self.assertEqual(n, 4)
        self.assertTrue(all(r["status"] == "PENDING" for r in rows))

    def test_existing_status_is_left_untouched(self):
        rows = [
            {"sku_code": "A", "status": "EXPIRED"},
            {"sku_code": "B", "status": "COMPLETED"},
            {"sku_code": "C", "status": "PENDING"},
        ]
        n = _default_blank_status_to_pending(rows)
        self.assertEqual(n, 0)
        self.assertEqual([r["status"] for r in rows], ["EXPIRED", "COMPLETED", "PENDING"])


class RestorePreciseLandingRateTests(SimpleTestCase):
    """A landing_rate pre-rounded to a whole rupee is restored to basic_rate x GST
    when (and only when) exactly one standard slab reproduces the rounded value."""

    def test_rounded_gst_value_is_restored(self):
        # Real City Mall PO-1420772 lines: basic x 1.05, file-rounded to a rupee.
        rows = [
            {"sku_code": "A", "basic_rate": "137.14", "landing_rate": "144"},
            {"sku_code": "B", "basic_rate": "150.48", "landing_rate": "158"},
            {"sku_code": "C", "basic_rate": "761.9", "landing_rate": "800"},
        ]
        n = _restore_precise_landing_rate(rows)
        self.assertEqual(n, 3)
        self.assertEqual(
            [r["landing_rate"] for r in rows],
            ["143.9970", "158.0040", "799.9950"],
        )

    def test_18_percent_slab_is_detected(self):
        rows = [{"sku_code": "A", "basic_rate": "137.14", "landing_rate": "162"}]
        n = _restore_precise_landing_rate(rows)
        self.assertEqual(n, 1)
        self.assertEqual(rows[0]["landing_rate"], "161.8252")

    def test_already_decimal_rate_is_untouched(self):
        rows = [{"sku_code": "A", "basic_rate": "150.48", "landing_rate": "158.004"}]
        n = _restore_precise_landing_rate(rows)
        self.assertEqual(n, 0)
        self.assertEqual(rows[0]["landing_rate"], "158.004")

    def test_margin_ratio_is_untouched(self):
        # x1.40 is a margin markup, not a GST slab — must be left as-is.
        rows = [{"sku_code": "A", "basic_rate": "100", "landing_rate": "140"}]
        n = _restore_precise_landing_rate(rows)
        self.assertEqual(n, 0)
        self.assertEqual(rows[0]["landing_rate"], "140")

    def test_exact_whole_rupee_gst_value_is_not_rewritten(self):
        # basic 200 x 1.05 = 210 exactly: already precise, nothing to change.
        rows = [{"sku_code": "A", "basic_rate": "200", "landing_rate": "210"}]
        n = _restore_precise_landing_rate(rows)
        self.assertEqual(n, 0)
        self.assertEqual(rows[0]["landing_rate"], "210")

    def test_missing_or_zero_inputs_are_skipped(self):
        rows = [
            {"sku_code": "A", "basic_rate": "100"},                       # no landing_rate
            {"sku_code": "B", "landing_rate": "105"},                     # no basic_rate
            {"sku_code": "C", "basic_rate": "0", "landing_rate": "0"},    # zero basic
            {"sku_code": "D", "basic_rate": "", "landing_rate": "144"},   # blank basic
        ]
        n = _restore_precise_landing_rate(rows)
        self.assertEqual(n, 0)
        self.assertNotIn("landing_rate", rows[0])
