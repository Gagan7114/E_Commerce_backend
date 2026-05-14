from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TransactionTestCase
from rest_framework.test import APIClient

from uploads.amazon_uploads import REPORTS, parse_uploaded_file


CREATE_SCHEMA_SQL = [
    "DROP SCHEMA IF EXISTS raw CASCADE",
    "DROP SCHEMA IF EXISTS quality CASCADE",
    "DROP SCHEMA IF EXISTS staging CASCADE",
    "DROP SCHEMA IF EXISTS reporting CASCADE",
    "DROP SCHEMA IF EXISTS master CASCADE",
    "DROP TABLE IF EXISTS public.master_sheet CASCADE",
    "DROP TABLE IF EXISTS public.amazon_asin_margin CASCADE",
    "DROP TABLE IF EXISTS public.fc_city_state_channel_master CASCADE",
    "CREATE SCHEMA raw",
    "CREATE SCHEMA quality",
    "CREATE SCHEMA staging",
    "CREATE SCHEMA reporting",
    "CREATE SCHEMA master",
    """
    CREATE TABLE raw.upload_file (
        upload_id BIGSERIAL PRIMARY KEY,
        main_table_name TEXT NOT NULL,
        raw_file_name TEXT NOT NULL,
        original_file_name TEXT NOT NULL,
        stored_file_path TEXT,
        file_hash TEXT,
        file_extension VARCHAR(20),
        uploaded_by TEXT,
        uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        status VARCHAR(40) NOT NULL DEFAULT 'uploaded',
        row_count INTEGER DEFAULT 0,
        error_count INTEGER DEFAULT 0,
        warning_count INTEGER DEFAULT 0,
        metadata JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE quality.validation_error (
        validation_error_id BIGSERIAL PRIMARY KEY,
        upload_id BIGINT NOT NULL,
        main_table_name TEXT NOT NULL,
        raw_file_name TEXT NOT NULL,
        row_number INTEGER,
        field_name TEXT,
        error_type TEXT NOT NULL,
        error_message TEXT NOT NULL,
        severity VARCHAR(20) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE quality.upload_validation_summary (
        summary_id BIGSERIAL PRIMARY KEY,
        upload_id BIGINT NOT NULL UNIQUE,
        main_table_name TEXT NOT NULL,
        raw_file_name TEXT NOT NULL,
        total_rows INTEGER DEFAULT 0,
        valid_rows INTEGER DEFAULT 0,
        error_rows INTEGER DEFAULT 0,
        warning_rows INTEGER DEFAULT 0,
        final_inserted_rows INTEGER DEFAULT 0,
        final_updated_rows INTEGER DEFAULT 0,
        status VARCHAR(40) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE staging."amazon data" (
        stg_amazon_data_id BIGSERIAL PRIMARY KEY,
        upload_id BIGINT NOT NULL,
        raw_row_number INTEGER NOT NULL,
        po_number TEXT,
        vendor_code TEXT,
        order_date DATE,
        status TEXT,
        product_name TEXT,
        asin TEXT,
        external_id_type TEXT,
        external_id TEXT,
        model_number TEXT,
        merchant_sku TEXT,
        catalog_number TEXT,
        availability TEXT,
        requested_quantity NUMERIC,
        accepted_quantity NUMERIC,
        asn_quantity NUMERIC,
        received_quantity NUMERIC,
        cancelled_quantity NUMERIC,
        remaining_quantity NUMERIC,
        ship_to_location TEXT,
        window_start DATE,
        window_end DATE,
        case_size NUMERIC,
        cost NUMERIC,
        currency TEXT,
        total_requested_cost NUMERIC,
        total_accepted_cost NUMERIC,
        total_received_cost NUMERIC,
        total_cancelled_cost NUMERIC,
        expected_date DATE,
        freight_terms TEXT,
        consolidation_id TEXT,
        cancellation_deadline DATE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE staging."appointment data" (
        stg_appointment_data_id BIGSERIAL PRIMARY KEY,
        upload_id BIGINT NOT NULL,
        raw_row_number INTEGER NOT NULL,
        appointment_id TEXT,
        status TEXT,
        appointment_time TIMESTAMP,
        creation_date DATE,
        pos TEXT,
        destination_fc TEXT,
        pro TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE public.master_sheet (
        format_sku_code VARCHAR,
        product_name TEXT,
        item TEXT,
        format TEXT,
        sku_sap_code VARCHAR,
        sku_sap_name VARCHAR,
        category TEXT,
        sub_category TEXT,
        case_pack INTEGER,
        per_unit VARCHAR,
        item_head TEXT,
        brand TEXT,
        uom TEXT,
        per_unit_value REAL,
        tax_rate NUMERIC,
        category_head TEXT,
        is_litre TEXT,
        is_litre_oil TEXT,
        packaging_cost NUMERIC
    )
    """,
    """
    CREATE TABLE public.amazon_asin_margin (
        id INTEGER PRIMARY KEY,
        asin VARCHAR,
        category TEXT,
        margin_percent NUMERIC
    )
    """,
    """
    CREATE TABLE public.fc_city_state_channel_master (
        id INTEGER PRIMARY KEY,
        fc VARCHAR,
        city VARCHAR,
        state VARCHAR,
        channel VARCHAR
    )
    """,
    """
    CREATE TABLE master.fc_master (
        fc_id BIGSERIAL PRIMARY KEY,
        fc_code TEXT NOT NULL UNIQUE,
        fc_name TEXT,
        city TEXT,
        state TEXT,
        region TEXT,
        is_active BOOLEAN NOT NULL DEFAULT true,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE reporting."appointment" (
        appointment_line_key TEXT NOT NULL UNIQUE,
        appointment_id TEXT NOT NULL,
        status TEXT,
        appointment_time TIMESTAMP,
        creation_date DATE,
        pos TEXT,
        destination_fc TEXT,
        pro TEXT,
        month TEXT,
        year INTEGER,
        upload_id BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE reporting."Amazon PO" (
        amazon_po_id BIGSERIAL PRIMARY KEY,
        source_line_key TEXT NOT NULL UNIQUE,
        po_number TEXT NOT NULL,
        order_date DATE,
        expiry_date DATE,
        status TEXT,
        availability_status TEXT,
        external_id TEXT,
        asin TEXT,
        merchant_sku TEXT,
        sku_code TEXT,
        sku_name TEXT,
        requested_qty NUMERIC,
        accepted_qty NUMERIC,
        received_qty NUMERIC,
        cancelled_qty NUMERIC,
        fulfillment_center TEXT,
        cost_price NUMERIC,
        total_requested_cost NUMERIC,
        total_accepted_cost NUMERIC,
        total_received_cost NUMERIC,
        total_cancelled_cost NUMERIC,
        vendor TEXT,
        days_to_expiry INTEGER,
        po_window INTEGER,
        po_status TEXT,
        item_status TEXT,
        item TEXT,
        sap_sku_name TEXT,
        sap_sku_code TEXT,
        category TEXT,
        sub_category TEXT,
        case_pack NUMERIC,
        requested_boxes NUMERIC,
        accepted_boxes NUMERIC,
        per_ltr_unit TEXT,
        per_liter NUMERIC,
        total_order_liters NUMERIC,
        total_accepted_liters NUMERIC,
        total_delivered_liters NUMERIC,
        total_order_amt_exclusive NUMERIC,
        total_deliver_amt_exclusive NUMERIC,
        po_month INTEGER,
        year INTEGER,
        item_head TEXT,
        city TEXT,
        state TEXT,
        distributor_margin NUMERIC,
        tax NUMERIC,
        brand TEXT,
        category_head TEXT,
        core_fresh_now TEXT,
        order_ltrs_cl NUMERIC,
        missed_ltrs NUMERIC,
        filled_ltrs NUMERIC,
        order_unit_cl NUMERIC,
        missed_unit NUMERIC,
        filled_units NUMERIC,
        fill_rate NUMERIC,
        miss_rate NUMERIC,
        helper TEXT,
        upload_id BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
]


def csv_file(name: str, content: str) -> SimpleUploadedFile:
    return SimpleUploadedFile(name, content.encode("utf-8"), content_type="text/csv")


class AmazonUploadTests(TransactionTestCase):
    reset_sequences = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with connection.cursor() as cur:
            for statement in CREATE_SCHEMA_SQL:
                cur.execute(statement)

    @classmethod
    def tearDownClass(cls):
        with connection.cursor() as cur:
            for schema in ["raw", "quality", "staging", "reporting", "master"]:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute("DROP TABLE IF EXISTS public.master_sheet CASCADE")
        super().tearDownClass()

    def setUp(self):
        self.client = APIClient()
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            email="admin@example.com",
            password="password",
        )
        self.client.force_authenticate(self.user)
        with connection.cursor() as cur:
            cur.execute(
                """
                TRUNCATE raw.upload_file, quality.validation_error,
                         quality.upload_validation_summary,
                         staging."amazon data", staging."appointment data",
                         reporting."Amazon PO", reporting."appointment",
                         public.master_sheet, master.fc_master
                RESTART IDENTITY
                """
            )

    def test_header_normalization_maps_aliases(self):
        rows, _, _ = parse_uploaded_file(
            config=REPORTS["AMAZON_PO"],
            content=(
                "PO,ASIN,External ID,Order date,Ship-to location\n"
                "PO1,ASIN1,EXT1,2026-05-01,FC1\n"
            ).encode(),
            extension=".csv",
        )
        self.assertEqual(rows[0]["po_number"], "PO1")
        self.assertEqual(rows[0]["external_id"], "EXT1")

        rows, _, _ = parse_uploaded_file(
            config=REPORTS["APPOINTMENT"],
            content=(
                "Appointment Id,Appointment Time,Destination FC\n"
                "A1,2026-05-02 10:30,FC1\n"
            ).encode(),
            extension=".csv",
        )
        self.assertEqual(rows[0]["appointment_id"], "A1")

    def test_excel_pasted_tabs_win_over_commas_inside_product_names(self):
        rows, issues, rows_received = parse_uploaded_file(
            config=REPORTS["AMAZON_PO"],
            content=(
                "PO\tVendor code\tOrder date\tStatus\tProduct name\tASIN\tExternal ID\tMerchant SKU\t"
                "Requested quantity\tShip-to location\n"
                "PO1\tVEND\t8-May-26\tConfirmed\tOil for Roasting, Frying, Baking | 1 Litre\t"
                "ASIN1\tEXT1\tMSKU1\t10\tDED5\n"
            ).encode(),
            extension=".csv",
        )
        self.assertEqual(rows_received, 1)
        self.assertEqual(rows[0]["po_number"], "PO1")
        self.assertEqual(rows[0]["order_date"], date(2026, 5, 8))
        self.assertEqual(rows[0]["ship_to_location"], "DED5")
        self.assertEqual(rows[0]["external_id"], "EXT1")
        self.assertFalse(
            [
                item
                for item in issues
                if item.get("error_type") == "missing_required_column"
            ]
        )

    def test_amazon_po_full_month_name_dates_parse(self):
        rows, issues, rows_received = parse_uploaded_file(
            config=REPORTS["AMAZON_PO"],
            content=(
                "PO,ASIN,External ID,Order date,Ship-to location,Cancellation deadline\n"
                "PO1,ASIN1,EXT1,14-May-2026,DED5,05-June-2026\n"
            ).encode(),
            extension=".csv",
        )
        self.assertEqual(rows_received, 1)
        self.assertEqual(rows[0]["order_date"], date(2026, 5, 14))
        self.assertEqual(rows[0]["cancellation_deadline"], date(2026, 6, 5))
        self.assertFalse(
            [
                item
                for item in issues
                if item.get("field_name") == "cancellation_deadline"
                and item.get("error_type") == "invalid_date"
            ]
        )

    def test_appointment_upload_stages_and_upserts(self):
        payload = {
            "report_type": "APPOINTMENT",
            "file": csv_file(
                "appointment.csv",
                "Appointment ID,Status,Appointment Time,Creation Date,POs,Destination FC,PRO\n"
                "APT1,Scheduled,2026-05-02 10:30,2026-05-01,PO1,FC1,PRO1\n",
            ),
        }
        response = self.client.post("/api/uploads", payload, format="multipart")
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_inserted_staging"], 1)
        self.assertEqual(response.data["rows_inserted_final"], 1)

        with connection.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM staging."appointment data"')
            self.assertEqual(cur.fetchone()[0], 1)
            cur.execute(
                'SELECT status, month, year FROM reporting."appointment" WHERE appointment_id = %s',
                ["APT1"],
            )
            self.assertEqual(cur.fetchone(), ("Scheduled", "MAY", 2026))

        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "APPOINTMENT",
                "file": csv_file(
                    "appointment-update.csv",
                    "Appointment ID,Status,Appointment Time,Creation Date,POs,Destination FC,PRO\n"
                    "APT1,Closed,2026-05-03 11:00,2026-05-01,PO1,FC1,PRO1\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_updated_final"], 1)
        with connection.cursor() as cur:
            cur.execute('SELECT COUNT(*), MAX(status) FROM reporting."appointment"')
            self.assertEqual(cur.fetchone(), (1, "Closed"))

    def test_direct_paste_upload_works_without_file_picker(self):
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "APPOINTMENT",
                "pasted_data": (
                    "Appointment ID\tStatus\tAppointment Time\tCreation Date\tPOs\tDestination FC\tPRO\n"
                    "APT-PASTE\tScheduled\t2026-05-04 09:00\t2026-05-01\tPO2\tFC2\tPRO2"
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_inserted_staging"], 1)
        self.assertEqual(response.data["rows_inserted_final"], 1)
        history_response = self.client.get("/api/uploads")
        self.assertEqual(history_response.status_code, 200, history_response.data)
        self.assertEqual(history_response.data["count"], 1)
        self.assertEqual(history_response.data["results"][0]["report_type"], "APPOINTMENT")
        with connection.cursor() as cur:
            cur.execute(
                'SELECT appointment_id, destination_fc FROM reporting."appointment" WHERE appointment_id = %s',
                ["APT-PASTE"],
            )
            self.assertEqual(cur.fetchone(), ("APT-PASTE", "FC2"))

    def test_appointment_ist_datetime_parses(self):
        rows, issues, rows_received = parse_uploaded_file(
            config=REPORTS["APPOINTMENT"],
            content=(
                "Appointment ID\tStatus\tAppointment Time\tCreation Date\tDestination FC\n"
                "APT-IST\tConfirmed\t23/05/2026 08:30 AM IST\t08/05/2026\tDED5\n"
            ).encode(),
            extension=".csv",
        )
        self.assertEqual(rows_received, 1)
        self.assertEqual(rows[0]["appointment_time"], datetime(2026, 5, 23, 8, 30))
        self.assertEqual(rows[0]["creation_date"], date(2026, 5, 8))
        self.assertFalse([item for item in issues if item.get("severity") == "error"])

    def test_appointment_same_id_with_different_pos_creates_po_rows(self):
        # Appointment rows are keyed by appointment_id + PO, so one appointment
        # can appear as separate final rows for each PO.
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "APPOINTMENT",
                "file": csv_file(
                    "appointment-lines.csv",
                    "Appointment ID,Status,Appointment Time,Creation Date,POs,Destination FC,PRO\n"
                    "5.45E+11,Confirmed,23/05/2026 08:30 AM IST,5/8/2026,8QWBFBWG,DED5,VAS1\n"
                    "5.45E+11,Confirmed,23/05/2026 08:30 AM IST,5/8/2026,56LAFBLG,DED5,VAS1\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_inserted_final"], 2)
        with connection.cursor() as cur:
            cur.execute(
                'SELECT appointment_id, pos, creation_date, month '
                'FROM reporting."appointment" ORDER BY pos'
            )
            rows = cur.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "545000000000")
        self.assertEqual([row[1] for row in rows], ["56LAFBLG", "8QWBFBWG"])
        self.assertEqual(rows[0][2], date(2026, 5, 8))
        self.assertEqual(rows[0][3], "MAY")

    def test_appointment_single_row_with_multiple_pos_splits_to_rows(self):
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "APPOINTMENT",
                "pasted_data": (
                    "Appointment ID\tStatus\tAppointment Time\tCreation Date\tPOs\tDestination FC\tPRO\n"
                    "APT-MULTI\tConfirmed\t23/05/2026 08:30 AM IST\t5/8/2026\tPO-A, PO-B; PO-C\tDED5\tVAS1\n"
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_inserted_final"], 3)
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT appointment_id, status, appointment_time, creation_date,
                       pos, destination_fc, pro, month, year
                  FROM reporting."appointment"
                 ORDER BY pos
                """
            )
            rows = cur.fetchall()
        self.assertEqual([row[4] for row in rows], ["PO-A", "PO-B", "PO-C"])
        self.assertTrue(all(row[0] == "APT-MULTI" for row in rows))
        self.assertTrue(all(row[1] == "Confirmed" for row in rows))
        self.assertTrue(all(row[5] == "DED5" for row in rows))
        self.assertTrue(all(row[6] == "VAS1" for row in rows))
        self.assertTrue(all(row[7] == "MAY" for row in rows))
        self.assertTrue(all(row[8] == 2026 for row in rows))

    def test_appointment_reupload_same_id_replaces_removed_pos(self):
        # Re-uploading the same appointment_id with a different PO should remove
        # the old PO row and keep only the latest uploaded PO set.
        first_csv = (
            "Appointment ID,Status,Appointment Time,Creation Date,POs,Destination FC,PRO\n"
            "APT-001,Confirmed,23/05/2026 08:30 AM IST,5/8/2026,OLD-PO,DED5,VAS1\n"
        )
        second_csv = (
            "Appointment ID,Status,Appointment Time,Creation Date,POs,Destination FC,PRO\n"
            "APT-001,Confirmed,23/05/2026 08:30 AM IST,5/8/2026,NEW-PO,DED5,VAS1\n"
        )
        r1 = self.client.post(
            "/api/uploads",
            {"report_type": "APPOINTMENT", "file": csv_file("a1.csv", first_csv)},
            format="multipart",
        )
        self.assertEqual(r1.status_code, 200, r1.data)
        self.assertEqual(r1.data["rows_inserted_final"], 1)
        self.assertEqual(r1.data["rows_updated_final"], 0)

        r2 = self.client.post(
            "/api/uploads",
            {"report_type": "APPOINTMENT", "file": csv_file("a2.csv", second_csv)},
            format="multipart",
        )
        self.assertEqual(r2.status_code, 200, r2.data)
        self.assertEqual(r2.data["rows_inserted_final"], 1)
        self.assertEqual(r2.data["rows_updated_final"], 0)

        with connection.cursor() as cur:
            cur.execute('SELECT COUNT(*), MAX(pos) FROM reporting."appointment"')
            count, pos = cur.fetchone()
        self.assertEqual(count, 1)
        self.assertEqual(pos, "NEW-PO")

    def test_direct_paste_with_no_data_rows_fails(self):
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "APPOINTMENT",
                "pasted_data": "Appointment ID\tStatus\tAppointment Time\tDestination FC",
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["status"], "failed")
        self.assertEqual(response.data["rows_received"], 0)
        self.assertEqual(response.data["rows_inserted_staging"], 0)
        self.assertIn("no_data_rows", {item["error_type"] for item in response.data["errors"]})

    def test_duplicate_check_ignores_failed_uploads_and_allows_reupload(self):
        bad_appointment = (
            "Appointment ID\tStatus\tAppointment Time\tDestination FC\n"
            "APT-BAD\tScheduled\tnot-a-date\tFC1"
        )
        response = self.client.post(
            "/api/uploads",
            {"report_type": "APPOINTMENT", "pasted_data": bad_appointment},
            format="multipart",
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["status"], "failed")

        response = self.client.post(
            "/api/uploads",
            {"report_type": "APPOINTMENT", "pasted_data": bad_appointment},
            format="multipart",
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["status"], "failed")
        self.assertNotEqual(response.data["status"], "duplicate")

        good_appointment = (
            "Appointment ID\tStatus\tAppointment Time\tDestination FC\n"
            "APT-SAME\tScheduled\t2026-05-04 09:00\tFC1"
        )
        response = self.client.post(
            "/api/uploads",
            {"report_type": "APPOINTMENT", "pasted_data": good_appointment},
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)

        response = self.client.post(
            "/api/uploads",
            {"report_type": "AMAZON_PO", "pasted_data": good_appointment},
            format="multipart",
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["status"], "failed")
        self.assertIn("missing_required_column", {item["error_type"] for item in response.data["errors"]})

        response = self.client.post(
            "/api/uploads",
            {"report_type": "APPOINTMENT", "pasted_data": good_appointment},
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["status"], "completed")
        self.assertEqual(response.data["rows_updated_final"], 1)

    def test_amazon_po_upload_enriches_and_upserts(self):
        with connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.master_sheet (
                    format, format_sku_code, product_name, brand, item,
                    sku_sap_name, sku_sap_code, category, sub_category,
                    case_pack, per_unit_value, per_unit, item_head,
                    category_head, uom, tax_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    "AMAZON",
                    "ASIN1",
                    "Oil",
                    "JIVO",
                    "Oil",
                    "SAP Oil",
                    "SAP1",
                    "Food",
                    "Oil",
                    10,
                    0.5,
                    "0.5 LTR",
                    "PREMIUM",
                    "Head",
                    "LTR",
                    Decimal("0.05"),
                ],
            )
            cur.execute(
                "INSERT INTO master.fc_master (fc_code, fc_name, city, state) VALUES (%s, %s, %s, %s)",
                ["FC1", "FC One", "Delhi", "Delhi"],
            )
            cur.execute(
                """
                INSERT INTO public.amazon_asin_margin (id, asin, category, margin_percent)
                VALUES (%s, %s, %s, %s)
                """,
                [1, "ASIN1", "Oil", Decimal("25.00")],
            )
            cur.execute(
                """
                INSERT INTO public.fc_city_state_channel_master (id, fc, city, state, channel)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [1, "FC1", "Delhi", "Delhi", "CORE"],
            )

        content = (
            "PO,Vendor code,Order date,Status,Product name,ASIN,External ID,Merchant SKU,"
            "Availability,Requested quantity,Accepted quantity,Received quantity,Cancelled quantity,"
            "Ship-to location,Window start,Window end,Case size,Total requested cost,"
            "Total accepted cost,Total received cost,Total cancelled cost,Expected date,Cancellation deadline\n"
            "PO1,VEND,2026-05-01,Open,Oil,ASIN1,EXT1,MSKU1,Available,100,90,80,20,"
            "FC1,2026-05-04,2026-05-05,10,1000,900,800,200,2026-05-10,2026-05-09\n"
        )
        response = self.client.post(
            "/api/uploads",
            {"report_type": "AMAZON_PO", "file": csv_file("amazon-po.csv", content)},
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_inserted_final"], 1)

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT sku_code, city, cost_price, requested_boxes, per_ltr_unit,
                       per_liter, total_order_liters, fill_rate, miss_rate, brand, tax,
                       distributor_margin, core_fresh_now
                  FROM reporting."Amazon PO"
                 WHERE po_number = %s
                """,
                ["PO1"],
            )
            row = cur.fetchone()
        self.assertEqual(row[0], "ASIN1")
        self.assertEqual(row[1], "Delhi")
        self.assertIsNone(row[2])
        self.assertEqual(row[3], Decimal("10"))
        self.assertEqual(row[4], "0.5 LTR")
        self.assertEqual(row[5], Decimal("0.5"))
        self.assertEqual(row[6], Decimal("50.0"))
        self.assertEqual(row[7], Decimal("0.8"))
        self.assertEqual(row[8], Decimal("0.2"))
        self.assertEqual(row[9], "JIVO")
        self.assertEqual(row[10], Decimal("0.05"))
        self.assertEqual(row[11], Decimal("25.00"))
        self.assertEqual(row[12], "CORE")

        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "AMAZON_PO",
                "file": csv_file(
                    "amazon-po-update.csv",
                    content.replace("ASIN1,EXT1,MSKU1", "ASIN1,EXT2,MSKU2")
                    .replace(",80,20,", ",90,10,"),
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_updated_final"], 1)
        with connection.cursor() as cur:
            cur.execute(
                'SELECT COUNT(*), MAX(received_qty), MAX(external_id), MAX(merchant_sku) '
                'FROM reporting."Amazon PO"'
            )
            self.assertEqual(cur.fetchone(), (1, Decimal("90"), "EXT2", "MSKU2"))

    def test_amazon_po_status_uses_excel_formula_order(self):
        with connection.cursor() as cur:
            cur.execute(
                "INSERT INTO master.fc_master (fc_code, fc_name, city, state) VALUES (%s, %s, %s, %s)",
                ["FC1", "FC One", "Delhi", "Delhi"],
            )

        content = (
            "PO,Order date,Status,ASIN,Availability,Requested quantity,Accepted quantity,"
            "Received quantity,Cancelled quantity,Ship-to location,Cancellation deadline\n"
            "PO-STATUS,2026-05-01,Confirmed,ASIN-EXPIRED,AC - Accepted: In stock,10,5,0,0,FC1,2000-01-01\n"
            "PO-STATUS,2026-05-01,Confirmed,ASIN-MOV,OS - Cancelled: Out of stock,10,0,0,0,FC1,2099-01-01\n"
            "PO-STATUS,2026-05-01,Closed,ASIN-CANCELLED,OS - Cancelled: Out of stock,10,0,0,0,FC1,2099-01-01\n"
            "PO-STATUS,2026-05-01,Confirmed,ASIN-COMPLETED,AC - Accepted: In stock,10,5,2,0,FC1,2099-01-01\n"
            "PO-STATUS,2026-05-01,Unconfirmed,ASIN-PENDING,AC - Accepted: In stock,10,0,0,0,FC1,2099-01-01\n"
        )
        response = self.client.post(
            "/api/uploads",
            {"report_type": "AMAZON_PO", "file": csv_file("po-status.csv", content)},
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["rows_inserted_final"], 5)

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT asin, po_status
                  FROM reporting."Amazon PO"
                 WHERE po_number = %s
                 ORDER BY asin
                """,
                ["PO-STATUS"],
            )
            rows = dict(cur.fetchall())

        self.assertEqual(rows["ASIN-CANCELLED"], "CANCELLED")
        self.assertEqual(rows["ASIN-COMPLETED"], "COMPLETED")
        self.assertEqual(rows["ASIN-EXPIRED"], "EXPIRED")
        self.assertEqual(rows["ASIN-MOV"], "MOV")
        self.assertEqual(rows["ASIN-PENDING"], "PENDING")

    def test_validation_errors_and_warnings_are_stored(self):
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "APPOINTMENT",
                "file": csv_file(
                    "bad-appointment.csv",
                    "Appointment ID,Status,Appointment Time,Destination FC\n"
                    ",Scheduled,not-a-date,FC1\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["status"], "failed")
        self.assertGreaterEqual(response.data["error_count"], 2)

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT error_type, severity
                  FROM quality.validation_error
                 WHERE upload_id = %s
                 ORDER BY validation_error_id
                """,
                [response.data["upload_id"]],
            )
            errors = cur.fetchall()
        self.assertIn(("missing_required_value", "error"), errors)
        self.assertIn(("invalid_date", "error"), errors)

        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "AMAZON_PO",
                "file": csv_file(
                    "missing-master.csv",
                    "PO,Order date,ASIN,Ship-to location,External ID\n"
                    "PO2,2026-05-01,ASIN-MISSING,UNKNOWN,EXT-MISSING\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["status"], "partially_successful")
        warning_types = {item["error_type"] for item in response.data["warnings"]}
        self.assertIn("master_sheet_missing", warning_types)
        self.assertIn("fc_master_missing", warning_types)

    def test_business_warning_database_error_returns_structured_response(self):
        def broken_warning_check(cur, *, config, upload_id):
            cur.execute("SELECT * FROM missing_upload_warning_table")

        with patch("uploads.amazon_uploads._add_business_warnings", broken_warning_check):
            response = self.client.post(
                "/api/uploads",
                {
                    "report_type": "AMAZON_PO",
                    "pasted_data": (
                        "PO\tOrder date\tStatus\tASIN\tRequested quantity\t"
                        "Ship-to location\tWindow end\tCase size\n"
                        "PO-WARN\t2026-05-01\tUnconfirmed\tASIN-WARN\t6\tFC1\t2026-05-25\t1"
                    ),
                },
                format="multipart",
            )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["status"], "partially_successful")
        self.assertIn(
            "business_warning_check_failed",
            {item["error_type"] for item in response.data["warnings"]},
        )

    def test_transform_database_error_returns_structured_response(self):
        def broken_transform(cur, *, config, upload_id):
            cur.execute("SELECT * FROM missing_upload_transform_table")

        with patch("uploads.amazon_uploads._run_transform", broken_transform):
            response = self.client.post(
                "/api/uploads",
                {
                    "report_type": "AMAZON_PO",
                    "pasted_data": (
                        "PO\tOrder date\tStatus\tASIN\tRequested quantity\t"
                        "Ship-to location\tWindow end\tCase size\n"
                        "PO-TRANSFORM\t2026-05-01\tUnconfirmed\tASIN-TRANSFORM\t6\tFC1\t2026-05-25\t1"
                    ),
                },
                format="multipart",
            )

        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["status"], "failed")
        self.assertIn("transform_failed", {item["error_type"] for item in response.data["errors"]})

    def test_summary_database_error_does_not_fail_successful_upload(self):
        def broken_summary(cur, **kwargs):
            cur.execute("SELECT * FROM missing_upload_summary_table")

        with patch("uploads.amazon_uploads._upsert_summary", broken_summary):
            response = self.client.post(
                "/api/uploads",
                {
                    "report_type": "AMAZON_PO",
                    "pasted_data": (
                        "PO\tOrder date\tStatus\tASIN\tRequested quantity\t"
                        "Ship-to location\tWindow end\tCase size\n"
                        "PO-SUMMARY\t2026-05-01\tUnconfirmed\tASIN-SUMMARY\t6\tFC1\t2026-05-25\t1"
                    ),
                },
                format="multipart",
            )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertIn(
            "summary_update_failed",
            {item["error_type"] for item in response.data["warnings"]},
        )

    def test_upload_detail_diagnostic_error_returns_upload(self):
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "AMAZON_PO",
                "pasted_data": (
                    "PO\tOrder date\tStatus\tASIN\tRequested quantity\t"
                    "Ship-to location\tWindow end\tCase size\n"
                    "PO-DIAG\t2026-05-01\tUnconfirmed\tASIN-DIAG\t6\tFC1\t2026-05-25\t1"
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)

        def broken_diagnostics(cur, upload_id, summary):
            cur.execute("SELECT * FROM missing_upload_diag_table")

        with patch("uploads.amazon_uploads._amazon_upload_diagnostics", broken_diagnostics):
            detail_response = self.client.get(f"/api/uploads/{response.data['upload_id']}")

        self.assertEqual(detail_response.status_code, 200, detail_response.data)
        diagnostic_errors = detail_response.data["diagnostics"].get("diagnostic_errors", [])
        self.assertEqual(diagnostic_errors[0]["type"], "amazon_po_diagnostics_failed")

    def test_non_litre_master_gaps_do_not_create_formula_warnings(self):
        with connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.master_sheet (
                    format, format_sku_code, product_name, category,
                    sub_category, per_unit, uom
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    "AMAZON",
                    "SOLID-1",
                    "Solid product 800g",
                    "SEEDS",
                    "BASIL SEEDS",
                    "800 GMS",
                    "GMS",
                ],
            )
            cur.execute(
                "INSERT INTO master.fc_master (fc_code, fc_name, city, state) VALUES (%s, %s, %s, %s)",
                ["FC1", "FC1", "City", "State"],
            )

        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "AMAZON_PO",
                "file": csv_file(
                    "solid-product.csv",
                    "PO,Order date,Status,Product name,ASIN,External ID,Requested quantity,Ship-to location\n"
                    "PO3,2026-05-01,Confirmed,Solid product 800g,SOLID-1,SOLID-1,2,FC1\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["status"], "completed")
        self.assertEqual(response.data["warning_count"], 0)

        with connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.master_sheet (
                    format, format_sku_code, product_name, category,
                    sub_category, case_pack, per_unit, uom,
                    sku_sap_name
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    "AMAZON",
                    "LIQUID-1",
                    "Liquid product 1 litre",
                    "OLIVE",
                    "POMACE",
                    12,
                    "1 LTR",
                    "LTR",
                    "LIQUID PRODUCT",
                ],
            )

        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "AMAZON_PO",
                "file": csv_file(
                    "liquid-product.csv",
                    "PO,Order date,Status,Product name,ASIN,External ID,Requested quantity,Ship-to location\n"
                    "PO4,2026-05-01,Confirmed,Liquid product 1 litre,LIQUID-1,LIQUID-1,2,FC1\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["status"], "completed")
        self.assertEqual(response.data["warning_count"], 0)
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT per_liter, total_order_liters
                  FROM reporting."Amazon PO"
                 WHERE po_number = %s
                   AND external_id = %s
                """,
                ["PO4", "LIQUID-1"],
            )
            self.assertEqual(cur.fetchone(), (Decimal("1"), Decimal("2")))

    def test_amazon_po_uses_raw_case_size_when_master_case_pack_is_blank(self):
        with connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.master_sheet (
                    format, format_sku_code, product_name, category,
                    sub_category, per_unit, uom, sku_sap_name
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    "AMAZON",
                    "CASE-FALLBACK",
                    "Fallback case product",
                    "OLIVE",
                    "POMACE",
                    "500 MLS",
                    "MLS",
                    "CASE PRODUCT",
                ],
            )
            cur.execute(
                "INSERT INTO master.fc_master (fc_code, fc_name, city, state) VALUES (%s, %s, %s, %s)",
                ["FC1", "FC1", "City", "State"],
            )

        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "AMAZON_PO",
                "file": csv_file(
                    "case-fallback.csv",
                    "PO,Order date,Status,Product name,ASIN,External ID,Requested quantity,Ship-to location,Case size\n"
                    "PO5,2026-05-01,Confirmed,Fallback case product,CASE-FALLBACK,CASE-FALLBACK,12,FC1,6\n",
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["status"], "completed")
        self.assertEqual(response.data["warning_count"], 0)

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT case_pack, requested_boxes, per_liter, total_order_liters
                  FROM reporting."Amazon PO"
                 WHERE po_number = %s
                """,
                ["PO5"],
            )
            self.assertEqual(
                cur.fetchone(),
                (Decimal("6.0000"), Decimal("2.0000000000000000"), Decimal("0.5"), Decimal("6.0")),
            )

    def test_price_upload_type_is_not_supported(self):
        response = self.client.post(
            "/api/uploads",
            {
                "report_type": "PRICE",
                "pasted_data": (
                    "URL\tASIN\tSELLER\n"
                    "https://example.com\tA1\tSeller\n"
                ),
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(response.data["detail"], "Unsupported report_type.")
