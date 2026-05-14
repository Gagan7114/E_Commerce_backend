# Amazon Upload Backend And Frontend

## Supported Modules

- Amazon PO
- appointment

Price upload is intentionally removed from this Amazon uploader.

Not included: Amazon MP, price, Sec Sales Master Range, Sec Sales Master Daily, Inventory master.

## Tables

Raw metadata:

- `raw.upload_file`

Validation:

- `quality.validation_error`
- `quality.upload_validation_summary`

Staging:

- `staging."amazon data"`
- `staging."appointment data"`

Final reporting:

- `reporting."Amazon PO"`
- `reporting."appointment"`

Master lookup:

- `public.master_sheet`
- `master.fc_master`

## Backend Endpoints

- `POST /api/uploads`
- `GET /api/uploads`
- `GET /api/uploads/{upload_id}`
- `GET /api/reports/amazon-po`
- `GET /api/reports/appointment`
- `GET /api/master/products`
- `GET /api/master/fcs`

Uploads accept multipart form data with `report_type` and optional `uploaded_by`.
Data can be supplied as direct pasted text using `pasted_data`; the API still accepts `file` for CSV/XLSX if needed.

Allowed `report_type` values:

- `AMAZON_PO`
- `APPOINTMENT`

`PRICE` is not accepted by the API.

## Report Mapping

`AMAZON_PO`:

- main table name: `Amazon PO`
- raw file name: `amazon data`
- staging: `staging."amazon data"`
- final: `reporting."Amazon PO"`
- persistence: upsert
- upsert key: `source_line_key = md5(po_number | asin)`

`APPOINTMENT`:

- main table name: `appointment`
- raw file name: `appointment data`
- staging: `staging."appointment data"`
- final: `reporting."appointment"`
- persistence: upsert
- upsert key: `appointment_line_key = md5(appointment_id | pos | destination_fc | pro)`

## Validation

Common behavior:

- Direct paste is supported from Excel/Google Sheets, including the header row.
- CSV and XLSX files are still supported by the backend.
- Headers are normalized and mapped by aliases.
- Raw row numbers are preserved.
- Numeric parse failures create row errors.
- Date parse failures create row errors.
- Duplicate file hashes return `duplicate` unless `reprocess=true`.

Amazon PO required fields:

- `po_number`
- `order_date`
- `ship_to_location`
- `asin`

Amazon PO warnings:

- master sheet mapping missing
- FC master mapping missing
- `case_pack` missing
- `per_unit_value` missing

Appointment required fields:

- `appointment_id`
- `appointment_time`
- `destination_fc`

Appointment report shape:

- Final display columns match the reduced appointment view: `Appointment Id`, `Status`, `Appointment Time`, `Creation Date`, `POs`, `Destination FC`, `PRO`, `MONTH`, `YEAR`.
- `ASN` is intentionally ignored by the uploader and hidden from the appointment report.
- `MONTH` is stored as the uppercase month name derived from `appointment_time`.
- `YEAR` is derived from `appointment_time`.
- Repeated appointment IDs are allowed when the PO/FC/PRO line is different, matching the workbook.
- Scientific notation IDs such as `5.45E+11` are normalized to full integer text where the pasted value still contains enough information.
- For Excel-pasted appointment creation dates, `5/8/2026` is treated as May 8, 2026; explicit zero-padded `08/05/2026` remains day/month.

## Amazon PO Formula Summary

- The report layout follows the 60-column `Amazon PO.xlsx` shape.
- `expiry_date = cancellation_deadline` when present, otherwise `window_end`, otherwise `expected_date`
- `days_to_expiry = expiry_date - current_date`
- `po_window = expiry_date - order_date`
- `po_status` is inferred from raw status, availability, accepted/received quantity, cancellation quantity, and expiry date.
- `item_status` is inferred as full/short supplied for completed rows.
- `vendor = RK WORLD` for vendor code `0M7KK`; otherwise the raw vendor code is used.
- `sku_code = raw ASIN`
- `sku_name = raw Product name`
- `requested_boxes = requested_qty / NULLIF(case_pack, 0)`
- `accepted_boxes = accepted_qty / NULLIF(case_pack, 0)`
- `per_ltr_unit = public.master_sheet.per_unit`
- `per_liter` stores the numeric multiplier used for calculations.
- `tax = public.master_sheet.tax_rate`
- `total_order_liters = requested_qty * COALESCE(per_liter, 0)`
- `total_accepted_liters = accepted_qty * COALESCE(per_liter, 0)`
- `total_delivered_liters = received_qty * COALESCE(per_liter, 0)`
- `po_month = month from order_date`
- `year = year from order_date`
- `distributor_margin = public.amazon_asin_margin.margin_percent` by matching `ASIN = public.amazon_asin_margin.asin`
- `core_fresh_now = public.fc_city_state_channel_master.channel` by matching `Fulfillment Center = public.fc_city_state_channel_master.fc`
- `missed_ltrs` is blank for MOV/CANCELLED rows, otherwise `(requested_qty - received_qty) * COALESCE(per_liter, 0)`
- `filled_ltrs = received_qty * COALESCE(per_liter, 0)`
- `fill_rate = received_qty / NULLIF(requested_qty, 0)`
- `miss_rate` is blank for cancelled rows, 0 for pending/MOV rows, otherwise `1 - fill_rate`
- `helper = INCLUDE` for active confirmed rows inside the current expiry window; otherwise `EXCLUDE`

## Master Data Import

The raw Amazon PO upload does not contain all final report columns. Columns such as `ITEM`, `SAP SKU Code`, `Category`, `Case Pack`, `PER LTR UNIT`, `City`, `State`, and `Brand` come from master data.

Amazon product enrichment now uses `public.master_sheet`, not `master.product_master`.

Amazon PO matching order:

- `ASIN` = `public.master_sheet.format_sku_code` where `format = 'AMAZON'`
- `External ID` = `public.master_sheet.format_sku_code`
- `Merchant SKU` = `public.master_sheet.item`
- `Product name` = `public.master_sheet.product_name`

Mapped columns:

- `ITEM` = `public.master_sheet.item`
- `SAP SKU NAME` = `public.master_sheet.sku_sap_name`
- `SAP SKU Code` = `public.master_sheet.sku_sap_code`
- `Category` = `public.master_sheet.category`
- `Sub Category` = `public.master_sheet.sub_category`
- `Case Pack` = `public.master_sheet.case_pack`
- `PER LTR UNIT` = `public.master_sheet.per_unit`
- `Per Liter` calculation value = `public.master_sheet.per_unit_value`
- `Tax` = `public.master_sheet.tax_rate`
- `Brand` = `public.master_sheet.brand`
- `Category Head` = `public.master_sheet.category_head`
- `Distributor Margin` = `public.amazon_asin_margin.margin_percent` matched by ASIN
- `CORE/FRESH/NOW` = `public.fc_city_state_channel_master.channel` matched by fulfillment center

Rows with fulfillment centers missing from `public.fc_city_state_channel_master` keep `CORE/FRESH/NOW` blank.

Use the reference workbook to seed those masters:

```bash
python manage.py import_amazon_po_reference "C:\Users\Udaykaran\Downloads\Amazon PO.xlsx"
```

After import, missing Master Sheet/FC mapping warnings should disappear for matched rows. Remaining `case_pack_missing` or `per_unit_value_missing` warnings mean those values are also blank in `public.master_sheet`.

## Frontend Pages

Nested under the Amazon platform shell:

- `/platform/amazon/uploads` direct paste upload form
- `/platform/amazon/uploads/history`
- `/platform/amazon/uploads/:upload_id`
- `/platform/amazon/reports/amazon-po`
- `/platform/amazon/reports/appointment`

Convenience redirects exist for:

- `/uploads`
- `/uploads/history`
- `/reports/amazon-po`
- `/reports/appointment`

## Known Assumptions

- `Amazon PO.xlsx` contains pasted values, not live Excel formulas, so formulas were inferred from the visible columns and sample data.
- `Raw Amazon PO data.xlsx` and `Amazon PO.xlsx` are not a row-by-row identical pair. The reference workbook is treated as the report layout and master-data source.
- Business-specific rules for `po_status`, `item_status`, and `helper` should be confirmed if the inferred rules need adjustment.
