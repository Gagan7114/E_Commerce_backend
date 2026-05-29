from django.db import migrations


# Amazon Marketplace GST MTR B2B report ("amazon mp"). Each row is one
# invoice line from the Amazon-supplied GST_MTR_B2B export. The report is
# stored faithfully ("same format" as the raw file) — every one of the 90
# source columns is kept as TEXT so exact strings (2-digit-year datetimes,
# GSTINs, postal codes, signed decimals) survive a round-trip untouched.
#
# Natural key: (order_id, shipment_item_id, transaction_type) — verified
# collision-free across the sample export (Shipment / Cancel / Refund lines
# for the same shipment item stay distinct on transaction_type).
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS amazon_mp (
    id                              BIGSERIAL PRIMARY KEY,
    seller_gstin                    TEXT,
    invoice_number                  TEXT,
    invoice_date                    TEXT,
    transaction_type                TEXT,
    order_id                        TEXT,
    shipment_id                     TEXT,
    shipment_date                   TEXT,
    order_date                      TEXT,
    shipment_item_id                TEXT,
    quantity                        TEXT,
    item_description                TEXT,
    asin                            TEXT,
    hsn_sac                         TEXT,
    sku                             TEXT,
    item_serial_no                  TEXT,
    product_tax_code                TEXT,
    bill_from_city                  TEXT,
    bill_from_state                 TEXT,
    bill_from_country               TEXT,
    bill_from_postal_code           TEXT,
    ship_from_city                  TEXT,
    ship_from_state                 TEXT,
    ship_from_country               TEXT,
    ship_from_postal_code           TEXT,
    ship_to_city                    TEXT,
    ship_to_state                   TEXT,
    ship_to_country                 TEXT,
    ship_to_postal_code             TEXT,
    invoice_amount                  TEXT,
    tax_exclusive_gross             TEXT,
    total_tax_amount                TEXT,
    cgst_rate                       TEXT,
    sgst_rate                       TEXT,
    utgst_rate                      TEXT,
    igst_rate                       TEXT,
    compensatory_cess_rate          TEXT,
    principal_amount                TEXT,
    principal_amount_basis          TEXT,
    cgst_tax                        TEXT,
    sgst_tax                        TEXT,
    igst_tax                        TEXT,
    utgst_tax                       TEXT,
    compensatory_cess_tax           TEXT,
    shipping_amount                 TEXT,
    shipping_amount_basis           TEXT,
    shipping_cgst_tax               TEXT,
    shipping_sgst_tax               TEXT,
    shipping_utgst_tax              TEXT,
    shipping_igst_tax               TEXT,
    shipping_cess_tax_amount        TEXT,
    gift_wrap_amount                TEXT,
    gift_wrap_amount_basis          TEXT,
    gift_wrap_cgst_tax              TEXT,
    gift_wrap_sgst_tax              TEXT,
    gift_wrap_utgst_tax             TEXT,
    gift_wrap_igst_tax              TEXT,
    gift_wrap_compensatory_cess_tax TEXT,
    item_promo_discount             TEXT,
    item_promo_discount_basis       TEXT,
    item_promo_discount_tax         TEXT,
    shipping_promo_discount         TEXT,
    shipping_promo_discount_basis   TEXT,
    shipping_promo_discount_tax     TEXT,
    gift_wrap_promo_discount        TEXT,
    gift_wrap_promo_discount_basis  TEXT,
    gift_wrap_promo_discount_tax    TEXT,
    tcs_cgst_rate                   TEXT,
    tcs_cgst_amount                 TEXT,
    tcs_sgst_rate                   TEXT,
    tcs_sgst_amount                 TEXT,
    tcs_utgst_rate                  TEXT,
    tcs_utgst_amount                TEXT,
    tcs_igst_rate                   TEXT,
    tcs_igst_amount                 TEXT,
    warehouse_id                    TEXT,
    fulfillment_channel             TEXT,
    payment_method_code             TEXT,
    credit_note_no                  TEXT,
    credit_note_date                TEXT,
    irn_number                      TEXT,
    irn_filing_status               TEXT,
    irn_date                        TEXT,
    irn_error_code                  TEXT,
    bill_to_city                    TEXT,
    bill_to_state                   TEXT,
    bill_to_country                 TEXT,
    bill_to_postalcode              TEXT,
    customer_bill_to_gstid          TEXT,
    customer_ship_to_gstid          TEXT,
    buyer_name                      TEXT,
    CONSTRAINT amazon_mp_unique UNIQUE (order_id, shipment_item_id, transaction_type)
);

CREATE INDEX IF NOT EXISTS amazon_mp_asin_idx ON amazon_mp (asin);
CREATE INDEX IF NOT EXISTS amazon_mp_invoice_number_idx ON amazon_mp (invoice_number);
"""

REVERSE_SQL = """
DROP INDEX IF EXISTS amazon_mp_invoice_number_idx;
DROP INDEX IF EXISTS amazon_mp_asin_idx;
DROP TABLE IF EXISTS amazon_mp;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("platforms", "0033_reapply_primary_delivery_fallbacks"),
    ]

    operations = [
        migrations.RunSQL(sql=CREATE_SQL, reverse_sql=REVERSE_SQL),
    ]
