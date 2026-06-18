from django.db import migrations


class Migration(migrations.Migration):
    """Flipkart Secondary — State-wise sales table.

    Stores the Flipkart B2C "Sales Report" GST export (the `Sales Report` sheet:
    a flat header row + invoice-level rows, 60 columns incl. delivery/billing
    state). Like `amazon_mp`, every column is kept as TEXT so the raw export
    round-trips untouched (GSTINs, tax rates/amounts, 2-digit dates, pincodes).
    Selected via the ASIN/State toggle in the Flipkart Secondary uploader; the
    existing `flipkartSec` (ASIN) uploader is unchanged.

    Upsert dedup key matches the frontend `flipkart_state` config:
    (order_id, order_item_id, event_type, event_sub_type).
    """

    dependencies = [
        ("uploads", "0053_amazon_sec_state_month_day_year"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.flipkart_state_sales (
                id                          bigserial PRIMARY KEY,
                seller_gstin                text,
                order_id                    text,
                order_item_id               text,
                product_title               text,
                fsn                         text,
                sku                         text,
                hsn_code                    text,
                event_type                  text,
                event_sub_type              text,
                order_type                  text,
                fulfilment_type             text,
                order_date                  text,
                order_approval_date         text,
                item_quantity               text,
                order_shipped_from_state    text,
                warehouse_id                text,
                price_before_discount       text,
                total_discount              text,
                seller_share                text,
                bank_offer_share            text,
                price_after_discount        text,
                shipping_charges            text,
                final_invoice_amount        text,
                type_of_tax                 text,
                taxable_value               text,
                cst_rate                    text,
                cst_amount                  text,
                vat_rate                    text,
                vat_amount                  text,
                luxury_cess_rate            text,
                luxury_cess_amount          text,
                igst_rate                   text,
                igst_amount                 text,
                cgst_rate                   text,
                cgst_amount                 text,
                sgst_rate                   text,
                sgst_amount                 text,
                tcs_igst_rate               text,
                tcs_igst_amount             text,
                tcs_cgst_rate               text,
                tcs_cgst_amount             text,
                tcs_sgst_rate               text,
                tcs_sgst_amount             text,
                total_tcs_deducted          text,
                buyer_invoice_id            text,
                buyer_invoice_date          text,
                buyer_invoice_amount        text,
                customer_billing_pincode    text,
                customer_billing_state      text,
                customer_delivery_pincode   text,
                customer_delivery_state     text,
                usual_price                 text,
                is_shopsy_order             text,
                tds_rate                    text,
                tds_amount                  text,
                irn                         text,
                business_name               text,
                business_gst_number         text,
                beneficiary_name            text,
                imei                        text,
                created_at                  timestamp without time zone DEFAULT now()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS flipkart_state_sales_dedup_key
                ON public.flipkart_state_sales
                (order_id, order_item_id, event_type, event_sub_type);
            CREATE INDEX IF NOT EXISTS idx_flipkart_state_sales_delivery_state
                ON public.flipkart_state_sales (customer_delivery_state);
            CREATE INDEX IF NOT EXISTS idx_flipkart_state_sales_order_date
                ON public.flipkart_state_sales (order_date);
            """,
            reverse_sql=r"""
            DROP TABLE IF EXISTS public.flipkart_state_sales;
            """,
        ),
    ]
