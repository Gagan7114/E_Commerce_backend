from django.db import models


class SapBilling(models.Model):
    """Net billed quantity per Amazon-PO line, reconciled from the SAP HANA
    ``REPORT_SALES_ANALYSIS`` proc for customer **R K Worldinfocom**
    (``CardCode = CUSTA000048``). Populated periodically by ``sync_sap_billing``
    from rows where ``Type`` is Sales / Sales Return and ``U_MART_DOC_NO`` (the PO
    number) matches an Amazon PO. Verified units: SAP ``Quantity`` is in EACHES,
    identical to ``reporting."Amazon PO".accepted_qty`` — no case-pack conversion.

    The shipment planner LEFT JOINs this table on
    ``(po_number, sap_item_code) = (Amazon PO.po_number, Amazon PO.sap_sku_code)``
    to gate out already-billed units: ``shippable = accepted_qty − billed_qty``.

    Keyed by ``(po_number, sap_item_code)``. The whole table is RK-World billing,
    so the sync rebuilds it wholesale each run (no stale rows for POs that age out
    of the look-back window).
    """

    po_number = models.CharField(max_length=64)          # UPPER(TRIM(U_MART_DOC_NO))
    sap_item_code = models.CharField(max_length=64)       # UPPER(TRIM(ItemCode)) == Amazon PO.sap_sku_code
    billed_qty = models.DecimalField(max_digits=16, decimal_places=2, default=0)  # net eaches, ≥ 0
    # [{doc_num, doc_date, qty, amount}] — one entry per SAP invoice touching this line
    invoices = models.JSONField(default=list, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sap_billing"
        constraints = [
            models.UniqueConstraint(fields=["po_number", "sap_item_code"], name="sap_billing_po_item_uq"),
        ]
        indexes = [
            models.Index(fields=["po_number"], name="sap_billing_po_idx"),
        ]

    def __str__(self):
        return f"{self.po_number}/{self.sap_item_code}: {self.billed_qty}"
