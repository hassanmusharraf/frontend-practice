from django.db import models
from django.forms import ValidationError
# from django.contrib.postgres.fields import ArrayField
from portal.base import BaseModel
from portal.choices import (
    POUploadStatusChoices,
    ConsignmentTypeChoices,
    ConsoleStatusChoices,
    ConsignmentDocumentTypeChoices,
    ConsignmentStatusChoices,
    PurchaseOrderStatusChoices,
    UserGridPreferences,
    WeightUnitChoices,
    PackageStatusChoices,
    GLCodeChoices,
    ConsignmentCreationSteps,
    OrderTypeChoices,
    POImportFormatsChoices
)
from django.db.models import Sum, Q
from django.db.models.signals import pre_save, post_delete, post_save
from .signals import (
    create_audit_trail, po_audit_trail, poline_audit_trail, delete_file_from_storage, notify_consignment_update, notify_po, notify_po_line
)
from core.fields import MSSQLJSONField
from django.db.models import Max
from django.db.models.functions import Cast, Substr
from django.db.models import IntegerField
from .managers import ConsignmentManager
from decimal import Decimal


class PurchaseOrder(BaseModel):
    reference_number = models.CharField(max_length=100, null=True)  # e.g. "TEST001" or EXTERNALPOKEY2 from Aramex request
    customer_reference_number = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True, null=True)
    group_code = models.CharField(max_length=50, blank=True, null=True)   # e.g. "WMWHSE80"
    order_date = models.DateField(blank=True, null=True) 
    type = models.CharField(max_length=50, blank=True, null=True)     # e.g. "ZNB"
    expected_delivery_date = models.DateField(blank=True, null=True)
    order_due_date = models.DateField(blank=True, null=True)
    open_quantity = models.DecimalField(max_digits=20, decimal_places=2) 
    notes = models.TextField(blank=True, null=True)
    inco_terms = models.CharField(max_length=50, blank=True, null=True)     # e.g. "EXW"
    payment_terms = models.CharField(max_length=100, blank=True, null=True)
    origin_country = models.CharField(max_length=50, blank=True, null=True)
    destination_country = models.CharField(max_length=50, blank=True, null=True)
    supplier = models.ForeignKey('entities.Supplier', on_delete=models.PROTECT, related_name="purchase_orders")
    client = models.ForeignKey('entities.Client', on_delete=models.PROTECT, related_name="purchase_orders")
    storerkey = models.ForeignKey('entities.StorerKey', on_delete=models.PROTECT, related_name="purchase_orders")
    buyer_details = MSSQLJSONField(default=dict,null=True,blank=True)
    seller_address_line_1 = models.CharField(max_length=255, null=True, blank=True)
    seller_address_line_2 = models.CharField(max_length=255, null=True, blank=True)
    seller_city = models.CharField(max_length=255, null=True, blank=True)
    seller_state = models.CharField(max_length=255, null=True, blank=True)
    seller_country = models.CharField(max_length=255, null=True, blank=True)
    seller_postal_code = models.CharField(max_length=255, null=True, blank=True)
    seller_phone_number = models.CharField(max_length=255, null=True, blank=True)
    seller_tax_number = models.CharField(max_length=255, null=True, blank=True)
    seller_email = models.EmailField(max_length=255, null=True, blank=True)
    is_asn = models.BooleanField(default=False)
    plant_id = models.CharField(max_length=255)
    center_code = models.CharField(max_length=255)
    status = models.CharField(max_length=100, choices=PurchaseOrderStatusChoices.choices, default=PurchaseOrderStatusChoices.OPEN)
    
    ##po_type 
    order_type = models.CharField(max_length=5,choices=OrderTypeChoices,default=OrderTypeChoices.BTS)


    # objects = PurchaseOrderManager()
        
    def __str__(self):
        return self.customer_reference_number
    
    def update_status(self):
        po_line_status = PurchaseOrderLine.objects.filter(purchase_order_id=self.id).values_list("status", flat=True)
        
        if not po_line_status:
            return 

        unique_statuses = set(po_line_status)

        if PurchaseOrderStatusChoices.PARTIALLY_FULFILLED in unique_statuses:
            self.status = PurchaseOrderStatusChoices.PARTIALLY_FULFILLED
            
        elif PurchaseOrderStatusChoices.OPEN in unique_statuses:
            self.status = PurchaseOrderStatusChoices.OPEN
            
        elif PurchaseOrderStatusChoices.CLOSED in unique_statuses:
            self.status = PurchaseOrderStatusChoices.CLOSED

        elif unique_statuses == {PurchaseOrderStatusChoices.CANCELLED}:
            self.status = PurchaseOrderStatusChoices.CANCELLED

        # print(self.status)

        self.save(update_fields=["status"])

    def update_quantity(self):
        sum = self.lines.all().aggregate(sum=Sum('quantity'))
        self.open_quantity = sum["sum"]    
        self.save()
        
    @property
    def storerkeys(self):
        """
        Returns all storerkeys linked to this purchase order.
        """
        return self.supplier.storerkeys.all() if self.supplier else []
    
    class Meta:
        indexes = [
            models.Index(fields=['customer_reference_number'], name='po_crn_idx'),
            models.Index(fields=['reference_number'], name='po_reference_idx'),
            models.Index(fields=['supplier'], name='po_supplier_idx'),
            models.Index(fields=['client'], name='po_client_idx'),
            models.Index(fields=['storerkey'], name='po_storerkey_idx'),
            models.Index(fields=['supplier', 'storerkey', 'is_deleted'], name='po_supplier_storer_deleted_idx'),
            models.Index(fields=['status', 'is_deleted'], name='po_status_deleted_idx'),
            models.Index(fields=['created_at', 'is_deleted'], name='po_created_deleted_idx'),
            models.Index(fields=['is_asn', 'status'], name='po_asn_status_idx'),
            models.Index(fields=['order_type'], name='po_order_type_idx'),
            models.Index(fields=['created_at'], name='po_created_idx'),  # For date range queries
        ]

pre_save.connect(po_audit_trail, sender=PurchaseOrder) 
# post_save.connect(notify_po, sender=PurchaseOrder)


class PurchaseOrderLine(BaseModel):
    purchase_order = models.ForeignKey(PurchaseOrder, on_delete=models.CASCADE, related_name="lines")
    reference_number = models.CharField(max_length=128, null=True)  # e.g. "TEST001" or EXTERNALPOKEY2 from Aramex request
    customer_reference_number = models.CharField(max_length=128)
    notes = models.TextField(blank=True, null=True)
    alternate_unit = models.CharField(max_length=128, blank=True, null=True)  # delete
    order_due_date = models.DateField(blank=True, null=True)
    expected_delivery_date = models.DateField(blank=True, null=True)
    stock_number = models.CharField(max_length=128, blank=True, null=True) # delete
    is_chemical = models.BooleanField(default=False)
    is_dangerous_good = models.BooleanField(default=False)
    product_code = models.CharField(max_length=128, blank=True, null=True)
    description = models.TextField(blank=True, null=True)  # Maps to "Descr" or "description" field
    quantity = models.DecimalField(max_digits=16, decimal_places=2)        # Total quantity ordered
    sku = models.CharField(max_length=128, blank=True, null=True)
    unit_price = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)    # delete
    unit_cost = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True) # delete
    weight = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)    # delete
    volume = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)    # delete
    length = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)    # delete
    width = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True) # delete
    height = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)    # delete
    source_location = models.CharField(max_length=128, null=True, blank=True)
    hs_code = models.CharField(max_length=128, blank=True, null=True)
    initial_promise_date = models.DateTimeField(blank=True, null=True)
    new_promise_date = models.DateTimeField(blank=True, null=True)
    open_quantity = models.DecimalField(max_digits=16, decimal_places=2)     # Quantity pending shipment
    fulfilled_quantity = models.DecimalField(max_digits=16, decimal_places=2, default=0)  # Quantity already shipped
    processed_quantity = models.DecimalField(max_digits=16, decimal_places=2, default=0)  # Quantity shipped in progress
    status = models.CharField(max_length=128, choices=PurchaseOrderStatusChoices.choices, default=PurchaseOrderStatusChoices.OPEN)
    inco_terms = models.CharField(max_length=128, blank=True, null=True)
    batch = models.CharField(max_length=256, null=True, blank=True)     
    lot = models.CharField(max_length=256, null=True, blank=True)     
    expiry_date = models.DateField(null=True, blank=True) 
    manufacturing_date = models.DateField(null=True, blank=True) 
    origin_country = models.CharField(max_length=64, blank=True, null=True)


    def update_quantities(self):
        processed_qty = (
            self.packaging_allocations
            .filter(consignment_packaging__status=PackageStatusChoices.NOT_RECEIVED)
            .aggregate(total=Sum("allocated_qty"))["total"] or Decimal("0")
        )

        self.processed_quantity = processed_qty
        self.open_quantity = (self.quantity or 0) - processed_qty - (self.fulfilled_quantity or 0)

        self.save(update_fields=["processed_quantity", "open_quantity"])
    
    def update_status(self):
        if self.open_quantity == 0 and self.processed_quantity == 0:
            self.status = PurchaseOrderStatusChoices.CLOSED
        elif self.fulfilled_quantity != 0:
            self.status = PurchaseOrderStatusChoices.PARTIALLY_FULFILLED
        # else:
        #     self.status = PurchaseOrderStatusChoices.OPEN 

        # print(self.status)
        self.save(update_fields=["status"])
        
    def clean(self):
        """
        Validate the fields of the model.
        """
        errors = {}

        if self.quantity < 0:
            errors['quantity'] = "Quantity cannot be negative."

        if self.fulfilled_quantity < 0:
            errors['fulfilled_quantity'] = "Fulfilled quantity cannot be negative."
        elif self.fulfilled_quantity > self.quantity:
            errors['fulfilled_quantity'] = "Fulfilled quantity cannot exceed quantity."

        if self.processed_quantity < 0:
            errors['processed_quantity'] = "Processed quantity cannot be negative."

        # calculated_open = self.calculated_open_qty()
        if self.processed_quantity > (self.quantity - self.fulfilled_quantity):
            errors['processed_quantity'] = "Processed quantity cannot exceed remaining open quantity."

        if self.open_quantity < 0:
            errors['open_quantity'] = "Open quantity cannot be negative."

        if errors:
            raise ValidationError(errors)
            
    def save(self, *args, **kwargs):
        self.open_quantity = self.calculated_open_qty()
        self.clean()
        super().save(*args, **kwargs)
    
    def calculated_open_qty(self):
        return self.quantity - self.fulfilled_quantity - self.processed_quantity
    
    # objects = PurchaseOrderLineManager()
    def __str__(self):
        return f"{self.purchase_order.customer_reference_number} - {self.customer_reference_number}"
    
    class Meta:

        unique_together = ["customer_reference_number", "purchase_order"]

        indexes = [
            models.Index(fields=['purchase_order'], name='pol_po_idx'),
            models.Index(fields=['purchase_order', 'customer_reference_number'], name='pol_po_crn_idx'),
            models.Index(fields=['purchase_order', 'status', 'is_deleted'], name='pol_po_status_idx'),
            models.Index(fields=['status'], name='pol_status_idx'),
            models.Index(fields=['status', 'is_deleted'], name='pol_status_deleted_idx'),
            models.Index(fields=['sku'], name='pol_sku_idx'),
            models.Index(fields=['is_dangerous_good'], name='pol_dg_idx'),
            models.Index(fields=['is_chemical'], name='pol_chemical_idx'),
            models.Index(fields=['open_quantity'], name='pol_open_qty_idx'),
            models.Index(fields=['product_code'], name='pol_product_code_idx'),
            models.Index(fields=['created_at', 'is_deleted'], name='pol_created_deleted_idx'),
        ]
        
    

pre_save.connect(poline_audit_trail, sender=PurchaseOrderLine)
# post_save.connect(notify_po_line, sender=PurchaseOrderLine)


class Consignment(BaseModel):
    consignment_id = models.CharField(max_length=20, unique=True)
    # purchase_order = models.ManyToManyField(PurchaseOrder,through="ConsignmentPurchaseOrder", related_name="consignments",blank=True)
    purchase_order_lines = models.ManyToManyField(PurchaseOrderLine,through="ConsignmentPOLine", related_name="consignments",blank=True)
    adhoc = models.ForeignKey("adhoc.AdhocPurchaseOrder", on_delete=models.CASCADE, related_name="consignments", null=True, blank=True)
    type = models.CharField(max_length=10, choices=ConsignmentTypeChoices.choices, default=ConsignmentTypeChoices.PO_BASED)
    supplier = models.ForeignKey("entities.Supplier", on_delete=models.CASCADE, related_name="consignments", null=True, blank=True)
    client = models.ForeignKey("entities.Client", on_delete=models.CASCADE, related_name="consignments", null=True, blank=True)
    consignor_address = models.ForeignKey("portal.AddressBook", on_delete=models.SET_NULL, null=True, blank=True, related_name="consignment_consignor_addresses")
    delivery_address = models.ForeignKey("portal.AddressBook", on_delete=models.SET_NULL, null=True, blank=True, related_name="consignment_delivery_addresses")
    consignment_status = models.CharField(max_length=50, choices=ConsignmentStatusChoices.choices, default=ConsignmentStatusChoices.PENDING_FOR_APPROVAL)
    requested_pickup_datetime = models.DateTimeField(null=True, blank=True)
    actual_pickup_datetime = models.DateTimeField(null=True, blank=True)
    pickup_timezone = models.CharField(max_length=255)
    packages = MSSQLJSONField(default=list)
    # packages = ArrayField(models.CharField(max_length=100), default=list, blank=True)
    freight_forwarder = models.ForeignKey("portal.FreightForwarder", on_delete=models.SET_NULL, null=True, blank=True, related_name="consignments")
    rejection_code = models.ForeignKey("portal.RejectionCode", on_delete=models.SET_NULL, null=True, blank=True, related_name="consignments")
    rejection_reason = models.TextField(null=True, blank=True)
    cancellation_remarks = models.TextField(null=True, blank=True)
    additional_instructions = models.TextField(null=True, blank=True)
    console = models.ForeignKey("workflows.Console", on_delete=models.SET_NULL ,null=True, blank=True,related_name="consignments")
    # bol = models.OneToOneField("workflows.BOL", on_delete=models.SET_NULL, null=True, blank=True,related_name="consignment")
    xml = models.ForeignKey("workflows.XML", on_delete=models.SET_NULL, null=True, blank=True)
    is_completed = models.BooleanField(default=True)
    created_by = models.ForeignKey("accounts.User", on_delete=models.CASCADE, null=True, blank=True)
    gl_code = models.CharField(max_length=50, choices=GLCodeChoices.choices, default=GLCodeChoices.CODE_56000100)
    last_bol_gen_at = models.DateTimeField(null=True, blank=True)
    last_bol_gen_by = models.ForeignKey("accounts.User", on_delete=models.SET_NULL, null=True, blank=True, related_name="bol_generated_by")
    step = models.CharField(max_length=10, choices=ConsignmentCreationSteps.choices, default=ConsignmentCreationSteps.STEP_1)
    objects = ConsignmentManager()
    def __str__(self):
        return f"Consignment {self.consignment_id}"

    def save(self, *args, **kwargs):
        if not self.consignment_id:

            if self.consignment_status == ConsignmentStatusChoices.DRAFT:
                max_num = (
                    Consignment.objects
                    .filter(consignment_status=ConsignmentStatusChoices.DRAFT)
                    .annotate(num_part=Cast(Substr('consignment_id', 6), IntegerField()))
                    .aggregate(max_num=Max('num_part'))
                    .get('max_num')
                ) or 0  # Handle None

                self.consignment_id = f"DRAFT{max_num + 1}"

        elif self.consignment_id and self.consignment_id.startswith("DRAFT") and self.consignment_status == ConsignmentStatusChoices.PENDING_FOR_APPROVAL:

            max_num = (
                Consignment.objects
                .filter(~Q(consignment_status=ConsignmentStatusChoices.DRAFT))
                .annotate(num_part=Cast(Substr('consignment_id', 4), IntegerField()))
                .aggregate(max_num=Max('num_part'))
                .get('max_num')
            ) or 0  # Handle None

            self.consignment_id = "PKU{0:0=5d}".format(max_num + 1)

        super().save(*args, **kwargs)
                
            # if not self.consignment_id:
            #     consignment = Consignment.objects.all().order_by('-created_at')[:1].only('consignment_id')
            #     if consignment.exists():
            #         self.consignment_id = "PKU{0:0=5d}".format(int(consignment.first().consignment_id[3:]) + 1)
            #     else:
            #         self.consignment_id = "PKU00001"
            # return super().save(*args, **kwargs)
    
    def update_console_status(self):
        from workflows.apis import log_console_audit
        
        console = self.console
        
        """
        All Delivered → Console DELIVERED

        All Received → Console RECEIVED_AT_DESTINATION

        Any Delivered but not all → PARTIALLY_DELIVERED

        Any Received but not all → PARTIALLY_RECEIVED

        Any At Custom → AT_CUSTOM

        Any Out for Delivery → IN_TRANSIT

        Any Pickup Completed → IN_TRANSIT

        All Rejected / Cancelled / FF Assigned → FREIGHT_FORWARDER_ASSIGNED

        Else → keep previous or set NEW
        """
        

        if console:
            old_status = console.console_status
            
            all_statuses = list(
                Consignment.objects.filter(console=console)
                .values_list("consignment_status", flat=True)
                .distinct()
            )

            if not all_statuses:
               return

            statuses = set(all_statuses)
            total = len(all_statuses)

            delivered_count = all_statuses.count(ConsignmentStatusChoices.DELIVERED)
            received_count = all_statuses.count(ConsignmentStatusChoices.RECEIVED_AT_DESTINATION)

            new_status = None

            # --- PRIORITY CHECKS ---
            if delivered_count == total:
                new_status = ConsoleStatusChoices.DELIVERED

            elif received_count == total:
                new_status = ConsoleStatusChoices.RECEIVED_AT_DESTINATION

            elif ConsignmentStatusChoices.DELIVERED in statuses:
                new_status = ConsoleStatusChoices.PARTIALLY_DELIVERED

            elif (
                ConsignmentStatusChoices.PARTIALLY_RECEIVED in statuses
                or ConsignmentStatusChoices.RECEIVED_AT_DESTINATION in statuses
            ):
                new_status = ConsoleStatusChoices.PARTIALLY_RECEIVED

            elif ConsignmentStatusChoices.AT_CUSTOM in statuses:
                new_status = ConsoleStatusChoices.AT_CUSTOM

            elif (
                ConsignmentStatusChoices.OUT_FOR_DELIVERY in statuses
                or ConsignmentStatusChoices.PICKUP_COMPLETED in statuses
            ):
                new_status = ConsoleStatusChoices.IN_TRANSIT

            elif all(
                s in {
                    ConsignmentStatusChoices.REJECTED,
                    ConsignmentStatusChoices.CANCELLED,
                    ConsignmentStatusChoices.FREIGHT_FORWARDER_ASSIGNED,
                }
                for s in statuses
            ):
                new_status = ConsoleStatusChoices.FREIGHT_FORWARDER_ASSIGNED

            # Fallback if nothing matched
            if not new_status:
                new_status = old_status or ConsoleStatusChoices.NEW

            # --- Apply and log if changed ---
            if new_status != old_status:
                console.console_status = new_status
                console.save(update_fields=["console_status"])
                log_console_audit(console=console, old_status=old_status)
            
    def get_po_lines(self, filter_kwargs=None):
        qs = self.po_lines.select_related("purchase_order_line")
        return qs.filter(**filter_kwargs) if filter_kwargs else qs.all()

    class Meta:
        indexes = [
            models.Index(fields=['consignment_id'], name='consignment_id_idx'),
            models.Index(fields=['supplier'], name='consignment_supplier_idx'),
            models.Index(fields=['client'], name='consignment_client_idx'),
            models.Index(fields=['console'], name='consignment_console_idx'),
            models.Index(fields=['consignment_status'], name='consignment_status_idx'),
            models.Index(fields=['consignment_status', 'is_deleted'], name='consignment_status_deleted_idx'),
            models.Index(fields=['supplier', 'consignment_status', 'is_deleted'], name='cons_supplier_status_idx'),
            models.Index(fields=['console', 'consignment_status'], name='consignment_console_status_idx'),
            models.Index(fields=['created_by'], name='consignment_created_by_idx'),
            models.Index(fields=['created_at', 'is_deleted'], name='cons_created_deleted_idx'),
            # models.Index(fields=['adhoc'], name='consignment_adhoc_idx'),
            # models.Index(fields=['xml'], name='consignment_xml_idx'),
            # models.Index(fields=['requested_pickup_datetime'], name='consignment_pickup_datetime_idx'),
        ]

pre_save.connect(create_audit_trail, sender=Consignment)
# post_save.connect(notify_consignment_update, sender=Consignment) 



class ConsignmentPOLine(BaseModel):
    consignment = models.ForeignKey(Consignment, on_delete=models.CASCADE, related_name = 'consignment_po_line')
    purchase_order_line = models.ForeignKey(PurchaseOrderLine, on_delete=models.CASCADE, related_name="consignments_po_lines")
    allocated_qty = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    
    # ## Compliance data
    hs_code = models.CharField(max_length=20, null=True, blank=True)
    eccn = models.CharField(max_length=20, null=True, blank=True)
    dg_class = models.ForeignKey("entities.DangerousGoodClass",on_delete=models.SET_NULL,null=True, blank=True) 
    dg_category = models.ForeignKey("entities.DangerousGoodCategory",on_delete=models.SET_NULL,null=True, blank=True)
    dg_note = models.TextField(null=True, blank=True)
    compliance_dg = models.BooleanField(default=False)
    compliance_chemical = models.BooleanField(default=False)
    country_of_origin = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['consignment'], name='cpl_consignment_idx'),
            models.Index(fields=['purchase_order_line'], name='cpl_po_line_idx'),
            models.Index(fields=['consignment', 'purchase_order_line'], name='cpl_consignment_po_idx'),
            models.Index(fields=['compliance_dg'], name='cpl_compliance_dg_idx'),
            models.Index(fields=['compliance_chemical'], name='cpl_compliance_chemical_idx'),
            models.Index(fields=['allocated_qty'], name='cpl_allocated_qty_idx'),
        ]

class ConsignmentAuditTrail(BaseModel):
    consignment = models.ForeignKey(Consignment, on_delete=models.CASCADE, related_name="audit_trails")
    updated_by = models.ForeignKey("accounts.User", on_delete=models.CASCADE)
    


class ConsignmentAuditTrailField(BaseModel):
    audit_trail = models.ForeignKey(ConsignmentAuditTrail, on_delete=models.CASCADE, related_name="fields")
    title = models.CharField(max_length=150, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    attachments = MSSQLJSONField(default=list)
    field_name = models.CharField(max_length=100)
    old_value = models.TextField(null=True, blank=True)
    new_value = models.TextField(null=True, blank=True)
    


class ConsignmentPackaging(BaseModel):
    package_id = models.CharField(max_length=50,unique=True,null=True, blank=True,db_index=True)
    draft_package_id = models.CharField(max_length=50, null=True, blank=True)
    consignment = models.ForeignKey(Consignment, on_delete=models.CASCADE, related_name="packagings",db_index=True)
    packaging_type = models.ForeignKey("portal.PackagingType", on_delete=models.CASCADE, related_name="consignment_packagings",db_index=True,blank=True, null=True)
    status = models.CharField(max_length=20, choices=PackageStatusChoices.choices, default=PackageStatusChoices.NOT_RECEIVED,db_index=True)
    received_date_time = models.DateTimeField(null=True, blank=True)
    time_zone = models.CharField(max_length=255,null=True, blank=True)
    weight = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    weight_unit = models.CharField(max_length=20, choices=WeightUnitChoices.choices, blank=True, null=True)
    allocated_lines = models.ManyToManyField("PurchaseOrderLine",through="PackagingAllocation", related_name="packages", blank=True)
    order_type = models.CharField(max_length=20, choices=OrderTypeChoices.choices, default=None, null=True, blank=True)
    is_kit = models.BooleanField(default=False)
    def clean(self):
        if not self.is_draft and self.packaging_type is None:
            raise ValidationError("packaging_type is required")
    
    def __str__(self):
        return f"{self.package_id} Consignment {self.consignment.id}"

    class Meta:
        unique_together = ("draft_package_id", "consignment")
        
        indexes = [
            models.Index(fields=['consignment'], name='pkg_consignment_idx'),
            models.Index(fields=['packaging_type'], name='pkg_type_idx'),
            models.Index(fields=['status'], name='pkg_status_idx'),
            models.Index(fields=['package_id'], name='pkg_id_idx'),
            models.Index(fields=['draft_package_id'], name='pkg_draft_id_idx'),
            models.Index(fields=['consignment', 'status'], name='pkg_consignment_status_idx'),
            models.Index(fields=['status', 'is_deleted'], name='pkg_status_deleted_idx'),
            models.Index(fields=['received_date_time'], name='pkg_received_dt_idx'),
            models.Index(fields=['created_at'], name='pkg_created_idx'),
            # models.Index(fields=['is_kit'], name='pkg_is_kit_idx'),
        ]



class ComprehensiveReport(BaseModel):
    user = models.ForeignKey("accounts.User", on_delete=models.SET_NULL, null=True)
    from_date = models.DateTimeField()
    to_date = models.DateTimeField()
    status = MSSQLJSONField(default=list)
    consignment_ids = MSSQLJSONField(default=list)
    report_generation_status = models.CharField(max_length=20, choices=POUploadStatusChoices.choices, default=POUploadStatusChoices.QUEUE,db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=['user'], name='report_user_idx'),
            models.Index(fields=['report_generation_status'], name='report_gen_status_idx'),
            models.Index(fields=['from_date', 'to_date'], name='report_date_range_idx'),
            models.Index(fields=['created_at'], name='report_created_idx'),
            models.Index(fields=['user', 'report_generation_status'], name='report_user_status_idx'),
        ]


class PackagingAllocation(BaseModel):
    consignment_packaging = models.ForeignKey(ConsignmentPackaging, on_delete=models.CASCADE, related_name="allocations",db_index=True)
    purchase_order_line = models.ForeignKey("PurchaseOrderLine", on_delete=models.CASCADE, related_name="packaging_allocations",db_index=True, null=True, blank=True)
    is_dangerous_good = models.BooleanField(default=False)
    allocated_qty = models.DecimalField(max_digits=8, decimal_places=2, default=0, help_text="Quantity from the PO line allocated to this packaging unit.")

    # class Meta:
        # unique_together = ("consignment_packaging", "purchase_order_line")

    def __str__(self):
        return f"{self.allocated_qty} in Packaging {self.consignment_packaging.id}"
    
    class Meta:
        indexes = [
            models.Index(fields=['consignment_packaging'], name='alloc_pkg_idx'),
            models.Index(fields=['purchase_order_line'], name='alloc_po_line_idx'),
            models.Index(fields=['consignment_packaging', 'purchase_order_line'], name='alloc_pkg_po_idx'),
            models.Index(fields=['is_dangerous_good'], name='alloc_is_dg_idx'),
            models.Index(fields=['allocated_qty'], name='alloc_qty_idx'),
        ]




class ConsignmentPOLineBatch(BaseModel):
    consignment = models.ForeignKey(Consignment,on_delete=models.CASCADE, related_name="batches")
    purchase_order_line = models.ForeignKey(PurchaseOrderLine,on_delete=models.CASCADE, related_name="batches")
    number = models.CharField(max_length=25)
    expiry_date = models.DateTimeField()
    quantity = models.DecimalField(max_digits=8, decimal_places=2)


# To be deleted after migration
# class ConsignmentPOLineCompliance(BaseModel):
#     consignment = models.ForeignKey(Consignment,on_delete=models.CASCADE, related_name="compliances")
#     purchase_order_line = models.ForeignKey(PurchaseOrderLine,on_delete=models.CASCADE, related_name="compliances")
#     hs_code = models.CharField(max_length=20, null=True, blank=True)
#     eccn = models.CharField(max_length=20, null=True, blank=True)
#     dg_class = models.ForeignKey("entities.DangerousGoodClass",on_delete=models.SET_NULL,null=True, blank=True,related_name="compliances") 
#     dg_category = models.ForeignKey("entities.DangerousGoodCategory",on_delete=models.SET_NULL,null=True, blank=True,related_name="compliances")
#     dg_note = models.TextField(null=True, blank=True)
#     compliance_dg = models.BooleanField(default=False)
#     compliance_chemical = models.BooleanField(default=False)
#     country_of_origin = models.CharField(max_length=50, null=True, blank=True)

    # class Meta:
    #     unique_together = ("consignment", "purchase_order_line")
    #     indexes = [
    #         models.Index(fields=["consignment", "purchase_order_line"]),
    #     ]

class ConsignmentDocument(BaseModel):
    consignment = models.ForeignKey(Consignment, on_delete=models.CASCADE, related_name="documents", null=True, blank=True)
    # compliance = models.ForeignKey("ConsignmentPOLineCompliance", on_delete=models.CASCADE, related_name="documents", null=True, blank=True)
    document_type = models.CharField(max_length=50, choices=ConsignmentDocumentTypeChoices.choices)
    adhoc_line = models.ForeignKey("adhoc.AdhocPurchaseOrderLine", on_delete=models.CASCADE, related_name="consignment_documents", null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["consignment", "document_type"]),
            models.Index(fields=['consignment'], name='doc_consignment_idx'),
            models.Index(fields=['document_type'], name='doc_type_idx'),
            # models.Index(fields=['adhoc_line'], name='doc_adhoc_idx'),
        ]
    


class ConsignmentDocumentAttachment(BaseModel):
    document = models.ForeignKey(ConsignmentDocument, on_delete=models.CASCADE, related_name="attachments")
    file = models.FileField(upload_to="consignments/documents/")
post_delete.connect(delete_file_from_storage, sender=ConsignmentDocumentAttachment) 



class DangerousGoodDocuments(BaseModel):
    file = models.FileField(upload_to="consignments/documents/")
    consignment_po_line = models.ForeignKey("operations.ConsignmentPOLine",on_delete=models.CASCADE,related_name="dg_files")



class ConsignmentFFDocument(BaseModel):
    consignment = models.ForeignKey(Consignment, on_delete=models.CASCADE, related_name="ff_documents")
    file = models.FileField(upload_to="consignments/ff_docs/")  
    


# class ConsignmentStaging(BaseModel):
#     user = models.ForeignKey("accounts.User", on_delete=models.CASCADE)
#     purchase_orders = models.ManyToManyField(PurchaseOrder, related_name="staging_consignments")
#     consignor_address = models.ForeignKey("portal.AddressBook", on_delete=models.SET_NULL, null=True, blank=True, related_name="stage_consignment_consignor_addresses")
#     delivery_address = models.ForeignKey("portal.AddressBook", on_delete=models.SET_NULL, null=True, blank=True, related_name="stage_consignment_delivery_addresses")
#     pickup_datetime = models.DateTimeField(null=True, blank=True)
#     pickup_timezone = models.CharField(max_length=255, null=True, blank=True)
#     is_update = models.BooleanField(default=False)
#     existing_consignment_id = models.CharField(max_length=100, null=True, blank=True)
    
#     def __str__(self):
#         return f"Consignment in progress for {self.purchase_order.reference_number} by {self.user.username}"

#     def get_po_lines(self, filter_kwargs=None):
#         qs = self.po_lines.select_related("purchase_order_line")
#         return qs.filter(**filter_kwargs) if filter_kwargs else qs.all()
    # class Meta:
    #     unique_together = ["user", "purchase_order", "is_update", "existing_consignment_id"]
    #     ordering = ("-created_at",)      



# class ConsignmentPOLineStaging(BaseModel):
#     consignment = models.ForeignKey(ConsignmentStaging, on_delete=models.CASCADE, related_name="po_lines")
#     purchase_order_line = models.ForeignKey(PurchaseOrderLine, on_delete=models.CASCADE, related_name="staging_consignments")
#     sku = models.CharField(max_length=100, null=True, blank=True)
#     po_ref = models.CharField(max_length=100)
#     po_line_ref = models.CharField(max_length=100)
#     qty_to_fulfill = models.DecimalField(max_digits=8, decimal_places=2)
#     qty_packed = models.DecimalField(max_digits=8, decimal_places=2, default=0)
#     qty_remaining = models.DecimalField(max_digits=8, decimal_places=2)
#     packages = MSSQLJSONField(default=list)
#     # packages = ArrayField(models.CharField(max_length=100), default=list, blank=True )
#     hs_code = models.CharField(max_length=100, null=True, blank=True)
#     is_dangerous_good = models.BooleanField(default=False) 
#     manufacturing_country = models.CharField(max_length=100, blank=True, null=True)
#     eccn = models.BooleanField(default=False)   
    


# class ConsignmentPackagingStaging(BaseModel):
#     package_id = models.CharField(max_length=50)
#     weight = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
#     weight_unit = models.CharField(max_length=20, choices=WeightUnitChoices.choices, blank=True, null=True)
#     consignment = models.ForeignKey(ConsignmentStaging, on_delete=models.CASCADE, related_name="stage_packagings")
#     packaging_type = models.ForeignKey("portal.PackagingType", on_delete=models.CASCADE, related_name="consignment_stage_packagings")



# class PackagingAllocationStaging(BaseModel):
#     consignment_packaging = models.ForeignKey(ConsignmentPackagingStaging, on_delete=models.CASCADE, related_name="allocations")
#     po_line = models.ForeignKey(ConsignmentPOLineStaging, on_delete=models.CASCADE, related_name="packaging_allocations")
#     allocated_qty = models.DecimalField(max_digits=8, decimal_places=2, default=0, help_text="Quantity from the PO line allocated to this packaging unit.")
    


# class ConsignmentDocumentStaging(BaseModel):
#     consignment = models.ForeignKey(ConsignmentStaging, on_delete=models.CASCADE, related_name="documents")
#     document_type = models.CharField(max_length=50, choices=ConsignmentDocumentTypeChoices.choices)
    


# class ConsignmentDocumentAttachmentStaging(BaseModel):
#     document = models.ForeignKey(ConsignmentDocumentStaging, on_delete=models.CASCADE, related_name="attachments")
#     file = models.FileField(upload_to="consignments/documents/")
    


class AWBFile(BaseModel):
    consignment = models.ForeignKey(Consignment,on_delete=models.CASCADE, related_name="consignment_awb")
    file = models.FileField(upload_to="consignments/awb/")



class PurchaseOrderDetail(BaseModel):
    purchase_order = models.OneToOneField(PurchaseOrder, on_delete=models.CASCADE)
    created_by = models.CharField(max_length=255, null=True, blank=True)
    updated_by = models.CharField(max_length=255, null=True, blank=True)
    apportion_rule = models.CharField(max_length=255, null=True, blank=True)
    buyer_address1 = models.CharField(max_length=255, null=True, blank=True)
    buyer_address2 = models.CharField(max_length=255, null=True, blank=True)
    buyer_address3 = models.CharField(max_length=255, null=True, blank=True)
    buyer_address4 = models.CharField(max_length=255, null=True, blank=True)
    buyer_cid = models.CharField(max_length=255, null=True, blank=True)
    buyer_city = models.CharField(max_length=255, null=True, blank=True)
    buyer_email = models.EmailField(max_length=255, null=True, blank=True)
    buyer_name = models.CharField(max_length=255, null=True, blank=True)
    buyer_phone = models.CharField(max_length=255, null=True, blank=True)
    buyer_reference = models.CharField(max_length=255, null=True, blank=True)
    buyer_state = models.CharField(max_length=255, null=True, blank=True)
    buyer_vat = models.CharField(max_length=255, null=True, blank=True)
    buyer_zip = models.CharField(max_length=255, null=True, blank=True)
    closed_date = models.DateTimeField(null=True, blank=True)
    consignment = models.CharField(max_length=255, null=True, blank=True)
    destination_countryid = models.IntegerField(null=True, blank=True)
    effective_date = models.DateTimeField(null=True, blank=True)
    expected_receipt_date = models.DateTimeField(null=True, blank=True)
    external_pokey2 = models.CharField(max_length=255, null=True, blank=True)
    extern_pokey = models.CharField(max_length=255, null=True, blank=True)
    forte_flag = models.CharField(max_length=255, null=True, blank=True)
    notes = models.TextField(null=True, blank=True)
    origin_countryid = models.IntegerField(null=True, blank=True)
    other_reference = models.CharField(max_length=255, null=True, blank=True)
    place_of_delivery = models.CharField(max_length=255, null=True, blank=True)
    place_of_discharge = models.CharField(max_length=255, null=True, blank=True)
    place_of_issue = models.CharField(max_length=255, null=True, blank=True)
    place_of_loading = models.CharField(max_length=255, null=True, blank=True)
    pmt_term = models.CharField(max_length=255, null=True, blank=True)
    # po_date = models.DateTimeField(null=True, blank=True)
    # po_group = models.CharField(max_length=255, null=True, blank=True)
    po_id = models.CharField(max_length=255, null=True, blank=True)
    po_key = models.CharField(max_length=255, null=True, blank=True)
    # po_type = models.CharField(max_length=255, null=True, blank=True)
    seller_address1 = models.CharField(max_length=255, null=True, blank=True)
    seller_address2 = models.CharField(max_length=255, null=True, blank=True)
    seller_address3 = models.CharField(max_length=255, null=True, blank=True)
    seller_address4 = models.CharField(max_length=255, null=True, blank=True)
    seller_cid = models.CharField(max_length=255, null=True, blank=True)
    seller_city = models.CharField(max_length=255, null=True, blank=True)
    seller_email = models.EmailField(max_length=255, null=True, blank=True)
    seller_name = models.CharField(max_length=255, null=True, blank=True)
    seller_phone = models.CharField(max_length=255, null=True, blank=True)
    seller_reference = models.CharField(max_length=255, null=True, blank=True)
    seller_state = models.CharField(max_length=255, null=True, blank=True)
    seller_vat = models.CharField(max_length=255, null=True, blank=True)
    seller_zip = models.CharField(max_length=255, null=True, blank=True)
    serial_key = models.IntegerField(null=True, blank=True)
    signatory = models.CharField(max_length=255, null=True, blank=True)
    source_location = models.CharField(max_length=255, null=True, blank=True)
    source_version = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=255, null=True, blank=True)
    storer_key = models.CharField(max_length=255, null=True, blank=True)
    susr1 = models.CharField(max_length=255, null=True, blank=True)
    susr2 = models.CharField(max_length=255, null=True, blank=True)
    susr3 = models.CharField(max_length=255, null=True, blank=True)
    susr4 = models.CharField(max_length=255, null=True, blank=True)
    susr5 = models.CharField(max_length=255, null=True, blank=True)
    terms_note = models.CharField(max_length=255, null=True, blank=True)
    trans_method = models.CharField(max_length=255, null=True, blank=True)
    vessel = models.CharField(max_length=255, null=True, blank=True)
    vessel_date = models.DateTimeField(null=True, blank=True)
    whseid = models.CharField(max_length=255, null=True, blank=True)
    
    def __str__(self):
        return self.purchase_order.reference_number
    

    
class PurchaseOrderLineDetail(BaseModel):
    purchase_order_line = models.OneToOneField(PurchaseOrderLine, on_delete=models.CASCADE)
    created_by = models.CharField(max_length=255, null=True, blank=True)
    updated_by = models.CharField(max_length=255, null=True, blank=True)
    consignment = models.CharField(max_length=255, null=True, blank=True)
    effective_date = models.DateTimeField(null=True, blank=True)
    expediting_statusid = models.IntegerField(null=True, blank=True)
    extern_pokey = models.CharField(max_length=255, null=True, blank=True)
    forte_flag = models.CharField(max_length=255, null=True, blank=True)
    is_blocked = models.BooleanField(null=True, blank=True)
    item_number = models.IntegerField(null=True, blank=True)
    marks_container = models.CharField(max_length=255, null=True, blank=True)
    pack_key = models.CharField(max_length=255, null=True, blank=True)
    po_detail_id = models.CharField(max_length=255, null=True, blank=True)
    po_detail_key = models.CharField(max_length=255, null=True, blank=True)
    po_key = models.CharField(max_length=255, null=True, blank=True)
    po_line_number = models.CharField(max_length=255, null=True, blank=True)
    qc_auto_adjust = models.CharField(max_length=255, null=True, blank=True)
    qc_required = models.CharField(max_length=255, null=True, blank=True)
    qty_adjusted = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    qty_ordered = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    qty_received = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    qty_rejected = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    ready_for_collection_date = models.DateTimeField(null=True, blank=True)
    retail_sku = models.CharField(max_length=255, null=True, blank=True)
    serial_key = models.IntegerField(null=True, blank=True)
    sku = models.CharField(max_length=100)
    alt_sku = models.CharField(max_length=100, blank=True, null=True)
    manufacturer_sku = models.CharField(max_length=100, blank=True, null=True)
    extern_line_no = models.CharField(max_length=50, blank=True, null=True)
    sku_cube = models.FloatField(null=True, blank=True)
    sku_hgt = models.FloatField(null=True, blank=True)
    sku_wgt = models.FloatField(null=True, blank=True)
    sku_description = models.CharField(max_length=255, null=True, blank=True)
    source_location = models.CharField(max_length=255, null=True, blank=True)
    source_version = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=255, null=True, blank=True)
    storer_key = models.CharField(max_length=255, null=True, blank=True)
    susr1 = models.CharField(max_length=255, null=True, blank=True)
    susr2 = models.CharField(max_length=255, null=True, blank=True)
    susr3 = models.CharField(max_length=255, null=True, blank=True)
    susr4 = models.CharField(max_length=255, null=True, blank=True)
    susr5 = models.CharField(max_length=255, null=True, blank=True)
    unit_ship = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    unit_price = models.FloatField(null=True, blank=True)
    uom = models.CharField(max_length=255, null=True, blank=True)
    whseid = models.CharField(max_length=255, null=True, blank=True)
    
    

class PurchaseOrderUpload(BaseModel):
    status = models.CharField(max_length=15, choices=POUploadStatusChoices.choices, default=POUploadStatusChoices.IN_PROGRESS)
    uploaded_file = models.FileField(upload_to="po/upload/")
    error_file = models.FileField(upload_to="po/upload/errors/", null=True, blank=True)
    uploaded_by = models.ForeignKey("accounts.User", on_delete=models.SET_NULL, null=True, blank=True)
    file_format = models.CharField(max_length=15, choices=POImportFormatsChoices.choices, default=POImportFormatsChoices.PO)



class TemporaryPurchaseOrder(BaseModel):
    data = MSSQLJSONField(default=list)



class UserGridPreferences(BaseModel):
    user = models.ForeignKey("accounts.User",on_delete=models.CASCADE, null=False, blank=False)
    grid_name = models.CharField(max_length=50,choices=UserGridPreferences.choices,default="")
    order_list = MSSQLJSONField(default=list)

    def __str__(self):
        return f"Grid Order for {self.user.username}"



class AuditTrail(BaseModel):
    po_audit_trail = models.ForeignKey(PurchaseOrder, on_delete=models.CASCADE, related_name="po_audit_trail",null=True,blank=True)
    po_line_audit_trail = models.ForeignKey(PurchaseOrder, on_delete=models.CASCADE, related_name="po_line_audit_trail",null=True,blank=True)

    updated_by = models.ForeignKey("accounts.User", on_delete=models.CASCADE)
    


class AuditTrailField(BaseModel):
    audit_trail = models.ForeignKey(AuditTrail, on_delete=models.CASCADE, related_name="audit_trail_fields")
    title = models.CharField(max_length=150, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    attachments = MSSQLJSONField(default=list)
    field_name = models.CharField(max_length=100)
    old_value = models.TextField(null=True, blank=True)
    new_value = models.TextField(null=True, blank=True)