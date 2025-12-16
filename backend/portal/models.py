from django.db import models
from .base import BaseModel
from portal.choices import Address, PackagingTypeChoices, WeightUnitChoices, DimensionUnitChoices, MOTModeChoices, MeasurementTypeChoices,ShipmentTypeChoices
from django.db.models import JSONField
from .choices import NotificationChoices
from core.fields import MSSQLJSONField
class MOT(BaseModel):
    mot_type = models.CharField(max_length=255, unique=True)
    mode = models.CharField(max_length=10, choices=MOTModeChoices.choices)
    
    def __str__(self):
        return self.mot_type
    
    

class FreightForwarder(BaseModel):
    name = models.CharField(max_length=255, unique=True)
    scac = models.CharField(max_length=255)
    mc_dot = models.CharField(max_length=255)
    mot = models.ManyToManyField(MOT, related_name="freight_forwarders")
    
    def __str__(self):
        return self.name
    

    
class AddressBook(BaseModel):
    address_name = models.CharField(max_length=255)
    address_type = models.CharField(max_length=10, choices=Address.choices)
    client = models.ForeignKey("entities.Client", null=True, blank=True, on_delete=models.CASCADE,related_name="addresses")
    supplier = models.ForeignKey("entities.Supplier", null=True, blank=True, on_delete=models.CASCADE,related_name="addresses")
    storerkey = models.ForeignKey("entities.StorerKey", null=True, blank=True, on_delete=models.CASCADE,related_name="addresses")
    address_line_1 = models.CharField(max_length=255)
    address_line_2 = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=100)
    state = models.CharField(max_length=100)
    country = models.CharField(max_length=100)
    zipcode = models.CharField(max_length=20)
    latitude = models.DecimalField(max_digits=9, decimal_places=6, blank=True, null=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6, blank=True, null=True)
    mobile_no = models.TextField()
    alternate_mobile_no = models.TextField(null=True, blank=True)
    responsible_person_name = models.CharField(max_length=255)

    def __str__(self):
        return f"{self.address_line_1}, {self.city}, {self.state},"

    class Meta:
        indexes = [
            models.Index(fields=['client'], name='addr_client_idx'),
            models.Index(fields=['supplier'], name='addr_supplier_idx'),
            models.Index(fields=['storerkey'], name='addr_storerkey_idx'),
            models.Index(fields=['address_type'], name='addr_type_idx'),
            models.Index(fields=['client', 'address_type'], name='addr_client_type_idx'),
            models.Index(fields=['supplier', 'address_type'], name='addr_supplier_type_idx'),
        ]

class PackagingType(BaseModel):
    package_name = models.CharField(max_length=255)
    package_type = models.CharField(max_length=100, choices=PackagingTypeChoices.choices)
    measurement_method = models.CharField(max_length=50, choices=MeasurementTypeChoices.choices)
    supplier = models.ForeignKey("entities.Supplier", on_delete=models.CASCADE)
    description = models.TextField(blank=True, null=True)
    # weight = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    # weight_unit = models.CharField(max_length=20, choices=WeightUnitChoices.choices, blank=True, null=True)
    length = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    width = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    height = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    dimension_unit = models.CharField(max_length=20, choices=DimensionUnitChoices.choices, blank=True, null=True)
    is_stackable = models.BooleanField(default=False)

    class Meta:
        unique_together = ["package_name","supplier"]
        
    def __str__(self):
        return self.package_type
    
    

class GLAccount(BaseModel):
    gl_code = models.CharField(max_length=100,null=False,blank=False)
    shipment_type = models.CharField(max_length=255,choices=ShipmentTypeChoices.choices,default=ShipmentTypeChoices.Freight_Related_Expenses)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['gl_code', 'shipment_type'], name='unique_glcode_shipmenttype')
        ]

    def __str__(self):
        return self.gl_code
    

    
class CostCenterCode(BaseModel):
    cc_code = models.CharField(max_length=100, unique=True)
    plant_id = models.CharField(max_length=100)
    center_code = models.CharField(max_length=100)
    sloc = models.CharField(max_length=100)

    def __str__(self):
        return self.cc_code
    class Meta:
        unique_together = ["plant_id","center_code","sloc"]
    


class RejectionCode(BaseModel):
    rejection_code = models.CharField(max_length=100, unique=True)
    
    def __str__(self):
        return self.rejection_code
    


class DropDownValues(BaseModel):
    dropdown_name = models.CharField(max_length=100)
    label = models.CharField(max_length=256)
    value = models.CharField(max_length=256)
    parent_item = models.ForeignKey(
            'self',
            on_delete=models.CASCADE,
            null=True,
            blank=True,
            related_name='children'
        )
   
    class Meta:
        unique_together = ('label', 'parent_item')
 
    def __str__(self):
        return self.dropdown_name + ' | ' + self.label + ' | ' + self.value
   
    def save(self, *args, **kwargs):
        if not self.value:
            self.value = self.label
        return super().save(*args, **kwargs)
    


class Notification(BaseModel):
    
    header = models.CharField(max_length=255)
    type = models.CharField(max_length=20, choices=NotificationChoices.choices,default = NotificationChoices.CONSIGNMENT)
    message = models.TextField()
    hyperlink_value = MSSQLJSONField(default=dict, blank=True, null=True)
    attachment = models.CharField(max_length=150,null=True, blank=True)
    po_upload = models.ForeignKey("operations.PurchaseOrderUpload", on_delete=models.CASCADE, null=True, blank=True)

    # Receiver group flags
    users = models.ManyToManyField(
        "accounts.User",
        through='UserNotification',
        related_name='notifications',
        blank=True,
    )


    def __str__(self):
        return self.header
    


class UserNotification(BaseModel):
    
    user = models.ForeignKey("accounts.User", on_delete=models.CASCADE)
    notification = models.ForeignKey(Notification, on_delete=models.CASCADE, related_name='user_notifications')
    is_read = models.BooleanField(default=False)

    class Meta:
        unique_together = ('user', 'notification')

    def __str__(self):
        return self.notification.header