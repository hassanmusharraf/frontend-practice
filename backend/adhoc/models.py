from django.db import models
from portal.base import BaseModel


class AdhocPurchaseOrder(BaseModel):
    customer_reference_number = models.CharField(max_length=100, unique=True)
    reference_number = models.CharField(max_length=100, unique=True, null=True)
    supplier = models.ForeignKey('entities.Supplier', on_delete=models.CASCADE, related_name="adhoc", null=True, blank=True)
    client = models.ForeignKey('entities.Client', on_delete=models.CASCADE, related_name="adhoc", null=True, blank=True)
    storerkey = models.ForeignKey('entities.StorerKey', on_delete=models.CASCADE, related_name="adhoc", null=True, blank=True)
    plant_id = models.CharField(max_length=255)
    center_code = models.CharField(max_length=255)
    def __str__(self):
        return self.customer_reference_number
    
    

class AdhocPurchaseOrderLine(BaseModel):
    purchase_order = models.ForeignKey(AdhocPurchaseOrder, on_delete=models.CASCADE, related_name="lines")
    customer_reference_number = models.CharField(max_length=100, unique=True)
    reference_number = models.CharField(max_length=100, unique=True, null=True)
    sku = models.CharField(max_length=255, null=True, blank=True)
    
    def __str__(self):
        return self.customer_reference_number

