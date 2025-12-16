from django.contrib import admin
from .models import *

admin.site.register(PurchaseOrder)
admin.site.register(PurchaseOrderLine)
admin.site.register(PurchaseOrderDetail)
admin.site.register(PurchaseOrderLineDetail)
admin.site.register(Consignment)
admin.site.register(ConsignmentPackaging)
# admin.site.register(ConsignmentStaging)