from rest_framework.serializers import ModelSerializer
from rest_framework import serializers
from .models import Console
from operations.models import PurchaseOrder, Consignment, PurchaseOrderLine, PackagingAllocation, ConsignmentPackaging
from portal.serializers import GLAccountSerializer, CostCenterCodeSerializer, GetAddressBookSerializer, FreightForwarderSerializer
from operations.serializers import SupplierSerializer
from portal.models import PackagingType
from django.db.models import Count, F
# from portal.utils import get_cc_code

class ConsoleSerializer(ModelSerializer):
    class Meta:
        model = Console
        fields = '__all__'
        read_only_fields = ['console_id'] 
 
        
# class BOLGenerateSerializer(ModelSerializer):
#     # cc_code = CostCenterCodeSerializer(read_only=True)
#     # gl_account = GLAccountSerializer(read_only=True)
#     ship_from = GetAddressBookSerializer(read_only=True)
#     ship_to = GetAddressBookSerializer(read_only=True)
#     freight_forwarder = FreightForwarderSerializer(read_only=True)

#     class Meta:
#         model = BOL
#         fields = "__all__"

#     def to_representation(self, instance):
#         representation = super().to_representation(instance)

#         # Fetch related consignments and package IDs
#         consignment_ids = Consignment.objects.filter(bol=instance).values_list("id", flat=True)
#         package_qs = ConsignmentPackaging.objects.filter(consignment_id__in=consignment_ids).select_related("packaging_type")
#         package_ids = list(package_qs.values_list("id", flat=True))

#         # Prefetch allocations
#         packaging_allocations = PackagingAllocation.objects.filter(
#             consignment_packaging_id__in=package_ids
#         ).select_related("consignment_packaging__packaging_type","purchase_order_line").distinct()

#         packaging_weight_map = {
#             pkg.id: pkg.weight for pkg in package_qs
#         }        
#         # Prefetch staging weights into a mapping: {packaging_type_id: weight}
#         # staging_weights = dict(
#         #     ConsignmentPackagingStaging.objects.filter(
#         #         packaging_type_id__in=package_qs.values_list("packaging_type_id", flat=True)
#         #     ).values_list("packaging_type_id", "weight")s
#         # )

#         # Build sku_data list
#         sku_data = []
#         for pack in packaging_allocations:
#             consignment_packaging = pack.consignment_packaging
#             packaging_type = pack.consignment_packaging.packaging_type
#             box_weight = packaging_weight_map.get(consignment_packaging.id)
#             line = pack.purchase_order_line
#             sku_data.append({
#                 "description": line.description,
#                 "sku_qty": pack.allocated_qty,
#                 "box_type": packaging_type.package_type,
#                 "box_weight": box_weight
#             })

#         representation["sku_data"] = sku_data

#         # Build packages summary
#         dimension_summary = (
#             package_qs.values(
#                 "packaging_type_id",
#                 "packaging_type__package_type",
#                 "packaging_type__length",
#                 "packaging_type__width",
#                 "packaging_type__height",
#                 "packaging_type__dimension_unit"
#             )
#             .annotate(count=Count("packaging_type_id"))
#         )

#         unit_map = {
#             "Millimeter": "mm",
#             "Centimeter": "cm",
#             "Inch": "In",
#             "Foot": "ft",
#             "Yard": "yd"
#         }

#         packages = [
#             f"{item['count']} {item['packaging_type__package_type']} "
#             f"({item['packaging_type__length']} X {item['packaging_type__width']} X {item['packaging_type__height']}) "
#             f"{unit_map.get(item['packaging_type__dimension_unit'], item['packaging_type__dimension_unit'])} "
#             for item in dimension_summary
#         ]

#         representation["packages"] = packages
#         return representation

    

# class XMLGnerateSerializer(ModelSerializer):
#     cc_code = CostCenterCodeSerializer(read_only=True)
#     gl_account = GLAccountSerializer(read_only=True)
#     ship_from = GetAddressBookSerializer(read_only=True)
#     ship_to = GetAddressBookSerializer(read_only=True)
    
#     class Meta:
#         model = BOL
#         fields = "__all__"
        
#     def to_representation(self, instance):
#         representation = super().to_representation(instance)
                
#         # representation["po_lines"] = PurchaseOrderLine.objects.filter(packaging_allocations__consignment_packaging__consignment__bol=instance).values(
#         #     "reference_number", "customer_reference_number", "product_code", "description", "quantity")
        
#         representation["po_lines"] = PackagingAllocation.objects.filter(consignment_packaging__consignment__bol=instance).select_related("purchase_order_line").annotate(
#             reference_number=F("purchase_order_line__reference_number"),
#             customer_reference_number=F("purchase_order_line__customer_reference_number"),
#             product_code=F("purchase_order_line__product_code"),
#             description=F("purchase_order_line__description"),
#         ).values("reference_number", "customer_reference_number", "product_code", "description", "allocated_qty")

#         con = Consignment.objects.filter(bol_id = instance.pk).select_related("purchase_order").only("gl_code","purchase_order").first()
#         representation["gl_code"] = con.gl_code or None
#         representation["cc_code"] = get_cc_code(con.purchase_order.plant_id,con.purchase_order.center_code)
#         return representation
    