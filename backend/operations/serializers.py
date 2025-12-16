from rest_framework.serializers import ModelSerializer
from rest_framework import serializers
from .models import (AWBFile,ConsignmentDocumentAttachment, ConsignmentDocument, PurchaseOrder, PurchaseOrderLine, Consignment,
    #  ConsignmentStaging, ConsignmentDocumentAttachmentStaging, ConsignmentDocumentStaging,
    UserGridPreferences, ConsignmentFFDocument, ConsignmentPOLine,ConsignmentPOLineBatch,ConsignmentPackaging,PackagingAllocation,
    ComprehensiveReport)
from entities.serializers import ClientSerializer, SupplierSerializer, StorerKeyListSerializer
from portal.serializers import GetAddressBookSerializer
from adhoc.serializers import AdhocPurchaseOrderListSerializer, AdhocPurchaseOrderLineListSerializer
from operations.mixins import PurchaseOrderMixin


class PurchaseOrderSerializer(ModelSerializer):
    class Meta:
        model = PurchaseOrder
        fields = '__all__'


class PurchaseOrderListSerializer(ModelSerializer):
    class Meta:
        model = PurchaseOrder
        fields = ["reference_number", "customer_reference_number", "id"]


class GetPurchaseOrderSerializer(ModelSerializer):
    supplier = SupplierSerializer(read_only=True)
    client = ClientSerializer(read_only=True)
    storerkey = StorerKeyListSerializer(read_only=True)
    class Meta:
        model = PurchaseOrder
        fields = '__all__'


class PurchaseOrderLineSerializer(ModelSerializer,PurchaseOrderMixin):
    consignment_count = serializers.SerializerMethodField()
    class Meta:
        model = PurchaseOrderLine
        fields = '__all__'

    def get_consignment_count(self,instance):
        return self.line_related_consignments(instance,get_count=True)


class ConsignmentSerializer(ModelSerializer):
    purchase_orders = serializers.PrimaryKeyRelatedField(
        many=True, queryset=PurchaseOrder.objects.filter(is_active=True)
    )
    class Meta:
        model = Consignment
        fields = '__all__'


class ConsignmentAdhocSerializer(ModelSerializer):
    adhoc = AdhocPurchaseOrderListSerializer(read_only=True)
    class Meta:
        model = Consignment
        fields = '__all__'



class GetConsignmentSerializer(ModelSerializer):
    supplier = SupplierSerializer(read_only=True)
    client = ClientSerializer(read_only=True)
    purchase_order = PurchaseOrderSerializer(read_only=True)
    class Meta:
        model = Consignment
        fields = '__all__'
        

# class ConsignmentStagingSerializer(ModelSerializer):
#     class Meta:
#         model = ConsignmentStaging
#         fields = '__all__'


class ConsignmentDocumentAttachmentSerializer(ModelSerializer):
    class Meta:
        model = ConsignmentDocumentAttachment
        fields = ["id", "file"]
        

class AdhocConsignmentDocumentSerializer(ModelSerializer):
    adhoc_line = AdhocPurchaseOrderLineListSerializer(read_only=True)
    attachments = ConsignmentDocumentAttachmentSerializer(read_only=True, many=True)
    
    class Meta:
        model = ConsignmentDocument
        fields = '__all__'


# class ConsignmentDocumentAttachmentStagingSerializer(ModelSerializer):
#     class Meta:
#         model = ConsignmentDocumentAttachmentStaging
#         fields = ["id", "file"]


# class GetConsignmentDocumentStagingSerializer(ModelSerializer):
#     attachments = ConsignmentDocumentAttachmentStagingSerializer(many=True, read_only=True)
#     class Meta:
#         model = ConsignmentDocumentStaging
#         fields = ["id", "attachments", "document_type"]


# class GetConsignmentStagingSerializer(ModelSerializer):
#     delivery_address = GetAddressBookSerializer(read_only=True)
#     consignor_address = GetAddressBookSerializer(read_only=True)
#     documents = GetConsignmentDocumentStagingSerializer(read_only=True, many=True)
#     requested_pickup_datetime = serializers.DateTimeField(source="pickup_datetime")
#     class Meta:
#         model = ConsignmentStaging
#         fields = '__all__'


# class GetConsignmentSerializer(ModelSerializer):
#     supplier = SupplierSerializer(read_only=True)
#     client = ClientSerializer(read_only=True)
#     purchase_order = PurchaseOrderListSerializer(read_only=True)
#     adhoc = AdhocPurchaseOrderListSerializer(read_only=True)
#     delivery_address = GetAddressBookSerializer(read_only=True)
#     consignor_address = GetAddressBookSerializer(read_only=True)
#     attachments = serializers.SerializerMethodField()

#     def get_attachments(self, obj):
#         attachments = ConsignmentDocumentAttachment.objects.filter(document__consignment=obj)
#         return ConsignmentDocumentAttachmentSerializer(attachments, many=True).data
    
#     class Meta:
#         model = Consignment
#         fields = '__all__'


class GetConsignmentSerializer(ModelSerializer):
    supplier = SupplierSerializer(read_only=True)
    client = ClientSerializer(read_only=True)
    purchase_order = PurchaseOrderListSerializer(read_only=True)
    adhoc = AdhocPurchaseOrderListSerializer(read_only=True)
    delivery_address = GetAddressBookSerializer(read_only=True)
    consignor_address = GetAddressBookSerializer(read_only=True)
    attachments = serializers.SerializerMethodField()

    def get_attachments(self, obj):
        queryset1 = ConsignmentDocumentAttachment.objects.filter(document__consignment=obj)
        queryset2 = ConsignmentFFDocument.objects.filter(consignment=obj)
        
        attachments = list(queryset1) + list(queryset2)
        return [attachment.file.url for attachment in attachments]
    
    
    class Meta:
        model = Consignment
        fields = '__all__'


class OrderItemSerializer(serializers.Serializer):
    order = serializers.IntegerField()
    name = serializers.CharField()
    visibility = serializers.BooleanField()


class UserGridPreferecesSerializer(ModelSerializer):

    grid_name = serializers.CharField(max_length=100)
    order_list = serializers.ListSerializer(child = OrderItemSerializer())

    def validate_order_list(self, value):
        orders = set()
        names = set()

        for index, item in enumerate(value):
            order = item.get("order")
            name = item.get("name")

            if order in orders:
                raise serializers.ValidationError(
                    f"Duplicate 'order' found at index {index}: {order}"
                )
            if name in names:
                raise serializers.ValidationError(
                    f"Duplicate 'name' found at index {index}: '{name}'"
                )

            orders.add(order)
            names.add(name)

        return value

    class Meta:
        model = UserGridPreferences
        fields = '__all__'
        

class AWBFileUploadSerializer(serializers.ModelSerializer):
    consignment_id = serializers.CharField(write_only=True)
    class Meta:
        model = AWBFile
        fields = ['consignment_id', 'file']

    def validate(self, data):
        try:
            data['consignment'] = Consignment.objects.get(consignment_id=data['consignment_id'])
        except Consignment.DoesNotExist:
            raise serializers.ValidationError({'consignment_id': 'Invalid consignment ID'})
        return data

    def create(self, validated_data):
        validated_data.pop('consignment_id')  # Already used for lookup
        return AWBFile.objects.create(**validated_data)
    

class ConsignmentDocumentSerializer(ModelSerializer):
    class Meta:
        model = ConsignmentDocument
        fields = ["id", "consignment", "adhoc_line","document_type","file"]

    

class ConsignmentComplianceSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConsignmentPOLine
        fields = '__all__'

    


class ConsignmentBatchSerializer(serializers.ModelSerializer):

    class Meta:
        model = ConsignmentPOLineBatch
        fields = '__all__'



class PackagingAllocationSerializer(serializers.ModelSerializer):
    purchase_order_line = serializers.PrimaryKeyRelatedField(
        queryset=PurchaseOrderLine.objects.all()
    )

    class Meta:
        model = PackagingAllocation
        fields = ["id", "purchase_order_line", "allocated_qty", "is_dangerous_good"]


class ConsignmentPackagingSerializer(serializers.ModelSerializer):
    purchase_order_lines = serializers.ListField(write_only=True, required=False)

    class Meta:
        model = ConsignmentPackaging
        fields = [
            "id",
            "package_id",
            "consignment",
            "packaging_type",
            "status",
            "received_date_time",
            "time_zone",
            "weight",
            "weight_unit",
            "purchase_order_lines",
        ]

    def create(self, validated_data):
        purchase_order_lines = validated_data.pop("purchase_order_lines", [])

        # Delete existing package for the given consignment selected lines where draft package_id is null.
        ConsignmentPackaging.objects.filter(
            consignment=validated_data.get("consignment"),
            draft_package_id__isnull = True
        ).delete()

        package = ConsignmentPackaging.objects.create(**validated_data)
        lines_to_create = []
        for lines in purchase_order_lines:
                lines_to_create.append(PackagingAllocation(consignment_packaging=package,purchase_order_line_id = lines))
                
        if lines_to_create:            
            PackagingAllocation.objects.bulk_create(lines_to_create)

        return package



class ComprehensiveReportSerializer(serializers.ModelSerializer):
    status = serializers.JSONField()  # Accepts dict/list JSON
    consignment_ids = serializers.JSONField()
    class Meta:
        model = ComprehensiveReport
        fields = "__all__"