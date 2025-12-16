from rest_framework.serializers import ModelSerializer
from .models import AdhocPurchaseOrder, AdhocPurchaseOrderLine


class AdhocPurchaseOrderListSerializer(ModelSerializer):
    class Meta:
        model = AdhocPurchaseOrder
        fields = ["reference_number", "customer_reference_number", "id"]


class AdhocPurchaseOrderLineListSerializer(ModelSerializer):
    class Meta:
        model = AdhocPurchaseOrderLine
        fields = ["reference_number", "customer_reference_number", "id"]