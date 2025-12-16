from rest_framework.serializers import ModelSerializer
from rest_framework import serializers
from .models import Client, Hub, ClientUser, Supplier, SupplierUser, Operations, StorerKey, DangerousGoodClass,DangerousGoodCategory, MaterialMaster, StorerKeyReminder
from accounts.serializers import UserListSerializer

class HubSerializer(ModelSerializer):
    class Meta:
        model = Hub
        fields = '__all__'


class StorerKeySerializer(ModelSerializer):
    class Meta:
        model = StorerKey
        fields = '__all__'


class StorerKeyListSerializer(ModelSerializer):
    hub_code = serializers.CharField(source="hub.hub_code")
    hub_id = serializers.CharField(source="hub.id")
    class Meta:
        model = StorerKey
        fields = ["id", "storerkey_code", "name", "hub_code","hub_id"]


class ClientSerializer(ModelSerializer):
    class Meta:
        model = Client
        fields = '__all__'


class ClientUserSerializer(ModelSerializer):
    class Meta:
        model = ClientUser
        fields = '__all__'



class SupplierSerializer(ModelSerializer):
    class Meta:
        model = Supplier
        fields = '__all__'



class GetSupplierSerializer(ModelSerializer):
    storerkeys = StorerKeyListSerializer(read_only=True, many=True)
    
    class Meta:
        model = Supplier
        fields = '__all__'


class SupplierUserSerializer(ModelSerializer):
    class Meta:
        model = SupplierUser
        fields = '__all__'


class OperationsSerializer(ModelSerializer):
    class Meta:
        model = Operations
        fields = '__all__'


class GetClientSerializer(ModelSerializer):
    hub = HubSerializer(read_only=True)
    class Meta:
        model = Client
        fields = '__all__'


class GetSupplierSerializer(ModelSerializer):
    client = ClientSerializer(read_only=True)
    storerkeys = StorerKeyListSerializer(read_only=True, many=True)
    class Meta:
        model = Supplier
        fields = '__all__'


class GetClientUserSerializer(ModelSerializer):
    user = UserListSerializer(read_only=True)
    suppliers = GetSupplierSerializer(read_only=True, many=True)
    storerkeys = StorerKeyListSerializer(read_only=True, many=True)
    client = ClientSerializer(read_only=True)
    class Meta:
        model = ClientUser
        fields = '__all__'

class GetClientSupplierAndStorerSerializer(ModelSerializer):
    suppliers = SupplierSerializer(many=True, read_only=True)  # Nested supplier serializer
    storerkeys = StorerKeySerializer(many=True, read_only=True) # Nested storerkey serializer
    class Meta:
        model = Client
        fields = '__all__'

class GetSupplierUserSerializer(ModelSerializer):
    user = UserListSerializer(read_only=True)
    supplier = SupplierSerializer(read_only=True)
    storerkeys = StorerKeyListSerializer(read_only=True, many=True)
    class Meta:
        model = SupplierUser
        fields = '__all__'
        
        
class GetOperationsSerializer(ModelSerializer):
    user = UserListSerializer(read_only=True)
    hubs = HubSerializer(read_only=True, many=True)
    storerkeys = StorerKeyListSerializer(read_only = True, many=True)
    class Meta:
        model = Operations
        fields = '__all__'
        
class StorerKeyReminderSerializer(serializers.ModelSerializer):
    class Meta:
        model = StorerKeyReminder
        fields = '__all__'
        
class GetStorerKeySerializer(ModelSerializer):
    from portal.serializers import CostCenterCodeSerializer

    client = ClientSerializer(read_only=True)
    hub = HubSerializer(read_only=True)
    cc_code = CostCenterCodeSerializer(read_only=True)
    reminders = serializers.SerializerMethodField()
    class Meta:
        model = StorerKey
        fields = '__all__'

    def get_reminders(self, obj):
        reminders = obj.reminders.order_by('-updated_at')
        return StorerKeyReminderSerializer(reminders, many=True).data


class DangerousGoodCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = DangerousGoodCategory
        fields = ["id", "dg_class", "name"]

class DangerousGoodClassSerializer(serializers.ModelSerializer):
    categories = serializers.ListField(
        child=serializers.CharField(), write_only=True, required=False
    )
    categories_data = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = DangerousGoodClass
        fields = ["id", "name", "categories", "categories_data"]

    def get_categories_data(self, obj):
        return list(obj.categories.values("id", "name"))

    def _set_categories(self, dg_class, categories, replace=False):
        if replace:
            dg_class.categories.all().delete()
        if categories:
            objs = [DangerousGoodCategory(dg_class=dg_class, name=cat) for cat in categories]
            DangerousGoodCategory.objects.bulk_create(objs)

    def create(self, validated_data):
        categories = validated_data.pop("categories", [])
        dg_class = DangerousGoodClass.objects.create(**validated_data)
        self._set_categories(dg_class, categories)
        return dg_class

    def update(self, instance, validated_data):
        categories = validated_data.pop("categories", None)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        if categories is not None:
            self._set_categories(instance, categories, replace=True)
        return instance

    
class MaterialSerializer(serializers.ModelSerializer):
    class Meta:
        model = MaterialMaster
        fields = '__all__'