from rest_framework.serializers import ModelSerializer
from rest_framework import serializers
from decimal import Decimal, InvalidOperation
from .models import AddressBook, PackagingType, MOT, FreightForwarder, GLAccount, CostCenterCode, RejectionCode,Notification
from entities.serializers import ClientSerializer, SupplierSerializer


class MOTSerializer(ModelSerializer):
    class Meta:
        model = MOT
        fields = '__all__'


class FreightForwarderSerializer(ModelSerializer):
    class Meta:
        model = FreightForwarder
        fields = '__all__'


class GetFreightForwarderSerializer(ModelSerializer):
    mot = MOTSerializer(many = True)
    class Meta:
        model = FreightForwarder
        fields = '__all__'


class AddressBookSerializer(ModelSerializer):
    class Meta:
        model = AddressBook
        fields = '__all__'


class GetAddressBookSerializer(ModelSerializer):
    client = ClientSerializer(read_only=True)
    supplier = SupplierSerializer(read_only=True)
    class Meta:
        model = AddressBook
        fields = '__all__'


class PackagingTypeSerializer(ModelSerializer):
    class Meta:
        model = PackagingType
        fields = '__all__'


class GetPackagingTypeSerializer(ModelSerializer):
    supplier = SupplierSerializer(read_only=True)
    class Meta:
        model = PackagingType
        fields = '__all__'


class GLAccountSerializer(ModelSerializer):
    class Meta:
        model = GLAccount
        fields = '__all__'

class CostCenterCodeSerializer(ModelSerializer):
    class Meta:
        model = CostCenterCode
        fields = '__all__'


class RejectionCodeSerializer(ModelSerializer):
    class Meta:
        model = RejectionCode
        fields = '__all__'


class PostAddressBookSerializer(serializers.ModelSerializer):
    latitude = serializers.DecimalField(
        max_digits=9,
        decimal_places=7,
        required=False,
        allow_null=True
    )
    longitude = serializers.DecimalField(
        max_digits=9,
        decimal_places=7,
        required=False,
        allow_null=True
    )

    class Meta:
        model = AddressBook
        fields = '__all__'

    def validate_latitude(self, value):
        return self._validate_decimal_field(value, "latitude")

    def validate_longitude(self, value):
        return self._validate_decimal_field(value, "longitude")

    def _validate_decimal_field(self, value, field_name):
        if value in [None, '']:
            return None  # Allow blank/null

        try:
            value = Decimal(value)
        except (InvalidOperation, TypeError, ValueError):
            raise serializers.ValidationError(f"{field_name.capitalize()} must be a valid decimal number.")

        # Check digit constraints
        sign, digits, exponent = value.as_tuple()
        total_digits = len(digits)
        decimal_places = -exponent if exponent < 0 else 0

        if total_digits > 9:
            raise serializers.ValidationError(f"{field_name.capitalize()} must not exceed 9 digits in total.")
        if decimal_places > 7:
            raise serializers.ValidationError(f"{field_name.capitalize()} must not exceed 7 digits after the decimal point.")

        return value
    

    
class NotificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Notification
        fields = '__all__'