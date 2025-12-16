from django.db import models
from portal.base import BaseModel
from portal.choices import ServiceTypeChoices,MeasurementTypeChoices,OperationUserRole,OrderTypeChoices


class Hub(BaseModel):
    hub_code = models.CharField(max_length=255, unique=True)
    name = models.CharField(max_length=255)
    location = models.TextField(blank=True, null=True)
    
    def __str__(self):
        return self.name



class Client(BaseModel):
    client_code = models.CharField(max_length=100, unique=True)
    name = models.CharField(max_length=255)
    # show_parent_add = models.BooleanField(default=False)
    # can_add_shipto_add = models.BooleanField(default=False)
    # timezone = models.CharField(max_length=255)
    # service_type = models.CharField(max_length=50, choices=ServiceTypeChoices.choices)
    # measurement_method = models.CharField(max_length=50, choices=MeasurementTypeChoices.choices)

    def __str__(self):
        return self.name
        
    class Meta:
        indexes = [
            models.Index(fields=['client_code'], name='client_code_idx'),
            models.Index(fields=['name'], name='client_name_idx'),
            models.Index(fields=['is_active'], name='client_active_idx'),
        ]



class StorerKey(BaseModel):
    storerkey_code = models.CharField(max_length=100, unique=True)
    aramex_wms_storerkey = models.CharField(max_length=100)   ## This srorerkey is refer in aramex wms 
    name = models.CharField(max_length=255)
    client = models.ForeignKey(Client, on_delete=models.RESTRICT,related_name="storerkeys")
    hub = models.ForeignKey(Hub, on_delete=models.CASCADE, related_name="storerkeys")
    cc_code = models.ForeignKey("portal.CostCenterCode", null=True, blank=True, on_delete=models.RESTRICT,related_name="storerkeys")

    generate_asn = models.BooleanField(default=False)
    hs_code_validation = models.BooleanField(default=False)
    hs_code = models.CharField(max_length=255, null=True, blank=True)
    eccn_validation = models.BooleanField(default=False)
    chemical_good_handling = models.BooleanField(default=False)
    adhoc_applicable = models.BooleanField(default=False)
    expediting_applicable = models.BooleanField(default=False)
    order_type = models.CharField(max_length=5,choices=OrderTypeChoices,default=OrderTypeChoices.BTS)

    show_parent_add = models.BooleanField(default=False)
    can_add_shipto_add = models.BooleanField(default=False)
    timezone = models.CharField(max_length=255)
    service_type = models.CharField(max_length=50, choices=ServiceTypeChoices.choices)
    measurement_method = models.CharField(max_length=50, choices=MeasurementTypeChoices.choices, default=MeasurementTypeChoices.IMPERIAL_SYSTEM)




    def __str__(self):
        return self.storerkey_code +" | "+ self.name
    

    class Meta:
        indexes = [
            models.Index(fields=['storerkey_code'], name='sk_code_idx'),
            models.Index(fields=['client'], name='sk_client_idx'),
            models.Index(fields=['hub'], name='sk_hub_idx'),
            models.Index(fields=['client', 'hub'], name='sk_client_hub_idx'),
            models.Index(fields=['hs_code_validation'], name='sk_hs_idx'),
            models.Index(fields=['chemical_good_handling'], name='sk_chemical_idx'),
            models.Index(fields=['is_active'], name='sk_active_idx'),
            models.Index(fields=['order_type'], name='sk_order_type_idx'),
            # models.Index(fields=['generate_asn'], name='sk_asn_idx'),
        ]



class StorerKeyReminder(BaseModel):
    storerkey = models.ForeignKey(StorerKey, on_delete=models.CASCADE,related_name="reminders")
    name = models.CharField(max_length=200)
    trigger_days = models.IntegerField(default=0)

    class Meta:
        unique_together = ["storerkey", "name"]



class Supplier(BaseModel):
    supplier_code = models.CharField(max_length=100, unique=True)
    name = models.CharField(max_length=255)
    address = models.TextField()
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="suppliers")
    storerkeys = models.ManyToManyField(StorerKey, related_name="suppliers")

    def save(self, *args, **kwargs):
        if not self.is_active: 
            users = SupplierUser.objects.filter(supplier=self, is_active=True)
            users.update(is_active=False) 
        super().save(*args, **kwargs) 
    
    def __str__(self):
        return self.name

    class Meta:
        indexes = [
            models.Index(fields=['supplier_code'], name='supplier_code_idx'),
            models.Index(fields=['client'], name='supplier_client_idx'),
            models.Index(fields=['name'], name='supplier_name_idx'),
            models.Index(fields=['is_active'], name='supplier_active_idx'),
            models.Index(fields=['client', 'is_active'], name='supplier_client_active_idx'),
            models.Index(fields=['created_at'], name='supplier_created_idx'),
        ]



class ClientUser(BaseModel):
    user = models.OneToOneField("accounts.User", on_delete=models.CASCADE, related_name="client_profile")
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="client_users")
    suppliers = models.ManyToManyField(Supplier, related_name="client_user_profiles", blank=True)  # Subset of client's suppliers
    storerkeys = models.ManyToManyField(StorerKey, related_name="client_users")
    
    def __str__(self):
        return f"{self.user.username} ({self.client.name})"

    class Meta:
        indexes = [
            models.Index(fields=['client'], name='cu_client_idx'),
            models.Index(fields=['user'], name='cu_user_idx'),
            models.Index(fields=['is_active'], name='cu_active_idx'),
            models.Index(fields=['client', 'is_active'], name='cu_client_active_idx'),
        ]



class SupplierUser(BaseModel):
    user = models.OneToOneField("accounts.User", on_delete=models.CASCADE, related_name="supplier_profile")
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE, related_name="supplier_users")
    storerkeys = models.ManyToManyField(StorerKey, related_name="supplier_users")
    
    def __str__(self):
        return f"{self.user.username} ({self.supplier.name})"
    
    class Meta:
        indexes = [
            models.Index(fields=['supplier'], name='su_supplier_idx'),
            models.Index(fields=['user'], name='su_user_idx'),
            models.Index(fields=['is_active'], name='su_active_idx'),
            models.Index(fields=['supplier', 'is_active'], name='su_supplier_active_idx'),
        ]



class Operations(BaseModel):
    user = models.OneToOneField("accounts.User", on_delete=models.CASCADE, related_name="operations_profile")
    hubs = models.ManyToManyField(Hub, related_name="operations", blank=True)  
    storerkeys = models.ManyToManyField(StorerKey, related_name="operations", blank=True)
    access_level = models.CharField(max_length=50,choices=OperationUserRole.choices,default=OperationUserRole.L3)

    def __str__(self):
        return f"{self.user.username}"



class DangerousGoodClass(BaseModel):
    name = models.CharField(max_length=128, unique=True)

    def __str__(self):
        return f"{self.code} - {self.name}"
    


class DangerousGoodCategory(BaseModel):
    dg_class = models.ForeignKey(DangerousGoodClass, on_delete=models.CASCADE, related_name="categories")
    name = models.CharField(max_length=255)



class MaterialMaster(BaseModel):
    ##Relations
    storerkey = models.ForeignKey("entities.StorerKey", on_delete=models.CASCADE, related_name="material_master")
    hub = models.ForeignKey("entities.Hub", on_delete=models.CASCADE, related_name="material_master")

    ##Required fields for validation
    product_code = models.CharField(max_length=128)
    is_chemical = models.BooleanField(default=False)
    is_dangerous_good = models.BooleanField(default=False)
    
    description = models.TextField(blank=True, null=True)
    uom = models.CharField(max_length=128,blank=True, null=True)
    unit_price = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    unit_cost = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    weight = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    volume = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    length = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    width = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    height = models.DecimalField(max_digits=16, decimal_places=2, blank=True, null=True)
    hs_code = models.CharField(max_length=128, blank=True, null=True)
    stock_number = models.CharField(max_length=128, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    alternate_unit = models.CharField(max_length=128, blank=True, null=True)
    alternate_sku = models.CharField(max_length=128, blank=True, null=True)
    origin_country = models.CharField(max_length=50, blank=True, null=True)
    to_expire_days = models.PositiveSmallIntegerField(default=0)
    to_delivery_days = models.PositiveSmallIntegerField(default=0)
    to_best_by_days = models.PositiveSmallIntegerField(default=0)
    is_kit = models.BooleanField(default=False)
    is_stackable = models.BooleanField(default=False)
    inspection_required = models.BooleanField(default=False)
    shelf_life = models.PositiveSmallIntegerField(default=0)
    shelf_life_indicator = models.CharField(max_length=256, null=True, blank=True)
    sku_class = models.CharField(max_length=256, null=True, blank=True)
    retail_sku = models.CharField(max_length=256, null=True, blank=True)
    hazmat_codes_keys = models.CharField(max_length=256, null=True, blank=True)
    susr1 = models.CharField(max_length=256, null=True, blank=True)
    susr2 = models.CharField(max_length=256, null=True, blank=True)
    susr3 = models.CharField(max_length=256, null=True, blank=True)
    susr4 = models.CharField(max_length=256, null=True, blank=True)
    susr5 = models.CharField(max_length=256, null=True, blank=True)


    class Meta:
        unique_together = ["product_code","storerkey","hub"]    
    
        indexes = [
            models.Index(fields=['storerkey'], name='mm_storerkey_idx'),
            models.Index(fields=['hub'], name='mm_hub_idx'),
            models.Index(fields=['product_code'], name='mm_product_code_idx'),
            models.Index(fields=['is_dangerous_good'], name='mm_dg_idx'),
            models.Index(fields=['is_chemical'], name='mm_chemical_idx'),
            models.Index(fields=['storerkey', 'product_code'], name='mm_storerkey_product_idx'),
            models.Index(fields=['is_stackable'], name='mm_stackable_idx'),
        ]