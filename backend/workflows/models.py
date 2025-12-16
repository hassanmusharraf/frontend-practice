from django.db import models
from portal.base import BaseModel
from portal.choices import ConsoleStatusChoices
from django.core.files.storage import default_storage


class Console(BaseModel):
    console_id = models.CharField(max_length=100, unique=True)
    console_status = models.CharField(max_length=100, choices=ConsoleStatusChoices.choices, default=ConsoleStatusChoices.NEW)
    freight_forwarder = models.ForeignKey("portal.FreightForwarder", on_delete=models.PROTECT, related_name="consoles",null=True,blank=True)
    gl_account = models.ForeignKey("portal.GLAccount", on_delete=models.PROTECT, related_name="consoles",null=True,blank=True)
    last_bol_generated_at = models.DateTimeField(null=True, blank=True)
    last_bol_generated_by = models.ForeignKey("accounts.User", on_delete=models.SET_NULL, null=True, blank=True, related_name="console_generated_by")
    
    def __str__(self):
        return self.console_id
    
    def save(self, *args, **kwargs):
        if not self.console_id:
            console = Console.objects.all().order_by('-created_at')[:1].only('console_id')
            if console.exists():
                self.console_id = "CN{0:0=6d}".format(int(console.first().console_id[2:]) + 1) 
            else:
                self.console_id = "CN000001" 
        return super().save(*args, **kwargs)

    class Meta:
        indexes = [
            models.Index(fields=['console_id'], name='console_id_idx'),
            models.Index(fields=['console_status'], name='console_status_idx'),
            models.Index(fields=['freight_forwarder'], name='console_ff_idx'),
            models.Index(fields=['gl_account'], name='console_gl_idx'),
            models.Index(fields=['created_at'], name='console_created_idx'),
            models.Index(fields=['console_status', 'created_at'], name='console_status_created_idx'),
        ]

# class ConsoleFFDocument(BaseModel):
#     console = models.ForeignKey(Console, on_delete=models.CASCADE, related_name="console_ff_documents")
#     file = models.FileField(upload_to="console/ff_docs/")  

#     def delete(self, *args, **kwargs):
#         if self.file: 
#             default_storage.delete(self.file.path)  # Delete file from storage
#         super().delete(*args, **kwargs)

# class BOL(BaseModel):
#     console = models.ForeignKey(Console, on_delete=models.CASCADE, related_name="bols")
#     bol_id = models.CharField(max_length=100, unique=True)
#     cc_code = models.ForeignKey("portal.CostCenterCode", on_delete=models.SET_NULL, null=True, blank=True, related_name="bols") 
#     ship_from = models.ForeignKey("portal.AddressBook", on_delete=models.PROTECT, related_name="bol_ship_from")
#     ship_to = models.ForeignKey("portal.AddressBook", on_delete=models.PROTECT, related_name="bol_ship_to")
#     freight_forwarder = models.ForeignKey("portal.FreightForwarder", on_delete=models.PROTECT, related_name="bols")
#     supplier = models.ForeignKey("entities.Supplier", on_delete=models.PROTECT, related_name="bols")
#     gl_account = models.ForeignKey("portal.GLAccount", on_delete=models.PROTECT, related_name="bols",null=True,blank=True)
#     is_bol_locked = models.BooleanField(default=False)
    
    # def __str__(self):
    #     return self.bol_id
    
    # def save(self, *args, **kwargs):
    #     if not self.bol_id:
    #         existing_bols = BOL.objects.filter(console=self.console).order_by('-bol_id')
    #         if existing_bols.exists():
    #             last_suffix = int(existing_bols.first().bol_id[-2:])
    #             new_suffix = f"{last_suffix + 1:02d}"
    #         else:
    #             new_suffix = "01"
    #         self.bol_id = f"{self.console.console_id}{new_suffix}"
    #     return super().save(*args, **kwargs)


class ConsoleAuditTrail(BaseModel):
    console = models.ForeignKey(Console, on_delete=models.CASCADE, related_name="audit_trails")
    updated_by = models.ForeignKey("accounts.User", on_delete=models.CASCADE)
    

class ConsoleAuditTrailField(BaseModel):
    audit_trail = models.ForeignKey(ConsoleAuditTrail, on_delete=models.CASCADE, related_name="fields")
    field_name = models.CharField(max_length=100)
    old_value = models.TextField(null=True, blank=True)
    new_value = models.TextField(null=True, blank=True)
    

class XML(BaseModel):
    xml_id = models.CharField(max_length=100, unique=True)
    generated_by = models.ForeignKey("accounts.User", on_delete=models.PROTECT)
    file_path = models.CharField(max_length=255)
    
    def __str__(self):
        return self.xml_id
    
    def save(self, *args, **kwargs):
        if not self.xml_id:
            xml = XML.objects.all().order_by('-created_at')[:1].only('xml_id')
            if xml.exists():
                self.xml_id = "XML{0:0=7d}".format(int(xml.first().xml_id[3:]) + 1) 
            else:
                self.xml_id = "XML0000001" 
        return super().save(*args, **kwargs)

    class Meta:
        indexes = [
            models.Index(fields=['xml_id'], name='xml_id_idx'),
            models.Index(fields=['generated_by'], name='xml_generated_by_idx'),
            models.Index(fields=['created_at'], name='xml_created_idx'),
        ]