from django.contrib import admin
from .models import *


admin.site.register(Client)
admin.site.register(StorerKey)
admin.site.register(ClientUser)
admin.site.register(Supplier)
admin.site.register(SupplierUser)
admin.site.register(Operations)
admin.site.register(Hub)