from django.contrib import admin
from .models import *

admin.site.register(Console)
# admin.site.register(BOL)
admin.site.register(ConsoleAuditTrail)
admin.site.register(ConsoleAuditTrailField)