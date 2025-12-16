from crequest.middleware import CrequestMiddleware
from django.db import models
from .signals import track_field_changes, create_logs, notify_consignment_update, notify_po
import copy

class ConsignmentQuerySet(models.QuerySet):
    def update(self, **kwargs):
        from .models import Consignment

        request = CrequestMiddleware.get_request()
        updated_by = getattr(request, "this_user", None)

        # Fetch old instances (only needed fields to reduce memory)
        old_instances = list(self)
        old_map = {obj.pk: copy.deepcopy(obj) for obj in old_instances}

        # Perform the actual bulk update
        updated_count = super().update(**kwargs)

        # Fetch updated instances in one query
        updated_instances = Consignment.objects.filter(pk__in=old_map.keys())

        for new_instance in updated_instances:
            old_instance = old_map.get(new_instance.pk)

            # Track changes
            field_changes = track_field_changes(new_instance, old_instance, "Consignment", **kwargs)

            # Create logs
            create_logs(field_changes, new_instance, "Consignment", updated_by)

            # Send notifications
            # notify_consignment_update(self.model, new_instance, created=False)

        return updated_count
    
# class PurchaseOrderQuerySet(models.QuerySet):
    
#     def update(self, **kwargs):
#         from .models import PurchaseOrder
        
#         instances = list(self)
#         for instance in instances:

#             # request = CrequestMiddleware.get_request()
#             # updated_by = request.this_user if request else None

#             notify_po(self.model, instance=instance, created=False)
            

#         return super().update(**kwargs)
    

# class PurchaseOrderLineQuerySet(models.QuerySet):
    
#     def update(self, **kwargs):
#         from .models import PurchaseOrder
        
#         instances = list(self)
#         for instance in instances:

#             # request = CrequestMiddleware.get_request()
#             # updated_by = request.this_user if request else None

#             notify_po(self.model, instance=instance, created=False)
            

        return super().update(**kwargs)
    

class ConsignmentManager(models.Manager):
    def get_queryset(self):
        return ConsignmentQuerySet(self.model, using=self._db) 
    

# class PurchaseOrderManager(models.Manager):
#     def get_queryset(self):
#         return PurchaseOrderQuerySet(self.model, using=self._db) 
    

# class PurchaseOrderLineManager(models.Manager):
#     def get_queryset(self):
#         return PurchaseOrderLineQuerySet(self.model, using=self._db)