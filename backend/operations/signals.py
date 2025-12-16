from crequest.middleware import CrequestMiddleware
from .notifications import NotificationService
from portal.choices import ConsignmentStatusChoices, NotificationChoices, Role, OperationUserRole
import os


def create_title(model,field_name,new_value,old_value):

    if model == "Consignment":
        return f'Consignment {new_value}' if field_name == 'consignment_status' else 'Field Changed'
    elif model == "PO":
        pass
    elif model == "PO-Line":
        pass

    return ""


def track_field_changes(instance, old_instance,model,**kwargs,):
    
    """Detect field changes between old and new instances."""
    field_changes = []

    for field in instance._meta.get_fields():
        if hasattr(field, 'attname'):  # Skip related fields, only check actual fields
            field_name = field.attname
            old_value = getattr(old_instance, field_name, None)
            if kwargs:
                new_value = kwargs.get(field_name, old_value)
            else:
                new_value = getattr(instance, field_name, None)

            if old_value != new_value:

                title = create_title(model,field_name,new_value,old_value)
                
                field_changes.append({
                    "field_name": field_name,
                    "old_value": old_value,
                    "new_value": new_value,
                    "title" : title,
                    'description' : f'{field_name.replace("_", " ").title()} changed from {old_value} to {new_value}'
                })

    return field_changes
    

def create_logs(field_changes, instance, model,updated_by=None):
    from .models import ConsignmentAuditTrail, ConsignmentAuditTrailField,AuditTrail,AuditTrailField

    """Created audit trail logs for field changes."""
    if field_changes:
            if model == "Consignment":

                audit_trail = ConsignmentAuditTrail.objects.create(
                    consignment=instance,
                    updated_by=updated_by,
                )

                for change in field_changes:
                    ConsignmentAuditTrailField.objects.create(
                        audit_trail=audit_trail,
                        title = change['title'],
                        description = change['description'],
                        field_name=change['field_name'],
                        old_value=change['old_value'],
                        new_value=change['new_value']
                    )

            else:
                audit_trail = AuditTrail.objects.create(
                    model=instance,
                    updated_by=updated_by,
                )

                for change in field_changes:
                    AuditTrailField.objects.create(
                        audit_trail=audit_trail,
                        title = change['title'],
                        description = change['description'],
                        field_name=change['field_name'],
                        old_value=change['old_value'],
                        new_value=change['new_value']
                    )


def awb_file_added_audit_trail(instance):
    consignment = instance.consignment
    file_name = os.path.basename(instance.file.name)
    updated_by = get_current_user()
    if not updated_by:
        return
    
    field_changes = [{
        'title' : "AWB Files Added" ,
        'description': f"AWB file '{file_name}' added by {updated_by}",
        'field_name': "",
        'old_value': "",
        'new_value': ""
    }]
    
    create_logs(field_changes, consignment,"Consignment", updated_by)
    

def awb_file_deleted_audit_trail(instance):
    consignment = instance.consignment
    file_name = os.path.basename(instance.file.name)
    updated_by = get_current_user()
    if not updated_by:
        return
    
    field_changes = [{
        'title' : "AWB Files Deleted" ,
        'description': f"AWB file '{file_name}' deleted by {updated_by}",
        'field_name': "",
        'old_value': "",
        'new_value': ""
    }]
    
    create_logs(field_changes, consignment,"Consignment", updated_by)



def create_audit_trail(sender, instance, **kwargs):
    from .models import Consignment,ConsignmentAuditTrail

    """
    Pre-save signal for creating audit trail entries for the Consignment model.
    """
    if not instance._state.adding and instance.is_completed:
        updated_by = get_current_user()
        if not updated_by:
            return
        
        old_instance = Consignment.objects.get(pk=instance.pk)
        existing_logs = ConsignmentAuditTrail.objects.filter(consignment_id = instance.pk)

        if not existing_logs:

            field_changes = [{
                'title' : "Consignment Created" ,
                'description': f"Created by {updated_by.name}",
                'field_name': "",
                'old_value': "",
                'new_value': ""
            }]
            
            create_logs(field_changes, instance,"Consignment", updated_by)

        else:

            field_changes = track_field_changes(instance, old_instance,"Consignment",**kwargs)
            create_logs(field_changes, instance,"Consignment", updated_by)


def po_audit_trail(sender, instance, **kwargs):
    from .models import PurchaseOrder

    """
    Pre-save signal for creating audit trail entries for the PO model.
    """
    if not instance._state.adding and instance.id:
        user = get_current_user()
        if not user:
            return
        updated_by = user  
        old_instance = PurchaseOrder.objects.get(pk=instance.pk)

        field_changes = track_field_changes(instance, old_instance,"PO",**kwargs)

        create_logs(field_changes, instance,"PO", updated_by)


def poline_audit_trail(sender, instance, **kwargs):
    from .models import PurchaseOrderLine

    """
    Pre-save signal for creating audit trail entries for the PO model.
    """
    if not instance._state.adding and instance.id:
        user = get_current_user()
        if not user:
            return
        updated_by = user
        old_instance = PurchaseOrderLine.objects.get(pk=instance.pk)

        field_changes = track_field_changes(instance, old_instance,"PO",**kwargs)

        create_logs(field_changes, instance,"PO", updated_by)


def get_current_user():
    """
    Middleware to get the current user from the request.
    This is used to set the `updated_by` field in audit trails.
    """
    request = CrequestMiddleware.get_request()
    if request:
        return request.this_user if hasattr(request, 'this_user') else None
    else:
        return None

    
def delete_file_from_storage(sender, instance, **kwargs):
    if instance.file:
        instance.file.delete(save=False)  # delete file from storage (disk/S3)


def notify_consignment_update(sender, instance, created, **kwargs):

    """
    Pre-save signal for updating consignment notifications.
    """
    from .models import Consignment
    from portal.models import Notification
    
    user = get_current_user()
    if not created:

        new_status = instance.consignment_status if instance else None
        if new_status == ConsignmentStatusChoices.DRAFT:
            return
        
        if Notification.objects.filter(
            type = NotificationChoices.CONSIGNMENT,
            message__icontains = f"{instance.consignment_id} created",
        ).exists():
            return
        
        if new_status == ConsignmentStatusChoices.PENDING_FOR_APPROVAL:
            NotificationService.notify_consignment_created(instance, user)
            return
        
        handler = getattr(NotificationService, f"consignment_update", None)
        if handler:
            handler(instance, user)


def notify_po(sender, instance, created, **kwargs):
    user = get_current_user()
    if created:
        NotificationService.po_created(instance, user=user)
        return
    
    NotificationService.po_update(instance=instance, user=user)


def notify_po_line(sender, instance, created, **kwargs):
    user = get_current_user()
    if created:
        NotificationService.po_created(instance, user=user)
        return
    
    NotificationService.po_update(instance=instance, user=user)
