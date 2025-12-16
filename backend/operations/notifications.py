from core.response import StandardResponse, ServiceError
from django.db import transaction
from django.db.models import Q
from portal.choices import ConsignmentStatusChoices, NotificationChoices, Role, OperationUserRole
from portal.models import Notification, UserNotification
from accounts.models import User


class NotificationService:

    # Example notification templates (optional)
    notifications = {
        "pending_for_approval": {
            "header": "New Pickup Request {consignment_id} created",
            "message": "A new Pickup Request {consignment_id} has been created"
        }
    }


    @staticmethod
    def get_users_by_roles(instance=None, roles=None, ops_roles=None, notify_supplier=False, notify_client=False):
        """
        Returns a list of users filtered by roles, operation roles, and optionally supplier/client relationships.
        """
        filters = Q()

        if roles:
            filters |= Q(role__in=roles)
        if ops_roles:
            filters |= Q(operations_profile__access_level__in=ops_roles)

        # Base user queryset
        users_qs = User.objects.filter(filters).distinct()

        # Include supplier/client users if applicable
        if instance:
            extra_q = Q()
            if notify_supplier and getattr(instance, "supplier", None):
                extra_q |= Q(supplier_profile__supplier=instance.supplier)
            if notify_client and getattr(instance, "client", None):
                extra_q |= Q(client_profile__client=instance.client)

            if extra_q:
                users_qs = users_qs.union(User.objects.filter(extra_q).distinct())
                # combined_qs = users_qs | User.objects.filter(extra_q).distinct()
                # combined_qs = combined_qs.distinct()
            
        return list(users_qs)


    @classmethod
    @transaction.atomic
    def send_notification(cls, users, header, message, type, hyperlink_value=None,attachment=None):
        """
        Sends a notification to a list of users and links them through UserNotification.
        """
        try:
            if not users:
                return

            user_ids = [u.id for u in users]
            User.objects.filter(id__in=user_ids).update(has_notif=True)

            notification = Notification.objects.create(
                header=header,
                type=type,
                message=message,
                hyperlink_value=hyperlink_value,
                attachment=attachment,
            )

            UserNotification.objects.bulk_create(
                [UserNotification(user=u, notification=notification) for u in users]
            )

        except Exception as e:
            transaction.set_rollback(True)
            raise ServiceError(error=f"Notification error: {str(e)}", success=False, status=500)
            # return StandardResponse(success=False, status=500, errors=[f"Notification error: {str(e)}"])


    @classmethod
    def notify_consignment_created(cls, instance, user):
        try:
            users = cls.get_users_by_roles(
                instance=instance,
                roles=[Role.ADMIN, Role.OPERATIONS],
                ops_roles=[OperationUserRole.L1],
                notify_supplier=True,
                notify_client=True,
            )

            header = "New Pickup Request {consignment_id} created."
            message = "A new Pickup Request {consignment_id} has been created" + f" by {user.name}"

            return cls.send_notification(
                users=users,
                header=header,
                message=message,
                type=NotificationChoices.CONSIGNMENT,
                hyperlink_value={"consignment_id": instance.consignment_id},
            )

        except Exception as e:
            return StandardResponse(success=False, status=500, errors=[str(e)])


    @classmethod
    def consignment_update(cls, instance, user, header=None, message=None, hyperlink_value={}):
        try:
            consignment_status = instance.consignment_status

            supplier_status = [
                ConsignmentStatusChoices.REJECTED,
                ConsignmentStatusChoices.CANCELLED,
                ConsignmentStatusChoices.PENDING_CONSOLE_ASSIGNMENT,
                ConsignmentStatusChoices.DELIVERED,
                ConsignmentStatusChoices.RECEIVED_AT_DESTINATION
            ]
            client_status = [ConsignmentStatusChoices.DELIVERED,ConsignmentStatusChoices.RECEIVED_AT_DESTINATION]

            users = cls.get_users_by_roles(
                instance=instance,
                roles=[Role.ADMIN],
                ops_roles=[OperationUserRole.L1, OperationUserRole.L2],
                notify_supplier=True if consignment_status in supplier_status else False,
                notify_client=True if consignment_status in client_status else False,
            )

            if not header:
                header = "Pickup Request {consignment_id} " + consignment_status
            
            if not message:
                message = "Pickup Request {consignment_id} has been " + f"{consignment_status} by {user.name}"

            if not hyperlink_value:
                hyperlink_value = {"consignment_id": instance.consignment_id}
            
            return cls.send_notification(
                users=users,
                header=header,
                message=message,
                type=NotificationChoices.CONSIGNMENT,
                hyperlink_value=hyperlink_value,
            )

        except Exception as e:
            return StandardResponse(success=False, status=500, errors=[str(e)])


    @classmethod
    def po_created(cls, instance, user):
        from operations.models import PurchaseOrder

        try:
            users = cls.get_users_by_roles(
                roles=[Role.ADMIN],
                ops_roles=[OperationUserRole.L1, OperationUserRole.L2],
                notify_supplier=True,
                notify_client=True,
            )

            po_number = (
                instance.customer_reference_number
                if isinstance(instance, PurchaseOrder)
                else instance.purchase_order.customer_reference_number
            )

            header = "New PO {customer_reference_number} Issued"
            message = (
                "PO {customer_reference_number} has been issued " + f"by {user.name}. "
                "Please review the PO details, update Promised Dates or create the corresponding Pickups, as applicable."
            )

            return cls.send_notification(
                users=users,
                header=header,
                message=message,
                type=NotificationChoices.PURCHASE_ORDER,
                hyperlink_value={"customer_reference_number": po_number},
            )

        except Exception as e:
            return StandardResponse(success=False, status=500, errors=[str(e)])


    @classmethod
    def po_update(cls, instance, user):
        from operations.models import PurchaseOrder

        try:
            users = cls.get_users_by_roles(
                roles=[Role.ADMIN],
                ops_roles=[OperationUserRole.L1, OperationUserRole.L2],
                notify_supplier=True,
                notify_client=True,
            )

            po_number = (
                instance.customer_reference_number if instance.__class__ == PurchaseOrder else instance.purchase_order.customer_reference_number
            )

            header = "New PO {customer_reference_number} Updated"
            message = (
                "PO {customer_reference_number} has been modified" + f" by {user.name} "
                "Please review and take necessary actions as applicable."
            )

            return cls.send_notification(
                users=users,
                header=header,
                message=message,
                type=NotificationChoices.PURCHASE_ORDER,
                hyperlink_value={"customer_reference_number": po_number},
            )

        except Exception as e:
            return StandardResponse(success=False, status=500, errors=[str(e)])



    @classmethod
    def notify_po_file_upload(cls,user,header,message,po_upload):

        try:
            users = cls.get_users_by_roles(
                roles=[Role.ADMIN],
                ops_roles=[OperationUserRole.L1, OperationUserRole.L2],
                # notify_supplier=True,
                # notify_client=True,
            )
            if user not in users:
                users.append(user)

            attachment = None
            if po_upload:
                if po_upload.error_file:
                    attachment = po_upload.error_file.url
                elif po_upload.uploaded_file:
                    attachment = po_upload.uploaded_file.url

            return cls.send_notification(
                users=users,
                header=header,
                message=message,
                type=NotificationChoices.PO_FILE_UPLOAD,
                attachment=attachment,
                # hyperlink_value={"customer_reference_number": po_number},
            )
        
        except ServiceError as e:
            raise ServiceError(error=str(e), success=False, status=500)  
        except Exception as e:
            raise ServiceError(error=str(e), success=False, status=500)



    @classmethod
    def notify_comprehensive_report(cls,user,header,message,attachment):

        try:
            users = cls.get_users_by_roles(
                roles=[Role.ADMIN],
                ops_roles=[OperationUserRole.L1, OperationUserRole.L2],
                # notify_supplier=True,
                # notify_client=True,
            )
            if user not in users:
                users.append(user)

            return cls.send_notification(
                users=users,
                header=header,
                message=message,
                type=NotificationChoices.COMPREHENSIVE_REPORT,
                attachment=attachment,
            )
        
        except ServiceError as e:
            raise ServiceError(error=str(e), success=False, status=500)  
        except Exception as e:
            raise ServiceError(error=str(e), success=False, status=500)
            