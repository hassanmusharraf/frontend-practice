from rest_framework.views import APIView
from django.core.exceptions import ValidationError
from django.db.models import ProtectedError
from core.response import StandardResponse
from .models import AddressBook, PackagingType, MOT, FreightForwarder, GLAccount, CostCenterCode, RejectionCode, Notification, UserNotification
from .serializers import PostAddressBookSerializer, GetFreightForwarderSerializer, FreightForwarderSerializer, MOTSerializer, AddressBookSerializer, PackagingTypeSerializer, GetAddressBookSerializer, GetPackagingTypeSerializer, GLAccountSerializer , CostCenterCodeSerializer, RejectionCodeSerializer
from .utils import get_all_fields          
from .mixins import SearchAndFilterMixin, PaginationMixin
from .choices import Role
from django.db.models import Q, F, Value, CharField
from operations.services import ConsignmentWorkflowServices
from operations.models import Consignment 
from django.db import transaction
from django.db.models.functions import Concat
from django.conf import settings


class MOTView(SearchAndFilterMixin, PaginationMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = MOT.objects.get(id=id) 
            except (MOT.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=MOTSerializer(obj).data)
        
        
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        search = request.GET.get("q", "")
        fields = get_all_fields(MOT, ignore_fields=[], include_relational_fields=False)
        queryset = MOT.objects.all()
        if search:
            queryset = self.apply_search(fields, queryset, search.strip())

        filters = self.make_filters_list(request)
        if filters:
            apply_filters = self.appy_dynamic_filter(filters)  
            queryset = queryset.filter(apply_filters)

        count = queryset.count()
        paginate_result = self.paginate_results(queryset, pg, limit)
        return StandardResponse(
            success=True,
            data=MOTSerializer(paginate_result, many=True).data,
            count=count,
            status=200
        )
            
    
    def post(self, request, *args, **kwargs):
        serializer = MOTSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="MOT created successfully.", data={"id":obj.id})
    
    
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = MOT.objects.get(id=id)
            ff = FreightForwarder.objects.filter(mot = obj)
            if ff.exists():
                return StandardResponse(status=400, success=False, errors=["Cannot update MOT as it is associated with a Freight Forwarder."])
        except (MOT.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = MOTSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="MOT updated successfully.", data={"id":obj.id})
    
    
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = MOT.objects.get(id=id)
            obj.delete()
        except (MOT.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete MOT as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="MOT deleted successfully.")
        
           

class FreightForwarderView(SearchAndFilterMixin, PaginationMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = FreightForwarder.objects.get(id=id) 
            except (FreightForwarder.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetFreightForwarderSerializer(obj).data)
        
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        search = request.GET.get("q", "")
        fields = get_all_fields(FreightForwarder, ignore_fields=[], include_relational_fields=False)
        queryset = FreightForwarder.objects.prefetch_related("mot").all()
        if search:
            queryset = self.apply_search(fields, queryset, search.strip())
        
        filters = self.make_filters_list(request)
        if filters:
            apply_filters = self.appy_dynamic_filter(filters)  
            queryset = queryset.filter(apply_filters)

        count = queryset.count()
        paginate_result = self.paginate_results(queryset, pg, limit)
        return StandardResponse(
            success=True,
            data=GetFreightForwarderSerializer(paginate_result, many=True).data,
            count=count,
            status=200
        )
            
    
    def post(self, request, *args, **kwargs):
        serializer = FreightForwarderSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Freight Forwarder created successfully.", data={"id":obj.id})
    
    
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = FreightForwarder.objects.get(id=id)
        except (FreightForwarder.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = FreightForwarderSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Freight Forwarder updated successfully.", data={"id":obj.id})
    
    
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = FreightForwarder.objects.get(id=id)
            obj.delete()
        except (FreightForwarder.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete Freight Forwarder as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Freight Forwarder deleted successfully.")
        
           

class AddressBookView(SearchAndFilterMixin, PaginationMixin, APIView):
    
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = AddressBook.objects.get(id=id) 
            except (AddressBook.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetAddressBookSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            fields = get_all_fields(AddressBook, ignore_fields=[], include_relational_fields=False)            
            
            storerkey = request.GET.get("storerkey")
            if storerkey not in [None, "", "undefined", "null"]:
                queryset = AddressBook.objects.filter(storerkey=storerkey).select_related("storerkey")

            client = request.GET.get("client")
            if client not in [None, "", "undefined", "null"]:
                queryset = AddressBook.objects.filter(client=client).select_related("client")
            
            supplier = request.GET.get("supplier")
            if supplier not in [None, "", "undefined", "null"]:
                queryset = AddressBook.objects.filter(supplier=supplier).select_related("supplier")
                
            if search:
                queryset = self.apply_search(fields, queryset, search.strip())
            count = queryset.count()
            paginate_result = self.paginate_results(queryset, pg, limit)
            return StandardResponse(
                success=True,
                data=GetAddressBookSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
        
    def post(self, request, *args, **kwargs):
        serializer = PostAddressBookSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="AddressBook created successfully.", data={"id":obj.id})
    
    
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = AddressBook.objects.get(id=id)
        except (AddressBook.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = PostAddressBookSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="AddressBook updated successfully.", data={"id":obj.id})
    
    
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = AddressBook.objects.get(id=id)
            obj.delete()
        except (AddressBook.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete address as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="AddressBook deleted successfully.")
        
            

class PackagingTypeView(SearchAndFilterMixin, PaginationMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = PackagingType.objects.get(id=id) 
            except (PackagingType.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetPackagingTypeSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            fields = get_all_fields(PackagingType, ignore_fields=[], include_relational_fields=False)            
            queryset = PackagingType.objects.filter(supplier=request.GET.get("supplier"))
                
            if search:
                queryset = self.apply_search(fields, queryset, search.strip())
            count = queryset.count()
            paginate_result = self.paginate_results(queryset, pg, limit)
            return StandardResponse(
                success=True,
                data=GetPackagingTypeSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
        
    @transaction.atomic
    def post(self, request, *args, **kwargs):
        
        try:
            consignment_id = request.data.get("consignment_id")
            quantity = request.data.get("quantity")

            serializer = PackagingTypeSerializer(data=request.data)
            if not serializer.is_valid():
                return StandardResponse(status=400, success=False, errors=serializer.errors)
            obj = serializer.save()

            if consignment_id and quantity:
                consignment = Consignment.objects.filter(consignment_id=consignment_id).only("id","consignment_id").first()

                _, error =ConsignmentWorkflowServices.create_packages(consignment, obj, request.data)
                if error:
                    transaction.set_rollback(True)
                    return StandardResponse(status=400, success=False, errors=error)
                
                
            return StandardResponse(status=201, message="Packaging type created successfully.", data={"id":obj.id})

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(errors=[str(e)], status=500)
    

    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = PackagingType.objects.prefetch_related("consignment_packagings").get(id=id)
            if obj.consignment_packagings.exists():
               return StandardResponse(status=400, success=False, errors=["This package is currently being used in a consignment and cannot be updated."]) 
        except (PackagingType.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = PackagingTypeSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="PackagingType updated successfully.", data={"id":obj.id})
    
    
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = PackagingType.objects.get(id=id)
            obj.delete()
        except (PackagingType.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete packaging type as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="PackagingType deleted successfully.")
      

class GLAccountView(SearchAndFilterMixin, PaginationMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):

        if id != "list":
            try:
                obj = GLAccount.objects.get(id=id)
            except (GLAccount.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GLAccountSerializer(obj).data)
        
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        search = request.GET.get("q", "")
        fields = get_all_fields(GLAccount, ignore_fields=[], include_relational_fields=False)            
        queryset = GLAccount.objects.all()
        if search:
            queryset = self.apply_search(fields, queryset, search.strip())

        filters = self.make_filters_list(request)
        if filters:
            apply_filters = self.appy_dynamic_filter(filters)  
            queryset = queryset.filter(apply_filters)

        count = queryset.count()
        paginate_result = self.paginate_results(queryset, pg, limit)
        return StandardResponse(
            success=True,
            data=GLAccountSerializer(paginate_result, many=True).data,
            count=count,
            status=200
        )    
        
    def post(self, request, *args, **kwargs):
        serializer = GLAccountSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="GL Account created successfully.", data={"id":obj.id})
    
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = GLAccount.objects.get(id=id)
        except (GLAccount.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = GLAccountSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="GL Account updated successfully.", data={"id":obj.id})
    
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = GLAccount.objects.get(id=id)
            obj.delete()
        except (GLAccount.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete GL Account as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="GL Account deleted successfully.")


class CostCenterCodeView(SearchAndFilterMixin, PaginationMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):
        
        if id != "list":
            try:
                obj = CostCenterCode.objects.get(id=id) 
            except (CostCenterCode.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=CostCenterCodeSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            fields = get_all_fields(CostCenterCode, ignore_fields=[], include_relational_fields=False)            
            queryset = CostCenterCode.objects.all().order_by("-updated_at")
            if search:
                queryset = self.apply_search(fields, queryset, search.strip())

            filters = self.make_filters_list(request)
            if filters:
                apply_filters = self.appy_dynamic_filter(filters)  
                queryset = queryset.filter(apply_filters)

                
            count = queryset.count()
            paginate_result = self.paginate_results(queryset, pg, limit)
            return StandardResponse(
                success=True,
                data=CostCenterCodeSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
        
    def post(self, request, *args, **kwargs):
        serializer = CostCenterCodeSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Cost Center Code created successfully.", data={"id":obj.id})
    
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = CostCenterCode.objects.get(id=id)
        except (CostCenterCode.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = CostCenterCodeSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Cost Center Code updated successfully.", data={"id":obj.id})

    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = CostCenterCode.objects.get(id=id)
            obj.delete()
        except (CostCenterCode.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete Cost Center Code as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Cost Center Code deleted successfully.")


class RejectionView(SearchAndFilterMixin, PaginationMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):
        
        if id != "list":
            try:
                obj = RejectionCode.objects.get(id=id) 
            except (RejectionCode.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=RejectionCodeSerializer(obj).data)
        
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        search = request.GET.get("q", "")
        fields = get_all_fields(RejectionCode, ignore_fields=[], include_relational_fields=False)            
        queryset = RejectionCode.objects.all()
        if search:
            queryset = self.apply_search(fields, queryset, search.strip())

        filters = self.make_filters_list(request)
        if filters:
            apply_filters = self.appy_dynamic_filter(filters)  
            queryset = queryset.filter(apply_filters)
            
        count = queryset.count()
        paginate_result = self.paginate_results(queryset, pg, limit)
        return StandardResponse(
            success=True,
            data=RejectionCodeSerializer(paginate_result, many=True).data,
            count=count,
            status=200
        )
    
    def post(self, request, *args, **kwargs):
        serializer = RejectionCodeSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Rejection Code created successfully.", data={"id":obj.id})
    
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = RejectionCode.objects.get(id=id)
        except (RejectionCode.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = RejectionCodeSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Rejection Code updated successfully.", data={"id":obj.id})
    
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = RejectionCode.objects.get(id=id)
            obj.delete()
        except (RejectionCode.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete Rejection Code as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Rejection Code deleted successfully.")
    

# class OracleTestAPI(APIView):
    
#     def post(self, request, *args, **kwargs):
        
#         return StandardResponse(status=200, data=request.data)
    

# import cx_Oracle
# import datetime
# from rest_framework.views import APIView
# from rest_framework.response import Response  # Adjust if using a custom StandardResponse

# # Sample StandardResponse wrapper could look like this:
# def OracleTestAPIResponse(status, data):
#     return Response(data, status=status)

# class OracleTestAPI(APIView):
    
#     def post(self, request, *args, **kwargs):
#         # Extract the mapping and the data array from the request body.
#         mapping = request.data.get("mapping", {})
#         data_array = request.data.get("data", [])
        
#         # Validate that we have mapping and at least one data record.
#         if not mapping:
#             return OracleTestAPIResponse(status=400, data={"error": "Mapping data is required."})
#         if not data_array:
#             return OracleTestAPIResponse(status=400, data={"error": "Data array is empty."})
        
#         # Initialize Oracle connection variables.
#         conn = None
#         cursor = None
#         try:
#             # Initialize the Oracle client with your Instant Client directory.
#             # cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_23_7")
            
#             # # Create a DSN (Data Source Name) using your Oracle connection details.
#             # dsn_tns = cx_Oracle.makedsn("10.0.5.152", 1521, service_name="ebs_TESTAPP")
            
#             # # Establish the connection.
#             # conn = cx_Oracle.connect(user="APPS", password="appsa24ksa", dsn=dsn_tns)
#             # cursor = conn.cursor()
            
#             # # Iterate over every record in the data array.
#             # for record in data_array:
#             #     # Create a dictionary that will store the actual database column names and their associated values.
#             #     db_data = {}
#             #     for request_field, db_column in mapping.items():
#             #         # You can add transformation logic here, for example converting date strings to date objects.
#             #         db_data[db_column] = record.get(request_field)
                
#             #     # Check if required fields (if any) are present; you might want to handle defaults
                
#             #     # Construct the list of columns and the corresponding placeholders.
#             #     columns = ", ".join(db_data.keys())
#             #     # Create bind variable placeholders :1, :2, etc.
#             #     placeholders = ", ".join(f":{i + 1}" for i in range(len(db_data)))
#             #     values = tuple(db_data.values())
                
#             #     # Dynamically build the INSERT statement.
#             #     insert_sql = f"INSERT INTO ads_po_shipping_mapping_stg ({columns}) VALUES ({placeholders})"
                
#             #     # Execute the insert with the bound values.
#             #     cursor.execute(insert_sql, values)
            
#             # # Commit all inserts to the database.
#             # conn.commit()
#             return OracleTestAPIResponse(status=200, data={"message": "Data inserted successfully!"})
            
#         except cx_Oracle.DatabaseError as error:
#             # Log or print the error appropriately.
#             error_message = f"Database error occurred: {error}"
#             return OracleTestAPIResponse(status=500, data={"error": error_message})
#         finally:
#             if cursor:
#                 cursor.close()
#             if conn:
#                 conn.close()


class NotificationAPIView(PaginationMixin,APIView):
    

    def get(self, request, id=None):
        """
        Fetch user notifications with filters for 'all', 'read', 'unread', or 'count'.
        Supports pagination and keyword search.
        """

        user = request.this_user
        pg = int(request.GET.get("pg", 0))
        limit = int(request.GET.get("limit", 25))
        q = (request.GET.get("q") or "").strip()

        values = [
            "id",
            "header",
            "type",
            "message",
            "created_at",
            "hyperlink_value",
            "user_notifications__is_read",
            "attachment"
        ]

        base_qs = Notification.objects.all()
        filters = Q(user_notifications__user=user)

        if q:
            filters &= Q(hyperlink_value__icontains=q) | Q(header__icontains=q) | Q(message__icontains=q)

        user.has_notif = False
        user.save()

        if id == "count":
            unread_count = UserNotification.objects.filter(user=user, is_read=False).count()
            return StandardResponse(success=True, data=[], count=unread_count, status=200)

        if id == "read":
            filters &= Q(user_notifications__is_read=True)
        elif id == "unread":
            filters &= Q(user_notifications__is_read=False)

        notif_qs = base_qs.filter(filters).distinct().order_by("-created_at")

        notifications_qs = notif_qs.values(*values)

        total_count = notifications_qs.count()

        paginated_result = self.paginate_results(notifications_qs, pg, limit)

        for item in paginated_result:
            item["is_read"] = item.pop("user_notifications__is_read", False)

        return StandardResponse(success=True, data=paginated_result, count=total_count, status=200)


    @transaction.atomic
    def post(self, request, id=None):
        user = request.this_user

        if not id:
            return StandardResponse(
                success=False,
                status=400,
                errors=["Notification ID is required"]
            )

        try:
            qs = UserNotification.objects.filter(user=user, is_read=False)
            
            # Mark all notifications as read
            if id == "mark_as_read":
                qs.update(is_read=True)

            # Mark single notification as read
            else:
                qs.filter(notification_id=id).update(is_read=True)

            # If no unread notifications remain, set has_notif = False
            if not qs.exists():
                user.has_notif = False
                user.save(update_fields=["has_notif"])

            return StandardResponse(
                status=200,
                success=True,
                message="Notification marked as read"
            )

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(
                status=500,
                success=False,
                errors=[str(e)]
            )


class UserNotificationAPIView(PaginationMixin,APIView):
    
    def get(self,request):
        fields = ["id","notif__id","notif__title","notif__type","notif__description","notif__attachments"]
        queryset = None
        role = request.current_user.role
        if role == Role.OPERATIONS or role == Role.ADMIN:
            queryset =UserNotification.objects.filter(operations_archive__in= request.current_user.id).select_related("notif").values(*fields).order_by("-created_at")
        if role == Role.SUPPLIER_USER :
            queryset= UserNotification.objects.filter(supplier_archive__in= request.current_user.id).select_related("notif").values(*fields).order_by("-created_at")
        if role == Role.CLIENT_USER :
            queryset = UserNotification.objects.filter(client_archive__in= request.current_user.id).select_related("notif").values(*fields).order_by("-created_at")

        count = queryset.count()
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        paginated_result = self.paginate_results(queryset,pg,limit)
        return StandardResponse(
                success=True,
                data=paginated_result,
                count=count,
                status=200
            )

    def post(self, request):
        try:
            notif_ids = request.data.get("id")

            if not notif_ids:
                return StandardResponse(status=400, success=False, message="Notification ID is required.")

            if not isinstance(notif_ids, list):
                notif_ids = [notif_ids]

            notifications = Notification.objects.filter(id__in=notif_ids)
            if not notifications.exists():
                return StandardResponse(status=400, success=False, message=f"Invalid notification Id's {notif_ids}.")

            user_id = str(request.this_user.id)
            role = request.this_user.role

            bulk_notif = []
            bulk_archive = []
            for notif in notifications:

                obj, _ = UserNotification.objects.get_or_create(notif=notif)

                notif_field = None
                archive_field = None

                if role in [Role.OPERATIONS, Role.ADMIN]:
                    notif_field = "operations_receivers"
                    archive_field = "operations_archive"
                elif role == Role.SUPPLIER_USER:
                    notif_field = "supplier_receivers"
                    archive_field = "supplier_archive"
                elif role == Role.CLIENT_USER:
                    notif_field = "client_receivers"
                    archive_field = "client_archive"

                if notif_field and archive_field:
                    # Efficient mutation using sets
                    notif_list = set(getattr(notif, notif_field) or [])
                    archive_list = set(getattr(obj, archive_field) or [])

                    if user_id in notif_list:
                        notif_list.remove(user_id)
                        setattr(notif, notif_field, list(notif_list))

                    if user_id not in archive_list:
                        archive_list.add(user_id)
                        setattr(obj, archive_field, list(archive_list))
                    
                    bulk_notif.append(notif)
                    bulk_archive.append(obj)

            if bulk_notif:
                Notification.objects.bulk_update(bulk_notif, [notif_field])
            if bulk_archive:
                UserNotification.objects.bulk_update(bulk_archive, [archive_field])

            return StandardResponse(status=201, message="Notification archived successfully.")

        except Exception as e:
            return StandardResponse(
                status=400,
                success=False,
                message=f"Something went wrong while archiving the notification: {e}"
            )