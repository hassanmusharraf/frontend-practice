from rest_framework.views import APIView
from django.core.exceptions import ValidationError
from django.db.models import ProtectedError, RestrictedError
from core.response import StandardResponse
from django.db import transaction, IntegrityError
from .models import Client, Hub, Supplier, ClientUser, SupplierUser, Operations, StorerKey, DangerousGoodClass, MaterialMaster, StorerKeyReminder
from .serializers import( GetClientSupplierAndStorerSerializer,GetStorerKeySerializer, StorerKeySerializer, ClientSerializer,
    HubSerializer, GetClientSerializer, GetSupplierUserSerializer, GetClientUserSerializer, ClientUserSerializer, SupplierSerializer,
    SupplierUserSerializer, OperationsSerializer, GetOperationsSerializer, GetSupplierSerializer, DangerousGoodClassSerializer,
    MaterialSerializer, StorerKeyReminderSerializer)
from portal.utils import get_all_fields
from accounts.serializers import UserPostSerializer, UserPutSerializer
from accounts.models import User
from portal.choices import Role, OperationUserRole
from portal.mixins import SearchAndFilterMixin, PaginationMixin
from core.decorators import role_required

class HubView(SearchAndFilterMixin, PaginationMixin, APIView):

    @role_required(OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = Hub.objects.get(id=id) 
            except (Hub.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=HubSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            
            # fields = get_all_fields(Hub, ignore_fields=[], include_relational_fields=False)
            fields = ["hub_code","name","location"]
            queryset = Hub.objects.all().order_by("-updated_at")
            
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
                data=HubSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
            
    @role_required(OperationUserRole.L1)
    def post(self, request, *args, **kwargs):
        serializer = HubSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Hub created successfully.", data={"id":obj.id})
    
    @role_required(OperationUserRole.L1)
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = Hub.objects.get(id=id)
        except (Hub.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = HubSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Hub updated successfully.", data={"id":obj.id})
    
    @role_required(OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = Hub.objects.get(id=id)
            obj.delete()
        except (Hub.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete hub as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Hub deleted successfully.")
        

class StorerKeyView(SearchAndFilterMixin, PaginationMixin, APIView):

    def _process_reminders(self, reminders, storerkey_obj):
        """
        Helper method to validate and save reminders.
        Returns None on success, or StandardResponse on failure.
        """
        for reminder in reminders:
            name = reminder.get("name")
            if not name:
                return StandardResponse(
                    status=400,
                    success=False,
                    errors=["Each reminder must include 'name'."]
                )

            # Optional: validate reminder with serializer
            reminder_data = {
                **reminder,
                "storerkey": storerkey_obj.id
            }
            reminder_serializer = StorerKeyReminderSerializer(data=reminder_data)
            if not reminder_serializer.is_valid():
                return StandardResponse(
                    status=400,
                    success=False,
                    errors=reminder_serializer.errors
                )
            reminder_serializer.save()

        return None
    
    @role_required(OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        
        if id and id != "list":
            try:
                obj = StorerKey.objects.prefetch_related("reminders").get(id=id) 
            except (StorerKey.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetStorerKeySerializer(obj).data)
        
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        search = request.GET.get("q", "")
        # fields = get_all_fields(StorerKey, ignore_fields=[], include_relational_fields=False)
        fields = ["storerkey_code","name","client__name","hub__name"]
        queryset = StorerKey.objects.select_related("client", "hub").prefetch_related("reminders").all().order_by("-updated_at")
        
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
            data=GetStorerKeySerializer(paginate_result, many=True).data,
            count=count,
            status=200
        )
          
            
    @role_required(OperationUserRole.L1)
    @transaction.atomic()
    def post(self, request, *args, **kwargs):
        serializer = StorerKeySerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()

        if obj.expediting_applicable:
            reminders = request.data.get("reminders")
            
            error = self._create_update_reminder(reminders, obj)
            if error:    
                return error

        return StandardResponse(status=201, message="Storer Key created successfully.", data={"id": obj.id})

    @role_required(OperationUserRole.L1)
    @transaction.atomic()
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = StorerKey.objects.prefetch_related("suppliers").get(id=id)
            if obj.suppliers.all().exists():
                return StandardResponse(status=400, success=False, errors=["This is already associated with the supplier you can't update this."])
            
        except (StorerKey.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = StorerKeySerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        
        obj = serializer.save()
        if obj.expediting_applicable:
            reminders = request.data.get("reminders")

            error = self._create_update_reminder(reminders, obj)
            if error:    
                return error

        return StandardResponse(status=200, message="StorerKey updated successfully.")
    
    @role_required(OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = StorerKey.objects.get(id=id)
            obj.delete()
        except (StorerKey.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete StorerKey as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="StorerKey deleted successfully.")
        
    
    def _create_update_reminder(self,reminders,obj):
        
        if not reminders:
            return StandardResponse(status=400, success=False, errors=["Reminders are required when expediting is applicable."])
        
        try:
            for reminder in reminders:

                reminder["storerkey"] = obj
                name = reminder.get("name")

                if not name:
                    transaction.set_rollback(True)
                    return StandardResponse(status=400,success=False,errors=["Each reminder must include 'storerkey' and 'name'."])
                
                StorerKeyReminder.objects.update_or_create(
                    storerkey=obj,
                    name=name,
                    defaults=reminder
                )

            return None

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=400, success=False, errors=[str(e)])

class ClientView(SearchAndFilterMixin, PaginationMixin, APIView):

    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = Client.objects.prefetch_related("suppliers", "storerkeys").get(id=id) 
            except (Client.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetClientSupplierAndStorerSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            filters = self.make_filters_list(request)
            
            # fields = get_all_fields(Client, ignore_fields=[], include_relational_fields=False)
            fields = ["client_code","name"]
            user = request.this_user
            if user.role == Role.CLIENT_USER:
                try:
                    client_user = ClientUser.objects.only("client_id").get(user=user)
                    queryset = Client.objects.filter(id=client_user.client_id)
                except ClientUser.DoesNotExist:
                    return StandardResponse(success=False,data=[],status=400,errors=["User not found"])

            elif user.role == Role.ADMIN:
                queryset = Client.objects.all().order_by("-updated_at")

            elif user.role == Role.OPERATIONS and user.profile().access_level == OperationUserRole.L1:
                queryset = Client.objects.all().order_by("-updated_at")
            
            queryset = queryset.order_by("-updated_at")

            if not queryset:
                return StandardResponse(
                success=True,
                data=[],
                count=0,
                status=200
            )
            
            if search:
                queryset = self.apply_search(fields, queryset, search.strip())
                
            if filters:
                apply_filters = self.appy_dynamic_filter(filters)  
                queryset = queryset.filter(apply_filters)
                
            count = queryset.count()
            paginate_result = self.paginate_results(queryset, pg, limit)
            return StandardResponse(
                success=True,
                data=GetClientSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
        
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def post(self, request, *args, **kwargs):
        serializer = ClientSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Client created successfully.", data={"id":obj.id})
    
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = Client.objects.get(id=id)
        except (Client.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = ClientSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Client updated successfully.", data={"id":obj.id})
    
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = Client.objects.get(id=id)
            obj.delete()
        
            return StandardResponse(status=200, message="Client deleted successfully.")
        
        except (Client.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete Client as it is protected by related {', '.join(related_models)}"])
        except RestrictedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.restricted_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete Client as it is restricted by related {', '.join(related_models)}"])
        except Exception as e:
            return StandardResponse(status=500, success=False, errors=[str(e)])


class ClientUserView(SearchAndFilterMixin, PaginationMixin, APIView):
    transform_fields = {
        "username" : "user__username",
        "name" : "user__name",
        "role" : "user__role",
        "client_name" : "client__name"
    }
    
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = ClientUser.objects.get(id=id) 
            except (ClientUser.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetClientUserSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            # fields = get_all_fields(ClientUser, ignore_fields=[], include_relational_fields=False)   
            fields = ["user__name","user__username","client__name","client__client_code"]         
            queryset = ClientUser.objects.prefetch_related("suppliers").select_related("user", "client").filter(client=request.GET.get("client")).order_by("-updated_at")
            if search:
                queryset = self.apply_search(fields, queryset, search.strip())
            
            filters = self.make_filters_list(request)
            if filters:
                for f in apply_filters:
                    f['column'] = self.transform_fields.get(f['column'], f['column'])
                apply_filters = self.appy_dynamic_filter(filters)  
                queryset = queryset.filter(apply_filters)
                
            count = queryset.count()
            paginate_result = self.paginate_results(queryset, pg, limit)
            return StandardResponse(
                success=True,
                data=GetClientUserSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
            
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def post(self, request, *args, **kwargs):
        try:
            with transaction.atomic():       
                request.data["role"] = Role.CLIENT_USER  
                username=request.data.get("username")
                if User.objects.filter(username=username).exists():
                    return StandardResponse(status=40, success=False, errors=[f"Username {username} already taken"])  
                   
                user_seralizer = UserPostSerializer(data=request.data)
                if not user_seralizer.is_valid():
                    transaction.set_rollback(True)
                    return StandardResponse(status=400, success=False, errors=user_seralizer.errors)
                user_obj = user_seralizer.save()
                
                request.data["user"] = user_obj.id
                if not request.data.get("suppliers"):
                    request.data["suppliers"] = []
                client_user_serializer = ClientUserSerializer(data=request.data)
                if not client_user_serializer.is_valid():
                    transaction.set_rollback(True)
                    return StandardResponse(status=400, success=False, errors=client_user_serializer.errors)
                clientuser_obj = client_user_serializer.save()
                
            return StandardResponse(status=201, message="Client created successfully.", data={"id":clientuser_obj.id})
        
        except IntegrityError as e:
            return StandardResponse(status=400, success=False, message="Database error.", errors=[str(e)])
        
        except Exception as e:  
            return StandardResponse(status=400, success=False, message="An unexpected error occurred.", errors=[str(e)])
    
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = ClientUser.objects.get(id=id)
        except (ClientUser.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        username=request.data.get("username")
        if User.objects.filter(username=username).exclude(client_profile=obj).exists():
            return StandardResponse(status=40, success=False, errors=[f"Username {username} already taken"])  
                
        serializer = ClientUserSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        user_serializer = UserPutSerializer(obj.user, data=request.data, partial=True)
        if not user_serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=user_serializer.errors)
        serializer.save()
        user_serializer.save()
        return StandardResponse(status=201, message="Client updated successfully.", data={"id":obj.id})
    
    @role_required(Role.CLIENT_USER,OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = ClientUser.objects.get(id=id)
            obj.user.delete()
            obj.delete()
        except (ClientUser.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete client user as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Client User deleted successfully.")
        

class SupplierView(SearchAndFilterMixin, PaginationMixin, APIView):

    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        
        base_query = Supplier.objects.select_related("client")
       
        if id != "list":
            try:
                instance = base_query.get(id=id) 
            except (Supplier.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetSupplierSerializer(instance).data)
        
        pg = request.GET.get("pg", 0)
        limit = request.GET.get("limit",25 )
        search = request.GET.get("q", "")
        # fields = get_all_fields(Supplier, ignore_fields=[], include_relational_fields=False)
        fields = ["supplier_code","name","client__name"]
        queryset = None
        user = request.this_user
        if user.role == Role.SUPPLIER_USER:
            try:
                # supplier_user = SupplierUser.objects.only("supplier_id").get(user=user)
                # queryset = base_query.filter(id=supplier_user.supplier_id)
                queryset = base_query.filter(id=user.profile().supplier.id)
            except SupplierUser.DoesNotExist:
                return StandardResponse(success=False,data=[],status=400,errors=["User not found"])
            
        elif user.role == Role.ADMIN:
            queryset = base_query.all().order_by("-updated_at")

        elif user.role == Role.OPERATIONS and user.profile().access_level == OperationUserRole.L1:
            queryset = base_query.all().order_by("-updated_at")
        
        if not queryset.exists():
            return StandardResponse(success=True,data=[],count=0,status=200)

        queryset = queryset.order_by("-updated_at")
        count = queryset.count()

        if search:
            queryset = self.apply_search(fields, queryset, search.strip())
        
        filters = self.make_filters_list(request)
        if filters:
            apply_filters = self.appy_dynamic_filter(filters)  
            queryset = queryset.filter(apply_filters)
            

        queryset = self.paginate_results(queryset, pg, limit)
        
        data = []
        for item in queryset:

            data.append({
            "id": item.id,
            "created_at": item.created_at,
            "updated_at": item.updated_at,
            "is_active": item.is_active,
            "is_deleted": item.is_deleted,
            "deleted_at": item.deleted_at,
            "supplier_code": item.supplier_code,
            "name": item.name,
            "address": item.address,

            "client": {
                # "id": item.client.id,
                # "created_at": item.client.created_at,
                # "updated_at": item.client.updated_at,
                # "is_active": item.client.is_active,
                # "is_deleted": item.client.is_deleted,
                # "deleted_at": item.client.deleted_at,
                # "client_code": item.client.client_code,
                "name": item.client.name,
                # "generate_asn": item.client.generate_asn,
                # "hs_code_validation": item.client.hs_code_validation,
                # "hs_code": item.client.hs_code,
                # "eccn_validation": item.client.eccn_validation,
                # "show_parent_add": item.client.show_parent_add,
                # "can_add_shipto_add": item.client.can_add_shipto_add,
                # "timezone": item.client.timezone,
                # "service_type":item.client.service_type,
                # "measurement_method": item.client.measurement_method
            },

            # "storerkeys": [
            #     {
            #     "id": skey.id,
            #     "storerkey_code": skey.storerkey_code,
            #     "name": skey.name,
            #     "hub_code": skey.hub.hub_code,
            #     "hub_id": skey.hub.id
            #     }
            #      for skey in item.storerkeys.all()],
            
        })

        return StandardResponse(
            success=True,
            # data=GetSupplierSerializer(queryset, many=True).data,
            data=data,
            count=count,
            status=200
        )
            
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def post(self, request, *args, **kwargs):
        serializer = SupplierSerializer(data=request.data)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Supplier created successfully.", data={"id":obj.id})
    
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = Supplier.objects.get(id=id)
        except (Supplier.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        serializer = SupplierSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        return StandardResponse(status=201, message="Supplier created successfully.", data={"id":obj.id})
    
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = Supplier.objects.get(id=id)
            obj.delete()
        except (Supplier.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete supplier as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Supplier deleted successfully.")
        

class SupplierUserView(SearchAndFilterMixin, PaginationMixin, APIView):
    
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = SupplierUser.objects.get(id=id) 
            except (SupplierUser.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetSupplierUserSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            # fields = get_all_fields(SupplierUser, ignore_fields=[], include_relational_fields=False) 
            fields = ["user__name","user__username","supplier__client__name","supplier__client__client_code"]           
            queryset = SupplierUser.objects.select_related("user", "supplier").filter(supplier=request.GET.get("supplier")).order_by("-updated_at")
            
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
                data=GetSupplierUserSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )
            
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def post(self, request, *args, **kwargs):
        try:
            with transaction.atomic():                
                request.data["role"] = Role.SUPPLIER_USER
                username=request.data.get("username")
                if User.objects.filter(username=username).exists():
                    return StandardResponse(status=400, success=False, errors=[f"Username {username} already taken"])  
          
                user_seralizer = UserPostSerializer(data=request.data)
                if not user_seralizer.is_valid():
                    transaction.set_rollback(True)
                    return StandardResponse(status=400, success=False, errors=user_seralizer.errors)
                user_obj = user_seralizer.save()
                
                request.data["user"] = user_obj.id
                supplier_user_serializer = SupplierUserSerializer(data=request.data)
                if not supplier_user_serializer.is_valid():
                    transaction.set_rollback(True)
                    return StandardResponse(status=400, success=False, errors=supplier_user_serializer.errors)
                supplier_user_obj = supplier_user_serializer.save()
                
            return StandardResponse(status=201, message="Supplier User created successfully.", data={"id":supplier_user_obj.id})
        
        except IntegrityError as e:
            return StandardResponse(status=400, success=False, message="Database error.", errors=[str(e)])
        
        except Exception as e:  
            return StandardResponse(status=400, success=False, message="An unexpected error occurred.", errors=[str(e)])
    
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = SupplierUser.objects.get(id=id)
        except (SupplierUser.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        username=request.data.get("username")
        if User.objects.filter(username=username).exclude(supplier_profile=obj).exists():
            return StandardResponse(status=40, success=False, errors=[f"Username {username} already taken"])  
          
        serializer = SupplierUserSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        user_serializer = UserPutSerializer(obj.user, data=request.data, partial=True)
        if not user_serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=user_serializer.errors)
        serializer.save()
        user_serializer.save()
        return StandardResponse(status=201, message="Supplier User updated successfully.", data={"id":obj.id})
    
    @role_required(Role.SUPPLIER_USER,OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = SupplierUser.objects.get(id=id)
            obj.user.delete()
            obj.delete()
        except (SupplierUser.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete supplier user as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="supplier User deleted successfully.")
        

class OperationsView(SearchAndFilterMixin, PaginationMixin, APIView):

    @role_required(OperationUserRole.L1)
    def get(self, request, id=None, *args, **kwargs):
        if id != "list":
            try:
                obj = Operations.objects.get(id=id) 
            except (Operations.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            return StandardResponse(status=200, data=GetOperationsSerializer(obj).data)
        else:
            pg = request.GET.get("pg") or 0
            limit = request.GET.get("limit") or 25
            search = request.GET.get("q", "")
            # fields = get_all_fields(Operations, ignore_fields=[], include_relational_fields=False) 
            fields = ["user__name","user__username","access_level"]          
            queryset = Operations.objects.prefetch_related("hubs","storerkeys").all().order_by("-updated_at")
            
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
                data=GetOperationsSerializer(paginate_result, many=True).data,
                count=count,
                status=200
            )

    @role_required(OperationUserRole.L1)
    def post(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                username=request.data.get("username")
                if User.objects.filter(username=username).exists():
                    return StandardResponse(status=400, success=False, errors=[f"Username {username} already taken"])  
                          
                user_seralizer = UserPostSerializer(data=request.data)
                if not user_seralizer.is_valid():
                    return StandardResponse(status=400, success=False, errors=user_seralizer.errors)
                user_obj = user_seralizer.save()
                
                request.data["user"] = user_obj.id
                if request.data["role"] == Role.ADMIN:
                    request.data.setdefault("hubs", [])
                    request.data.setdefault("storerkeys", [])
                    del request.data["access_level"]
                    # request.data.setdefault("access_level", OperationUserRole.L3)
                else:
                    hubs = request.data.get("hubs")
                    storerkeys = request.data.get("storerkeys")

                    if not hubs or not storerkeys:
                        transaction.set_rollback(True)
                        return StandardResponse(
                            errors=["Both 'hubs' and 'storerkeys' are required and cannot be empty."],
                            success=False,
                            status=400
                        )
                print(request.data)
                operations_serializer = OperationsSerializer(data=request.data)
                if not operations_serializer.is_valid():
                    transaction.set_rollback(True)
                    return StandardResponse(status=400, success=False, errors=operations_serializer.errors)
                operations_obj = operations_serializer.save()
                
            return StandardResponse(status=201, message="Operations created successfully.", data={"id":operations_obj.id})
        
        except IntegrityError as e:
            return StandardResponse(status=400, success=False, message="Database error.", errors=[str(e)])
        
        except Exception as e:  
            return StandardResponse(status=400, success=False, message="An unexpected error occurred.", errors=[str(e)])
    
    @role_required(OperationUserRole.L1)
    def put(self, request, id=None, *args, **kwargs):
        try:
            obj = Operations.objects.get(id=id)
        except (Operations.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        username=request.data.get("username")
        if User.objects.filter(username=username).exclude(operations_profile=obj).exists():
            return StandardResponse(status=40, success=False, errors=[f"Username {username} already taken"])  
          
        serializer = OperationsSerializer(obj, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        user_serializer = UserPutSerializer(obj.user, data=request.data, partial=True)
        if not user_serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=user_serializer.errors)
        serializer.save()
        user_serializer.save()
        return StandardResponse(status=201, message="Operations updated successfully.", data={"id":obj.id})
    
    @role_required(OperationUserRole.L1)
    def delete(self, request, id=None, *args, **kwargs):
        try:
            obj = Operations.objects.get(id=id)
            obj.user.delete()
            obj.delete()
        except (Operations.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        except ProtectedError as e:
            related_models = {str(related._meta.verbose_name_plural).capitalize() for related in e.protected_objects}
            return StandardResponse(status=400, success=False, errors=[f"Cannot delete operations as it is protected by related {', '.join(related_models)}"])
        return StandardResponse(status=200, message="Operations deleted successfully.")
        

class DangerousGoodAPI(APIView, SearchAndFilterMixin):
    
    @role_required(OperationUserRole.L1,Role.SUPPLIER_USER)
    def get(self, request,name=None):
        
        search = request.GET.get("q", "")
        if name == 'list':
            try:
                queryset = DangerousGoodClass.objects.all().prefetch_related("categories").only("id","name")
                search_fields = ["name","categories__name"]
                
                if search:
                    queryset = self.apply_search(search_fields, queryset, search.strip())
                
                filters = self.make_filters_list(request)
                if filters:
                    apply_filters = self.appy_dynamic_filter(filters)  
                    queryset = queryset.filter(apply_filters)
                    
                dangerous_goods = [{
                    "id" : dg.id,
                    "name" : dg.name,
                    "categories" : list(dg.categories.all().values("id", "name"))
                } for dg in queryset]

                return StandardResponse(success=True, status=200, data=dangerous_goods)
            except Exception as e:
                return StandardResponse(success=False, status=400, errors=[str(e)])

        try:
            obj = DangerousGoodClass.objects.prefetch_related("categories").only("id","name").filter(name=name).first()

            if not obj:
                return StandardResponse(status=200, success=True, data=[])
            
            response_data = {
                "id" : obj.id,
                "name" : obj.name,
                "categories" : list(obj.categories.all().values("id", "name"))
            }
            
            return StandardResponse(status=200, success=True, data=response_data)
        except (DangerousGoodClass.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        except Exception as e:
            return StandardResponse(success=False, status=400, errors=[str(e)])


    @role_required(OperationUserRole.L1)
    @transaction.atomic
    def post(self,request,name=None):

        serializer = DangerousGoodClassSerializer(data=request.data)
        if not serializer.is_valid():
            transaction.set_rollback(True)
            return StandardResponse(serializer.errors, status=400)
        serializer.save()

        return StandardResponse(serializer.data, status=201)

    @role_required(OperationUserRole.L1)
    def patch(self, request, name=None):
        try:
            dg_class = DangerousGoodClass.objects.get(name=name)
        except (DangerousGoodClass.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])

        serializer = DangerousGoodClassSerializer(dg_class,data=request.data)
        if not serializer.is_valid():
            transaction.set_rollback(True)
            return StandardResponse(serializer.errors, status=400)
        serializer.save()

        return StandardResponse(serializer.data, status=200)
    
    @role_required(OperationUserRole.L1)
    def delete(self, request, name=None):
        try:
            obj = DangerousGoodClass.objects.get(name=name)
            obj.delete()
            return StandardResponse(status=200, message="Dangerous Good deleted successfully.")
        except (DangerousGoodClass.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])




class MaterialMasterAPI(APIView,PaginationMixin,SearchAndFilterMixin):
    fields = ["id","product_code","storerkey__id","storerkey__name","hub__id","hub__name","hub__hub_code","is_active","is_chemical","is_dangerous_good"]
    
    def _check_storerkey_and_hub(self,storerkey,hub):
        
        storerkey = StorerKey.objects.filter(storerkey_code = storerkey).first()
        hub = Hub.objects.filter(hub_code = hub).first()

        if not storerkey:
            return None, None, StandardResponse(status=400, success=False, errors=["Invalid storerkey code"])
        if not hub:
            return None, None, StandardResponse(status=400, success=False, errors=["Invalid hub code"])
        
        if not storerkey.hub == hub:
            return None, None,  StandardResponse(status=400, success=False, errors=["Storerkey does not belong to the specified hub"])
        
        return storerkey, hub, None
    

    def __storerkey__dict(self,materials):
        data = [{
            "id": m.id,
            "product_code": m.product_code,
            "description": m.description,
            "uom": m.uom,
            "unit_price": m.unit_price,
            "unit_cost": m.unit_cost,
            "weight": m.weight,
            "volume": m.volume,
            "length": m.length,
            "width": m.width,
            "height": m.height,
            "hs_code": m.hs_code,
            "stock_number": m.stock_number,
            "notes": m.notes,
            "alternate_unit": m.alternate_unit,
            "alternate_sku": m.alternate_sku,
            "origin_country": m.origin_country,
            "to_expire_days": m.to_expire_days,
            "to_delivery_days": m.to_delivery_days,
            "to_best_by_days": m.to_best_by_days,
            "is_active": m.is_active,
            "is_chemical": m.is_chemical,
            "is_dangerous_good": m.is_dangerous_good,
            "is_kit": m.is_kit,
            "is_stackable": m.is_stackable,
            "inspection_required": m.inspection_required,
            "shelf_life": m.shelf_life,
            "shelf_life_indicator": m.shelf_life_indicator,
            "sku_class": m.sku_class,
            "retail_sku": m.retail_sku,
            "hazmat_codes_keys": m.hazmat_codes_keys,
            "susr1": m.susr1,
            "susr2": m.susr2,
            "susr3": m.susr3,
            "susr4": m.susr4,
            "susr5": m.susr5,
            "storerkey": {
                "id": m.storerkey.id,
                "name": m.storerkey.name,
                "code": getattr(m.storerkey, "storerkey_code", None), 
            },
            "hub": {
                "id": m.hub.id,
                "name": m.hub.name,
                "hub_code": m.hub.hub_code,
            }
        } for m in materials]

        return data
    

    @role_required(OperationUserRole.L1)
    def get(self, request, product_code=None, *args, **kwargs):

        q = request.GET.get("q", "").strip()
        qs = MaterialMaster.objects.select_related("storerkey", "hub").order_by("-updated_at")
        if product_code and product_code != "list":
            material = qs.filter(product_code=product_code).first()
            if not material:
                return StandardResponse(status=404, success=False, errors=["Material not found"])
            data = self.__storerkey__dict([material])
            return StandardResponse(status=200, data=data[0])
        
        pg = request.GET.get("pg", 0)
        limit = request.GET.get("limit", 25)
        
        search_fields = [
            "product_code","description","storerkey__name","storerkey__storerkey_code","hub__name","hub__hub_code",
            "description","uom","unit_price","unit_cost"
        ]

        if q:
            qs = self.apply_search(search_fields, qs, q)

        filters = self.make_filters_list(request)
        if filters:
            apply_filters = self.appy_dynamic_filter(filters)  
            qs = qs.filter(apply_filters)
                    

        # materials = base.filter(product_code__icontains=q).all().order_by("-updated_at")
        count = qs.count()
        paginated_materials = self.paginate_results(qs, pg, limit)
        data = self.__storerkey__dict(paginated_materials)    
        return StandardResponse(status=200, data=data,count=count)

    @role_required(OperationUserRole.L1)
    def post(self, request, *args, **kwargs):

        storerkey,hub,error = self._check_storerkey_and_hub(request.data.get("storerkey"), request.data.get("hub"))
        if error:
            return error
        request.data["storerkey"] = storerkey.id
        request.data["hub"] = hub.id
        serializer = MaterialSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return StandardResponse(status=201, message="Material created successfully")
        return StandardResponse(status=400, success=False, errors=serializer.errors)
    
    @role_required(OperationUserRole.L1)
    def put(self, request, product_code=None):
        try:
            material = MaterialMaster.objects.get(product_code=product_code)
        except MaterialMaster.DoesNotExist:
            return StandardResponse(status=404, success=False, errors=["Material not found"])
        
        storerkey,hub,error = self._check_storerkey_and_hub(request.data.get("storerkey"), request.data.get("hub"))
        if error:
            return error

        request.data["storerkey"] = storerkey.id
        request.data["hub"] = hub.id
        serializer = MaterialSerializer(material, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return StandardResponse(status=200, message="Material updated successfully")
        return StandardResponse(status=400, success=False, errors=serializer.errors)
    
    @role_required(OperationUserRole.L1)
    def delete(self, request, product_code=None):
        try:
            material = MaterialMaster.objects.get(product_code=product_code)
            material.delete()
            return StandardResponse(status=200, message="Material deleted successfully")
        except MaterialMaster.DoesNotExist:
            return StandardResponse(status=404, success=False, errors=["Material not found"])
        

class MaterialMasterBulkImportAPI(APIView):
    @role_required(OperationUserRole.L1)
    @transaction.atomic()
    def post(self, request, *args, **kwargs):
        data = request.data.get("data", [])
        if not isinstance(data, list) or not data:
            return StandardResponse(status=400, success=False, errors=["Invalid or empty data payload"])

        storerkey_codes = {item.get("storerkey") for item in data}
        hub_codes = {item.get("hub") for item in data}

        storerkeys = {sk.storerkey_code: sk for sk in StorerKey.objects.filter(storerkey_code__in=storerkey_codes)}
        hubs = {h.hub_code: h for h in Hub.objects.filter(hub_code__in=hub_codes)}

        valid_items = []
        errors = []

        for idx, item in enumerate(data):
            index = idx + 1
            sk_code = item.get("storerkey")
            hub_code = item.get("hub")

            storerkey = storerkeys.get(sk_code)
            hub = hubs.get(hub_code)

            if not storerkey:
                errors.append(f"Item {index}: Invalid storerkey '{sk_code}'")
                continue

            if not hub:
                errors.append(f"Item {index}: Invalid hub '{hub_code}'")
                continue

            if storerkey.hub_id != hub.id:
                errors.append(f"Item {index}: Storerkey '{sk_code}' does not belong to hub '{hub_code}'")
                continue

            item["storerkey"] = storerkey.id
            item["hub"] = hub.id
            valid_items.append(item)

        if errors:
            return StandardResponse(status=400, success=False, errors=errors)

        serializer = MaterialSerializer(data=valid_items, many=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)

        serializer.save()
        return StandardResponse(status=201, message="Materials created successfully")