from rest_framework.views import APIView
from django.core.exceptions import ValidationError
from rest_framework.response import Response
import jwt
from django.conf import settings
from .models import User, RecentlySearch, UserPreference
from core.response import StandardResponse
import json
from operations.models import PurchaseOrder, Consignment
from portal.utils import get_all_fields
from portal.choices import Role
from portal.mixins import SearchAndFilterMixin
from datetime import datetime, timedelta, timezone


class LoginAPI(APIView):
    def post(self, request, *args, **kwargs):
        try:
            user = User.objects.get(username=request.data.get("username"),is_active =True)
        except (User.DoesNotExist, ValidationError):
            return StandardResponse(status=403, success=False, errors=["Invalid Credentials"])
        
        if not user.check_password(request.data.get("password")):
            return StandardResponse(status=403, success=False, errors=["Invalid Credentials"])
        
        exp_in_min = 800 if settings.DEBUG else 60 
        payload = {
            "user_id": str(user.id),
            "iat": datetime.now(timezone.utc), ## issued at
            "exp": datetime.now(timezone.utc) + timedelta(minutes=exp_in_min),  # expiry
        }

        token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
        
        user_preference = UserPreference.objects.filter(user=user).first()
        preference = user_preference.preference if user_preference else {}
        
        data = {
            "token": token,
            "name": user.name,
            "username": user.username,
            "role": user.role,
            "id": user.id,
            "preference": preference, 
            "force_change_password": user.force_change_password
        }
        
        profile = None
        
        if user.role == Role.SUPPLIER_USER:
            profile = user.supplier_profile
            data.update({
                "supplier_id": profile.id,
                "supplier_user_id": profile.supplier.id
            })
            
        if user.role == Role.CLIENT_USER:
            profile = user.client_profile
            data.update({
                "client_id": profile.id,
                "client_user_id": profile.client.id
            })
            
        if user.role == Role.OPERATIONS:
            profile = user.operations_profile
            data.update({
                "operations_id": profile.id,
                "access_level" : profile.access_level
            })
            
        if profile:
            storerkey_ids = list(profile.storerkeys.values_list("id", flat=True))

            if not storerkey_ids:
                return StandardResponse(status=403, success=False, errors=["The user don't have any storer keys assigned!"])
                
            
        return StandardResponse(status=200, data=data)
       

class ChangePasswordAPI(APIView):
    def post(self, request, *args, **kwargs):
        username = request.data.get("username")
        password = request.data.get("password")
        
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            return StandardResponse(status=403, success=False, errors=["User does not exists"])
        
        user.set_password(password)
        user.force_change_password = False
        user.save()
        
        return StandardResponse(status=200, data=[], message="Password Updated successfully")
        
    
    
class PreferenceAPI(APIView):
    def post(self, request, *args, **kwargs):
        preference = request.data.get("preference")

        if not preference:
            return StandardResponse(status=400, message="Preference data is required")

        UserPreference.objects.update_or_create(
            user=request.this_user, 
            defaults={"preference": preference}
        )

        return StandardResponse(status=200, message="Saved Successfully")
        
        
               
class RecentlySearchView(APIView):
    
    def get(self, request, id=None, *args, **kwargs):
        obj, _ = RecentlySearch.objects.get_or_create(user=request.this_user)
        field_mapping = {
            "purchase_order": obj.purchase_order,
            "consignment": obj.consignment,
            "pickup": obj.pickup,
        }

        if id in field_mapping:
            try:
                data = json.loads(field_mapping[id]) 
            except json.JSONDecodeError:
                data = [] 

            return Response(data=data, status=200)

        return StandardResponse(status=404, success=False, errors=["Not found"])
        
             
    def post(self, request, id=None, *args, **kwargs):
        obj, _ = RecentlySearch.objects.get_or_create(user=request.this_user)
        new_value = request.data.get("value")

        if not new_value:
            return StandardResponse(status=400, success=False, errors=["Value is required"])

        field_mapping = {
            "purchase_order": "purchase_order",
            "consignment": "consignment",
            "pickup": "pickup",
        }

        if id in field_mapping:
            field_name = field_mapping[id]

            try:
                data_list = json.loads(getattr(obj, field_name))
            except json.JSONDecodeError:
                data_list = []

            if new_value in data_list:
                data_list.remove(new_value)

            data_list.append(new_value)
            data_list = data_list[-20:]

            setattr(obj, field_name, json.dumps(data_list))
            obj.save()

            return StandardResponse(status=200, message="Saved Successfully")

        return StandardResponse(status=400, success=False, errors=["Invalid ID"])
    
    
class GlobalSearchView(SearchAndFilterMixin, APIView):
    def get(self, request, id=None, *args, **kwargs):
        if id == "purchase_order":
            search = request.GET.get("q", "")
            queryset = PurchaseOrder.objects.filter(is_active=True).order_by("-created_at")
            fields = get_all_fields(PurchaseOrder, ignore_fields=[], include_relational_fields=False)
            if search:
                queryset = self.apply_search(fields, queryset, search)
            return Response(data=queryset.values("id", "reference_number"), status=200) 
            # return StandardResponse(
            #     success=True,
            #     data=queryset.values("id", "reference_number"),
            #     status=200
            # )         
        elif id == "consignment":
            search = request.GET.get("q", "")
            queryset = Consignment.objects.all()
            fields = get_all_fields(Consignment, ignore_fields=[], include_relational_fields=False)
            if search:
                queryset = self.apply_search(fields, queryset, search)
            return StandardResponse(data=queryset.values("id", "consignment_id"), status=200) 
        
        return StandardResponse(status=400, success=False, errors=["Invalid ID"])