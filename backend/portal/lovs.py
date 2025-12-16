from rest_framework.views import APIView
from rest_framework.response import Response
from core.response import StandardResponse
from entities.models import Client, Supplier, Hub, StorerKey, Operations, MaterialMaster
from operations.models import PurchaseOrder, Consignment, PurchaseOrderLine
from .models import MOT, FreightForwarder, RejectionCode, GLAccount, DropDownValues
from workflows.models import Console
from django.db.models import F, Value, Q
from django.db.models.functions import Concat
from .models import PackagingType, AddressBook, CostCenterCode
from django.db.models import Q
from .choices import Role,PurchaseOrderStatusChoices, ConsoleStatusChoices, OperationUserRole, ConsignmentStatusChoices 
from adhoc.models import AdhocPurchaseOrderLine
import pandas as pd
from .mixins import SearchAndFilterMixin
from entities.models import ClientUser, DangerousGoodClass
from operations.mixins import FilterMixin
from operations.mixins import PaginationMixin
from core.decorators import role_required

lov_options = {
    "manufacturing_country": {
        "fields": ["id", "value", "label"],
        "model": DropDownValues,
        "filters": {"dropdown_name": "ISO2"},
        "order_by" : "label"
    },
    "operations": {
        "fields": ["id", "access_level","user__name","user__username","user__role","is_active"],
        "model": Operations,
        "filters": {},
        "select_related": ["user"],
        "order_by": "user__name"
    },
    "client": {
        "fields": ["id", "client_code", "name","is_active"],
        "model": Client,
        "filters": {},
        "order_by": "name"
    },
    "client-user": {
        "fields": ["user__username","user__name", "user__role", "client__name", "is_active"],
        "model": ClientUser,
        "filters": {},
        "select_related": ["user","client"],
        "order_by": "client__name"

    },
    "hub": {
        "fields": ["id", "hub_code", "name","location", "is_active"],
        "model": Hub,
        "filters": {},
        "order_by": "name"
    },
    "rejection-code": {
        "fields": ["id", "rejection_code"],
        "model": RejectionCode,
        "filters": {},
    },
    "gl-account": {
        "fields": ["id", "gl_code", "shipment_type","is_active"],
        "model": GLAccount,
        "filters": {},
    },
    "supplier": {
        "fields": ["id", "supplier_code", "name","client","client__name","address","is_active"],
        "search_fields": ["name", "supplier_code","is_active","address"],
        "model": Supplier,
        "filters": {},
        "select_related" : ["client"],
        "order_by": "name"
    },
    "mot": {
        "fields": ["id", "mot_type", "mode","is_active"],
        "model": MOT,
        "filters": {},
    },
    "freight-forwarder": {
        "fields": ["id", "name", "mot","mot__mot_type", "mc_dot", "scac", "is_active"],
        "model": FreightForwarder,
        "filters": {},
        "distinct_fields" : {"id"},
        "order_by": "name"
    },
    "consignment": {
        "fields": ["id", "consignment_id","type", "delivery_address","supplier__name","client__name", "packages",
                    "consignment_status"],
        "model": Consignment,
        "filters": {}, 
        "order_by": "-consignment_id",
        "select_related": ["supplier","client","delivery_address","console"],
        "Q" : ~Q(consignment_status__in = [ConsignmentStatusChoices.DRAFT])
    },
    "purchase-order": {
        "fields": ["id", "reference_number", "customer_reference_number","supplier__name","description", "open_quantity", "type", "order_due_date"],
        "model": PurchaseOrder,
        "filters": {},
        "select_related": ["supplier"]
    },
    "purchase-order-line": {
        "fields": ["id", "reference_number", "customer_reference_number","product_code"],
        "model": PurchaseOrderLine,
        "query_params": ["purchase_order"],
        "lookup_field": {"purchase_order" : "purchase_order__customer_reference_number"},
        "select_related": ["purchase_order"]
    },
    "client-storer-key": {
        "fields": ["id", "storerkey_code", "name"],
        "model": StorerKey,
        "filters": {},
        "query_params": ["client","hub"]
    },
    "package-type": {
        "fields": ["id", "package_name", "package_type", "measurement_method"],
        "model": PackagingType,
        "filters": {},
        "query_params": ["supplier"]
    },
    "storer-key": {
        "fields": ["id", "storerkey_code", "name","client","hub","timezone","service_type","measurement_method", "is_active"],
        "model": StorerKey,
        "filters": {},
    },
    "supplier-storer-key": {
        "fields": ["id", "storerkey_code", "name"],
        "model": StorerKey,
        "filters": {},
        "query_params": ["suppliers"]
    },
    "address": {
        "fields": ["id", "address_name", "address_type", "address_line_1", "address_line_2", "city", "state", "country", "zipcode", "mobile_no", "alternate_mobile_no", "responsible_person_name", "latitude", "longitude"],
        "model": AddressBook,
        "filters": {},
    },
    "client-address": {
        "fields": ["id", "address_name", "address_type", "address_line_1", "address_line_2", "city", "state", "country", "zipcode", "mobile_no", "alternate_mobile_no", "responsible_person_name", "latitude", "longitude"],
        "model": AddressBook,
        "filters": {},
        "query_params": ["client"]
    },
    "storerkey-address": {
        "fields": ["id", "address_name", "address_type", "address_line_1", "address_line_2", "city", "state", "country", "zipcode", "mobile_no", "alternate_mobile_no", "responsible_person_name", "latitude", "longitude"],
        "model": AddressBook,
        "filters": {},
        "query_params": ["storerkey"]
    },
    "supplier-address": {
        "fields": ["id", "address_name", "address_type", "address_line_1", "address_line_2", "city", "state", "country", "zipcode", "mobile_no", "alternate_mobile_no", "responsible_person_name", "latitude", "longitude"],
        "model": AddressBook,
        "filters": {},
        "query_params": ["supplier"]
    },
    "console": {
        "fields": ["id", "console_id","console_status","gl_account", "last_bol_generated_at", "last_bol_generated_by"],
        "model": Console,
        "filters": {},
        "order_by" : "-console_id",
        "select_related": ["gl_account","last_bol_generated_by"],
        # "Q" : ~Q(console_status__in = [ConsoleStatusChoices.CANCELLED,ConsoleStatusChoices.DELIVERED,ConsoleStatusChoices.RECEIVED_AT_DESTINATION])
    },
    "cc-code": {
        "fields": ["id","cc_code","plant_id","center_code","sloc"],
        "model": CostCenterCode,
        "filters": {},
        "order_by" : "-created_at",
    },
    "dangerous-good": {
        "fields": ["id","name","categories__name"],
        "model": DangerousGoodClass,
        "filters": {},
        "order_by" : "-created_at",
    },
    "material_master": {
        "fields": ["id","product_code","description","storerkey__name","storerkey__storerkey_code","hub__name","hub__hub_code","hs_code",
            "uom","unit_price","unit_cost","susr1","susr2","susr3","susr4","susr5","is_chemical","is_dangerous_good","is_kit","is_stackable"
        ],
        "model": MaterialMaster,
        "filters": {},
        "order_by" : "-updated_at",
    }
}

def _build_filters(request, query_params, static_filters,lookup_field):
    """Build dynamic filters from query parameters and static ones."""
    filters = {
        "is_deleted": False,
        "is_active": True,
    }
    if query_params:
        for param in query_params:
            value = request.GET.get(param)
            if value is not None:
                if (param and lookup_field) and param in lookup_field[param]:
                    param = lookup_field[param]
                filters[param] = value
    filters.update(static_filters or {})
    return filters

def _apply_search_filter(queryset, fields, search_query):
    """Apply case-insensitive search across specified fields."""
    search_filter = Q()
    for field in fields:
        search_filter |= Q(**{f"{field}__icontains": search_query})
    return queryset.filter(search_filter)


class LovApiView(APIView,SearchAndFilterMixin,PaginationMixin):
    def get(self, request, key, *args, **kwargs):

        lov_config = lov_options.get(key)
        if not lov_config:
            return StandardResponse(status=404, success=False, errors=["Key Not Found"])

        ## Config
        model = lov_config.get("model")
        fields = lov_config.get("fields", [])
        order_by = lov_config.get("order_by")
        query_params = lov_config.get("query_params", [])
        static_filters = lov_config.get("filters", {})
        distinct_fields = lov_config.get("distinct_fields",{})
        q_filters = lov_config.get("Q",Q())
        select_related = lov_config.get("select_related",[])
        lookup_field = lov_config.get("lookup_field",[])
        search_query = request.GET.get("q","").strip()
        search_fields = lov_config.get("search_fields", [])

        ## Pagination
        pg = request.GET.get("pg")
        limit = request.GET.get("limit")
        
        if not model:
            return StandardResponse(status=500, success=False, errors=["Invalid configuration for the given key."])

        try:
            filters = _build_filters(request, query_params, static_filters,lookup_field)
            queryset = model.objects.filter(**filters).select_related(*select_related)

            if q_filters:
                queryset = queryset.filter(q_filters)
            if distinct_fields:
                queryset = queryset.distinct() ## dont add the fields into distinct it will not work for mssql
            if order_by:
                queryset = queryset.order_by(order_by)

            if search_query:
                queryset = _apply_search_filter(queryset, search_fields if search_fields else fields, search_query)

            if pg and limit:
                count = queryset.count()
                paginate_result = self.paginate_results(queryset, pg, limit)
                serialized_data = self.filter_annotations_by_fields(paginate_result, key, fields)
                return StandardResponse(data=serialized_data, status=200,count = count)
            else:
                serialized_data = self.filter_annotations_by_fields(queryset, key, fields)
                return Response(data=serialized_data, status=200)

        except Exception as e:
            return StandardResponse(status=500, success=False, errors=f"Internal Server Error: {str(e)}")
            
          
    
class SupplierLOVAPI(APIView,SearchAndFilterMixin,PaginationMixin):

    @role_required(Role.ADMIN, Role.SUPPLIER_USER, OperationUserRole.L1, OperationUserRole.L2)
    def get(self, request,*args, **kwargs):

        lov_config = lov_options.get("supplier")
        if not lov_config:
            return StandardResponse(status=404, success=False, errors=["Supplier Not Found in Lovs"])

        role = request.this_user.role
    
        ## Config
        model = lov_config.get("model")
        fields = lov_config.get("fields", [])
        order_by = lov_config.get("order_by")
        query_params = lov_config.get("query_params", [])
        static_filters = lov_config.get("filters", {})
        distinct_fields = lov_config.get("distinct_fields",{})
        q_filters = lov_config.get("Q",Q())
        select_related = lov_config.get("select_related",[])
        lookup_field = lov_config.get("lookup_field",[])
        search_query = request.GET.get("q","").strip()
        search_fields = lov_config.get("search_fields", [])

        ## Pagination
        pg = request.GET.get("pg")
        limit = request.GET.get("limit")
        
        if not model:
            return StandardResponse(status=500, success=False, errors=["Invalid configuration for the given key."])

        try:
            filters = _build_filters(request, query_params, static_filters,lookup_field)
            if role == Role.SUPPLIER_USER:
                profile = request.this_user.profile()
                filters["id"] = profile.supplier.id

                
            queryset = model.objects.filter(**filters).select_related(*select_related)
            if q_filters:
                queryset = queryset.filter(q_filters)
            if distinct_fields:
                queryset = queryset.distinct() ## dont add the fields into distinct it will not work for mssql
            if order_by:
                queryset = queryset.order_by(order_by)

            if search_query:
                queryset = _apply_search_filter(queryset, search_fields if search_fields else fields, search_query)

            if pg and limit:
                count = queryset.count()
                paginate_result = self.paginate_results(queryset, pg, limit)
                serialized_data = self.filter_annotations_by_fields(paginate_result, "supplier", fields)
                return StandardResponse(data=serialized_data, status=200,count = count)
            else:
                serialized_data = self.filter_annotations_by_fields(queryset, "supplier", fields)
                return Response(data=serialized_data, status=200)

        except Exception as e:
            # Log the exception properly in production code
            return StandardResponse(status=500, success=False, errors=f"Internal Server Error: {str(e)}")
        


class PurchaseOrderLovApi(FilterMixin, PaginationMixin, APIView):
    def get(self, request, *args, **kwargs):

         ## Pagination
        pg = request.GET.get("pg",0)
        limit = request.GET.get("limit",50)
        q = request.GET.get("q", "").strip()


        filters = {}

        if request.GET.get("consignment_create") == "true":
            filters["lines__isnull"] = False    

        filters["is_asn"] = (request.this_user.role == Role.CLIENT_USER)

        filters = self.build_filter(request.this_user,"PO",filters)
        filters['lines__status__in']=PurchaseOrderStatusChoices.OPEN,PurchaseOrderStatusChoices.PARTIALLY_FULFILLED
        filters["status__in"] = PurchaseOrderStatusChoices.OPEN,PurchaseOrderStatusChoices.PARTIALLY_FULFILLED
        
        # Base queryset
        queryset = (
            PurchaseOrder.objects
            .filter(**filters)
            .select_related("supplier")
            .distinct()
        )

        if q:
             queryset = queryset.filter(
                Q(customer_reference_number__icontains=q)
            )

        queryset = queryset.values(
            "id", "reference_number", "customer_reference_number",
            "supplier__name", "description", "open_quantity",
            "type", "order_due_date"
        ).order_by("-created_at")


        # if pg and limit:
        #     paginated_result = self.paginate_results(queryset,pg,limit)
        #     return Response(data=paginated_result, status=200)
        # return Response(data=queryset, status=200)

        paginated_result = self.paginate_results(queryset,pg,limit)
        return Response(data=paginated_result, status=200)



class AdhocPurchaseOrderLineLovApi(APIView):
    def get(self, request, *args, **kwargs):
        try:
            consignment = Consignment.objects.get(consignment_id=request.GET.get("consignment_id"))
        except:
            return StandardResponse(status=400, success=False, errors=["Consignment does not exists"])
        
        objs = AdhocPurchaseOrderLine.objects.filter(purchase_order=consignment.adhoc)
        return Response(data=objs.distinct().values("id", "reference_number", "customer_reference_number"), status=200)
        
        
    
class StorerKeyByHubLOV(APIView):
    def get(self, request, *args, **kwargs):
        hubs = request.GET.get("hubs")
        storerkeys = (
            StorerKey.objects.filter(hub_id__in=hubs.split(","))
            .annotate(
                hub_code=F("hub__hub_code"), 
                label=Concat(F("storerkey_code"), Value("("), F("hub__hub_code"), Value(")")), 
            )
            .values("id", "storerkey_code", "name", "hub_code", "label") 
        )
        return Response(data=storerkeys, status=200)
    


class HubByStorerKeyLOV(APIView):

    def get(self, request, *args, **kwargs):
        storerkey_ids = request.GET.getlist("storerkeys")
        if not storerkey_ids:
            return Response(data=[], status=200)

        storerkeys = StorerKey.objects.filter(storerkey_code__in=storerkey_ids).select_related("hub")

        hubs = []
        seen = set()

        for storer in storerkeys:
            hub = storer.hub
            if hub and hub.id not in seen:
                hubs.append({
                    "id": hub.id,
                    "hub_code": hub.hub_code,
                    "name": hub.name
                })
                seen.add(hub.id)

        return Response(data=hubs, status=200)
    


class SuppliersByStorerKeyLOV(APIView):
    def get(self, request, *args, **kwargs):
        storer_keys = request.GET.get("storer_keys")
        if not storer_keys:
            return Response(data=[], status=200)
            
        suppliers = (
            Supplier.objects.filter(storerkeys__in=storer_keys.split(","))
            .annotate(
                storerkey_code=F("storerkeys__storerkey_code"), 
                label=Concat(F("supplier_code"), Value("("), F("storerkeys__storerkey_code"), Value(")")), 
            )
            .values("id", "name", "supplier_code", "storerkey_code", "label")
        )
        return Response(data=suppliers, status=200)
        


class PlantByIdLOV(APIView):
    def get(self, request, *args, **kwargs):
        try:
            plant_ids = (
            PurchaseOrder.objects
            .exclude(plant_id="")
            .order_by("plant_id")  
            .values_list("plant_id", flat=True)
            .distinct()
            )
        except Exception as e:
            return Response(data={"error": str(e)}, status=500)
        return Response(data=list(plant_ids), status=200)
    


class CenterCodeByIdLOV(APIView):
    def get(self, request, *args, **kwargs):
        try:
            center_codes = (
                PurchaseOrder.objects
                .exclude(center_code="")
                .order_by("center_code")  
                .values_list("center_code", flat=True)
                .distinct()
            )
        except Exception as e:
            return Response(data={"error": str(e)}, status=500)
        return Response(data=list(center_codes), status=200)
    


class ConsoleBOLGeneratedByLOV(APIView):
    def get(self, request, *args, **kwargs):
        try:
            console_bol_generated_by = (
                Console.objects
                .exclude(last_bol_generated_by=None)
                .order_by("last_bol_generated_by__name")  
                .values_list("last_bol_generated_by__name", flat=True)
                .distinct()
            )
        except Exception as e:
            return Response(data={"error": str(e)}, status=500)
        return Response(data=list(console_bol_generated_by), status=200)
    


class ConsignmentCreatedBy(APIView):
    def get(self, request, *args, **kwargs):
        try:
            consignment_created_by = (
                Consignment.objects
                .exclude(created_by=None)
                .order_by("created_by__name")  
                .values_list("created_by__name", flat=True)
                .distinct()
            )
        except Exception as e:
            return Response(data={"error": str(e)}, status=500)
        return Response(data=list(consignment_created_by), status=200)
    

class AvailableConsolesLOV(APIView,PaginationMixin):
    
    
    def get(self, request):

        lov_config = lov_options.get("console")
        if not lov_config:
            return StandardResponse(status=500, success=False, errors=["lovs configuration not setup in the system"])

        ## Config
        fields = lov_config.get("fields", [])
        select_related = lov_config.get("select_related",[])

        ## Pagination
        pg = request.GET.get("pg",0)
        limit = request.GET.get("limit",10)
        

        try:
            queryset = (
                Console.objects
                .select_related(*select_related)
                .exclude(
                    console_status__in = [
                        ConsoleStatusChoices.CANCELLED,
                        ConsoleStatusChoices.PICKUP_REJECTED,
                        ConsoleStatusChoices.DELIVERED,
                        ConsoleStatusChoices.RECEIVED_AT_DESTINATION,
                    ]
                )
                .order_by("-console_id")
                .values(*fields)
            )

            count = queryset.count()
            paginated_result = self.paginate_results(queryset, pg, limit)
                    
            return StandardResponse(data=paginated_result, status=200, count=count)
            
        except Exception as e:
            return StandardResponse(status=500, success=False, errors=f"Internal Server Error: {str(e)}")
     
    

def add_drop_down_values():

    file_path = '/home/apsis/Development/Dev/PickupTool/aramex-pickup-tool/backend/portal/countries_iso_codes.xlsx'
    df = pd.read_excel(file_path)

    DropDownValues.objects.filter(dropdown_name='ISO2').delete()
    for index, row in df.iterrows():
        print(index)
        iso2_code = row['ISO2']  

        DropDownValues.objects.get_or_create(
            dropdown_name='ISO2',
            label=row["Manufacturing Country"],
            value=iso2_code,
            parent_item=None
        )