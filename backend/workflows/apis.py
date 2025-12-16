from rest_framework.views import APIView
from rest_framework.response import Response
from django.core.exceptions import ValidationError
from django.db import transaction
from core.response import StandardResponse
from .models import Console,ConsoleAuditTrail, ConsoleAuditTrailField
from portal.mixins import SearchAndFilterMixin, PaginationMixin
from django.db.models import Count, F
from operations.models import Consignment
from portal.choices import ConsignmentStatusChoices, ConsoleStatusChoices, PackageStatusChoices, Role, OperationUserRole
from portal.models import  FreightForwarder
from .serializers import ConsoleSerializer
from crequest.middleware import CrequestMiddleware
from django.db.models import Q
from datetime import datetime
from uuid import UUID
from django.shortcuts import get_object_or_404
from core.decorators import role_required
from operations.services import ConsignmentServices

def log_console_audit(console, old_consignments=None, new_consignments=None, old_status=None):
    changes = []
    
    request = CrequestMiddleware.get_request()
    created_by = None
    if request:
        created_by = request.this_user

    if old_consignments != new_consignments:
            changes.append({
                "field_name": "consignments",
                "old_value": str(old_consignments),
                "new_value": str(new_consignments),
            })

    if old_status and old_status != console.console_status:
        changes.append({
            "field_name": "console_status",
            "old_value": old_status,
            "new_value": console.console_status,
        })

    if not changes:
        return 

    audit = ConsoleAuditTrail.objects.create(console=console, updated_by=created_by)
    fields = [
        ConsoleAuditTrailField(
            audit_trail=audit,
            field_name=change["field_name"],
            old_value=change["old_value"],
            new_value=change["new_value"]
        ) for change in changes
    ]

    ConsoleAuditTrailField.objects.bulk_create(fields)
    
    console.updated_at = audit.created_at
    console.save()
        
        
class GetFreeConsignmentsAPI(APIView):
    def get(self, request, console_id=None, *args, **kwargs):
        try:
            obj = Console.objects.get(console_id=console_id) 
        except (Console.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        consingments = Consignment.objects.select_related("purchase_order").filter(
            console__isnull=True,
            consignment_status__in= [ConsignmentStatusChoices.PENDING_CONSOLE_ASSIGNMENT,ConsignmentStatusChoices.PENDING_BID]
            # freight_forwarder=obj.freight_forwarder,
        ).values("id", "consignment_id").order_by("-created_at")
        
        return Response(data=consingments, status=200)   


class AddConsignmentsToConsoleAPI(APIView):

    @role_required(OperationUserRole.L1,OperationUserRole.L2)
    @transaction.atomic
    def post(self, request, *args, **kwargs): 
        try:
            obj = Console.objects.get(console_id=request.data.get("console_id")) 
        except (Console.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        old_consignments = Consignment.objects.filter(console=obj)
        old_consignment_ids = list(old_consignments.values_list("consignment_id", flat=True))

        consignment_ids = request.data.get("consignment_ids")
        warning = request.data.get("warning")
        if not consignment_ids:
            return StandardResponse(status=400, errors=["Consignment IDs cannot be empty."])
        
        
        consignments = Consignment.objects.filter(consignment_id__in=consignment_ids)
        
        if warning:
            old_destinations = set(old_consignments.values_list("delivery_address_id", flat=True))
            if len(old_destinations) == 1:
                destination_address = set(consignments.values_list("delivery_address_id", flat=True))
                if not destination_address.issubset(old_destinations):
                    warning_msg = "The selected destination differs from the expected destination for this console. Please verify the details. You may still proceed to create a consolidation request or generate the BOL if this is intentional."
                    return StandardResponse(status=200, data={"warning": True, "message": warning_msg})
        
        try:
            if obj.freight_forwarder:
                consignments.update(console=obj,consignment_status=ConsignmentStatusChoices.FREIGHT_FORWARDER_ASSIGNED)
                # create_update_bol(console=obj, consignments=consignments)
            else:
                consignments.update(console=obj,consignment_status=ConsignmentStatusChoices.CONSOLE_ASSIGNED)
            
            new_consignment_ids = list(Consignment.objects.filter(console=obj).values_list("consignment_id", flat=True))

            
            log_console_audit(console=obj, old_consignments=old_consignment_ids, new_consignments=new_consignment_ids)
            
            for consignment in consignments:
                ConsignmentServices.notify_consignment_update(request.this_user,consignment)
            
            return StandardResponse(status=201, message="Consignment added Successfully.") 
        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=400, success=False, errors=[str(e) or "something went wrong"])
            
    
class ConsoleView(SearchAndFilterMixin, PaginationMixin, APIView):
    transform_fields = {
        "freight_forwarder": "freight_forwarder__name",
        "last_bol_generated_by": "last_bol_generated_by__name",
        "gl_account": "gl_account__gl_code",
        "purchase_order": "purchase_order__customer_reference_number",
        "adhoc": "adhoc__customer_reference_number",
        # "bol_generated": "last_bol_generated_at",
        "consignments" : "consignment_count"
    }
        
    def appy_dynamic_filter(self, filters):
        query_filter = Q()
        for filter_item in filters:
            column = filter_item["column"]
            operator = filter_item["operator"]
            value = filter_item["value"]
            
            if column == "bol_generated":
                if value == "true":
                    query_filter &= Q(last_bol_generated_at__isnull=False)
                else:
                    query_filter &= Q(last_bol_generated_at__isnull=True)

            elif column in ['last_bol_generated_at','updated_at']:

                if value:
                    orm_operator = self.operator_mapping.get(operator, '')
                    query_filter &= self.apply_date_filter(value,orm_operator,column,query_filter) # handle invalid date format
                    continue
            else:
                if isinstance(value, list):
                    orm_operator = '__in'
                else:
                    orm_operator = self.operator_mapping.get(operator, '')
                
                filter_key = f"{column}{orm_operator}"
                query_filter &= Q(**{filter_key: value})
            
        return query_filter
        
    def _tranform_object(self, data, list=True):
        if list:
            data.update({
                "freight_forwarder": data.pop("freight_forwarder__name"),
                "gl_account": data.pop("gl_account__gl_code"),
                "last_bol_generated_by": data.pop("last_bol_generated_by__name")
            })
        else:
            data.update({
                # "purchase_order": data.pop("purchase_order__customer_reference_number"),
                # "adhoc": data.pop("adhoc__customer_reference_number")
            })
        

    @role_required(OperationUserRole.L1,OperationUserRole.L2,OperationUserRole.L3)
    def get(self, request, id=None, *args, **kwargs):
        pg = request.GET.get("pg") or 0
        limit = request.GET.get("limit") or 25
        search = request.GET.get("q", "")

        if id != "list":
            try:
                obj = Console.objects.get(console_id=id) 
            except (Console.DoesNotExist, ValidationError):
                return StandardResponse(status=400, success=False, errors=["Object not found"])
            
            fields = ["id", "consignment_id", "consignment_status",
                    "last_bol_generated_by","last_bol_generated_at",
                    "packages_delivered","total_packages","delivered_percent"
                ]
            
            queryset = Consignment.objects.filter(console=obj).prefetch_related("packagings").annotate(
                last_bol_generated_by=F("last_bol_gen_by__name"),
                last_bol_generated_at=F("last_bol_gen_at"),
                packages_delivered=Count("packagings", filter=Q(packagings__status=PackageStatusChoices.RECEIVED)),
                total_packages = Count("packagings"),
                delivered_percent = (F("packages_delivered") / F("total_packages")) * 100
                # gl_code=F("console__gl_account__gl_code")
            ).values(*fields).order_by("-consignment_id").distinct()
            
        else:

            fields = ["console_id", "console_status","consignment_count",
                    "freight_forwarder__name", "gl_account__gl_code", "last_bol_generated_at",
                    "last_bol_generated_by__name", "updated_at"
                    ]
            
            status = request.GET.get("status", "")
                        
            queryset = (
                Console.objects.select_related("freight_forwarder", "gl_account","last_bol_generated_by")
                .annotate(consignment_count=Count("consignments"))
                .filter().values(*fields).order_by("-console_id")
            )
            if status and status != "all":
                queryset = queryset.filter(console_status=status)

        apply_filters = self.make_filters_list(request)
    
        if apply_filters:
            for f in apply_filters:
                f['column'] = self.transform_fields.get(f['column'], f['column'])
            apply_filters = self.appy_dynamic_filter(apply_filters)  
            queryset = queryset.filter(apply_filters)
                
            
        if search:
            queryset = self.apply_search(fields, queryset, search)  


        count = queryset.count()
        paginate_result = self.paginate_results(queryset, pg, limit)
        
        list = id == "list"
        for data in paginate_result:
            self._tranform_object(data, list)
            
        return StandardResponse(success=True, data=paginate_result, count=count, status=200)
    

    @role_required(OperationUserRole.L1,OperationUserRole.L2)
    @transaction.atomic
    def post(self, request, *args, **kwargs):
        consignment_ids = request.data.get("consignment_ids")
        warning = request.data.get("warning")
        if not consignment_ids:
            return StandardResponse(status=400, success=False, errors=["Consignment IDs cannot be empty."])
        
        consignments = Consignment.objects.filter(consignment_id__in=consignment_ids)
        
        if not consignments.exists():
            return StandardResponse(status=404, errors=["Consignments not found."])
        
        # ff_assined_ids = set(consignments.values_list("freight_forwarder_id", flat=True))
        
        # if len(ff_assined_ids) > 1:
        #     return StandardResponse(status=400, success=False, errors=["Console can be created on same Frieght Forwarder Assigned."])
        
        if warning:
            destination_address = set(consignments.values_list("delivery_address_id", flat=True))
            if len(destination_address) > 1:
                warning_msg = "The selected destination differs from the expected destination for this console. Please verify the details. You may still proceed to create a consolidation request or generate the BOL if this is intentional."
                return StandardResponse(status=200, data={"warning": True, "message": warning_msg})
        
        
        # request.data["freight_forwarder"] = ff_assined_ids.pop()
        console_serializer = ConsoleSerializer(data=request.data)
        if not console_serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=console_serializer.errors)
        
        try:
            console = console_serializer.save()
                
            # create_update_bol(console=console, consignments=consignments)
            
            consignments.update(console=console,consignment_status = ConsignmentStatusChoices.CONSOLE_ASSIGNED)
            
            for consignment in consignments:
                ConsignmentServices.notify_consignment_update(request.this_user,consignment)
                
            return StandardResponse(status=201, message="Console created Successfully.") 
        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=400, success=False, errors=[str(e) or "something went wrong"])
            
    @role_required(OperationUserRole.L1,OperationUserRole.L2)
    @transaction.atomic
    def delete(self, request, id=None, *args, **kwargs):
        try:
            console = Console.objects.get(console_id=request.GET.get("console_id")) 
        except (Console.DoesNotExist, ValidationError):
            return StandardResponse(status=400, success=False, errors=["Object not found"])
        
        old_consignments = list(Consignment.objects.filter(console=console).values_list("consignment_id", flat=True))
        try:
            consignments = Consignment.objects.filter(consignment_id=id)
            consignments.update(console=None,consignment_status=ConsignmentStatusChoices.PENDING_CONSOLE_ASSIGNMENT)
            
            new_consignments = list(Consignment.objects.filter(console=console).values_list("consignment_id", flat=True))

            # BOLServices.remove_consignments_from_bol()
            
            log_console_audit(console=console, old_consignments=old_consignments, new_consignments=new_consignments)
            
            if len(old_consignments) == 1:
                console.console_status = ConsoleStatusChoices.CANCELLED
                console.save()

            return StandardResponse(status=201, message="Consignment removed Successfully.")
        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=400, success=False, errors=[str(e) or "something went wrong"])
    
class ConsoleMetaData(APIView):
    
    @role_required(OperationUserRole.L1,OperationUserRole.L2,OperationUserRole.L3)
    def get(self, request, id=None, *args, **kwargs):

        if not id:
            return StandardResponse(success=False, message="ID is required", status=400)
        
        fields = ["id","console_id", "console_status","freight_forwarder_id","last_bol_generated_at", "last_bol_generated_by", "updated_at","gl_account_id","gl_code"]
        
        console = Console.objects.filter(console_id=id).annotate(
            gl_code = F("gl_account__gl_code"),
            ).values(*fields).first()

        if not console:
            return StandardResponse(success=False, message="Console not found", status=404)

        consignments = list(Consignment.objects.filter(console_id = console["id"]).values("id"))
        console["consignments"] = consignments

        return StandardResponse(success=True, data=console, status=200)

class ConsoleFFAssignAPI(SearchAndFilterMixin, PaginationMixin, APIView):

    @role_required(OperationUserRole.L1,OperationUserRole.L2)
    @transaction.atomic
    def post(self, request, *args, **kwargs):
        try:
            freight_forwarder = request.data.get("freight_forwarder")
            actual_pickup_datetime = request.data.get("actual_pickup_datetime")
            id = request.data.get("id")


            if not all([freight_forwarder, id, actual_pickup_datetime]):
                return StandardResponse(status=400, success=False, errors=["Missing required fields."])

            try:
                freight_forwarder = UUID(freight_forwarder)
            except ValueError:
                return StandardResponse(status=400, success=False, errors=["Invalid UUID format for freight_forwarder."])

            # files = [file for key, file_list in request.FILES.lists() if key.startswith("ff_documents") for file in file_list]
            # if not files:
            #     return StandardResponse(status=400, success=False, errors=["FF Documents cannot be empty."])
            
            try:
                actual_pickup_datetime = datetime.strptime(actual_pickup_datetime, "%Y-%m-%d %I:%M %p")
            except ValueError:
                return StandardResponse(status=400, success=False, errors=["Invalid datetime format. Expected 'YYYY-MM-DD HH:MM AM/PM'."])
        
            console = get_object_or_404(Console, id=id)
            ff = get_object_or_404(FreightForwarder, id=freight_forwarder)
        
            consignments = Consignment.objects.filter(console=console)
            if not consignments.exists():
                return StandardResponse(status=404, success=False, errors=["Consignments not found."])

            # ConsoleFFDocument.objects.filter(console=console).delete()
            # ConsoleFFDocument.objects.bulk_create([ConsoleFFDocument(console=console, file=file) for file in files])

            formatted_date = actual_pickup_datetime.strftime("%Y-%m-%d %H:%M:%S")

            consignments.update(
                freight_forwarder=ff,
                consignment_status=ConsignmentStatusChoices.FREIGHT_FORWARDER_ASSIGNED,
                actual_pickup_datetime=formatted_date,
            )

            console.freight_forwarder = ff
            console.console_status = ConsoleStatusChoices.FREIGHT_FORWARDER_ASSIGNED
            console.save(update_fields=["freight_forwarder","console_status"])

            for consignment in consignments:
                ConsignmentServices.notify_consignment_update(request.this_user,consignment)

            # create_update_bol(console=console, consignments=consignments)

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=400, success=False, errors=[str(e)])

        return StandardResponse(status=201, success=True, message="Freight Forwarder Assigned Successfully.")
    

class ConsolePickupReject(APIView):

    @role_required(OperationUserRole.L1,OperationUserRole.L2)
    def post(self,request):

        console_id = request.data.get("console_id")
        if not console_id:
            return StandardResponse(status=400,success=False,message="Missing 'console_id' in request.")
        
        console = Console.objects.filter(console_id=console_id).first()
        if not console:
            return StandardResponse(status=400,success=False,message=f"Invalid Console id : {console_id}")
        
        consignments = Consignment.objects.filter(console = console)
        if not consignments:
            return StandardResponse(status=400,success=False,message=f"No consignments found for this console {console_id}")
        
        try:
            with transaction.atomic():
                consignments.update(consignment_status = ConsignmentStatusChoices.PENDING_BID,freight_forwarder = None,console="")
                console.console_status = ConsoleStatusChoices.PICKUP_REJECTED
                console.freight_forwarder = None
                console.save()
                return StandardResponse(status=200,message="Console pickup-rejected")

        except Exception as e:
            return StandardResponse(status=500,success=False,message="An error occurred while rejecting the console pickup.")
        




