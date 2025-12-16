import json
from django.db import transaction
from django.db.models import Sum, F, Q, Count
from rest_framework.views import APIView
from core.response import StandardResponse
from operations.services import ConsignmentStepHandler
from operations.models import (
    Consignment,
    PurchaseOrder,
    PurchaseOrderLine,
    ConsignmentPOLine,
    ConsignmentPOLineBatch,
    ConsignmentPackaging,
    PackagingAllocation,
    ConsignmentDocument,
    ConsignmentDocumentAttachment,
    ComprehensiveReport
)
from operations.services import POLineService, ConsignmentWorkflowServices, ComprehensiveReportService
from rest_framework.parsers import MultiPartParser, FormParser
from operations.serializers import ConsignmentComplianceSerializer, ConsignmentPackagingSerializer, ConsignmentSerializer, ComprehensiveReportSerializer
from portal.choices import ConsignmentStatusChoices, ConsignmentDocumentTypeChoices
from portal.models import PackagingType
from rest_framework import viewsets
from django.core.files.storage import default_storage
from operations.utils import update_files, addresses_and_pickup
import uuid
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from operations.consignment_apis import ConsignmentListAPI
from django.utils import timezone
from dateutil.relativedelta import relativedelta
from operations.mixins import FilterMixin
from portal.mixins import SearchAndFilterMixin


class CreateDraftConsignmentAPI(APIView):
    @transaction.atomic
    def get(self, request, *args, **kwargs):
        try:

            user = request.this_user
            if not user:
                return StandardResponse(status=400, errors=["User Required"])
            
            consignment, _ = ConsignmentWorkflowServices.create_draft_consignment(user)

            return StandardResponse(
                data = {
                    "consignment_id": consignment.consignment_id, 
                    "step": consignment.step
                },
                status=200
            )
        

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=500, errors=[str(e)], success=False) 



class ConsignmentCreationAPI(APIView):

    @transaction.atomic
    def post(self, request, *args, **kwargs):

        try:
            user = request.this_user
            consignment_ref = request.data.get("consignment_id")
            
            if not consignment_ref:
                return StandardResponse(success=False,errors = ["Consignment ID required"], status=400)
            
            consignment = Consignment.objects.filter(
                consignment_id=consignment_ref,
                # created_by=user,
                consignment_status__in=[ConsignmentStatusChoices.DRAFT, ConsignmentStatusChoices.PENDING_FOR_APPROVAL,ConsignmentStatusChoices.REJECTED] 
            ).first()
            
            if not consignment:
                return StandardResponse(success=False,errors = [f"Invalid Consignment ID {consignment_ref} / This action is not allowed for this consignment"], status=400)

            is_draft = True if consignment.consignment_status == ConsignmentStatusChoices.DRAFT else False
            message, error = ConsignmentStepHandler.create_update_consignment(consignment=consignment, user=user)

            if error:
                transaction.set_rollback(True)
                return StandardResponse(success=False,errors=[error], status=400)
            
            if is_draft:
                return StandardResponse(
                    message=message,
                    data = {"consignment_id": consignment.consignment_id},
                    status=201)
            
            return StandardResponse(
                message=message,
                data = {"consignment_id": consignment.consignment_id},
                status=200)
        
        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success=False,errors=[error], status=500)



class ConsignmentSummaryCountsAPI(APIView):

    def get(self, request, id = None, *args, **kwargs):
        
        if not id:
            return StandardResponse(success=False, errors=["consignment_id is required"], status=400)
        
        data, errors = ConsignmentStepHandler.get_counts(id)
        if errors:
            return StandardResponse(success=False, errors=errors, status=400)
        return StandardResponse(data = data, status=200)



class ConsignmentHoverAPI(APIView):
    def get(self, request, id = None, *args, **kwargs):
        try:
            if not id:
                return StandardResponse(success=False, errors=["consignment_id is required"], status=400)
            

            hover_data, errors = ConsignmentStepHandler.consignment_hover_details(id)
            if errors:
                return StandardResponse(success=False, errors=errors, status=400)
            
            return StandardResponse(data = hover_data, status=200)
        except Exception as e:  
            return StandardResponse(success=False, errors=[str(e)], status=500)



class ConsignmentDgItemsAPI(APIView):

    def get(self, request,id=None, *args, **kwargs):    


        if not id:
            return StandardResponse(success=False, errors=["consignment_id is required"], status=400)
        
        consignment = Consignment.objects.filter(consignment_id=id).only("id").first()
        if not consignment:
            return StandardResponse(success=False, errors=["Invalid Consignment ID"], status=400)
        
        dg_items, errors = ConsignmentStepHandler.dg_item_details(consignment)
        if errors:
            return StandardResponse(success=False, errors=errors, status=400)
        return StandardResponse(data = dg_items, status=200)
    


class ComplianceDetailsAPI(APIView):
    parser_classes = [MultiPartParser, FormParser]


    def get(self, request, *args, **kwargs):

        try:
            purchase_order_line_id = request.query_params.get("purchase_order_line") ## UUID 
            consignment_id = request.query_params.get("consignment_id") ## cosnignment_id ref

            if not consignment_id or not purchase_order_line_id:
                return StandardResponse(success=False, errors=["consignment_id or purchase_order_line_id required"], status=400)

            
            consignment = Consignment.objects.filter(
                consignment_id=consignment_id, is_active=True
            ).only("id").first()
            if not consignment:
                return StandardResponse(success=False, errors=["Invalid Consignment ID"], status=400)

            line = PurchaseOrderLine.objects.filter(id=purchase_order_line_id).only("id").first()
            if not line:
                return StandardResponse(success=False, errors=["Invalid Line ID"], status=400)


            compliance_details, error = ConsignmentStepHandler.get_compliance_details(consignment, line)
            if error:
                return StandardResponse(success=False, errors=[error], status=400)
            
            return StandardResponse(data = compliance_details, status=200)

        except Exception as e:
            return StandardResponse(success=False, errors=[str(e)], status=500)
        

    @transaction.atomic
    def update_compliance_details(self, request, *args, **kwargs):
        try:
            purchase_order_line = request.data.get("purchase_order_line") ## UUID 
            consignment_id = request.data.get("consignment_id") ## cosnignment_id
            if not consignment_id:
                return StandardResponse(success=False,errors = ["consignment id is required"], status=400)
            
            if not purchase_order_line or not consignment_id :
                return StandardResponse(success=False, errors = ["purchase_order_line or consignment id is required"], status=400)
            
            
            consignment = Consignment.objects.filter(
                consignment_id=consignment_id
            ).only("id").first()
            if not consignment:
                return StandardResponse(success=False,errors = ["Invalid Consignment ID"], status=400)
            
            line = PurchaseOrderLine.objects.filter(
                id=purchase_order_line,
            ).only("id","is_chemical","is_dangerous_good").first()

            data = {
                "details" : request.data.get("details"),
                "consignment" : consignment.id,
                "purchase_order_line" : request.data.get("purchase_order_line"),
                "hs_code" : request.data.get("hs_code"),
                "eccn" : request.data.get("eccn"),
                "document_type" : request.data.get("document_type"),
                "dg_class" : request.data.get("dg_class"),
                "dg_category" : request.data.get("dg_category"),
                "dg_note" : request.data.get("dg_note"),
                "attachments" : request.FILES.getlist("attachments"),
                "compliance" : request.data.get("compliance"),
                "compliance_dg" : request.data.get("compliance_dg"),
                "compliance_chemical" : request.data.get("compliance_chemical"),
                "batch_data" : request.data.get("batch_data"),
                "country_of_origin" : request.data.get("country_of_origin"),
                "deleted_doc_ids" : [request.data.get(key) for key in request.data if key.startswith("deleted_doc_ids")]
            }


            if not line:
                return StandardResponse(success=False, errors=["Invalid Line ID"], status=400)


            _, error = ConsignmentStepHandler.save_compliance_details(consignment,line,data)
            if error:
                transaction.set_rollback(True)
                return StandardResponse(success=False, errors=[error], status=400)

            # if line.is_chemical:
            #     _, error = ConsignmentStepHandler.save_batch_details(data)
            #     if error:
            #         transaction.set_rollback(True)
            #         return StandardResponse(success=False, errors=[error], status=400)
            

            return StandardResponse(message="Details saved successfully", status=200)

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success=False, errors=[str(e)], status=500)
        except json.JSONDecodeError as e:
            return StandardResponse(success=False, errors=[f"Invalid batch_data format: {str(e)}"], status=400)

    
    def post(self, request, *args, **kwargs):
        return self.update_compliance_details(request)
        
    
    def patch(self, request, *args, **kwargs):
        return self.update_compliance_details(request)
        

    def put(self, request, *args, **kwargs):
        return self.update_compliance_details(request)
    


class SelectedLinesAPI(viewsets.ModelViewSet):

    queryset = ConsignmentPackaging.objects.all()
    serializer_class = ConsignmentPackagingSerializer

    def list(self, request, *args, **kwargs):

        consignment =Consignment.objects.filter(consignment_id=request.query_params.get("consignment_id")).first()

        lines, error = POLineService.get_po_line_details_by_lines(consignment=consignment)
        if error:
            return StandardResponse(success=False,errors = [error], status=400)

        return StandardResponse(data = lines, status=200)

    @transaction.atomic
    def create(self, request, *args, **kwargs):
        try:
            consignment = Consignment.objects.filter(
                consignment_id=request.data.get("consignment_id")
            ).only("id").first()
            
            if not consignment:
                return StandardResponse(status=400, success=False, errors=["Invalid Consignment ID"])
            
            _ ,error = ConsignmentStepHandler.create_consignment_selected_line(consignment,request.data)

            if error:
                return StandardResponse(status=400, success=False, errors=[error])

            ConsignmentWorkflowServices.remove_orphan_allocations(consignment)

            return StandardResponse(message = "Line added successfully", status=200)

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success=False, errors=[str(e)], status=500)
        

    def update(self, request, *args, **kwargs):

        try:
            consignment = Consignment.objects.filter(
                consignment_id=request.data.get("consignment_id"),consignment_status=ConsignmentStatusChoices.DRAFT).only("id").first()
            
            if not consignment:
                return StandardResponse(status=400, success=False, errors=["Invalid Consignment ID"])
            
            _ ,error = ConsignmentStepHandler.create_consignment_selected_line(consignment,request.data)

            if error:
                return StandardResponse(status=400, success=False, errors=[error])

            return StandardResponse({"message": "Line added successfully"}, status=200)

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse({"error": str(e)}, status=500)
    


class ConsignmentPackagesAPI(APIView):

    def get(self, request,id=None, *args, **kwargs):
        
        if not id:
            return StandardResponse(status=400, success=False, errors=["consignment_id is required"])
        
        package_list, errors = ConsignmentStepHandler.get_consignment_packages(consignment_id=id)
        if errors:
            return StandardResponse(success=False, errors = [errors], status=400)
        
        count = len(package_list)

        if not package_list:
            return StandardResponse(data = [], status=200)
        
        return StandardResponse(data = package_list, status=200,count=count)
    
    
    @transaction.atomic
    def post(self, request, id= None, *args, **kwargs):
        
        try:

            consignment = Consignment.objects.filter(consignment_id=request.data.get("consignment_id")).first()
            if not consignment:
                return StandardResponse(success=False, errors=["Invalid Consignment ID"], status=400)
            
            packaging_type = PackagingType.objects.filter(id=request.data.get("packaging_type")).only("id").first()
            if not packaging_type:
                return StandardResponse(success=False, errors=["Invalid Packaging Type"], status=400)
        
            message ,error = ConsignmentStepHandler.create_packages(consignment=consignment, packaging_type=packaging_type, data=request.data)
            if error:
                return StandardResponse(status=400, success=False, errors=[error])

            return StandardResponse(message = message, status=200)
        
        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success=False, errors=[str(e)], status=500)
        
        except (ValueError, TypeError):
            return StandardResponse(status=400, success=False, errors=["Invalid quantity"])





    def delete(self, request, id=None, *args, **kwargs):
        if id is None:
            return StandardResponse(success=False, errors=["ID required"], status=400)
        
        ConsignmentPackaging.objects.filter(id=id).delete()

        return StandardResponse({"message": "Package deleted successfully"}, status=200)



class PoLineAllocationAPI(APIView):
    
    @transaction.atomic
    def pack_po_line(self,request,id=None):
        try:
            errors = None
            message = None
            pol_id = request.data.get("pol_id")
            packages = request.data.get("packages", [])

            cosnignment_ref = request.data.get("consignment_id")
            if not cosnignment_ref:
                return StandardResponse(success=False,errors=["Consignment ID is required"], status=400)
            
            if not pol_id:
                return StandardResponse(success=False,errors=["Line ID (pol_id) is required"], status=400)

            if not packages:
                return StandardResponse(success=False,errors=["At least one package is required"], status=400)

            consignment = Consignment.objects.prefetch_related("purchase_order_lines").filter(consignment_id=cosnignment_ref).first()
            if not consignment:
                return StandardResponse(success=False,errors=[f"Invalid Consignment ID: {cosnignment_ref}"], status=400)

            po_line = consignment.purchase_order_lines.filter(id=pol_id).only("id","customer_reference_number","purchase_order").first()
            # po_line = PurchaseOrderLine.objects.select_related("purchase_order").filter(id=pol_id).only("id","customer_reference_number","purchase_order").first()
            if not po_line:
                return StandardResponse(success=False,errors=[f"Invalid Line ID: {pol_id} / This line is not associated with this consignment"], status=400)

            if consignment.consignment_status == ConsignmentStatusChoices.DRAFT:
                message, errors = ConsignmentStepHandler.pack_po_line(consignment, po_line, packages)
            else:
                message, errors = ConsignmentStepHandler.pack_po_line(consignment, po_line, packages)
                POLineService.update_line_quantities([po_line])

            if errors:
                transaction.set_rollback(True)
                return StandardResponse(success=False,errors=[errors], status=400)

            return StandardResponse(message=message, status=200)
            

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success=False,errors=[str(e)], status=500)


    def get(self, request, id=None, *args, **kwargs):
        """
        Get packages for a consignment with allocated quantities for a specific PO line.
        """
        # --- Validate parameters ---
        if not id:
            return StandardResponse(success=False, errors=["Purchase Order Line ID is required"], status=400)

        consignment_ref = request.query_params.get("consignment_id")
        if not consignment_ref:
            return StandardResponse(success=False, errors=["Consignment ID is required"], status=400)

        # --- Safe UUID conversion ---
        try:
            uuid_id = uuid.UUID(id)
        except ValueError:
            return StandardResponse(success=False, errors=["Invalid Purchase Order Line ID"], status=400)

        # --- Fetch packages ---
        packages = list(
            ConsignmentPackaging.objects
            .filter(
                consignment__consignment_id=consignment_ref,
                draft_package_id__isnull=False
            )
            .values("id", "draft_package_id", "package_id","order_type")
            .annotate(is_stackable=F("packaging_type__is_stackable"),
                      length=F("packaging_type__length"),
                      width = F("packaging_type__width"),
                      height = F("packaging_type__height"),
                      dimension_unit=F("packaging_type__dimension_unit"),
                      weight = F("weight"),
                      weight_unit = F("weight_unit"),
                      packaging_type=F("packaging_type__package_type"))
            .order_by("created_at")
        )

        if not packages:
            return StandardResponse(data=[], status=200)

        # --- Precompute allocated quantities for all package IDs in one query ---
        package_ids = [pkg["id"] for pkg in packages]

        allocations = (
            PackagingAllocation.objects
            .filter(
                purchase_order_line_id=uuid_id,
                consignment_packaging_id__in=package_ids,
                consignment_packaging__draft_package_id__isnull=False
            )
            .values("consignment_packaging_id")
            .annotate(total_allocated=Sum("allocated_qty"))
        )

        # --- Build a dict of {package_id: allocated_qty} ---
        allocation_map = {
            a["consignment_packaging_id"]: a["total_allocated"] or 0
            for a in allocations
        }

        # --- Merge back into packages ---
        for pkg in packages:
            pkg["allocated_qty"] = allocation_map.get(pkg["id"], 0)

        count = len(packages)
        return StandardResponse(data=packages, status=200, count=count)
    

    def post(self, request, id=None, *args, **kwargs):
        return self.pack_po_line(request,id)
        

    def patch(self, request, *args, **kwargs):
        return self.pack_po_line(request,id)


    def delete(self, request, id=None, *args, **kwargs):
        if id is None:
            return StandardResponse(success=False, errors=["ID required"], status=400)
        
        PackagingAllocation.objects.filter(id=id).delete()

        return StandardResponse(success=False, errors=["Allocation deleted successfully"], status=200)



class ConsignmentFileUploadAPI(APIView):
    parser_classes = [MultiPartParser, FormParser]
    
    def get(self, request, id=None, *args, **kwargs):
        consignment = Consignment.objects.filter(consignment_id=id).only("id").first()
        if not consignment:
            return StandardResponse(success=False, errors=["Invalid Consignment ID"], status=400)

        data , error = ConsignmentWorkflowServices.get_attachments(consignment)
        if error:
            return StandardResponse(success=False, errors= [error], status=400)
        
        count = len(data)
        return StandardResponse(status=200, data=data,count=count)

    
    @transaction.atomic
    def post(self, request, *args, **kwargs):
        consignment_id = request.data.get("consignment_id")
        document_type = request.data.get("document_type")
        excluded_urls = request.data.get("excluded_attachment_urls", [])
        files = request.FILES.getlist("attachments")

        # error = validate_file_size(files)
        # if error:
        #     return StandardResponse(success=False, errors=[error], status=400)
        
        deleted_doc_ids = []
        for key in request.POST.keys():
            if key.startswith('deleted_doc_ids'):
                deleted_doc_ids.extend(request.POST.getlist(key))

        consignment = Consignment.objects.filter(consignment_id=consignment_id).only("id").first()

        if not consignment:
            return StandardResponse(success=False, errors=["Invalid Consignment ID"], status=400)
        
        if not document_type:
            return StandardResponse(success=False, errors=["Document type is required"], status=400)
        
        filters = {"consignment":consignment, "document_type":document_type}
        _ , errors= update_files(deleted_doc_ids=deleted_doc_ids, filters=filters, files=files)

        if errors:
            transaction.set_rollback(True)
            return StandardResponse(success=False, errors=errors, status=400)
        
        return StandardResponse(status=201, message="Documents Updated Successfully.")
    
    
    @transaction.atomic
    def delete(self, request, id=None, *args, **kwargs):
        is_update = request.GET.get("is_update") == "true"
        user = request.this_user.id
        existing_consignment_id = request.GET.get("consignment_id") if is_update else None

        if is_update and not existing_consignment_id:
            return StandardResponse(status=400, success=False, errors=["Consignment ID is required"])

        error, consignment, error_msg, _ = self.check_stage_po_consignment_exists(
            id, user, is_update, existing_consignment_id
        )
        if error:
            return StandardResponse(status=400, success=False, errors=[error_msg])

        try:
            attachments = ConsignmentDocumentAttachment.objects.filter(document__consignment=consignment)
            for attachment in attachments:
                if attachment.file:
                    default_storage.delete(attachment.file.path)
                attachment.delete()

            ConsignmentDocument.objects.filter(consignment=consignment).delete()

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(status=400, success=False, errors=[str(e)])

        return StandardResponse(status=201, message="Documents Deleted Successfully.")
        


class ConsignmentStepHandlerAPI(APIView):


    def get(self, request, id=None, *args, **kwargs):

        user = request.this_user
        step = request.query_params.get("step", 0)
        if not step:
            return StandardResponse(success = False,errors =[ "Step is required"], status=400)
        step = step[5:]
        
        consignment = (
            Consignment.objects.only("id")
            .filter(consignment_id=id)
            .first()
        )

        handler = getattr(ConsignmentStepHandler, f"data_get_step_{step}", None)
        if not handler:
            return StandardResponse(success = False,errors = ["Invalid step"], status=400)
        data, error = handler(consignment=consignment, user=user, data = request.data)  
        
        if error:
            return StandardResponse(success = False, errors = [error],status=400)

        count = len(data)
        return StandardResponse(
            data=data,
            status=200,
            count=count
        )

        
    @transaction.atomic
    def post(self, request,id =None, *args, **kwargs):

        try:
            if not id:
                return StandardResponse(success = False,errors =["Consignment ID is required"], status=400)
            
            user = request.this_user
            step = request.data.get("step", 0)
            if not step:
                return StandardResponse(success = False,errors = ["Step is required"], status=400)
            
            step = step[5:]
            consignment_ref = request.data.get("consignment_id", None)

            consignment = Consignment.objects.filter(
                consignment_id=consignment_ref,
                # created_by=user,
                # consignment_status=ConsignmentStatusChoices.DRAFT ## Commented this to handle editing after consignment_creation
            ).first()
            
            handler = getattr(ConsignmentStepHandler, f"handle_step_{step}", None)
            if not handler:
                return StandardResponse(success = False,errors = ["Invalid step"], status=400)
            con, error = handler(consignment=consignment,user=user,data = request.data)

            if error:
                transaction.set_rollback(True)
                return StandardResponse(success = False,errors = [error], status=400)

            return StandardResponse(
                data = {"consignment_id": con.consignment_id, "step": con.step},
                status=200)

        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success = False,errors = str(e), status=500)
        


class ConsignmentAddressAPI(APIView):

    def get(self, request, id=None, *args, **kwargs):
        
        if not id:
            return StandardResponse(status=400, success=False, errors=["Consignment ID required"])

        address, errors = addresses_and_pickup(consignment_id=id)
        if errors:
            return StandardResponse(status=400, success=False, errors=errors)
        
        return StandardResponse(status=200, data=address)
    
        # user = request.this_user.id
        # address = (Consignment.objects
        #     .select_related("consignor_address","delivery_address")
        #     .filter(consignment_id=consignment_ref, created_by=user)
        #     .values("id",
        #             "consignor_address_id",
        #             "delivery_address_id",
        #             "consignor_address__address_name",
        #             "delivery_address__address_name",
        #             "consignor_address__address_type",
        #             "delivery_address__address_type",
        #             "consignor_address__address_line_1",
        #             "delivery_address__address_line_1",
        #             "consignor_address__address_line_2",
        #             "delivery_address__address_line_2",)
        #     .first()
        # )
        
        # count = len(address)
        # return StandardResponse(status=200, data=address, count = count)
    
    @transaction.atomic
    def post(self, request,id=None, *args, **kwargs):

        user = request.this_user.id
        consignment_ref = request.data.get("consignment_id")
        consignment = Consignment.objects.filter(consignment_id=id).first()
        if not consignment:
            return StandardResponse(status=400, success=False, errors=["Consignment not found"])

        serializer = ConsignmentSerializer(consignment, data=request.data, partial=True)
        if not serializer.is_valid():
            return StandardResponse(status=400, success=False, errors=serializer.errors)
        obj = serializer.save()
        actual_pickup_datetime = request.data.get("actual_pickup_datetime", None)
        additional_instructions = request.data.get("additional_instructions", None)
        # pickup_timezone = request.data.get("pickup_timezone", None)

        consignment.actual_pickup_datetime = actual_pickup_datetime
        consignment.additional_instructions = additional_instructions
        # consignment.pickup_timezone = pickup_timezone
        consignment.save()

        return StandardResponse(status=201, message="Address Added Successfully.", data={"id":obj.id})
    


class ConsignmentComprehensiveReport(APIView,BaseCommand,FilterMixin, SearchAndFilterMixin):
    

    def get(self, request, *args, **kwargs):
        
            
        show_files = request.GET.get("show_files","false") == 'true'
        
        ## Is to show the latest uploaded comprehensive report file
        if show_files:
            comprehensive_report = (
                ConsignmentDocumentAttachment.objects
                .filter(document__document_type=ConsignmentDocumentTypeChoices.COMPREHENSIVE_REPORT)
                .only("file","created_at")
                .order_by("-created_at")
                .first()
            )
            
            user = ComprehensiveReport.objects.order_by("-created_at").values("user__name").first()

            data = {
                "status": "Success",
                "uploaded_file": comprehensive_report.file.url,
                "error_file": "",
                "created_at": comprehensive_report.created_at,
                "document_type" : "Pickup Comprehensive Report",
                "name": user.get("user__name")
            }

            if comprehensive_report:
                return StandardResponse(status=200, data=[data])
            return StandardResponse(status=200, data=[])

        ## If no show_files, then generate a new comprehensive report
        filters = Q()
        queryset = (
            Consignment.objects.filter(filters)
            .exclude(
                consignment_id__startswith = "DRAFT",
                consignment_status = ConsignmentStatusChoices.DRAFT
            )
            .select_related(
                "supplier", "client"
            )
            # .values(*ConsignmentListAPI.fields)
            .distinct()
            .order_by("-consignment_id")
        )
        
        queryset = ConsignmentListAPI.make_filters(self,request, queryset)
        
        consignment_ids = list(set(queryset.values_list("consignment_id", flat=True)))        
        status = list(set(queryset.values_list("consignment_status",flat=True)))

        data = request.data
        data["consignment_ids"] = consignment_ids
        data["status"] = status
        
        data["to_date"] = timezone.now()
        data["from_date"] = data["to_date"] - relativedelta(months=6)
        data["user"] = request.this_user.id

        serializer = ComprehensiveReportSerializer(data = data)
        
        if not serializer.is_valid():
            return StandardResponse(status=400, errors=serializer.errors)

        serializer.save()

        return StandardResponse(status=200, message="The report generation process has begun…")
    
    @transaction.atomic
    def post(self, request, id=None, *args, **kwargs):

        try:
            status = list(request.data.get("status",[]))
            from_date = datetime.strptime(request.data.get("from_date"), '%Y-%m-%d')
            to_date = datetime.strptime(request.data.get("to_date"), '%Y-%m-%d') + timedelta(days=1)
            if not from_date or not to_date:
                return StandardResponse(success = False,errors =[ "from_date and to_date are required"], status=400)
            
            print("Range:", from_date, "to", to_date, "status",status)

            filters = Q(
                created_at__gte=from_date,
                created_at__lt=to_date,
                consignment_status__in=status
            )
            queryset = (
                Consignment.objects.filter(
                    created_at__gte=from_date,
                    created_at__lt=to_date,
                    consignment_status__in=status
                )
                .exclude(
                    consignment_id__startswith = "DRAFT",
                    consignment_status = ConsignmentStatusChoices.DRAFT
                )
                .select_related(
                    "supplier", "client"
                )
                .distinct()
                .order_by("-consignment_id")
            )
            
            consignment_ids = []
            if queryset:
                consignment_ids = list(set(queryset.values_list("consignment_id", flat=True)))        
            
            data = request.data
            data["consignment_ids"] = consignment_ids
            data["status"] = status
            data["user"] = request.this_user.id

            ComprehensiveReport.objects.all().delete()
            serializer = ComprehensiveReportSerializer(data = data)

            if not serializer.is_valid():
                return StandardResponse(status=400, errors=serializer.errors)

            serializer.save()

            return StandardResponse(status=200, message="The report generation process has begun…")

            
        except Exception as e:
            transaction.set_rollback(True)
            return StandardResponse(success=False, errors=str(e), status=500)
        


class CheckProcessingPOs(APIView):

    @transaction.atomic
    def get(self, request, id=None):
        
        if not id:
            return StandardResponse(success=False, status=400, errors=["purchase order customer reference number required"])

        try:
            purchase_order = (
                PurchaseOrder.objects.select_for_update()
                .prefetch_related("lines")
                .filter(customer_reference_number = id)
                .first()
            )

            if not purchase_order:
                return StandardResponse(success=False, status=400, errors=["Invalid purchase order customer reference number"])
        
            lines = purchase_order.lines.select_for_update().all()
            
            if (
                ConsignmentPOLine.objects.filter(
                    purchase_order_line_id__in=lines.values_list("id", flat=True),
                    consignment__consignment_status = ConsignmentStatusChoices.DRAFT
                ).exists()
            ):
                transaction.set_rollback(True)
                return StandardResponse(status=400, success=False, errors=["Lines for this PO is currently being processed in a consignment."])
            
            return StandardResponse(status=200, success=True, message="PO is ok to use")
        
        except Exception as e:
            return StandardResponse(success=False, status=500, errors=[str(e)])







