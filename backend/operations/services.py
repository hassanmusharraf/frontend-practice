import json
import uuid
import pandas as pd
import os
from django.conf import settings
from pathlib import Path
from django.core.files import File
from decimal import Decimal
from core.response import StandardResponse, ServiceError
from django.db.models import F, Sum, Max, Value, Count, Q, Prefetch, Subquery, OuterRef
from django.db import transaction
from .unit_conversion import calculate_volume, convert_weight, convert_dimension
from .utils import get_allocated_quantity, addresses_and_pickup,parse_any_date
from .models import (ConsignmentPOLineBatch,Consignment,PurchaseOrderLine,PackagingAllocation, PurchaseOrder,
    ConsignmentPackaging, ConsignmentDocumentAttachment, ConsignmentDocument, ConsignmentPOLine, DangerousGoodDocuments,
    ConsignmentAuditTrailField)
from portal.choices import (
    ConsignmentStatusChoices, ConsignmentCreationSteps, ConsignmentDocumentTypeChoices, ConsoleStatusChoices, PackageStatusChoices, ConsignmentDocumentTypeChoices,
    PurchaseOrderStatusChoices
    )
from portal.utils import convert_to_decimal
from entities.models import Supplier, Client, MaterialMaster
from django.utils.timezone import is_aware, make_naive
from django.core.exceptions import ValidationError
from .notifications import NotificationService
from datetime import datetime
from django.db.models.functions import Coalesce
from workflows.models import Console
## Consignment
class ConsignmentWorkflowServices:
    

    @classmethod
    def check_processing_pos(cls, po_ids=None, exclude_consignment_id=None):
        """
        Checks if a purchase order is already in draft processing by another consignment.
        """
        qs = ConsignmentPOLine.objects.filter(
            consignment__consignment_status=ConsignmentStatusChoices.DRAFT,
            purchase_order_line__purchase_order__id__in=po_ids
        )

        # qs = ConsignmentPurchaseOrder.objects.filter(
        #     consignment__consignment_status=ConsignmentStatusChoices.DRAFT,
        #     purchase_order_id__in=po_ids
        # )

        if exclude_consignment_id:
            qs = qs.exclude(consignment_id=exclude_consignment_id)

        return qs.exists()
    

    @classmethod
    def create_draft_consignment(cls,user,supplier_id=None, client_id=None):
        """
        Creates a draft consignment for the given user and purchase orders.
        Args:
            user: The user creating the draft consignment.
            purchase_orders: The purchase orders to include in the consignment.
        returns:
            A tuple containing the created consignment and an error message, if any.
        """
        try:
            Consignment.objects.filter(created_by = user, consignment_status = ConsignmentStatusChoices.DRAFT).delete()

            consignment = Consignment.objects.create(
                supplier_id=supplier_id if supplier_id else None,
                client_id=client_id if client_id else None,
                consignment_status=ConsignmentStatusChoices.DRAFT,
                step = ConsignmentCreationSteps.STEP_1,
                created_by=user,
            )
            
            return consignment, None
        except Exception as e:
            return None, str(e)
    

    @classmethod
    def save_compliance_details(cls, consignment, line, data=None):
        """
        Saves compliance details for the given consignment.
        Args:
            consignment: The consignment instance to update.
            data: The input data containing compliance details.
        Returns:
            A tuple: (compliance_instance or None, error_message or None)
        """

        from .serializers import ConsignmentComplianceSerializer


        try:
            files = data.get("attachments",[])
            document_type = data.get("document_type")
            compliance_dg = True if str(data.get("compliance_dg")).lower() == "true" else False
            compliance_chemical = True if str(data.get("compliance_chemical")).lower() == "true" else False
            is_chemical = False
            errors = ""
            details = POLineService.logistics_flags(line.id,line.purchase_order)

            # check and update hscode and eccn
            if details.get("hs_code_validation") == True and (not data.get("hs_code") or data.get("hs_code").strip() == ""):
                errors += "HS Code is mandatory, "


            if details.get("eccn_validation") == True and (not data.get("eccn") or data.get("eccn").strip() == ""):
                errors += "ECCN is mandatory, "

                
            if details.get("is_dangerous_good") == True or line.is_dangerous_good or compliance_dg == True:
                if data.get("dg_category").strip() == "" or data.get("dg_class").strip() == "" :
                    errors += "Dangerous Good Category and Class are mandatory, "
                    

            if details.get("is_chemical") == True or details.get("chemical_good_handling") or line.is_chemical or compliance_chemical == True:
                batch_data_raw = data.get("batch_data")
                is_chemical = True
                
                if batch_data_raw in [None, ""]:
                    errors += "Chemical data is mandatory, "


            
            if files and not document_type:
                errors += "Document type is mandatory, "

            if errors:
                return None, errors
            
            
            deleted_doc_ids = []
            for key in data:
                if key.startswith("deleted_doc_ids"):
                    deleted_doc_ids.extend(data.get(key))

            
            compliance = ConsignmentPOLine.objects.filter(
                consignment=consignment,
                purchase_order_line=line
            ).first()

            # pass instance into serializer for update, otherwise create
            serializer = ConsignmentComplianceSerializer(
                instance=compliance,
                data=data,
                partial=True
            )

            if not serializer.is_valid():
                return None, serializer.errors
            compliance = serializer.save()

            # --- Handle file deletions ---
            # filters = {"consignment" : consignment, "compliance":compliance}
            # message , error = update_files(deleted_doc_ids=deleted_doc_ids, filters=filters, files=files)
            
            if deleted_doc_ids:
                qs = DangerousGoodDocuments.objects.filter(id__in=deleted_doc_ids)
                # delete files from storage first
                for att in qs:
                    if att.file:
                        att.file.delete(save=False)
                qs.delete()

            files_to_create = []
            # --- Handle new file attachments ---
            if files:
                for f in files:
                    if f:
                        files_to_create.append(DangerousGoodDocuments(consignment_po_line=compliance, file=f))

                if files_to_create:
                    DangerousGoodDocuments.objects.bulk_create(files_to_create)
                        

            if is_chemical:
                _, error = ConsignmentStepHandler.save_batch_details(data)
                if error:
                    return None, error

            return "", None

        except Exception as e:
            return None, str(e)
        except ValidationError as e:
            return {"success": False, "error": str(e)}
    

    @classmethod
    def get_compliance_details(cls, consignment=None, line=None):
        """
        Retrieves compliance details for the given consignment and purchase order line.
        """
        if not consignment or not line:
            return None, "Consignment and line are required"

        compliance_documents = [
            ConsignmentDocumentTypeChoices.DANGEROUS_GOOD,
            ConsignmentDocumentTypeChoices.MSDS,
        ]

        # get the compliance
        compliance = (
            ConsignmentPOLine.objects
            .select_related("dg_class", "dg_category", "consignment")
            .filter(consignment=consignment,purchase_order_line=line).
            first()
        )

        if not compliance:
            return [], None

        # get attachments belonging to this consignment & type
        attachments = (
            DangerousGoodDocuments.objects
            .filter(
                consignment_po_line = compliance,
            )
            .values("id", "file")
        )


        batch_qs = ConsignmentPOLineBatch.objects.filter(
            consignment=consignment,
            purchase_order_line=line,
        ).only("id", "number", "quantity")
        
        # Build response dictionary
        compliance_details = {
            "id": compliance.id,
            "consignment": compliance.consignment_id,
            "purchase_order_line": compliance.purchase_order_line_id,
            "hs_code": compliance.hs_code,
            "eccn": compliance.eccn,
            "country_of_origin": compliance.country_of_origin,
            "compliance_dg" : compliance.compliance_dg,
            "compliance_chemical" : compliance.compliance_chemical,
            "dg_class": {
                "id": compliance.dg_class_id,
                "name": compliance.dg_class.name if compliance.dg_class else None,
            },
            "dg_category": {
                "id": compliance.dg_category_id,
                "name": compliance.dg_category.name if compliance.dg_category else None,
            },
            "dg_note": compliance.dg_note,
            "attachments": attachments,
            "batch_data": list(
                batch_qs.values("id", "number", "quantity","expiry_date")
            ),
        }

        return compliance_details, None


    @classmethod
    def create_consignment_selected_line(cls,consignment,data):
        """
        This is to create and update consignment selected line
        Args:
            consignment: The consignment instance.
            data: The input data containing compliance details.
        returns:
            A tuple containing the updated consignment and an error message, if any.
        """
        try:
            
            if not consignment:
                return None, "Consignment is required"

            unique_line_ids = list(set(data.get("purchase_order_lines", [])))
            po_lines = (
                PurchaseOrderLine.objects
                .filter(id__in=unique_line_ids)
                .only("id")
                .distinct()
            )

            if po_lines.count() != len(unique_line_ids):
                return None, "Invalid Purchase Orders Lines"

            # Replace existing POs with new ones (clear previous associations)
            consignment.purchase_order_lines.set(po_lines)
            consignment.supplier = po_lines.first().purchase_order.supplier
            consignment.save(update_fields=["supplier"])
            
            return True, None
        
        except Exception as e:
            transaction.set_rollback(True)
            return None, str(e)        


    @classmethod
    def save_batch_details(cls, data=None):
        """
        Saves batch details for the given consignment and purchase order line.
        Args:
            data: The input data containing batch details.
        returns:
            A tuple containing the updated consignment and an error message, if any.
        """
        from .serializers import ConsignmentBatchSerializer

        try:

            existing_data =ConsignmentPOLineBatch.objects.filter(consignment = data.get("consignment"),purchase_order_line = data.get("purchase_order_line"))
            existing_data.delete()

            batch_data_raw = data.get("batch_data")
            if batch_data_raw:
                batch_data = json.loads(batch_data_raw)
                data_to_create = []
                # data["batch_data"] = batch_data
                for batch in batch_data:

                    if batch.get("number") in [None, ""] or batch.get("quantity") in [None, ""] or batch.get("expiry_date") in [None, ""]:
                        return None, "Batch details are mandatory"
                    
                    item = {
                            "consignment": data.get("consignment"),
                            "purchase_order_line": data.get("purchase_order_line"),
                            "number": batch.get("number"),
                            "quantity": batch.get("quantity"),
                            "expiry_date": batch.get("expiry_date")
                        }
                    
                    data_to_create.append(item)

                if not data_to_create:
                    return None, "Batch data is required"
                
                serializer = ConsignmentBatchSerializer(
                    data=data_to_create,
                    many=True
                )

                if not serializer.is_valid():
                    return None, serializer.errors
                batch_data = serializer.save()

                return batch_data, None
            

            return True, None
        except Exception as e:
            return None, str(e)
        

    @staticmethod
    def get_last_number(prefix, consignment=None):
        filters = {"draft_package_id__startswith": prefix}
        if consignment:
            filters["consignment"] = consignment

        latest_draft_id = (
            ConsignmentPackaging.objects
            .filter(**filters)
            .aggregate(max_id=Max("draft_package_id"))
            .get("max_id")
        )

        if latest_draft_id:
            try:
                return int(latest_draft_id[len(prefix):])
            except ValueError:
                return 0
        return 0


    @classmethod
    def create_packages(cls, consignment, packaging_type, data):
        """
        Create one or more ConsignmentPackaging records in bulk.
        Args:
            consignment: The consignment instance.
            packaging_type: The PackagingType instance.
            data: The input data containing package details.
        Returns:
            A tuple containing a success message and None.
        """

        prefix = "DRAFT"
        weight = convert_to_decimal(data.get("weight")) if data.get("weight") else None
        weight_unit = data.get("weight_unit") or None
        quantity = int(data.get("quantity") or 1)

        if quantity <= 0:
            raise ValueError("Quantity must be a positive integer.")

        last_number = cls.get_last_number(prefix, consignment)

        packages = []
        for i in range(1, quantity + 1):
            draft_id = f"{prefix}{last_number + i}"
            packages.append(
                ConsignmentPackaging(
                    consignment=consignment,
                    draft_package_id=draft_id,
                    packaging_type=packaging_type,
                    weight=weight,
                    weight_unit=weight_unit,
                    status = PackageStatusChoices.DRAFT,
                )
            )

        ConsignmentPackaging.objects.bulk_create(packages)

        return "Packages created successfully", None


    ##Not in use
    @classmethod
    def is_valid_line_to_pack(cls, po_line, package):
        """
        Validates that all packages maintain consistency in dangerous goods (DG) status
        and order type (BTS/BTO) when allocating a purchase order line.
        Args:
            po_line: The purchase order line instance being allocated.
            packages: The list of packages to which the line is being allocated.
        Returns:
            A tuple containing a list of packages to update and an error message, if any.
        """

        # Fetch existing allocations for the package
        existing_allocation = PackagingAllocation.objects.filter(
            consignment_packaging=package
        ).select_related("purchase_order_line").first() 

        if existing_allocation:
            # Determine existing flags
            existing_flags = {
                "is_dg": None,
                "order_type": None
            }


            flags = POLineService.logistics_flags(existing_allocation.purchase_order_line.id, existing_allocation.purchase_order_line.purchase_order)
            if existing_flags["is_dg"] is None:
                existing_flags["is_dg"] = flags["is_dangerous_good"]
            elif existing_flags["is_dg"] != flags["is_dangerous_good"]:
                return None, "Package contains mixed DG and Non-DG lines. Please reallocate."

            if existing_flags["order_type"] is None:
                existing_flags["order_type"] = flags["order_type"]
            elif existing_flags["order_type"] != flags["order_type"]:
                return None, f"Package contains mixed {existing_flags['order_type']} and {flags['order_type']} lines. Please reallocate."

            # Now check the incoming line
            incoming_flags = POLineService.logistics_flags(po_line.id, po_line.purchase_order)

            # Enforce DG consistency
            if existing_flags["is_dg"] is not None and existing_flags["is_dg"] != incoming_flags["is_dangerous_good"]:
                return None, "This package already contains DG/Non-DG lines. Mixing is not allowed."

            # Enforce BTS/BTO consistency
            if existing_flags["order_type"] is not None and existing_flags["order_type"] != incoming_flags["order_type"]:
                return None, f"This package already contains {existing_flags['order_type']} lines. Mixing with {incoming_flags['order_type']} is not allowed."

            # If package is empty, set its order_type
            # if not existing_allocation and not package.order_type:
            #     package.order_type = incoming_flags["order_type"]
            #     update_packages.append(package)

        return True, None


    @classmethod
    def pack_po_line(cls, consignment , po_line, packages, data = None):
        """
        This method packs purchase order line
        Args:
            consignment: The consignment instance.
            po_line: The purchase order line instance.
            packages: The list of packages to be packed.
            data: The input data containing package details.
        Returns:
            A tuple containing the updated consignment and an error message, if any.
        """
        try:
            new_allocations = []
            updated_allocations = []
            remove_allocations = []

            # Pre-fetch all consignment packages once
            consignment_packages = {
                p.package_id: p for p in ConsignmentPackaging.objects.filter(consignment=consignment)
            }
            draft_packages = {
                p.draft_package_id: p for p in ConsignmentPackaging.objects.filter(consignment=consignment)
            }


            for pkg in packages:
                draft_package_id = pkg.get("draft_package_id")
                allocated_qty = pkg.get("allocated_qty")
                package_id = pkg.get("package_id",None)

                if not draft_package_id and not package_id:
                    return None, "draft_package_id or package_id required"
            
                if allocated_qty is None:
                    return None, "allocated quantity required"

                package = consignment_packages.get(package_id) if package_id else draft_packages.get(draft_package_id)
                if not package:
                    return None, f"Invalid Package ID: {draft_package_id or package_id}"

                # Check if allocation already exists
                allocations = (
                    PackagingAllocation.objects
                    .filter(consignment_packaging__consignment=consignment)
                    .select_related("purchase_order_line")
                    .only("purchase_order_line")
                )
                
                package_allocation = allocations.filter(consignment_packaging=package).first()
                
                if package_allocation:

                    ## Get dg and oder_type for old
                    pa_flags = POLineService.logistics_flags(package_allocation.purchase_order_line.id, package_allocation.purchase_order_line.purchase_order)
                    pa_dg = pa_flags.get("is_dangerous_good", package_allocation.purchase_order_line.is_dangerous_good)            
                    pa_bt = package_allocation.purchase_order_line.purchase_order.order_type
                    
                    current_line_flags = POLineService.logistics_flags(package_allocation.purchase_order_line.id, package_allocation.purchase_order_line.purchase_order)
                    current_line_dg = current_line_flags.get("is_dangerous_good", po_line.is_dangerous_good)
                    current_line_bt = po_line.purchase_order.order_type
                    if pa_dg != current_line_dg:
                        return None, f"This package contains {'Dangerous Goods' if pa_dg else 'Non-Dangerous Goods'}. Please re-allocate to a different package."
                    if pa_bt != current_line_bt:
                        return None, f"This package contains {pa_bt} order. Please re-allocate to a different package."
                        

                existing_allocation = allocations.filter(consignment_packaging=package, purchase_order_line=po_line).first()
                
                if existing_allocation:
                    # Remove allocation if qty < 0
                    if Decimal(allocated_qty) <= 0:
                        remove_allocations.append(existing_allocation)
                        continue

                    existing_allocation.allocated_qty = allocated_qty
                    updated_allocations.append(existing_allocation)

                else:
                    if Decimal(allocated_qty) > 0:
                        new_allocations.append(PackagingAllocation(
                            consignment_packaging=package,
                            purchase_order_line=po_line,
                            allocated_qty=allocated_qty
                        ))

            # Apply DB changes
            if new_allocations:
                PackagingAllocation.objects.bulk_create(new_allocations)

            if updated_allocations:
                PackagingAllocation.objects.bulk_update(updated_allocations, ["allocated_qty"])

            if remove_allocations:
                PackagingAllocation.objects.filter(id__in=[a.id for a in remove_allocations]).delete()

            return "Packages packed successfully", None
        except Exception as e:
            return "", str(e)


    @staticmethod
    def generate_package_id():
        # Get all AR3 IDs
        ids = (
            ConsignmentPackaging.objects
            .filter(package_id__startswith="AR3")
            .values_list("package_id", flat=True)
        )

        # Extract numeric part
        numbers = [int(i[3:]) for i in ids if i and i[3:].isdigit()]

        # Find max
        latest_number = max(numbers) if numbers else 30000 - 1  # Start from AR30000
        return latest_number + 1


    @classmethod
    def update_packages_id(cls, consignment):
        try:
            # Only non-draft packages with package_id starting with AR3 or null
            packages = ConsignmentPackaging.objects.filter(
                consignment=consignment
            ).only("id", "package_id","status")

            if not packages:
                return None, "Minimum one package is required"
            
            draft_packages = packages.filter(status=PackageStatusChoices.DRAFT)
            

            next_id = cls.generate_package_id()

            for package in draft_packages:
                package.package_id = f"AR3{next_id:05d}"  # zero-padded
                package.status = PackageStatusChoices.NOT_RECEIVED
                next_id += 1

            ConsignmentPackaging.objects.bulk_update(draft_packages, ["package_id", "status"])

            return "Packages updated successfully", None

        except Exception as e:
            return "", str(e)


    @classmethod
    def update_po_lines_quantity(cls, consignment):
        
        """
        Increases processed_quantity by allocated_qty and decreases open_quantity,
        ensuring processed_quantity ≤ quantity and open_quantity ≥ 0.
        """
        
        try:
            lines = consignment.purchase_order_lines.all()

            POLineService.update_line_quantities(lines)
            
            # allocations = (
            #     PackagingAllocation.objects
            #     .filter(consignment_packaging__consignment=consignment)
            #     .values("purchase_order_line")
            #     .annotate(total_allocated=Sum("allocated_qty"))
            # )

            # for alloc in allocations:
            #     po_line_id = alloc["purchase_order_line"]
            #     total_allocated = alloc["total_allocated"]

            #     # Update in one SQL statement per PO line
            #     PurchaseOrderLine.objects.filter(id=po_line_id).update(
            #         processed_quantity=Least(
            #             F("quantity"),  # cap at quantity
            #             Greatest(Value(0), F("processed_quantity") + total_allocated)  # no less than 0
            #         ),
            #         open_quantity=Greatest(
            #             Value(0),  # at least 0
            #             F("open_quantity") - total_allocated
            #         )
            #     )

            return "PO lines updated successfully", None
        
        except Exception as e:
            return "", str(e)
           

    @classmethod
    def delete_unallocated_packages(cls, consignment):
        try:
            # Delete allocations with zero or negative quantity
            PackagingAllocation.objects.filter(consignment_packaging__consignment=consignment, allocated_qty__lte=0).delete()
            
            packagings = (
                ConsignmentPackaging.objects
                .prefetch_related("allocations")
                .filter(consignment=consignment)
                .annotate(total_allocated=Count('allocations', distinct=True))
                .order_by("total_allocated")
            )

            packagings_to_delete = packagings.filter(total_allocated__lte=0)
            packagings_to_delete.delete()

            return "Unallocated packages deleted successfully", None
        except Exception as e:
            return "", str(e)


    @staticmethod
    def has_mandatory_consignment_files(consignment):
        
        required_docs = [
            str(ConsignmentDocumentTypeChoices.COMMERCIAL_INVOICE).lower(),
            str(ConsignmentDocumentTypeChoices.PACKING_LIST).lower()
        ]

        
        uploaded_docs = list(
            ConsignmentDocumentAttachment.objects.filter(
                document__consignment=consignment
            ).values_list("document__document_type", flat=True)
        )

        uploaded_docs = [doc.lower() for doc in uploaded_docs]

        for type in required_docs:
            if type not in uploaded_docs:
                return False
            
        return True
        # if required_docs not in uploaded_docs:
        #     return False
        # return True
        # return required_docs.issubset(uploaded_docs)
  

    @staticmethod
    def consignment_create_and_update_cleanups(consignment):
        try:
            res = ConsignmentWorkflowServices.has_mandatory_consignment_files(consignment)
            if not res:
                return None, "COMMERCIAL INVOICE And PACKING LIST documents are required"
            
            _, errors = ConsignmentWorkflowServices.delete_unallocated_packages(consignment)
            if errors:
                return "", errors

            _, error = ConsignmentWorkflowServices.update_packages_id(consignment)
            if error:
                return "", error

            ## This will used only when consignment is creating not for update
            _, error = ConsignmentWorkflowServices.update_po_lines_quantity(consignment)
            if error:
                return "", error
            
            return "All OK", None
        
        except ServiceError as e:
            return "", str(e)

        except Exception as e:
            return "", str(e)
    
    
    @classmethod
    def create_update_consignment(cls, consignment, user=None):
        try:
            
            _,error=ConsignmentWorkflowServices.consignment_create_and_update_cleanups(consignment)
            if error:
                return None, error
            
            old_status = consignment.consignment_status

            consignment.consignment_status = ConsignmentStatusChoices.PENDING_FOR_APPROVAL
            consignment.save()
            
            if old_status == ConsignmentStatusChoices.DRAFT:

                NotificationService.notify_consignment_created(instance=consignment,user=user)
                return "Consignment created successfully", None

            return "Consignment updated successfully", None
        
        except Exception as e:
            return "", str(e)

    
    @classmethod
    def get_attachments(cls,consignment,filters={}):
        try:
            filters["consignment"] = consignment 
            documents = ConsignmentDocument.objects.filter(**filters).prefetch_related("attachments")

            data = [
                {
                    "id": doc.id,
                    "document_type": doc.document_type,
                    "attachments": [
                        {"id": att.id, "file": att.file.url}
                        for att in doc.attachments.all()
                    ],
                }
                for doc in documents
            ]

            return data, None
        except Exception as e:
            return "", str(e)


    @classmethod
    def get_counts(cls, consignment_id):
        if not consignment_id:
            return {}, "Consignment ID is required"

        consignment_po_line = ConsignmentPOLine.objects.filter(consignment__consignment_id=consignment_id).select_related("consignment")
        consignment = consignment_po_line.first().consignment

        if not consignment:
            return {}, "Invalid Consignment ID"

        po_lines = consignment.purchase_order_lines.all().select_related("purcahse_order")
        packages = consignment.packagings.all()
        packages_count = packages.count()

        # Purchase orders count
        po_count = po_lines.values("purchase_order_id").distinct().count()

        # PO lines queryset reused for all counts
        # po_lines_qs = PurchaseOrderLine.objects.filter(
        #     packaging_allocations__consignment_packaging__consignment_id=consignment.id
        # ).distinct()

        po_lines_count = po_lines.count()
        dg_lines_count = consignment_po_line.filter(compliance_dg=True).distinct().count()
        chemical_goods_count = consignment_po_line.filter(compliance_chemical=True).distinct().count()

        total_weight_kg = 0
        total_volume_m3 = 0
        
        for pkg in packages.select_related("packaging_type"):
            package_type = pkg.packaging_type

            total_weight_kg += convert_weight(pkg.weight, pkg.weight_unit, "Kilogram")
            total_volume_m3 += calculate_volume(
                package_type.length, package_type.width, package_type.height, package_type.dimension_unit
            )

        counts = {
            "purchase_orders": po_count,
            "po_lines": po_lines_count,
            "packages": packages_count,
            "dg_lines": dg_lines_count,
            "chemical_goods": chemical_goods_count,
            "weight": {
                "value": 0 if total_weight_kg < 1 else float(total_weight_kg),
                "unit": "Kilogram"
            },
            "volume": {
                "value": 0 if total_volume_m3 < 1 else round(float(total_volume_m3), 2),
                "unit": "Cubic Meter"
            },
            "consignment_id" : consignment.consignment_id,
            "consignment_status" : consignment.consignment_status
        }

        return counts, None


    @classmethod
    def dg_item_details(cls, consignment):
        try:
            details = (ConsignmentPOLine.objects
                .filter(Q(consignment=consignment) & (Q(compliance_dg = True) ))
                .values("id","purchase_order_line")
                .annotate(
                    sku = F('purchase_order_line__product_code'),
                    description = F('purchase_order_line__description'),
                    un_class = F('dg_class__name'),
                    un_category = F('dg_category__name'),
                    dg_notes = F('dg_note'),
                )
            )
            
            if details:
                for detail in details:
                    filters={"compliance_id" : detail["id"]},
                    # attachments, errors = cls.get_attachments(consignment, filters = filters)
                    # if errors:
                    #     return "", errors
                    
                    attachments = []
                    detail["attachments"] = attachments if attachments else []

            return details, None
        
        except Exception as e:
            return "", str(e)


    @classmethod
    def get_consignment_summary(cls, consignment):

        try:
            
            # Get Counts
            counts, errors = cls.get_counts(consignment)
            if errors:
                return "", errors
            
            packages = consignment.packagings.filter(draft_package_id__isnull=False)
            
            # Calculate total weight (convert all to kg for consistency)
            total_weight_kg = Decimal('0.0')
            total_volume_m3 = Decimal('0.0')
            packages_details = []

            for package in packages:

                # Calculate weight
                # total_weight_kg += package.weight / Decimal('1000') if package.weight else Decimal('0.0')
                total_weight_kg += convert_weight(package.weight, package.weight_unit, 'Kilogram')

                # Calculate volume
                packaging_type = package.packaging_type
                volume = calculate_volume(
                    packaging_type.length if packaging_type else None,
                    packaging_type.width if packaging_type else None,
                    packaging_type.height if packaging_type else None,
                    packaging_type.dimension_unit if packaging_type else None
                )
                total_volume_m3 += volume

                # Get po lines for the package
                po_lines = (PurchaseOrderLine.objects
                    .filter(
                        packaging_allocations__consignment_packaging=package,
                        packaging_allocations__consignment_packaging__draft_package_id__isnull = False
                    )
                    .distinct()
                    .values("id","customer_reference_number","reference_number")
                )
                
                package = {
                    "draft_package_id": package.draft_package_id,
                    "package_id": package.package_id,
                    "weight": package.weight,
                    "weight_unit": package.weight_unit,
                    "packaging_type": package.packaging_type.package_name if package.packaging_type else None,
                    "volume": volume,
                    "length": package.packaging_type.length if package.packaging_type else None,
                    "width": package.packaging_type.width if package.packaging_type else None,
                    "height": package.packaging_type.height if package.packaging_type else None,
                    "dimension_unit": package.packaging_type.dimension_unit if package.packaging_type else None,
                    "is_stackable": package.packaging_type.is_stackable if package.packaging_type else None,
                    "is_kit" : True ,
                    "po_lines": po_lines
                }
                packages_details.append(package)

            counts["weight"] = {
                "value": 0 if float(total_weight_kg) < 1 else float(total_weight_kg),
                "unit": "Kilogram"
            }
            counts["volume"] = {
                "value": 0 if float(total_volume_m3) < 1 else round(float(total_volume_m3), 2),
                "unit": "Cubic Meter"
            }

            dg_items, errors = cls.dg_item_details(consignment)
            if errors:
                return "", errors
            
            addresses, errors = addresses_and_pickup(consignment.consignment_id)
            if errors:
                return "", errors
            

            result = {
                "counts": counts,
                "packages": packages_details,
                "dg_items": dg_items,
                "addresses_and_pickup" : addresses
            }
            
            return result, None
        except Exception as e:
            return "", str(e)


    @classmethod
    def get_consignment_packages(cls, consignment_id):
        """
        Get packages for a given consignment with allocated quantities and compliance info.
        """

        consignment = Consignment.objects.filter(consignment_id=consignment_id).only("id").first()
        if not consignment:
            return StandardResponse(success=False, errors=["Invalid Consignment ID"], status=400)

        # Fetch packages with packaging type info
        packages_qs = (
            ConsignmentPackaging.objects
            .select_related("packaging_type")
            .filter(consignment=consignment)
            .order_by("created_at")
        )

        packages = list(packages_qs.values(
            "id", "draft_package_id", "package_id", "weight", "weight_unit", "is_kit","status"
        ).annotate(
            is_stackable=F("packaging_type__is_stackable"),
            length=F("packaging_type__length"),
            width=F("packaging_type__width"),
            height=F("packaging_type__height"),
            dimension_unit=F("packaging_type__dimension_unit"),
            packaging_type=F("packaging_type__package_type")
        ))

        package_ids = [pkg["id"] for pkg in packages]

        allocations = (
            PackagingAllocation.objects
            .filter(consignment_packaging_id__in=package_ids)
            .select_related("purchase_order_line__purchase_order")
            .values(
                "id",
                "consignment_packaging_id",
                "purchase_order_line_id",
                "allocated_qty",
                "purchase_order_line__customer_reference_number",
                "purchase_order_line__product_code",
                "purchase_order_line__description",
                "purchase_order_line__sku"
            )
            .annotate(
                purchase_order_id=F("purchase_order_line__purchase_order"),
                purchase_order_customer_reference_number=F("purchase_order_line__purchase_order__customer_reference_number")
            )
        )

        po_line_ids = {alloc["purchase_order_line_id"] for alloc in allocations}
        compliance_map = {
            comp.purchase_order_line_id: {
                "compliance_dg": comp.compliance_dg,
                "compliance_chemical" : comp.compliance_chemical,
                "hs_code": comp.hs_code,
                "country_of_origin": comp.country_of_origin,
                "dg_class": comp.dg_class.name if comp.dg_class else None,
                "dg_category": comp.dg_category.name if comp.dg_category else None
            }
            for comp in ConsignmentPOLine.objects.select_related("dg_class", "dg_category").filter(
                purchase_order_line_id__in=po_line_ids, consignment = consignment
            )
        }

        # Build allocation map
        allocation_map = {}
        for alloc in allocations:
            pkg_id = alloc["consignment_packaging_id"]
            po_id = alloc["purchase_order_line_id"]
            compliance = compliance_map.get(po_id, {})

            allocation_data = {
                "allocation_id": alloc["id"],
                "purchase_order_line_id": po_id,
                "purchase_order_id": alloc["purchase_order_id"],
                "purchase_order_customer_reference_number": alloc["purchase_order_customer_reference_number"],
                "customer_reference_number": alloc["purchase_order_line__customer_reference_number"],
                "sku": alloc["purchase_order_line__sku"],
                "description": alloc["purchase_order_line__description"],
                "allocated_qty": alloc["allocated_qty"],
                "product_code": alloc["purchase_order_line__product_code"],
                **compliance
            }

            allocation_map.setdefault(pkg_id, []).append(allocation_data)

        for pkg in packages:
            pkg["volume"] = calculate_volume(
                pkg.get("length", ""),
                pkg.get("width", ""),
                pkg.get("height", ""),
                pkg.get("dimension_unit", "")
            )

            pkg["allocations"] = allocation_map.get(pkg["id"], [])

        return packages, None


    @classmethod
    def consignment_hover_details(cls, consignment_id):
        
        """
        Get DG item details for a given consignment.
        Args:
            consignment_id (int): The ID of the consignment.
        Returns:
            A list of DG item objects, or an error message if any.
        """
        consignment = (
            Consignment.objects
            .select_related(
                "console",
                "supplier"
            )
            .prefetch_related(
                "packagings",
            )
            .filter(consignment_id=consignment_id)
            .first()
        )        
        
        if not consignment:
            return None, None

        purchase_orders = consignment.purchase_order_lines.all().values_list("purchase_order__customer_reference_number", flat=True).distinct()
        packages_qs = consignment.packagings.all()
        packages_count = packages_qs.count()

        total_weight = 0
        total_volume = 0
        for package in packages_qs:

            total_weight += convert_weight(
                package.weight,
                package.weight_unit
            )

            total_volume += calculate_volume(
                package.packaging_type.length if package.packaging_type else None,
                package.packaging_type.width if package.packaging_type else None,
                package.packaging_type.height if package.packaging_type else None,
                package.packaging_type.dimension_unit if package.packaging_type else None
            )

        address, errors = addresses_and_pickup(consignment_id=consignment_id)
        if errors:
            return StandardResponse(status=400, success=False, errors=errors)
        
        console = getattr(consignment.console, "console_id", None)
        freight_forwarder = getattr(consignment.console.freight_forwarder, "name", None) if consignment.console else None
        
        storerkeys = (
            PurchaseOrder.objects
            .filter(customer_reference_number__in=purchase_orders)
            .annotate(code=F('storerkey__storerkey_code'), name=F('storerkey__name'),hub=F('storerkey__hub__hub_code'))
            .values("name","code","hub")
            .distinct()
        )

        # storerkeys = consignment.supplier.storerkeys.all().values_list("name", flat=True).distinct(), None
        
        return {
            
            "consignment_id": consignment.consignment_id,
            "consignment_status" : consignment.consignment_status,
            "console": console,
            "freight_forwarder": freight_forwarder,
            "hub" : [sk["hub"] for sk in storerkeys],
            "purchase_orders": purchase_orders,
            "packages_count": packages_count,
            "total_weight": total_weight,
            "total_volume": total_volume,
            "storerkeys" : [sk["code"] for sk in storerkeys],
            "address" : address,
        }, None


    @classmethod
    def remove_orphan_allocations(cls, consignment):
        """
        Remove allocations for PO lines that are no longer linked to this consignment.
        - If consignment is in DRAFT: delete allocations directly.
        - If consignment is PENDING/APPROVAL (or later): restore allocated qty 
        to open qty before deleting allocations.
        """

        po_lines = consignment.purchase_order_lines.values_list("id", flat=True)

        orphan_allocations = PackagingAllocation.objects.filter(
            consignment_packaging__consignment=consignment
        ).exclude(purchase_order_line__in=po_lines)

        if not orphan_allocations.exists():
            return  # nothing to clean up

        if consignment.consignment_status == ConsignmentStatusChoices.DRAFT:
            orphan_allocations.delete()
            return

        #Aggregate allocated qty by line
        restore_qty = (
            orphan_allocations
            .values("purchase_order_line_id")
            .annotate(total_allocated=Sum("allocated_qty"))
        )

        #Update open_quantity in bulk
        for row in restore_qty:
            line_id = row["purchase_order_line_id"]
            qty_to_restore = row["total_allocated"] or 0
            if qty_to_restore > 0:
                PurchaseOrderLine.objects.filter(id=line_id).update(
                    open_quantity=F("open_quantity") + qty_to_restore,
                    processed_quantity=F("processed_quantity") - qty_to_restore
                )

        #Delete orphan allocations afterwards
        orphan_allocations.delete()



class ConsignmentStepHandler(ConsignmentWorkflowServices):

    
    @staticmethod
    def step_3_validations(consignment):
        """
        Validates:
        1. All PO lines have packaging allocations.
        2. All PO lines have compliance records.
        3. No package contains multiple DG classes.
        """
        
        # prefetch allocations, compliances, and batches in one go
        lines = (
            consignment.purchase_order_lines
            .only("id", "customer_reference_number", "reference_number")
            .prefetch_related(
                Prefetch(
                    "packaging_allocations",
                    queryset=PackagingAllocation.objects.filter(
                        consignment_packaging__consignment=consignment
                    ),
                    to_attr="cached_allocations",
                ),
                Prefetch(
                    "consignments_po_lines",
                    queryset=ConsignmentPOLine.objects.filter(
                        consignment=consignment, country_of_origin__isnull=False
                    ),
                    to_attr="cached_compliances",
                ),
            )
        )

        not_packed_lines = [
            line.customer_reference_number
            for line in lines if not line.cached_allocations
        ]

        compliance_not_found = [
            line.customer_reference_number
            for line in lines if not line.cached_compliances
        ]

        if not_packed_lines:
            return None, (
                "Not all PO lines are packed. Please pack the following lines: "
                + ", ".join(not_packed_lines)
            )

        if compliance_not_found:
            return None, (
                "Compliance not added for the following PO lines: "
                + ", ".join(compliance_not_found)
            )
        
        
        packages = (
            ConsignmentPackaging.objects
            .filter(consignment=consignment)
            .prefetch_related(
                Prefetch(
                    "allocations",
                    queryset=PackagingAllocation.objects.select_related("purchase_order_line"),
                    to_attr="cached_allocations",
                )
            )
        )

        po_line_ids = [
            alloc.purchase_order_line_id
            for pkg in packages for alloc in pkg.cached_allocations
        ]

        dg_classes_by_line = {
            line.purchase_order_line_id: line.dg_class
            for line in ConsignmentPOLine.objects.filter(
                consignment=consignment,
                purchase_order_line_id__in=po_line_ids,
                compliance_dg=True,
            ).only("purchase_order_line_id", "dg_class")
        }

        for package in packages:
            unique_dg_classes = {
                dg_classes_by_line.get(alloc.purchase_order_line_id)
                for alloc in package.cached_allocations
                if dg_classes_by_line.get(alloc.purchase_order_line_id)
            }
            if len(unique_dg_classes) > 1:
                return None, (
                    f"Items belonging to two different DG classes cannot be packed together."
                )

        return True, None

        # for line in lines:
        #     # allocations except PO_LINES are already in memory because of prefetch_related
        #     allocation_exists = line.packaging_allocations.filter(consignment_packaging__consignment = consignment)

        #     compliances_exists = line.consignments_po_lines.filter(consignment = consignment, country_of_origin__isnull=False)

        #     if not allocation_exists:
        #         not_packed_lines.append(line.customer_reference_number)

        #     if not compliances_exists:
        #         compliance_not_found.append(line.customer_reference_number)

        # if not_packed_lines:
        #     return None, (
        #         "Not all PO lines are packed. Please pack the following lines: "
        #         + ", ".join(not_packed_lines)
        #     )

        # if compliance_not_found:
        #     return None, (
        #         "Compliance not added for the following PO lines: "
        #         + ", ".join(compliance_not_found)
        #     )
        
        # packages = ConsignmentPackaging.objects.filter(consignment=consignment)

        # for package in packages:
        #     allocations = package.allocations.select_related("purchase_order_line").all()
        #     dg_classes = ConsignmentPOLine.objects.filter(consignment=consignment, purchase_order_line__in=[alloc.purchase_order_line for alloc in allocations],compliance_dg = True).only("dg_class").distinct()

        #     unique_dg_classes = set(dg.dg_class for dg in dg_classes if dg.dg_class)
        #     if len(unique_dg_classes) > 1:
        #         return None, f"Package {package.draft_package_id} contains multiple DG classes. Please reallocate."

        # return True, None


    @classmethod
    def handle_step_1(cls, consignment, user=None, data=None):
        """
        Handles the first step of consignment creation/edit.
        """
        po_crns = data.get("purchase_orders", [])
        supplier_id = data.get("supplier_id",None)
        client_id = data.get("client_id",None)
        MAX_PO_PER_CONSIGNMENT = 10

        if not po_crns:
            return None, "Purchase Order not found."

        # Validate uniqueness
        unique_po_crns = list(set(po_crns))
        if len(unique_po_crns) != len(po_crns):
            return None, "Duplicate Purchase Orders are not allowed."

        # Fetch POs
        purchase_orders = PurchaseOrder.objects.filter(
            customer_reference_number__in=po_crns
        ).only("id")

        if purchase_orders.count() != len(po_crns):
            return None, "One or more Purchase Orders were not found."

        if purchase_orders.count() > MAX_PO_PER_CONSIGNMENT:
            return None, f"Maximum {MAX_PO_PER_CONSIGNMENT} Purchase Orders allowed per consignment."

        # Check if POs are already in use in other consignments
        blocked = ConsignmentPOLine.objects.filter(
            purchase_order_line__purchase_order__in=purchase_orders, 
            consignment__consignment_status=ConsignmentStatusChoices.DRAFT
        ).exclude(consignment=consignment if consignment else None)

        if blocked.exists():
            return None, "One or more Purchase Orders are already being processed in another consignment."

        # Validate supplier & client
        if supplier_id and client_id:
            supplier = Supplier.objects.filter(id=supplier_id).only("id").first()
            client = Client.objects.filter(id=client_id).only("id").first()
            if not supplier or not client:
                return None, "Invalid Supplier or Client."

            Consignment.objects.filter(id = consignment.id).update(supplier = supplier, client = client)

        # # Create or reuse consignment
        # if not consignment.su:
        #     consignment, error = cls.create_draft_consignment(
        #         user, supplier_id=supplier.id, client_id=client.id
        #     )
        #     if error:
        #         return None, error

        return consignment, None


    @classmethod
    def handle_step_2(cls, consignment, user=None, data = None):
        """
        Handles the second step of the consignment process.
        Args: 
            consignment: The consignment instance to update.
            user: The user initiating the consignment process.
            data: The input data for the consignment process.
        returns:
            A tuple containing the updated consignment and an error message, if any.
        """

        # Example: Update consignment details based on provided data
        if not consignment:
            return None, "Invalid consignment id."

        try:
            ## uncomment this code while start calling the Post handle step 2 API
            ## also remove remove_orphan_allocations calling this into consignment-lines
            # if consignment.step in [ConsignmentCreationSteps.STEP_2 ,ConsignmentCreationSteps.STEP_3, ConsignmentCreationSteps.STEP_4, ConsignmentCreationSteps.STEP_5]:
            #     cls.remove_orphan_allocations(consignment)
            
            if consignment.step == ConsignmentCreationSteps.STEP_1:
                Consignment.objects.filter(pk=consignment.pk).update(
                    step=ConsignmentCreationSteps.STEP_2
                )
            
            return consignment, None
        except Exception as e:
            return None, str(e)


    @classmethod
    def handle_step_3(cls, consignment, user=None, data = None):
        """
        Handles the third step of the consignment process.
        Args:
            consignment: The consignment instance to update.
            data: The input data for the consignment process.
        returns:
            A tuple containing the updated consignment and an error message, if any.
        """
        if not consignment:
            return None, "Invalid consignment id."


        _ , errors = cls.step_3_validations(consignment)
        if errors:
            return None, errors
        
        packages = data.get("packages", [])

        # Fetch all objects first by their IDs
        # package_ids = [pkg.get("id") for pkg in packages if pkg.get("id")]
        existing_packages = ConsignmentPackaging.objects.filter(consignment=consignment)
        existing_packages_dict = {p.id: p for p in existing_packages}

        packages_to_update = []
        for package in packages:
            pkg_id = uuid.UUID(package.get("id"))
            obj = existing_packages_dict.get(pkg_id)
            if obj:  # Found the object
                obj.weight = package.get("weight")
                obj.weight_unit = package.get("weight_unit")
                obj.is_kit = package.get("is_kit")
                packages_to_update.append(obj)

        if packages_to_update:
            ConsignmentPackaging.objects.bulk_update(packages_to_update, ["weight", "weight_unit","is_kit"])

        # Update consignment step
        try:
            if consignment.step == ConsignmentCreationSteps.STEP_2:
                Consignment.objects.filter(pk=consignment.pk).update(
                    step=ConsignmentCreationSteps.STEP_3
                )
 
            return consignment, None            
        except Exception as e:
            return None, str(e)
        

    @classmethod
    def handle_step_4(cls, consignment, user=None, data = None):
        """
        Handles the third step of the consignment process.
        Args:
            consignment: The consignment instance to update.
            data: The input data for the consignment process.
        returns:
            A tuple containing the updated consignment and an error message, if any.
        """
        if not consignment:
            return None, "Invalid consignment id."

        # Example: Update consignment details based on provided data
        try:
            if consignment.step == ConsignmentCreationSteps.STEP_3:
                Consignment.objects.filter(pk=consignment.pk).update(
                    step=ConsignmentCreationSteps.STEP_4
                )
            return consignment, None
        except Exception as e:
            return None, str(e)
        

    @classmethod
    def handle_step_5(cls, consignment, user=None, data = None):
        """
        Handles the third step of the consignment process.
        Args:
            consignment: The consignment instance to update.
            data: The input data for the consignment process.
        returns:
            A tuple containing the updated consignment and an error message, if any.
        """

        if not consignment:
            return None, "Invalid consignment id."

        # Example: Update consignment details based on provided data
        try:
            if consignment.step == ConsignmentCreationSteps.STEP_4:
                Consignment.objects.filter(pk=consignment.pk).update(
                    step=ConsignmentCreationSteps.STEP_5
                )
            return consignment, None
        except Exception as e:
            return None, str(e)
        

    @classmethod
    def data_get_step_1(cls, consignment, user=None, data = None):

        # Get related purchase orders in one query
        if not consignment:
            return None, "Invalid consignment id."

        lines = consignment.purchase_order_lines.select_related("purchase_order")
        pos = (
            lines
            .values(
                "purchase_order__supplier__id",
                "purchase_order__supplier__name",
                "purchase_order__supplier__supplier_code",
                "purchase_order__supplier__client__id",
                "purchase_order__supplier__client__name",
                "purchase_order__storerkey__id",
                "purchase_order__storerkey__name",
                "purchase_order__storerkey__storerkey_code",
                "purchase_order__storerkey__measurement_method",
            )
            .annotate(
                po_id = F("purchase_order__id"),
                po_customer_reference_number = F("purchase_order__customer_reference_number")
            )
            .distinct()
        )
        
        if not pos:
            return [], None

        first_po = pos[0]
        supplier_detail = {
            "id": first_po["purchase_order__supplier__id"],
            "name": first_po["purchase_order__supplier__name"],
            "supplier_code": first_po["purchase_order__supplier__supplier_code"],
            "client": first_po["purchase_order__supplier__client__id"],
            "client_name": first_po["purchase_order__supplier__client__name"],
        }
        storerkey = {
            "id": first_po["purchase_order__storerkey__id"],
            "name": first_po["purchase_order__storerkey__name"],
            "storerkey_code": first_po["purchase_order__storerkey__storerkey_code"],
            "measurement_method" : first_po["purchase_order__storerkey__measurement_method"]
        }
        
        line_details = []
        for po in pos:
            
            filtered_po_line_ids = (
                lines.filter(purchase_order_id=po["po_id"])
                .values("id")
                .annotate(
                    po_id=F("purchase_order__id"),
                    po_customer_reference_number=F("purchase_order__customer_reference_number"),
                )
                .distinct()
            )
            
            po = { "id" : po["po_id"] , "customer_reference_number" : po["po_customer_reference_number"] }
            data = { "po" : po , "po_lines" : filtered_po_line_ids , "supplier" : supplier_detail, "storerkey" : storerkey }
            line_details.append(data)

        return line_details, None
    

    @classmethod
    def data_get_step_2(cls, consignment, user=None, data = None):
        if not consignment:
            return None, "Invalid consignment id."
        return cls.data_get_step_1(consignment,user=user,data=data)


    @classmethod
    def data_get_step_3(cls, consignment, user=None, data = None):
        if not consignment:
            return None, "Invalid consignment id."
        return cls.data_get_step_1(consignment,user=user,data=data)


    @classmethod
    def data_get_step_4(cls, consignment, user=None, data = None):
        if not consignment:
            return None, "Invalid consignment id."
        
        address, errors = addresses_and_pickup(consignment.consignment_id)
        if errors:
            return None, errors
        
        return address, None
    

    @classmethod
    def data_get_step_5(cls, consignment, user=None, data = None):
        # Get related purchase orders in one query
        if not consignment:
            return None, "Invalid consignment id."

        lines = consignment.purchase_order_lines.select_related("purchase_order")
        pos = (
            lines
            .values(
                "purchase_order__supplier__id",
                "purchase_order__supplier__name",
                "purchase_order__supplier__supplier_code",
                "purchase_order__supplier__client__id",
                "purchase_order__supplier__client__name",
            )
            .annotate(
                po_id = F("purchase_order__id"),
                po_customer_reference_number = F("purchase_order__customer_reference_number")
            )
            .distinct()
        )

        if not pos:
            return [], None

        # for po in pos:
        #     po["po_id"] = po["purchase_order__id"]

        first_po = pos[0]
        supplier_detail = {
            "id": first_po["purchase_order__supplier__id"],
            "name": first_po["purchase_order__supplier__name"],
            "supplier_code": first_po["purchase_order__supplier__supplier_code"],
            "client": first_po["purchase_order__supplier__client__id"],
            "client_name": first_po["purchase_order__supplier__client__name"],
        }

        line_details = []
        for po in pos:
            
            filtered_po_line_ids = (
                lines.filter(purchase_order_id=po["po_id"])
                .values("id")
                # .annotate(
                #     po_id=F("purchase_order__id"),
                # )
            )
            # line_detail = POLineService.get_po_line_details(consignment,po["po_id"])

            line_detail,errors = POLineService.get_po_line_details_by_lines(consignment,filtered_po_line_ids)
            if errors:
                return None, errors
            # po_lines_qs = (
            #     PurchaseOrderLine.objects
            #     .filter(id__in=filtered_po_line_ids)
            #     .select_related("purchase_order")
            #     # .values(*cls.fields)
            #     .values("id")
            #     .annotate(
            #         po_id=F("purchase_order__id"),
            #         po_customer_reference_number=F("purchase_order__customer_reference_number"),
            #         # po_reference_number=F("purchase_order__reference_number"),
            #     )
            # )

            po = { "id" : po["po_id"] , "customer_reference_number" : po["po_customer_reference_number"] }
            data = { "po" : po , "po_lines" : line_detail , "supplier" : supplier_detail }
            line_details.append(data)

        return line_details, None
    


class ConsignmentStatusService:
        

    @classmethod
    def approved(cls,consignments):
        """
        Approves the given consignment.
        """
        consignments.update(consignment_status=ConsignmentStatusChoices.PENDING_CONSOLE_ASSIGNMENT)

        
    @classmethod
    def cancelled(cls,consignments=[],cancellation_reason=""):
        """
        Cancels the given consignments.
        """

        not_allowed_status = [ConsignmentStatusChoices.DELIVERED,ConsignmentStatusChoices.RECEIVED_AT_DESTINATION]

        if consignments.filter(consignment_status__in=not_allowed_status).exists():
            raise ServiceError(error="consignments after delivery not allowed to cancel.")

        # _, error = POLineService.revert_consignments_lines_quantities(consignments)
        # if error:
        #     raise ServiceError(error=error)
        
        consignments.update(consignment_status=ConsignmentStatusChoices.CANCELLED,cancellation_remarks = cancellation_reason,console="")

        (
            ConsignmentPackaging.objects
            .filter(consignment__in=consignments)
            .update(status=PackageStatusChoices.CANCELLED)
        )
        
        po_lines = (
            PurchaseOrderLine.objects
            .filter(consignments_po_lines__consignment__in=consignments)
            .select_related("purchase_order")
            .distinct()
        )

        POLineService.update_line_quantities(po_lines)
        
        return consignments, None

    
    @classmethod
    def at_customs(cls,consignments):
        """
        Marks the consignments as customs cleared.
        """
        statuses = list(consignments.values_list("consignment_status", flat=True).distinct())
        if not statuses or len(statuses) > 1 or statuses[0] != ConsignmentStatusChoices.PICKUP_COMPLETED:
            raise ServiceError(error = "consignment not pickedup yet")
        
        consignments.update(consignment_status = ConsignmentStatusChoices.AT_CUSTOM)
        return consignments, None
    

    @classmethod
    def customs_cleared(cls,consignments):
        """
        Marks the consignments as customs cleared.
        """
        statuses = list(consignments.values_list("consignment_status", flat=True).distinct())
        if not statuses or len(statuses) > 1 or statuses[0] != ConsignmentStatusChoices.AT_CUSTOM:
            raise ServiceError(error = "one or more consignment not at customs")
        
        consignments.update(consignment_status = ConsignmentStatusChoices.CUSTOMS_CLEARED)
        return consignments, None
    

    @classmethod
    def out_for_delivery(cls,consignments):
        """
        Marks the consignments as out for delivery.
        Only pickup completed and customs cleared consignments are allowed.
        """
        
        statuses = list(consignments.values_list("consignment_status", flat=True).distinct())
        allowed_status = [
            ConsignmentStatusChoices.CUSTOMS_CLEARED,
            ConsignmentStatusChoices.PICKUP_COMPLETED
        ]

        if not statuses:
            raise ServiceError(error = "Unable to determine consignment statuses")
        
        invalid = set(statuses) - set(allowed_status)
        if invalid:
            raise ServiceError(error = f"consignment status {', '.join(invalid)} not allowed for out for delivery")
        
        consignments.update(consignment_status = ConsignmentStatusChoices.OUT_FOR_DELIVERY)
        return consignments, None


    # @classmethod
    # def delivered(cls, consignments):
    #     """
    #     Marks the consignments as customs cleared.
    #     """
    #     return consignments, None
    

    @classmethod
    def pickup_completed(cls, consignments):
        """Mark pickup as completed if all consignments have consoles."""
        if consignments.filter(console__isnull=True).exists():
            raise ServiceError(
                error="Some consignments are not linked to any console. Cannot mark as pickup completed."
            )

        consignments.update(consignment_status=ConsignmentStatusChoices.PICKUP_COMPLETED)
        return consignments, None


    @classmethod
    def rejected(cls, consignments, rejection_code, rejection_reason):
        """Reject consignments with reason and code."""

        not_allowed_status = [
            ConsignmentStatusChoices.PICKUP_COMPLETED,
            ConsignmentStatusChoices.AT_CUSTOM,
            ConsignmentStatusChoices.CUSTOMS_CLEARED,
            ConsignmentStatusChoices.OUT_FOR_DELIVERY,
            ConsignmentStatusChoices.DELIVERED,
            ConsignmentStatusChoices.RECEIVED_AT_DESTINATION,
        ]
        
        if not rejection_code:
            raise ServiceError(error="Rejection code is required.")
        
        consignment_statuses = list(consignments.values_list("consignment_status", flat=True).distinct())
        not_valid_statuses = set(consignment_statuses) - set(not_allowed_status)
        
        if not_valid_statuses:
            raise ServiceError(
                error=f"Consignment status {', '.join(not_valid_statuses)} not allowed for rejection."
            )
        
        consignments.update(
            consignment_status=ConsignmentStatusChoices.REJECTED,
            rejection_code_id=rejection_code,
            rejection_reason=rejection_reason,
        )

        return consignments, None


    @classmethod
    def delivered(cls, consignments):
        
        """Handle post-delivery updates."""
        try:
            statuses = list(consignments.values_list("consignment_status", flat=True).distinct())
            if len(statuses) !=1 or statuses[0] != ConsignmentStatusChoices.OUT_FOR_DELIVERY:
                raise ServiceError(error=f"{statuses} not allowed for delivery")
            
            consignment_ids = list(consignments.values_list("id",flat=True))
            consignments.update(consignment_status=ConsignmentStatusChoices.DELIVERED)
            
            (
                ConsignmentPackaging.objects
                .filter(consignment_id__in=consignment_ids)
                .update(status=PackageStatusChoices.DELIVERED)
            )

            ## Get all distinct lines 
            po_lines = (
                PurchaseOrderLine.objects
                .filter(consignments_po_lines__consignment__in=consignments)
                .select_related("purchase_order")
                .distinct()
            )

            if not po_lines:
                return consignments, None

            ## Update fullfilled, open, and processed quantities
            POLineService.update_line_quantities(po_lines)
            
            po_line_ids = list(po_lines.values_list("id", flat=True).distinct())
            # Update PARTIALLY_FULFILLED lines
            PurchaseOrderLine.objects.filter(
                id__in=po_line_ids, fulfilled_quantity__gt=0
            ).update(status=PurchaseOrderStatusChoices.PARTIALLY_FULFILLED)

            # Update CLOSED lines
            PurchaseOrderLine.objects.filter(
                id__in=po_line_ids,open_quantity=0,processed_quantity=0
            ).update(status=PurchaseOrderStatusChoices.CLOSED)

            purchase_orders = [po_line.purchase_order for po_line in po_lines]

            for po in purchase_orders:
                po.update_status()

            return consignments, None
        

        except Exception as e:
            raise ServiceError(error=str(e))


    @classmethod
    def pending_bid(cls, consignments):
        """
        Reset console assignment for pending bids.
        If a console dones not have any consignments after this operation, update its status to PICKUP_REJECTED.
        """

        console_ids = list(consignments.values_list("console", flat=True).distinct())
        consignments.update(console=None, consignment_status=ConsignmentStatusChoices.PENDING_BID)

        consoles_for_recalc = Console.objects.filter(
            id__in=console_ids,
            consignments__isnull=True,
        ).distinct()
        
        if consoles_for_recalc.exists():
            consoles_for_recalc.update(
                console_status=ConsoleStatusChoices.PICKUP_REJECTED
            )

        return consignments, None


    @classmethod
    def update_status_with_pickup_datetime(cls, consignments, status, actual_pickup_datetime):
        """Update status and pickup datetime if provided."""
        if actual_pickup_datetime not in ["", "null", "undefined", None]:
            try:
                actual_dt = datetime.strptime(actual_pickup_datetime, "%Y-%m-%d %I:%M %p")
            except ValueError:
                raise ServiceError(error="Invalid date format. Expected '%Y-%m-%d %I:%M %p'")

            consignments.update(
                consignment_status=status,
                actual_pickup_datetime=actual_dt.strftime("%Y-%m-%d %H:%M:%S"),
            )
        else:
            consignments.update(consignment_status=status)
        return consignments, None
    


class ConsignmentServices:

    @classmethod
    def notify_consignment_update(cls ,user ,instance):

        """
        Pre-save signal for updating consignment notifications.
        """
    
        header= None
        message= None
        hyperlink_value = {"consignment_id": instance.consignment_id}
        new_status = instance.consignment_status if instance else None
        console = instance.console if instance.console else None
        
        
        if new_status == ConsignmentStatusChoices.DRAFT or new_status == ConsignmentStatusChoices.PENDING_FOR_APPROVAL:
            return
        
        if new_status == ConsignmentStatusChoices.PENDING_CONSOLE_ASSIGNMENT:
    
            header = "Pickup Request {consignment_id} Approved"
            message = "Pickup Request {consignment_id} has been Approved by " + f"{user.name}."

        elif new_status == ConsignmentStatusChoices.CANCELLED:

            header = "Pickup Request {consignment_id} Cancelled"
            message = "Pickup Request {consignment_id} has been Cancelled by " + f"{user.name}."


        elif new_status == ConsignmentStatusChoices.REJECTED:
            
            if console and console.freight_forwarder:
                header = "Pickup Rejected by Freight Forwarder"
                message = "Pickup for the Console {console_id} is Rejected by FF " + f"{console.freight_forwarder.name}"
                hyperlink_value = {"console_id":console_id}

            else:
                header = "Pickup Request {consignment_id} Rejected"
                message = "Pickup Request {consignment_id} has been Rejected by " + f"{user.name}."
                

        elif new_status == ConsignmentStatusChoices.CONSOLE_ASSIGNED:

            header = "Console Assigned"
            message = "Console is Assigned/Created for the Pickup Request {consignment_id}"

        elif new_status == ConsignmentStatusChoices.FREIGHT_FORWARDER_ASSIGNED:
            ff_name = console.freight_forwarder.name
            console_id = console.console_id
            header = "Freight Forwarder Assigned"
            message = f"Freight Forwarder {ff_name} is assigned to the Console " + "{console_id}"
            hyperlink_value = {"console_id":console_id}

        elif new_status == ConsignmentStatusChoices.PICKUP_COMPLETED:

            header = "Pickup Completed"
            message = "Pickup of {consignment_id} has been Completed"

        elif new_status == ConsignmentStatusChoices.AT_CUSTOM:

            header = "Pickup At Custom"
            message = "Pickup {consignment_id} is reached to Custom"
        

        elif new_status == ConsignmentStatusChoices.OUT_FOR_DELIVERY:

            header = "Pickup is out for delivery"
            message = "Pickup {consignment_id} is out for delivery"


        elif new_status == ConsignmentStatusChoices.DELIVERED:

            header = "Pickup delivered"
            message = "Pickup {consignment_id} has been delivered to the client " + f"{instance.client.name}-{instance.client.client_code}."


        elif new_status == ConsignmentStatusChoices.RECEIVED_AT_DESTINATION:

            header = "Pickup Received"
            message = "Pickup {consignment_id} has been received at the destination " + f"{instance.client.name}-{instance.client.client_code}."



        # if Notification.objects.filter(
        #     type = NotificationChoices.CONSIGNMENT,
        #     message__icontains = f"{instance.consignment_id} created",
        # ).exists():
        #     return
        
        # if new_status == ConsignmentStatusChoices.PENDING_FOR_APPROVAL:
        #     NotificationService.notify_consignment_created(instance, user)
        #     return
        
        handler = getattr(NotificationService, f"consignment_update", None)
        if handler:
            handler(instance, user, header, message, hyperlink_value)



class POLineService:

    fields = [
        'id',
        'customer_reference_number',
        'reference_number',
        'sku',
        'quantity',
        'fulfilled_quantity',
        'open_quantity',
        'expected_delivery_date',
        'is_dangerous_good',
        'is_chemical',
        "product_code",
        "description"]
        # "purchase_order",
        # 'po_customer_reference_number',
        # 'po_reference_number']
    

    @classmethod
    def revert_consignments_lines_quantities(cls, consignments=None):
        """
        Revert PO line quantities for given consignments by decreasing processed_quantity
        and increasing open_quantity based on allocated quantities.
        
        Args:
            consignments: List of consignment objects or IDs (optional)
            
        Returns:
            tuple: (success_count, error_message) or (None, error_message)
        """

        try:
            with transaction.atomic():
                # IDs for all consignments
                consignment_ids = [c.id for c in consignments]
                po_line_ids = ConsignmentPOLine.objects.filter(
                    consignment__id__in=consignment_ids
                ).values_list("purchase_order_line_id", flat=True)


                allocated_data = (
                    PackagingAllocation.objects
                    .filter(
                        consignment_packaging__consignment__id__in=consignment_ids,
                        purchase_order_line_id__in=po_line_ids,
                    )
                    .values("purchase_order_line_id")
                    .annotate(total_allocated=Sum("allocated_qty"))
                )

                allocated_map = {
                    item["purchase_order_line_id"]: item["total_allocated"] or 0
                    for item in allocated_data
                }

                
                update_data = []
                for po_line_id, total_allocated in allocated_map.items():
                    if total_allocated > 0:
                        update_data.append(
                            PurchaseOrderLine(
                                id=po_line_id,
                                processed_quantity=F("processed_quantity") - total_allocated,
                                open_quantity=F("open_quantity") + total_allocated
                            )
                        )

                
                if update_data:
                    updated_count = PurchaseOrderLine.objects.bulk_update(
                        update_data,
                        ["processed_quantity", "open_quantity"],
                    )
                    return updated_count, None

                return 0, "No allocations found for the given consignments."

        except Exception as e:
            transaction.set_rollback(True)
            return None, f"Error reverting quantities: {str(e)}"
        

    @classmethod
    def revert_consignments_lines_quantites(cls,consignments=[]):
        """
        open the processing quantities of the specified consignment lines.
        """
        if not consignments:
            return None, "No consignments provided for reverting PO line quantities."
        
        for consignment in consignments:

            po_lines = PurchaseOrderLine.objects.filter(
            packaging_allocations__consignment_packaging__consignment=consignment
            ).annotate(
                total_allocated=Sum('packaging_allocations__allocated_qty')
            ).distinct()
    
            # Bulk update all affected PO lines in a single query
            PurchaseOrderLine.objects.bulk_update(
                [
                    PurchaseOrderLine(
                        id=po_line.id,
                        processed_quantity=po_line.processed_quantity - (po_line.total_allocated or 0),
                        open_quantity=po_line.open_quantity + (po_line.total_allocated or 0)
                    )
                    for po_line in po_lines
                ],
                fields=['processed_quantity', 'open_quantity']
            )


    @classmethod
    def logistics_flags(cls, line_id=None, purchase_order=None):
        """
        Collects logistics/compliance flags for a given PurchaseOrderLine.
        """
        result = {}

        line = (
            PurchaseOrderLine.objects
            .filter(id=line_id)
            .only("product_code")
            .first()
        )

        if line:
            # storerkeys_qs = line.purchase_order.storerkey
            purchase_order = purchase_order if purchase_order else line.purchase_order
            storerkey = purchase_order.storerkey
            hub = storerkey.hub

            mm = (
                MaterialMaster.objects
                .filter(
                    product_code=line.product_code,
                    storerkey=storerkey,
                    hub=hub
                )
                .values("is_dangerous_good", "is_chemical")
                .first()
            )
            if mm:
                if mm["is_dangerous_good"]:
                    result["is_dangerous_good"] = True
                if mm["is_chemical"]:
                    result["is_chemical"] = True

        # Fetch storer level validations
        # storerkeys = purchase_order.storerkeys
        # s = storerkey

        # Merge storer flags into result if any True
        # for s in storerkeys:
        if storerkey.generate_asn:
            result["generate_asn"] = True
        if storerkey.hs_code_validation:
            result["hs_code_validation"] = True
        if storerkey.eccn_validation:
            result["eccn_validation"] = True
        if storerkey.chemical_good_handling:
            result["chemical_good_handling"] = True
            result["is_chemical"] = True

        result["order_type"] = purchase_order.order_type

        return result


    @classmethod
    def get_po_line_details(cls, consignment ,purchase_order):

        """
        Fetches and returns details of all lines associated with a given purchase order.
        Args:
            consignment: The consignment object.
            purchase_order: The purchase order object.
        Return:
            list of purchase order line details
        """
        po_lines_qs = (PurchaseOrderLine.objects
            .filter(
                purchase_order_id=purchase_order
            )
            .select_related("purchase_order")
            .values(*cls.fields)
            .annotate(
                po_id=F("purchase_order__id"),
                po_customer_reference_number=F("purchase_order__customer_reference_number"),
                po_reference_number=F("purchase_order__reference_number"),
            )
        )
        return cls.add_additional_info(po_lines_qs, purchase_order, consignment)
    

    @classmethod
    def get_po_line_details_by_lines(cls,consignment , po_line_ids = None):
        """
        Fetch and return details grouped by purchase order.
        Args:
            po_line_ids: list/queryset of PurchaseOrderLine IDs
            consignment: The consignment object.
        Returns:
            list of enriched line details grouped by purchase order
        """

        if not consignment:
            return [], "consignment is required"
        
        if not po_line_ids:
            po_lines_qs = (
                consignment.purchase_order_lines.all()
                .select_related("purchase_order")
                .values(*cls.fields)
                .annotate(
                    po_id=F("purchase_order__id"),
                    po_customer_reference_number=F("purchase_order__customer_reference_number"),
                    po_reference_number=F("purchase_order__reference_number"),
                )
            )

        else:
            # Get PO lines with related purchase orders
            po_lines_qs = (
                PurchaseOrderLine.objects
                .filter(id__in=po_line_ids)
                .select_related("purchase_order")
                .values(*cls.fields)
                .annotate(
                    po_id=F("purchase_order__id"),
                    po_customer_reference_number=F("purchase_order__customer_reference_number"),
                    po_reference_number=F("purchase_order__reference_number"),
                )
            )

        lines_qs = cls.add_additional_info(po_lines_qs,purchase_order=None, consignment = consignment)

        return lines_qs, None
    

    @classmethod
    def add_additional_info(cls, po_lines_qs, purchase_order, consignment = None):
        """
        Sets extra fields for a given PO line based on the provided flags.
        """
        # Post-process the queryset in Python
        for line in po_lines_qs:
            
            po_id = line.get("po_id") if line.get("po_id") else line.get("purchase_order")

            purchase_order = PurchaseOrder.objects.filter(id=po_id).first()          
            flags = POLineService.logistics_flags(line['id'],purchase_order)

            line["is_dangerous_good"] = flags.get("is_dangerous_good", line.get("is_dangerous_good"))
            line["is_chemical"] = flags.get("is_chemical", line.get("is_chemical"))
            line["order_type"] = flags.get("order_type", line.get("order_type"))
            line["manufacturing_country"] = ""

            
            # Extra flags - set only if True
            for flag in ["generate_asn", "hs_code_validation", "eccn_validation", "chemical_good_handling"]:
                if flags.get(flag):
                    line[flag] = True
                else:
                    line[flag] = False
            
            if consignment:

                compliance_data = (ConsignmentPOLine.objects
                    .filter(consignment_id=consignment.id, purchase_order_line_id=line.get("id"))
                    .values("id","hs_code", "eccn","country_of_origin")
                    .first()
                )

                line["allocated_quantity"] = get_allocated_quantity(consignment.id,line.get("id"))
                if compliance_data["country_of_origin"]:
                    line["hs_code"] = compliance_data["hs_code"]
                    line["eccn"] = compliance_data["eccn"]
                    line["country_of_origin"] = compliance_data["country_of_origin"]
                    line["compliance_updated"] = True

        return po_lines_qs


    @classmethod
    def po_line_quantity_validations(cls, data, po_line_obj):

        """
        Validates and updates the quantity for a PO line.
        Automatically adjusts open_quantity to maintain consistency:
        quantity = delivered + processing + open

        Args:
            po_data (dict): Input data containing 'quantity'.
            po_line_obj (PurchaseOrderLine): Existing line with original values.

        Returns:
            tuple: (updated_po_data, error_message)
        """
        try:
            raw_quantity = data.get("quantity")
            if raw_quantity is None:
                return data, "'quantity' must be provided."

            # Convert all to Decimal for accuracy
            updated_quantity = Decimal(str(raw_quantity))
            delivered_qty = Decimal(po_line_obj.fulfilled_quantity)
            processing_qty = Decimal(po_line_obj.processed_quantity)
            total_allocated = delivered_qty + processing_qty

            # Rule: original_quantity must be >= delivered + processing
            if updated_quantity < total_allocated:
                return data, (
                    "Quantity must be greater than or equal to delivered + processing quantity "
                    f"({total_allocated})."
                )

            # Calculate new open_quantity
            calculated_open_quantity = updated_quantity - total_allocated

            # Rule: open_quantity must be >= 0
            if calculated_open_quantity < 0:
                return data, "Open quantity cannot be negative."

            # All good, update both in data
            data["quantity"] = updated_quantity
            data["open_quantity"] = calculated_open_quantity
            data["status"] = PurchaseOrderStatusChoices.OPEN if updated_quantity == calculated_open_quantity else PurchaseOrderStatusChoices.PARTIALLY_FULFILLED

            return data, ""

        except Exception as e:
            return data, f"Error during quantity validation: {str(e)}"
            

    @classmethod
    def update_line_quantities(cls, po_lines):
        po_line_ids = [line.id for line in po_lines]

        # Annotate processed quantity for all lines in 1 query
        annotated_lines = (
            PurchaseOrderLine.objects
            .filter(id__in=po_line_ids)
            .annotate(
                processed_qty_calc=Sum(
                    "packaging_allocations__allocated_qty",
                    filter=Q(packaging_allocations__consignment_packaging__status=PackageStatusChoices.NOT_RECEIVED)
                ),

                fulfilled_qty_calc=Sum(
                    "packaging_allocations__allocated_qty",
                    filter=Q(packaging_allocations__consignment_packaging__status__in=[
                        PackageStatusChoices.DELIVERED, PackageStatusChoices.RECEIVED]
                    )
                )
            )
        )

        lines_to_update = []
        for line in annotated_lines:
            processed_qty = line.processed_qty_calc or Decimal(0)
            fulfilled = line.fulfilled_qty_calc or Decimal(0)
            original = line.quantity or Decimal(0)

            line.processed_quantity = processed_qty
            line.fulfilled_quantity = fulfilled
            line.open_quantity = original - processed_qty - fulfilled

            if line.open_quantity < 0 or line.processed_quantity < 0 or line.fulfilled_quantity < 0:
                raise ServiceError(
                    error=f"Negative quantity calculated for PO Line ID {line.id}. "
                          "Please check allocations."
                )

            lines_to_update.append(line)

        PurchaseOrderLine.objects.bulk_update(lines_to_update, ["fulfilled_quantity", "processed_quantity", "open_quantity"])



class PurchaseOrderService:
    
    @staticmethod
    def update_open_quantity(po_qs):
        # Subquery to sum line quantities
        line_sum = (
            PurchaseOrderLine.objects
            .filter(purchase_order_id=OuterRef("id"))
            .values("purchase_order_id")
            .annotate(total=Sum("quantity"))
            .values("total")
        )

        # Update PO open quantity in DB in one go
        po_qs.update(
            open_quantity=Coalesce(Subquery(line_sum), Value(0))
        )



class ComprehensiveReportService:

    @classmethod
    def json_data(cls, consignments_qs):
        """
        Returns comprehensive report data in JSON format.
        Args:
            from_date and to_date: The date range for the report.
            both the arguments are datetime objects and format is 'YYYY-MM-DD'.
            from_date: The start date for the report.
            to_date: The end date for the report.
            status: The status of the consignments to include in the report.
        Returns:
            A dictionary containing the comprehensive report data in JSON format.
        """
        try:

            # Get only IDs
            consignment_ids = consignments_qs.values_list("consignment_id", flat=True)

            values = ["is_dangerous_good"]
            packages_qs = (
                PackagingAllocation.objects
                .filter(consignment_packaging__consignment__consignment_id__in=consignment_ids)
                .exclude(consignment_packaging__status__in=[PackageStatusChoices.DRAFT])
                .values(*values)
                .annotate(
                    package_id = F("consignment_packaging__package_id"),
                    pickup_id = F("consignment_packaging__consignment__consignment_id"),
                    console_id = F("consignment_packaging__consignment__console__console_id"),
                    weight = F("consignment_packaging__weight"),
                    weight_unit = F("consignment_packaging__weight_unit"),
                    length= F("consignment_packaging__packaging_type__length"),
                    width= F("consignment_packaging__packaging_type__width"),
                    height= F("consignment_packaging__packaging_type__height"),
                    dimension_unit= F("consignment_packaging__packaging_type__dimension_unit"),
                    package_type=F("consignment_packaging__packaging_type__package_type"),
                    is_stackable=F("consignment_packaging__packaging_type__is_stackable"),
                    is_kit = Value('-'),
                    supplier_customer_code = F("consignment_packaging__consignment__supplier__supplier_code"),
                    supplier_customer_name = F("consignment_packaging__consignment__supplier__name"),
                    transection_number=F("purchase_order_line__purchase_order__customer_reference_number"),
                    transaction_type= Value('PO'),
                    transaction_line = F("purchase_order_line__customer_reference_number"),
                    product_code=F("purchase_order_line__product_code"),
                    quantity = F("allocated_qty"),
                    uom = F("purchase_order_line__sku"),
                    pickup_creation_date= F("consignment_packaging__consignment__created_at"),
                    collection_approval_date=Value('-'),
                    approval_kpi = Value('-'),
                    green_light_release_date=Value('-'),
                    ncr_reference=Value('-'),
                    ncr_created_date=Value('-'),
                    ncr_closed_date=Value('-'),
                    pick_up_required_date=Value('-'),
                    actual_pickup_date=F("consignment_packaging__consignment__actual_pickup_datetime"),
                    customs_cleared_date=Value('-'),
                    actual_delivery_date=Value('-'),
                    collection_status=F("consignment_packaging__status"),
                    age_till_date = Value('-'),
                    age_range = Value('-'),
                    sloc = F("purchase_order_line__purchase_order__center_code"),
                    plant_code = F("purchase_order_line__purchase_order__plant_id"),
                    pickup_status=F("consignment_packaging__consignment__consignment_status"),
                    classification = Value('-'),
                    urgent_priority_order = Value('-'),
                    remarks = Value('-'),
                    hawb = Value('-'),
                    mawb = Value('-'),
                    bol = Value('-'),
                    nvision_sync_status=Value('NO'),
                    asn_aa_applicable = Value('-'),
                    asn_reference=Value('-'),
                    aa_reference=Value('-'),
                    carrier = F("consignment_packaging__consignment__console__freight_forwarder__name"),
                    sender_name = F("consignment_packaging__consignment__consignor_address__address_name"),
                    sender_address=F("consignment_packaging__consignment__consignor_address__address_name"),
                    sender_pincode=F("consignment_packaging__consignment__consignor_address__zipcode"),
                    sender_city=F("consignment_packaging__consignment__consignor_address__city"),
                    sender_state=F("consignment_packaging__consignment__consignor_address__state"),
                    sender_country=F("consignment_packaging__consignment__consignor_address__country"),
                    destination_name = F("consignment_packaging__consignment__delivery_address__address_name"),
                    destination_address=F('consignment_packaging__consignment__delivery_address__address_name'),
                    destination_pincode=F("consignment_packaging__consignment__delivery_address__zipcode"),
                    destination_city=F("consignment_packaging__consignment__delivery_address__city"),
                    destination_state=F("consignment_packaging__consignment__delivery_address__state"),
                    destination_country=F("consignment_packaging__consignment__delivery_address__country"),
                    created_by = F("consignment_packaging__consignment__created_by__name"),
                    approved_by = Value('-'),
                    dangerous_good_un_class=Value('-'),
                    dangerous_good_name=Value('-'),
                    dangerous_good_class=Value('-'),
                    eccn=Value('-'),
                    hscode=Value('-'),
                    quote_requested_date=Value('-'),
                    quote_approval_date=Value('-'),
                    shipped_date=Value('-'),
                    receiption_date=Value('-'),
                    receiption_status=Value('-'),
                    etd = Value('-'),
                    eta = Value('-'),
                    atd = Value('-'),
                    ata = Value('-'),
                    tracking_url = Value('-'),
                )
            )
            compliance_qs = (
                ConsignmentPOLine.objects
                .select_related('dg_class', 'dg_category')
                .filter(
                    consignment__consignment_id__in=consignment_ids,
                    purchase_order_line__customer_reference_number__in=packages_qs.values_list('transaction_line', flat=True),
                )
            )

            compliance_map = {
                (c.consignment.consignment_id, c.purchase_order_line.customer_reference_number): c
                for c in compliance_qs
            }


            audit_logs = ConsignmentAuditTrailField.objects.filter(
                audit_trail__consignment__consignment_id__in=consignment_ids
            )
            
            packages = []
            for p in packages_qs:
                # Make timezone-naive
                if p.get('actual_pickup_date') and is_aware(p['actual_pickup_date']):
                    p['actual_pickup_date'] = make_naive(p['actual_pickup_date'])

                if p.get('pickup_creation_date') and is_aware(p.get('pickup_creation_date')):
                    p['pickup_creation_date'] = make_naive(p['pickup_creation_date'])

                # Convert dimensions and weight
                l = convert_dimension(p.get('length'), p.get('dimension_unit'), "Inch")
                w = convert_dimension(p.get('width'), p.get('dimension_unit'), "Inch")
                h = convert_dimension(p.get('height'), p.get('dimension_unit'), "Inch")
                p["dimensions"] = f"{l} * {w} * {h}"
                p["weight"] = convert_weight(p.get('weight'), p.get('weight_unit'), "Pound")

                # Fill compliance
                comp = compliance_map.get((p['pickup_id'], p['transaction_line']))
                if comp:
                    p["is_dangerous_good"] = comp.compliance_dg,
                    p["is_chemical"] = comp.compliance_chemical,
                    p["dangerous_good_un_class"] = comp.dg_class.name if comp.dg_class else ""
                    p["dangerous_good_name"] = comp.dg_category.name if comp.dg_category else ""
                    p["dangerous_good_class"] = comp.dg_class.name if comp.dg_class else ""
                    p["eccn"] = comp.eccn or ""
                    p["hscode"] = comp.hs_code or ""

                
                ## Set dates from logs
                logs = audit_logs.filter(audit_trail__consignment__consignment_id=p["pickup_id"])
                
                creation_date = parse_any_date(p["pickup_creation_date"])
                approval_date = parse_any_date(
                    logs.filter(title__icontains="Pending Console Assignment")
                    .values_list("created_at", flat=True)
                    .first()
                )
                delivery_date = parse_any_date(
                    logs.filter(title__icontains="Consignment Delivered")
                    .values_list("created_at", flat=True)
                    .first()
                )

                requested_id_approval_kpi = None
                age_till_date = None

                if delivery_date:
                    age_till_date = (
                        datetime.strptime(delivery_date, "%Y-%m-%d").date() -
                        datetime.strptime(creation_date, "%Y-%m-%d").date() 
                    ).days

                if approval_date and creation_date:
                    requested_id_approval_kpi = (
                        datetime.strptime(approval_date, "%Y-%m-%d").date() -
                        datetime.strptime(creation_date, "%Y-%m-%d").date() 
                    ).days

                p["pickup_creation_date"] = creation_date
                p["collection_approval_date"] = approval_date
                p["approval_kpi"] = requested_id_approval_kpi if requested_id_approval_kpi else ""

                p["pick_up_required_date"] = parse_any_date(
                    logs.filter(title__icontains="Freight Forwarder Assigned")
                    .values_list("created_at", flat=True)
                    .first()
                )

                p["actual_pickup_date"] = parse_any_date(
                    logs.filter(title__icontains="Pickup Completed")
                    .values_list("created_at", flat=True)
                    .first()
                )

                p["actual_delivery_date"] = delivery_date

                p["quote_requested_date"] = parse_any_date(
                    logs.filter(title__icontains="Consignment Pending Bid")
                    .values_list("created_at", flat=True)
                    .first()
                )


                p["quote_approval_date"] = parse_any_date(
                    logs.filter(title__icontains="Consignment Console Assigned")
                    .values_list("created_at", flat=True)
                    .order_by("-updated_at")
                    .first()
                )
                
                p["age_till_date"] = "" if age_till_date == None else age_till_date

                p["shipped_date"] = parse_any_date(
                    logs.filter(title__icontains="Received At Destination")
                    .values_list("created_at", flat=True)
                    .first()
                )

                p["atd"] = parse_any_date(
                    logs.filter(title__icontains="Received At Destination")
                    .values_list("created_at", flat=True)
                    .first()
                )
    
                packages.append(p)

            return packages, None

        except Exception as e:
            return None, str(e)

    
    @classmethod
    def build_report(cls, data):
        try:
            qs = ConsignmentDocumentAttachment.objects.filter(document__document_type=ConsignmentDocumentTypeChoices.COMPREHENSIVE_REPORT)

            # delete files from storage first
            for att in qs:
                if att.file:
                    att.file.delete(save=False)
            qs.delete()

            df = pd.DataFrame(data)
            header_map = ComprehensiveReportService.get_headers()

            # Rename headers:
            df.rename(columns=header_map, inplace=True)

            # Reorder to match mapping exactly
            df = df[list(header_map.values())]

            report_dir = Path(settings.MEDIA_ROOT) / "consignments/documents/comprehensive_reports"
            report_dir.mkdir(parents=True, exist_ok=True)

            # filename = f"consignment_report_{generate_unique_id('')}.xlsx"
            filename = f"pickup_comprehensive_reports.xlsx"

            file_path = report_dir / filename

            # Save Excel file
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Report', index=False)
                worksheet = writer.sheets['Report']
                for cell in next(worksheet.iter_rows(min_row=1, max_row=1)):
                    cell.font = cell.font.copy(bold=True)

            # Save file to DB
            relative_path = os.path.relpath(file_path, settings.MEDIA_ROOT)
            file_url = f"{settings.MEDIA_URL}{relative_path.replace(os.sep, '/')}"

            document = ConsignmentDocument.objects.create(
                document_type=ConsignmentDocumentTypeChoices.COMPREHENSIVE_REPORT
            )
            with open(file_path, 'rb') as f:
                django_file = File(f)
                attachment = ConsignmentDocumentAttachment.objects.create(document=document)
                attachment.file.save(filename, django_file, save=True)

            return file_url, None

        except Exception as e:
            return None, str(e)

        

        
    @classmethod
    def get_headers(cls):

        header_map = {
            "package_id": "Package ID",
            "pickup_id": "Pickup ID",
            "console_id": "Console ID",
            "weight": "Package Weight (Lbs)",
            "length": "Length",
            "width": "Width",
            "height": "Height",
            "dimensions": "Package Dimensions (Inches)",
            "package_type": "Package Type",
            "is_stackable": "Is Stackable",
            "is_kit": "Is Kit",
            "supplier_customer_code": "Supplier Customer Code",
            "supplier_customer_name": "Supplier Customer Name",
            "transection_number": "Transaction Number",
            "transaction_type": "Transaction Type",
            "transaction_line": "Transaction Line",
            "product_code": "Product Code",
            "quantity": "Quantity",
            "uom": "UOM",
            "pickup_creation_date": "Pickup Creation Date",
            "collection_approval_date": "Collection Approval Date",
            "approval_kpi": "Request ID Approval KPI",
            "green_light_release_date": "Green Light Release Date",
            "ncr_reference": "NCR Reference",
            "ncr_created_date": "NCR Created Date",
            "ncr_closed_date": "NCR Closed Date",
            "pick_up_required_date": "Pick Up Required Date",
            "actual_pickup_date": "Actual Pickup Date",
            "customs_cleared_date": "Custom Cleared Date",
            "actual_delivery_date": "Actual Delivery Date",
            "collection_status": "Collection Status",
            "age_till_date": "Age Till Date",
            "age_range": "Age Range",
            "sloc": "SLOC",
            "plant_code": "Plant Code",
            "pickup_status": "Pickup Status",
            "classification": "Classification",
            "urgent_priority_order": "Urgent Priority Order",
            "remarks" : "Remarks",
            "hawb": "HAWB",
            "mawb": "MAWB",
            "bol":"BOL",
            "nvision_sync_status": "Nvision Sync Status",
            "asn_aa_applicable": "ASN AA Applicable",
            "asn_reference": "ASN Reference",
            "aa_reference": "AA Reference",
            "carrier": "Carrier",
            "sender_name": "Sender Name",
            "sender_address": "Sender Address",
            "sender_pincode": "Sender Pincode",
            "sender_city": "Sender City",
            "sender_state": "Sender State",
            "sender_country": "Sender Country",
            "destination_name": "Destination Name",
            "destination_address": "Destination Address",
            "destination_pincode": "Destination Pincode",
            "destination_city": "Destination City",
            "destination_state": "Destination State",
            "destination_country": "Destination Country",
            "created_by": "Created By",
            "approved_by": "Approved By",
            "is_dangerous_good": "Is Dangerous Good",
            "dangerous_good_un_class": "Dangerous Good UN Class",
            "dangerous_good_name": "Dangerous Good Name",
            "dangerous_good_class": "Dangerous Good Class",
            "eccn": "ECCN",
            "hscode": "HS Code",
            "quote_requested_date": "Quote Requested Date",
            "quote_approval_date": "Quote Approval Date",
            "shipped_date": "Shipped Date",
            "receiption_date": "Reception Date",
            "receiption_status": "Reception Status",
            "etd": "ETD",
            "eta": "ETA",
            "atd": "ATD",
            "ata": "ATA",
            "tracking_url": "Tracking URL"
        }

        return header_map



