import os
import base64
from collections import defaultdict

from django.conf import settings
from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from core.response import StandardResponse

from portal.models import CostCenterCode
from portal.choices import GLCodeChoices, PackageStatusChoices

from workflows.models import Console
from operations.models import (
    # PurchaseOrder,
    PurchaseOrderLine,
    Consignment,
    ConsignmentAuditTrail,
    ConsignmentAuditTrailField,
    ConsignmentPOLine,
    ConsignmentPackaging,
    PackagingAllocation,
)


class BOLServices:

    
    @classmethod
    def bol_context_data(cls, consignment, logo_base64):
        """
        Builds BOL context including both serialized BOL data (sku lines + packages)
        and consignment details like ship_from, ship_to, carrier info, etc.
        """
        # --- Prefetch related data efficiently ---
        po_lines = consignment.purchase_order_lines.select_related("purchase_order")
        consignment_po_lines = (
            ConsignmentPOLine.objects
            .select_related("dg_class", "dg_category")
            .filter(consignment=consignment)
        )

        package_qs = (
            ConsignmentPackaging.objects
            .filter(consignment=consignment)
            .select_related("packaging_type", "consignment")
        )

        packaging_allocations = (
            PackagingAllocation.objects
            .filter(
                consignment_packaging__consignment=consignment,
                purchase_order_line__in=po_lines,
            )
            .select_related(
                "consignment_packaging__packaging_type",
                "purchase_order_line",
            )
            .distinct()
        )

        # --- Helper Maps ---
        packaging_weight_map = {pkg.id: pkg.weight for pkg in package_qs}
        compliance_map = {
            (c.purchase_order_line.purchase_order.id, c.purchase_order_line.customer_reference_number): c
            for c in consignment_po_lines
        }

        unit_map = {
            "Millimeter": "mm",
            "Centimeter": "cm",
            "Inch": "In",
            "Foot": "ft",
            "Yard": "yd"
        }

        # --- SKU Data ---
        package_line_map = defaultdict(list)

        for pack in packaging_allocations:
            line = pack.purchase_order_line
            compliance = compliance_map.get(
                (line.purchase_order.id, line.customer_reference_number)
            )
            packaging_type = pack.consignment_packaging.packaging_type
            cc_code = BOLServices.get_cc_code(line.purchase_order.plant_id, line.purchase_order.center_code)

            package_line_map[pack.consignment_packaging.package_id].append({
                "description": line.description,
                "sku_qty": pack.allocated_qty,
                "dg_class": compliance.dg_class.name if compliance and compliance.dg_class else None,
                "dg_category": compliance.dg_category.name if compliance and compliance.dg_category else None,
                "cc_code": cc_code,
            })

        # --- Package Summary ---
        dimension_summary = (
            package_qs.values(
                "packaging_type_id",
                "packaging_type__package_type",
                "packaging_type__length",
                "packaging_type__width",
                "packaging_type__height",
                "packaging_type__dimension_unit",
                "weight",
                "weight_unit",
                "package_id"
            )
            .annotate(count=Count("packaging_type_id"))
        )

        packages = []
        for item in dimension_summary:
            package_id = item["package_id"]
            packages.append({
                "summary": {
                    "package_id": package_id,
                    "package_type": item["packaging_type__package_type"],
                    "weight": item["weight"],
                    "weight_unit": item["weight_unit"],
                    "dimensions": f"{item['packaging_type__length']} X {item['packaging_type__width']} X {item['packaging_type__height']} {unit_map.get(item['packaging_type__dimension_unit'], item['packaging_type__dimension_unit'])}",
                },
                "lines": package_line_map.get(package_id, []),
            })



        # --- Consignment Metadata ---
        po_qs = po_lines.only("purchase_order").distinct()
        purchase_orders = list(set([po.purchase_order.customer_reference_number for po in po_qs]))
        ff = consignment.console.freight_forwarder

        ship_from = {
            "name": consignment.supplier.name,
            "address_name": consignment.consignor_address.address_name,
            "address": consignment.consignor_address.address_line_1,
            "address2": consignment.consignor_address.address_line_2,
            "location": f"{consignment.consignor_address.city} / {consignment.consignor_address.state} / {consignment.consignor_address.zipcode}",
            "lat_lon": f"{consignment.consignor_address.latitude} / {consignment.consignor_address.longitude}",
        }

        ship_to = {
            "name": consignment.client.name,
            "address_name": consignment.delivery_address.address_name,
            "address": consignment.delivery_address.address_line_1,
            "address2": consignment.delivery_address.address_line_2,
            "location": f"{consignment.delivery_address.city} / {consignment.delivery_address.state} / {consignment.delivery_address.zipcode}",
            "lat_lon": f"{consignment.delivery_address.latitude} / {consignment.delivery_address.longitude}",
        }

        # --- Final Context ---
        context = {
            "logo_base64": logo_base64,
            "ship_from": ship_from,
            "ship_to": ship_to,
            "carrier_name": ff.name if ff else "-",
            "scac": ff.scac if ff and ff.scac else "-",
            "mc_dot": ff.mc_dot if ff and ff.mc_dot else "-",
            "consignment_id": consignment.consignment_id,
            "po_number": ", ".join(purchase_orders) if purchase_orders else "",
            "gl_account": BOLServices.generate_gl_code(consignment),
            "packages": packages,
        }

        return context



    @classmethod
    def serialized_bol_data(cls,consignment):
        """
        This will return Po-line data and packaging details for a given consignment.
        """

        po_lines = consignment.purchase_order_lines.all()
        consignment_po_line = (
            ConsignmentPOLine.objects
            .select_related('dg_class', 'dg_category')
            .filter(
                consignment=consignment,
                purchase_order_line__in=po_lines,
            )
        )


        representation = {}
        package_qs = ConsignmentPackaging.objects.filter(consignment=consignment).select_related("packaging_type","consignment")
        package_ids = list(package_qs.values_list("id", flat=True))

        # Prefetch allocations
        packaging_allocations = PackagingAllocation.objects.filter(
            consignment_packaging__consignment = consignment,
            purchase_order_line__in = po_lines,
        ).select_related("consignment_packaging__packaging_type","purchase_order_line").distinct()

        packaging_weight_map = {
            pkg.id: pkg.weight for pkg in package_qs
        }        


        compliance_map = {
            (c.consignment.consignment_id, c.purchase_order_line.customer_reference_number): c
            for c in consignment_po_line
        }
        
        # Build sku_data list
        unit_map = {
            "Millimeter": "mm",
            "Centimeter": "cm",
            "Inch": "In",
            "Foot": "ft",
            "Yard": "yd"
        }
        
        sku_data = []
        for pack in packaging_allocations:
            consignment_packaging = pack.consignment_packaging
            consignment_id = consignment.consignment_id
            line_customer_reference_number = pack.purchase_order_line.customer_reference_number
            compliance = compliance_map.get((consignment_id, line_customer_reference_number), None)
            consignment_packaging = pack.consignment_packaging
            packaging_type = pack.consignment_packaging.packaging_type
            box_weight = packaging_weight_map.get(consignment_packaging.id)
            line = pack.purchase_order_line
            cc_code = BOLServices.get_cc_code(pack.purchase_order_line.purchase_order.plant_id, pack.purchase_order_line.purchase_order.center_code)
            sku_data.append({
                "description": line.description,
                "sku_qty": pack.allocated_qty,
                "box_type": packaging_type.package_type,
                "box_weight": box_weight,
                "dimensions": f"{packaging_type.length} X {packaging_type.width} X {packaging_type.height} {unit_map.get(packaging_type.dimension_unit, packaging_type.dimension_unit)}",
                "dg_class": compliance.dg_class.name if compliance and compliance.dg_class else None,
                "dg_category": compliance.dg_category.name if compliance and compliance.dg_category else None,
                "cc_code" : cc_code
            })

        representation["sku_data"] = sku_data

        # Build packages summary
        dimension_summary = (
            package_qs.values(
                "packaging_type_id",
                "packaging_type__package_type",
                "packaging_type__length",
                "packaging_type__width",
                "packaging_type__height",
                "packaging_type__dimension_unit"
            )
            .annotate(count=Count("packaging_type_id"))
        )

        

        packages = [
            f"{item['count']} {item['packaging_type__package_type']} "
            f"({item['packaging_type__length']} X {item['packaging_type__width']} X {item['packaging_type__height']}) "
            f"{unit_map.get(item['packaging_type__dimension_unit'], item['packaging_type__dimension_unit'])} "
            for item in dimension_summary
        ]

        representation["packages"] = packages
        return representation
    


    @classmethod
    def bol_context(cls,consignment,serialized,logo_base64):
        po_lines = consignment.purchase_order_lines.all().select_related("purchase_order")
        po_qs = po_lines.only("purchase_order").distinct()
        purchase_orders = [po.purchase_order.customer_reference_number for po in po_qs]
        # cc_codes = [BOLServices.get_cc_code(po.purchase_order.plant_id,po.purchase_order.center_code) for po in po_qs]
        ff = consignment.console.freight_forwarder
        ship_from = {
            "name": consignment.supplier.name,
            "address_name": consignment.consignor_address.address_name,
            "address": consignment.consignor_address.address_line_1,
            "address2": consignment.consignor_address.address_line_2,
            "location": f"{consignment.consignor_address.city} / {consignment.consignor_address.state} / {consignment.consignor_address.zipcode}",
            "lat_lon": f"{consignment.consignor_address.latitude} / {consignment.consignor_address.longitude}",
        }
        ship_to = {
            "name": consignment.client.name,
            "address_name": consignment.delivery_address.address_name,
            "address": consignment.delivery_address.address_line_1,
            "address2": consignment.delivery_address.address_line_2,
            "location": f"{consignment.delivery_address.city} / {consignment.delivery_address.state} / {consignment.delivery_address.zipcode}",
            "lat_lon": f"{consignment.delivery_address.latitude} / {consignment.delivery_address.longitude}",
        }

        context = {
            'logo_base64': logo_base64,
            'sku_data': serialized.get("sku_data"),
            "ship_from": ship_from,
            "ship_to": ship_to,
            "carrier_name": ff.name if ff else "-",
            "scac" : ff.scac if ff.scac else "-",
            "mc_dot" : ff.mc_dot if ff.mc_dot else "-",
            "consignment_id": consignment.consignment_id,
            "po_number": ",".join(f'{po}' for po in purchase_orders),
            # "cc_code": ",".join(f'{cc}' for cc in cc_codes),
            "gl_account": BOLServices.generate_gl_code(consignment),
            "packages": serialized.get("packages"),
        }

        return context
    


    @classmethod
    def log_bol_generation(cls, user, console, consignment, filenames, **kwargs):

        """Automatically log BOL creation in AuditLog after saving."""

        console_id = console.id if console else None
        if not console_id:
            return  # Exit if console_id is not available
        
        # consignments = Consignment.objects.filter(console=console)
        consignments = consignment
        if not consignments:
            return

        if not isinstance(consignments,list):
            consignments = [consignment] if consignment else []

        with transaction.atomic():
            audit_trails = [
                ConsignmentAuditTrail(consignment=con, updated_by=user)
                for con in consignments
            ]
            ConsignmentAuditTrail.objects.bulk_create(audit_trails)

            audit_trail_fields = [
                ConsignmentAuditTrailField(
                    audit_trail=audit_trail,
                    title="BOL Generated",
                    description="BOL has been generated for this consignment.",
                    field_name="BOL Generation",
                    attachments=filenames
                )
                for audit_trail in audit_trails
            ]
            ConsignmentAuditTrailField.objects.bulk_create(audit_trail_fields)



    @classmethod
    def generate_gl_code(cls, consignment):
        
        if (consignment.consignor_address and consignment.delivery_address) and (consignment.consignor_address.country == consignment.delivery_address.country):
            consignment.gl_code = GLCodeChoices.CODE_56000100
            
        else:
            consignment.gl_code = GLCodeChoices.CODE_56010000
        
        consignment.save()
        return consignment.gl_code.value



    @classmethod
    def get_cc_code(cls, plant_id,center_code):

        if not plant_id or not center_code:
            return StandardResponse(status=400, success=False, errors=["plant_id and center_code required"]) 
        
        cc_code = CostCenterCode.objects.filter(plant_id = plant_id, center_code = center_code).only("cc_code").first()

        return cc_code
    


    @classmethod
    def update_console_consignment_bol_gen(cls, user,console,consignment):
        last_bol_generated_at = timezone.now()

        console.last_bol_generated_at = last_bol_generated_at
        console.last_bol_generated_by = user
        console.save()

        # Update console
        consignment.last_bol_gen_at = last_bol_generated_at
        consignment.last_bol_gen_by = user
        consignment.save()


    
    @classmethod
    def console_bol_context_data(cls, consignments, logo_base64):
        """
        Builds BOL context including both serialized BOL data (sku lines + packages)
        and consignment details like ship_from, ship_to, carrier info, etc.
        """

        consignment_ids = [consignment.id for consignment in consignments]
        consignment = consignments[0]

        po_lines = (
            PurchaseOrderLine.objects
            .filter(consignments_po_lines__consignment_id__in=consignment_ids)
            .distinct()
        )

        consignment_po_lines = (
            ConsignmentPOLine.objects
            .select_related("dg_class", "dg_category","purchase_order_line__purchase_order")
            .filter(consignment_id__in=consignment_ids)
        )

        package_qs = (
            ConsignmentPackaging.objects
            .filter(consignment_id__in=consignment_ids)
            .select_related("packaging_type", "consignment")
        )

        packaging_allocations = (
            PackagingAllocation.objects
            .filter(
                consignment_packaging__consignment_id__in=consignment_ids,
                purchase_order_line__in=po_lines,
            )
            .select_related(
                "consignment_packaging__packaging_type",
                "purchase_order_line",
            )
            .distinct()
        )

        # --- Helper Maps ---
        packaging_weight_map = {pkg.id: pkg.weight for pkg in package_qs}
        compliance_map = {
            (c.purchase_order_line.purchase_order.id, c.purchase_order_line.customer_reference_number): c
            for c in consignment_po_lines
        }

        unit_map = {
            "Millimeter": "mm",
            "Centimeter": "cm",
            "Inch": "In",
            "Foot": "ft",
            "Yard": "yd"
        }

        # --- SKU Data ---
        package_line_map = defaultdict(list)

        for pack in packaging_allocations:
            line = pack.purchase_order_line
            compliance = compliance_map.get(
                (line.purchase_order.id, line.customer_reference_number)
            )
            packaging_type = pack.consignment_packaging.packaging_type
            cc_code = BOLServices.get_cc_code(line.purchase_order.plant_id, line.purchase_order.center_code)

            package_line_map[pack.consignment_packaging.package_id].append({
                "description": line.description,
                "sku_qty": pack.allocated_qty,
                "dg_class": compliance.dg_class.name if compliance and compliance.dg_class else None,
                "dg_category": compliance.dg_category.name if compliance and compliance.dg_category else None,
                "cc_code": cc_code,
            })

        # --- Package Summary ---
        dimension_summary = (
            package_qs.values(
                "packaging_type_id",
                "packaging_type__package_type",
                "packaging_type__length",
                "packaging_type__width",
                "packaging_type__height",
                "packaging_type__dimension_unit",
                "weight",
                "weight_unit",
                "package_id"
            )
            .annotate(count=Count("packaging_type_id"))
        )

        packages = []
        for item in dimension_summary:
            package_id = item["package_id"]
            packages.append({
                "summary": {
                    "package_id": package_id,
                    "package_type": item["packaging_type__package_type"],
                    "weight": item["weight"],
                    "weight_unit": item["weight_unit"],
                    "dimensions": f"{item['packaging_type__length']} X {item['packaging_type__width']} X {item['packaging_type__height']} {unit_map.get(item['packaging_type__dimension_unit'], item['packaging_type__dimension_unit'])}",
                },
                "lines": package_line_map.get(package_id, []),
            })



        # --- Consignment Metadata ---
        po_qs = po_lines.only("purchase_order").distinct()
        purchase_orders = list(set([po.purchase_order.customer_reference_number for po in po_qs]))
        ff = consignment.console.freight_forwarder

        ship_from = {
            "name": consignment.supplier.name,
            "address_name": consignment.consignor_address.address_name,
            "address": consignment.consignor_address.address_line_1,
            "address2": consignment.consignor_address.address_line_2,
            "location": f"{consignment.consignor_address.city} / {consignment.consignor_address.state} / {consignment.consignor_address.zipcode}",
            "lat_lon": f"{consignment.consignor_address.latitude} / {consignment.consignor_address.longitude}",
        }

        ship_to = {
            "name": consignment.client.name,
            "address_name": consignment.delivery_address.address_name,
            "address": consignment.delivery_address.address_line_1,
            "address2": consignment.delivery_address.address_line_2,
            "location": f"{consignment.delivery_address.city} / {consignment.delivery_address.state} / {consignment.delivery_address.zipcode}",
            "lat_lon": f"{consignment.delivery_address.latitude} / {consignment.delivery_address.longitude}",
        }

        # --- Final Context ---
        context = {
            "logo_base64": logo_base64,
            "ship_from": ship_from,
            "ship_to": ship_to,
            "carrier_name": ff.name if ff else "-",
            "scac": ff.scac if ff and ff.scac else "-",
            "mc_dot": ff.mc_dot if ff and ff.mc_dot else "-",
            "consignment_id": consignment.consignment_id,
            "po_number": ", ".join(purchase_orders) if purchase_orders else "",
            "gl_account": BOLServices.generate_gl_code(consignment),
            "packages": packages,
        }

        return context
