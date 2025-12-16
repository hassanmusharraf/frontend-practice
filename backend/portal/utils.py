from django.db.models import Q, Count
import numpy as np
import decimal
from django.http import HttpResponse
from django.template.loader import get_template
from xhtml2pdf import pisa
import os
import shutil
from django.conf import settings
# from workflows.models import BOL
from portal.models import CostCenterCode
from collections import defaultdict
from operations.models import ConsignmentAuditTrail, ConsignmentAuditTrailField
from django.db import transaction
from portal.choices import GLCodeChoices
from core.response import StandardResponse
from datetime import datetime, time
import pytz


def get_all_fields(model, ignore_fields=[], include_relational_fields=True):
    fields = [f.name for f in model._meta.fields if not f.is_relation and f not in ignore_fields]
    
    if include_relational_fields:
        for f in model._meta.fields:
            if f.is_relation and f not in ignore_fields:
                related_model = f.related_model
                related_fields = [f"{f.name}__{rel_field.name}" for rel_field in related_model._meta.fields]
                fields.extend(related_fields)
    
    return fields
    
    
def convert_to_decimal(num, decimal_num=4, return_none=True):
    if isinstance(num, float) or isinstance(num, str):
        if num == "" and return_none:
            return None if return_none else round(0, decimal_num)
        try:
            return round(decimal.Decimal(num), decimal_num)
        except decimal.InvalidOperation:
            return round(0, decimal_num)
    elif isinstance(num, np.int64):
        return round(decimal.Decimal(str(num)), decimal_num)
    elif num == None:
        return None if return_none else round(0, decimal_num)
    else:
        return round(num, decimal_num)


def get_utc_range_for_date(date_str: str):
    """
    Convert a date string (YYYY-MM-DD) into UTC start and end datetimes.

    Args:
        date_str (str): Date in 'YYYY-MM-DD' format

    Returns:
        tuple: (utc_start, utc_end) as timezone-aware datetime objects
    """
    try:
        # Parse the date
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

        # Add start and end times
        local_start = datetime.combine(date_obj, time.min)  # 00:00:00
        local_end = datetime.combine(date_obj, time.max)    # 23:59:59.999999

        # Convert to UTC
        utc_start = pytz.UTC.localize(local_start)
        utc_end = pytz.UTC.localize(local_end)

        return utc_start, utc_end

    except ValueError:
        raise ValueError("Date must be in 'YYYY-MM-DD' format")
    
# def render_to_pdf(template_src, context_dict={}):
#     template = get_template(template_src)
#     html  = template.render(context_dict)
#     result = HttpResponse(content_type='application/pdf')
#     # result['Content-Disposition'] = 'attachment; filename="report.pdf"'  # You handle this in your view

#     font_config = {
#         'RedHatDisplay': {
#             'normal': os.path.join(settings.BASE_DIR, 'static', 'fonts', 'RedHatDisplay-Regular.ttf'),
#             'bold': os.path.join(settings.BASE_DIR, 'static', 'fonts', 'RedHatDisplay-Bold.ttf'),
#             # Add other variations if you have them
#         }
#     }

#     pisa_status = pisa.CreatePDF(
#         html,
#         dest=result,
#         font_config=font_config
#     )
#     if pisa_status.err:
#         return None
#     return result.content


## BOL Utils 

def render_to_pdf(template_src, context_dict={}, output_path=None):
    template = get_template(template_src)
    html = template.render(context_dict)

    if output_path:
        with open(output_path, "wb") as f:
            pisa_status = pisa.CreatePDF(
                html, dest=f
            )
            if pisa_status.err:
                return None
        return output_path
    else:
        result = HttpResponse(content_type='application/pdf')
        pisa_status = pisa.CreatePDF(
            html, dest=result
        )
        if pisa_status.err:
            return None
        return result.content
    
def render_to_xml(template_src, context_dict={}, output_path=None):
    template = get_template(template_src)
    xml_content = template.render(context_dict)

    if output_path:
        # Save the XML content to a file
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        return output_path
    else:
        # Fallback to returning an HTTP response if needed
        response = HttpResponse(xml_content, content_type='application/xml')
        response['Content-Disposition'] = 'attachment; filename="bill_of_lading.xml"'
        return response
    
def empty_directory(directory):
    os.makedirs(directory, exist_ok=True)
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # delete file or link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # delete subdirectory
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

def render_to_html(template_src, context_dict={}):
    template = get_template(template_src)
    html = template.render(context_dict)
    return html

# def remove_consignments_from_bol():
#     empty_bols = BOL.objects.annotate(consignment_count=Count('consignment')).filter(consignment_count=0)
#     empty_bols.delete()

# def create_update_bol(console, consignments):
#     all_bols = BOL.objects.filter(console=console)
#     cc_codes = CostCenterCode.objects.all()
    
#     batches = defaultdict(list)
    
#     for consignment in consignments:
#         if consignment.purchase_order:
#             cc_code = cc_codes.filter(plant_id=consignment.purchase_order.plant_id, center_code=consignment.purchase_order.center_code).first()
#         elif consignment.adhoc:
#             cc_code = cc_codes.filter(plant_id=consignment.adhoc.plant_id, center_code=consignment.adhoc.center_code).first()
#         key = (
#             consignment.consignor_address_id,
#             consignment.delivery_address_id,
#             console.freight_forwarder_id,
#             consignment.supplier_id,
#             cc_code.id if cc_code else None,
#         )
#         batches[key].append(consignment)
    

#     for key, consignment_list in batches.items():
#         ship_from_id, ship_to_id, ff_id, supplier_id, cc_code_id = key

#         existing_bol = all_bols.filter(
#             ship_from_id=ship_from_id,
#             ship_to_id=ship_to_id,
#             freight_forwarder_id=ff_id,
#             supplier_id=supplier_id,
#             cc_code_id=cc_code_id,
#         ).first()

#         if not existing_bol:
#             new_bol = BOL.objects.create(
#                 console=console,
#                 cc_code_id=cc_code_id,
#                 ship_from_id=ship_from_id,
#                 ship_to_id=ship_to_id,
#                 freight_forwarder_id=ff_id,
#                 supplier_id=supplier_id,
#                 # gl_account=console.gl_account
#             )
#             bol_to_assign = new_bol
#         else:
#             bol_to_assign = existing_bol

#         for consignment in consignment_list:
#             consignment.bol = bol_to_assign
#             consignment.save()
         
# def log_bol_generation(user, console, consignment, filenames, **kwargs):

#     """Automatically log BOL creation in AuditLog after saving."""

#     console_id = console.id if console else None
#     if not console_id:
#         return  # Exit if console_id is not available
    
#     # consignments = Consignment.objects.filter(console=console)
#     consignments = [consignment] if consignment else []

#     if not consignments:
#         return
    
#     with transaction.atomic():
#         audit_trails = [
#             ConsignmentAuditTrail(consignment=con, updated_by=user)
#             for con in consignments
#         ]
#         ConsignmentAuditTrail.objects.bulk_create(audit_trails)

#         audit_trail_fields = [
#             ConsignmentAuditTrailField(
#                 audit_trail=audit_trail,
#                 title="BOL Generated",
#                 description="BOL has been generated for this consignment.",
#                 field_name="BOL Generation",
#                 attachments=filenames
#             )
#             for audit_trail in audit_trails
#         ]
#         ConsignmentAuditTrailField.objects.bulk_create(audit_trail_fields)

# def generate_gl_code(consignment):
    
#     if (consignment.consignor_address and consignment.delivery_address) and (consignment.consignor_address.country == consignment.delivery_address.country):
#         consignment.gl_code = GLCodeChoices.CODE_56000100
        
#     else:
#         consignment.gl_code = GLCodeChoices.CODE_56010000
    
#     consignment.save()
#     return consignment.gl_code.value

# def get_cc_code(plant_id,center_code):
#     if not plant_id or not center_code:
#         return StandardResponse(status=400, success=False, errors=["plant_id and center_code required"]) 
    
#     cc_code = CostCenterCode.objects.filter(plant_id = plant_id, center_code = center_code).only("cc_code").first()

#     return cc_code