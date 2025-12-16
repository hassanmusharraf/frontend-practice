from .models import Consignment, PackagingAllocation
from django.db.models import Sum, F
import random
import string
from datetime import datetime, date, timedelta
from .models import ConsignmentDocumentAttachment, ConsignmentDocument
import re

def get_validated_storerkeys(storerkeys = None):

    fields_to_check = [
        "generate_asn",
        "hs_code_validation",
        "eccn_validation",
        "chemical_good_handling",
        "adhoc_applicable",
    ]

    fields = {}
    for s in storerkeys:
        for field in fields_to_check:
            value = getattr(s, field)
            if value is True and field not in fields:
                fields[field] = True

        if s.expediting_applicable is True and "expediting_applicable" not in fields:
            fields["expediting_applicable"] = True
            fields.setdefault("reminder_name", s.reminder_name)
            fields.setdefault("trigger_days", s.trigger_days)


    return fields



def get_allocated_quantity(consignment_id, line_id):
    return PackagingAllocation.objects.filter(
        consignment_packaging__consignment__id=consignment_id,
        purchase_order_line__id=line_id
    ).aggregate(allocated_quantity=Sum('allocated_qty'))['allocated_quantity'] or 0



def generate_unique_id(prefix: str) -> str:
    # Timestamp in YYMMDDHHMMSS format → always 12 digits
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")  # e.g., '250909131312'

    # Generate 2 random digits
    random_suffix = str(random.randint(10, 99))  # ensures it's always 2 digits

    # Combine everything
    return f"{prefix}{timestamp}{random_suffix}"




def update_files(deleted_doc_ids=None,filters={},files=None):
    """
    Update consignment document attachments.
    This checks if a document is new or an existing one,
    and updates the document with the new files if any.
    if no files are provided, it deletes the existing attachments.
    Arguments:
        deleted_doc_ids (list): ids of documents to be deleted
        filters (dict): filters to apply on ConsignmentDocument
        files (list): files to be attached to the consignment document
    Returns:
        message (str): success message
        errors (list): any errors encountered
    """
    try:
        # error = validate_file_size(files)
        # if error:
        #     return None, error
    
        if deleted_doc_ids:
            qs = ConsignmentDocumentAttachment.objects.filter(id__in=deleted_doc_ids)
            # delete files from storage first
            for att in qs:
                if att.file:
                    att.file.delete(save=False)
            qs.delete()

        if files and filters:
            document_obj = ConsignmentDocument.objects.filter(**filters).first()
            if not document_obj:
                document_obj = ConsignmentDocument.objects.create(**filters)
            
            files_to_create = []
            # --- Handle new file attachments ---
            if files:
                for f in files:
                    if f:
                        files_to_create.append(ConsignmentDocumentAttachment(document=document_obj, file=f))

                if files_to_create:
                    ConsignmentDocumentAttachment.objects.bulk_create(files_to_create)
            # ConsignmentDocumentAttachment.objects.bulk_create(
            #     [
            #         ConsignmentDocumentAttachment(document=document_obj, file=f)
            #         for f in files
            #     ]
            # )

        return "Files Updated Successfully", None
    
    except Exception as e:
        return "", str(e)



def serialize_address(address, role="supplier"):
    """Serialize an Address object to dict safely."""
    if not address:
        return {}

    entity = address.supplier if role == "supplier" else address.client
    address_key = "consignor_id" if role == "supplier" else "delivery_id"
    role_key = "supplier_id" if role == "supplier" else "client_id"
    return {
        role_key: getattr(entity, "id", None),
        address_key : getattr(address, "id", None),
        "name": getattr(entity, "name", None),
        "address": address.address_name,
        "address_type": address.address_type,
        "address_line_1": address.address_line_1,
        "address_line_2": address.address_line_2,
        "city": address.city,
        "state": address.state,
        "country": address.country,
        "responsible_person_name": address.responsible_person_name,
        "zipcode": address.zipcode,
        "mobile_no": address.mobile_no,
        "alternate_mobile_no": address.alternate_mobile_no,
    }


def addresses_and_pickup(consignment_id):
    """Return supplier & client addresses with pickup info."""
    if not consignment_id:
        return None, "Invalid consignment id."

    consignment = (
        Consignment.objects
        .select_related(
            "consignor_address__supplier",
            "delivery_address__client"
        )
        .only(
            "id", "consignment_id", "consignor_address", "delivery_address",
            "actual_pickup_datetime", "pickup_timezone", "additional_instructions"
        )
        .filter(consignment_id=consignment_id)
        .first()
    )
    
    if not consignment:
        return None, "Consignment not found."


    if not consignment.consignor_address or not consignment.delivery_address:
        result = {
            "supplier": {
                "supplier_id" : consignment.supplier.id if consignment.supplier else None,
                "name": consignment.supplier.name if consignment.supplier else None,
                "supplier_code" : consignment.supplier.supplier_code if consignment.supplier else None,
            },
            "client": {
                "client_id" : consignment.client.id if consignment.client else None,
                "name": consignment.client.name if consignment.client else None,
                "client_code" : consignment.client.client_code if consignment.client else None,
            },
        }
        return result, None



    result = {
        "supplier": serialize_address(consignment.consignor_address, role="supplier"),
        "client": serialize_address(consignment.delivery_address, role="client"),
        "actual_pickup_datetime": consignment.actual_pickup_datetime or "Not provided",
        "pickup_timezone": consignment.pickup_timezone or "Not provided",
        "additional_instructions": consignment.additional_instructions or "Not provided",
    }
    return result, None



def validate_file_size(files):

    max_size = 25 * 1024 * 1024  # 25 MB

    for f in files:
        if f.size > max_size:
            return f"File too large. Max size: 25 MB. Your file size: {f.size / 1024 / 1024:.2f} MB"
        
    return None
        



def parse_any_date(
    value,
    output_format="%Y-%m-%d",
    return_type="str",          # "str" or "date"
    prefer_day_first=True,      # affects ambiguous formats and dateutil
    excel_check=True,           # try interpret numeric as Excel serial
    raise_on_error=False
):
    """
    Parse many date formats into a normalized date.

    Args:
        value: str|int|float|datetime.date|datetime.datetime|None
        output_format: str, strftime format used when return_type="str"
        return_type: "str" or "date"
        prefer_day_first: if ambiguous, treat '01/02/2020' as day-first if True
        excel_check: try Excel serial number conversion for numeric inputs
        raise_on_error: if True, raise ValueError on failure; else return None

    Returns:
        str (formatted by output_format) or datetime.date or None
    """
    # 1) None or empty
    if value is None:
        return None

    # 2) If already a date/datetime
    if isinstance(value, date) and not isinstance(value, datetime):
        out = value if return_type == "date" else value.strftime(output_format)
        return out
    if isinstance(value, datetime):
        d = value.date()
        return d if return_type == "date" else d.strftime(output_format)

    # Normalize string
    s = str(value).strip()
    if not s:
        return None

    # 3) Excel serial numbers (e.g., "44000" or 44000 or float)
    if excel_check:
        # numeric-looking and reasonable magnitude -> try Excel serial conversion
        num_match = re.fullmatch(r"[+-]?\d+(\.\d+)?", s)
        if num_match:
            try:
                serial = float(s)
                # heuristics: typical excel serial >= 1 and not ridiculously small/large
                if 1 <= serial <= 60000:   # up to year ~ 2063; tweak if needed
                    # Excel epoch: 1899-12-30 (Excel serial 1 -> 1899-12-31, Excel has 1900 leap bug)
                    epoch = datetime(1899, 12, 30)
                    dt = epoch + timedelta(days=int(serial))
                    d = dt.date()
                    return d if return_type == "date" else d.strftime(output_format)
            except Exception:
                pass

    # 4) Try common formats (fast)
    # Order matters: prefer_day_first controls parse order for ambiguous ones
    day_first_formats = [
        "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%d %b %Y", "%d %B %Y",
        "%d-%b-%Y", "%d/%b/%Y", "%d-%m-%y", "%d/%m/%y"
    ]
    month_first_formats = [
        "%m-%d-%Y", "%m/%d/%Y", "%m.%d.%Y", "%b %d %Y", "%B %d %Y",
        "%m-%d-%y", "%m/%d/%y"
    ]
    machine_formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"]

    formats = (day_first_formats + month_first_formats + machine_formats) if prefer_day_first else (month_first_formats + day_first_formats + machine_formats)

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            d = dt.date()
            return d if return_type == "date" else d.strftime(output_format)
        except Exception:
            continue

    # 5) Try ISO-like partial matches (e.g., "2020-03")
    try:
        # year-month
        m = re.fullmatch(r"(\d{4})[-/\.](\d{1,2})$", s)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            d = date(y, mo, 1)
            return d if return_type == "date" else d.strftime(output_format)
    except Exception:
        pass

    # 6) Fallback to dateutil (if available)
    try:
        from dateutil.parser import parse as dateutil_parse
        try:
            dt = dateutil_parse(s, dayfirst=prefer_day_first, fuzzy=True)
            d = dt.date()
            return d if return_type == "date" else d.strftime(output_format)
        except Exception:
            pass
    except Exception:
        # dateutil not installed — that's okay, we tried many formats.
        pass

    # 7) failed
    if raise_on_error:
        raise ValueError(f"Unable to parse date: {value!r}")
    return None
