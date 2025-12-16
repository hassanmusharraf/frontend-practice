from django.db.models import Q
import json
from django.db.models import F,Count
from django.db.models import Func, IntegerField
from django.db.models.expressions import RawSQL
from datetime import datetime, timedelta
from .utils import get_utc_range_for_date

class SearchAndFilterMixin:
    operator_mapping = {
        '=': '',
        '!=': '__ne',
        '>': '__gt',
        '<': '__lt',
        '>=': '__gte',
        '<=': '__lte',
        'contains': '__icontains',
        "startsWith": '__istartswith',
        "endsWith": '__iendswith'
    }
    
    def search_query_filter(self, fields, search):
        if not search:
            return Q()
        
        search_query_filter = Q()
        for field in fields:
            search_query_filter |= Q(**{f"{field}__icontains": search})
        return search_query_filter


    def get_filtered_queryset(self, fields, base_queryset, filters):
            filtered_queryset = base_queryset.filter(filters).values(*fields)
            return list(filtered_queryset)
    
    
    def apply_search(self, fields, queryset, search):
        if search:
            search_filter = self.search_query_filter(fields, search)
            queryset_list = queryset.filter(search_filter)
        return queryset_list

    def make_filters_list(self, request):
        filters = []
        i = 0
        while f"filters[{i}][column]" in request.GET:
            column = request.GET.get(f"filters[{i}][column]")
            operator = request.GET.get(f"filters[{i}][operator]")
            value = request.GET.get(f"filters[{i}][value]")
            field = request.GET.get(f"filters[{i}][field]")
            
            if value is None:
                value = [value for key, value in request.GET.items() if key.startswith(f"filters[{i}][value][")]
                
            if column and operator and value:
                filters.append({
                    "column": column,
                    "operator": operator,
                    "value": value,
                    "field": field
                })
            i += 1
        return  filters
    
    def appy_dynamic_filter(self, filters):
        
        query_filter = Q()
        for filter_item in filters:
            column = filter_item["column"]
            field = filter_item["field"]
            operator = filter_item["operator"]
            value = filter_item["value"]
            if value == "true":
                value = True
            if value == "false":
                value = False
            if isinstance(value, list):
                orm_operator = '__in'
            else:
                orm_operator = self.operator_mapping.get(operator, '')
            
            if field and field != column:
                filter_key = f"{column}__{field}{orm_operator}"

            elif column in ['created_at','requested_pickup_datetime','actual_pickup_datetime']:
                query_filter &= self.apply_date_filter(value,orm_operator,column,query_filter)
                continue
            else:
                filter_key = f"{column}{orm_operator}"

            query_filter &= Q(**{filter_key: value})
            
        return query_filter


    def filter_annotations_by_fields(self, queryset, key, fields):

        annotation_mappings = {        
            "operations": {
            "user__role": "role",
            "user__username": "username",
            "user__name": "name",
            },

            "consignment": {
                # "purchase_order__reference_number" : "PO",
                "supplier__name" : "supplier__name",
                "client__name" : "client__name",
            }
        }

        annotations = annotation_mappings.get(key, {})

        if annotations:
            queryset = queryset.annotate(**{new: F(old) for old, new in annotations.items()})
            fields = [annotations.get(field, field) for field in fields] 

        return list(queryset.values(*fields))

    def filter_measured_annotations(self, queryset, key):

        count_annotation_fields = {
            # "packages":"packages",
        }

        jsonb_length_annotation_fields = {
            # "packages":"packages",
        }

        annotations_names = {
            # "packages":"packages_count",
        }

        annotations = {}
    
        if key not in annotations_names:
            return queryset
    
        if key in jsonb_length_annotation_fields:

            ## This is for MSSQL
            raw_sql = f"(SELECT COUNT(*) FROM OPENJSON({key}))"
            annotations[annotations_names[key]] = RawSQL(raw_sql, []) 
            
            ## This was for postgres 
            # annotations[annotations_names[key]] = JSONArrayLength(jsonb_length_annotation_fields[key]) 

        if key in count_annotation_fields:
            annotations[annotations_names[key]] = Count(count_annotation_fields[key]) 
            
        if annotations:
            queryset = queryset.annotate(**annotations)
            
        return queryset
        
    def apply_date_filter(self, date_str, operator, field_name, existing_filter):

        try:
            start_date = datetime.strptime(date_str, "%Y-%m-%d")
            next_day = start_date + timedelta(days=1)

            if operator == '':
                # Exact match for the whole day
                existing_filter &= Q(**{
                    f"{field_name}__gte": start_date,
                    f"{field_name}__lt": next_day
                })

            elif operator == "__gt":
                # Greater than the end of the specified day
                existing_filter &= Q(**{f"{field_name}__gt": next_day})

            elif operator == "__gte":
                existing_filter &= Q(**{f"{field_name}__gte": start_date})

            elif operator == "__lt":
                existing_filter &= Q(**{f"{field_name}__lt": start_date})

            elif operator == "__lte":
                # Less than or equal to the end of the specified day
                existing_filter &= Q(**{f"{field_name}__lt": next_day})

            else:
                # Fallback to exact-day filtering
                existing_filter &= Q(**{
                    f"{field_name}__gte": start_date,
                    f"{field_name}__lt": next_day
                })

        except ValueError:
            # Ignore invalid date formats
            pass

        return existing_filter

class PaginationMixin:
    
    def paginate_results(self, queryset_list, page_index, page_size):
        start = int(page_index) * int(page_size)
        end = (int(page_index) + 1) * int(page_size)
        return queryset_list[start:end]





# Custom annotation for jsonb_array_length()
class JSONArrayLength(Func):
    function = 'jsonb_array_length'
    output_field = IntegerField()
