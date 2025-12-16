from rest_framework.response import Response
from rest_framework.views import APIView
from django.db import models
from uuid import uuid4
from django.core.exceptions import ValidationError
from .constants import POST, PUT, DELETE, GET, GETALL
from django.db.models import Q
from rest_framework import serializers
from django.db.models import ProtectedError
from core.response import StandardResponse
from rest_framework import serializers


class BaseModel(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        abstract = True
        # ordering = ("id",)


class BaseAPIView(APIView):
    allowed_methods = [GET, GETALL, POST, PUT, DELETE]
    search_ignore_fields = []
    specific_fields = []
    archive_in_delete = False
    enable_specific_search = False
    distinct_fields = []

    def __init__(self):
        self.model = self.get_model()
        self.serializer = self.get_serializer_class()
        self.lookup = self.get_lookup()
        specific_fields = self.get_specific_fields()
        self.query_set = self.get_queryset()
        # self.get_single_query = self.get_queryset()
        self.order = self.get_order()

    def get_lookup(self):
        try:
            return self.lookup_field
        except BaseException:
            return "id"

    def get_serializer_class(self):
        return self.serializer_class

    def get_specific_fields(self):
        if self.specific_fields:
            return self.specific_fields
        return []

    def get_single_obj_serializer(self):
        try:
            return self.single_obj_serializer
        except BaseException:
            return self.get_serializer_class()

    def get_model(self):
        return self.model

    def get_order(self):
        try:
            return self.order
        except BaseException:
            return "-created_at"

    def get_queryset(self):
        try:
            return self.query_set.order_by(self.get_order())
        except BaseException:
            return self.model.objects.all().order_by(self.get_order())

    def get_post_serializer(self):
        try:
            return self.post_serializer
        except BaseException:
            return self.get_serializer_class()

    def get_put_serializer(self):
        try:
            return self.put_serializer
        except BaseException:
            try:
                return self.post_serializer
            except BaseException:
                return self.get_serializer_class()

    def get_extra_list_data(self):
        try:
            return self.extra_list_data
        except BaseException:
            return {}

    def check_if_method_allowed(self, method):
        if method not in self.allowed_methods:
            if method is GETALL:
                return Response({"msg": "Not Found"}, status=404)
            return Response({"msg": "Method not allowed"}, status=405)

    def search_query_filter(self, search_query):
        if not search_query:
            return Q()

        fields = []
        if self.specific_fields and self.enable_specific_search:
            fields = self.specific_fields
        else:
            fields = [f.name for f in self.model._meta.fields if not f.is_relation]
            if not self.enable_specific_search:
                fields.extend(self.specific_fields)
            if hasattr(self, "related_models"):
                for field_name, model_class in self.related_models.items():
                    related_fields = [
                        f.name for f in model_class._meta.fields if not f.is_relation
                    ]
                    fields.extend(
                        [f"{field_name}__{field}" for field in related_fields]
                    )

        search_query_filter = Q()
        for field in fields:
            if field not in self.search_ignore_fields:
                search_query_filter |= Q(**{f"{field}__icontains": search_query})
        return search_query_filter

    def get(self, request, id=None, *args, **kwargs):
        if id == "list":
            if not GETALL in self.allowed_methods:
                return Response({"msg": "Not Found"}, status=404)
            pg = request.GET.get("pageIndex") or 0
            limit = request.GET.get("pageSize") or 20
            search = request.GET.get("q", "")
            queryset = self.get_queryset()
            if self.archive_in_delete:
                queryset = queryset.filter(is_deleted=False)
            if search:
                queryset = queryset.filter(
                    self.search_query_filter(search_query=search)
                )
            if self.distinct_fields != []:
                queryset.distinct(*self.distinct_fields)
            for param in self.request.query_params:
                if not (
                    param in ["pageIndex", "pageSize", "q"] or param in self.search_ignore_fields
                ):
                    param_value = self.request.query_params[param]
                    if self.request.query_params[param] == "true":
                        param_value = True
                    if self.request.query_params[param] == "false":
                        param_value = False
                    queryset = queryset.filter(**{param: param_value})
            count = queryset.count()
            objs = queryset[int(pg) * int(limit): (int(pg) + 1) * int(limit)]
            return StandardResponse(
                success=True,
                data=self.serializer(objs, many=True).data,
                count=count,
                status=200
            )
        else:
            if not GET in self.allowed_methods:
                return StandardResponse(
                    success=False,
                    errors={"msg": "Method not allowed"},
                    status=405
                )
            try:
                self.serializer = self.get_single_obj_serializer()
                return StandardResponse(
                    success=True,
                    data=self.serializer(self.model.objects.get(id=id)).data,
                    status=200
                )
            except (self.model.DoesNotExist, ValidationError):
                return StandardResponse(
                    success=False,
                    errors={
                        "msg": str(self.model._meta).split(".")[1]
                        + " object does not exists, Invalid ID"
                    },
                    status=400
                )

    def post(self, request, *args, **kwargs):
        if not POST in self.allowed_methods:
            return Response({"msg": "Method not allowed"}, status=405)
        serializer = self.get_post_serializer()
        serializer = serializer(data=request.data)
        if serializer.is_valid():
            obj = serializer.save()
            return Response(
                data={
                    "msg": "Saved Successfully",
                    "id": obj.id,
                    "data": serializer.data,
                },
                status=201,
            )
        return Response(data=serializer.errors, status=400)

    def put(self, request, id=None, *args, **kwargs):
        if not PUT in self.allowed_methods:
            return Response({"msg": "Method not allowed"}, status=405)
        filter = {self.lookup: id}
        try:
            obj = self.model.objects.get(**filter)
        except (self.model.DoesNotExist, ValidationError):
            return Response(
                data={
                    "msg": str(self.model._meta).split(".")[1]
                    + " object does not exists"
                },
                status=400,
            )
        serializer = self.get_put_serializer()
        serializer = serializer(obj, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(data={"msg": "Saved Successfully"}, status=202)
        return Response(data=serializer.errors, status=400)

    def delete(self, request, id=None, *args, **kwargs):
        if not DELETE in self.allowed_methods:
            return Response({"msg": "Method not allowed"}, status=405)

        filter = {self.lookup: id}
        try:
            obj = self.model.objects.get(**filter)
            if self.archive_in_delete:
                obj.is_deleted = True
                obj.save()
            else:
                obj.delete()
            return Response(
                data={"msg": "Deleted successfully"},
                status=200,
            )
        except (self.model.DoesNotExist, ValidationError):
            return Response(
                data={
                    "msg": str(self.model._meta).split(".")[1]
                    + " object does not exist"
                },
                status=400,
            )
        except ProtectedError as e:
            related_models = {
                str(related._meta.verbose_name_plural).capitalize()
                for related in e.protected_objects
            }
            return Response(
                data={
                    "msg": f"Cannot delete {str(self.model._meta.verbose_name).capitalize()} as it is protected by related {', '.join(related_models)}"
                },
                status=400,
            )


def get_base_model_serializer(model, fields="__all__"):
    def create_meta_class():
        return type("Meta", (), {"model": model, "fields": fields})

    MetaClass = create_meta_class()

    class ModelSerializer(serializers.ModelSerializer):
        Meta = MetaClass

    return ModelSerializer
