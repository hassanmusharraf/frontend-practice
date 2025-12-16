from django.urls import path
from .apis import (ClientView, HubView, SupplierView, ClientUserView, SupplierUserView, OperationsView, StorerKeyView, 
    DangerousGoodAPI, MaterialMasterAPI,MaterialMasterBulkImportAPI)


urlpatterns = [
    path("hub/<str:id>/", HubView.as_view(), name="hub"),
    path("client/<str:id>/", ClientView.as_view(), name="client"),
    path("client-user/<str:id>/", ClientUserView.as_view(), name="client-user"),
    path("operations/<str:id>/", OperationsView.as_view(), name="operations"),
    path("storer-key/<str:id>/", StorerKeyView.as_view(), name="storer-key"),
    path("supplier/<str:id>/", SupplierView.as_view(), name="supplier"),
    path("supplier-user/<str:id>/", SupplierUserView.as_view(), name="supplier-user"),
    path("dangerous-good/<str:name>/", DangerousGoodAPI.as_view(), name="dangerous-good"),
    path("material/bulk_import/", MaterialMasterBulkImportAPI.as_view(), name="material"),
    path("material/<str:product_code>/", MaterialMasterAPI.as_view(), name="material"),
]