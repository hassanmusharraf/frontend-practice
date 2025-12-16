from django.urls import path
from .lovs import (
    AdhocPurchaseOrderLineLovApi, LovApiView, StorerKeyByHubLOV, SuppliersByStorerKeyLOV, PurchaseOrderLovApi, 
    PlantByIdLOV, CenterCodeByIdLOV,ConsoleBOLGeneratedByLOV, ConsignmentCreatedBy,HubByStorerKeyLOV, SupplierLOVAPI, AvailableConsolesLOV
    )
from .apis import  AddressBookView, PackagingTypeView, MOTView,FreightForwarderView, GLAccountView, CostCenterCodeView, RejectionView,NotificationAPIView,UserNotificationAPIView


urlpatterns = [
    path('lovs/console/bol-generated-by/', ConsoleBOLGeneratedByLOV.as_view(), name='lovs'),
    path('lovs/consignment/created-by/', ConsignmentCreatedBy.as_view(), name='consignment-created-by'),
    path('lovs/purchase-order/', PurchaseOrderLovApi.as_view(), name='lovs'),
    path('lovs/adhoc/purchase-order-line/by-consignment/', AdhocPurchaseOrderLineLovApi.as_view(), name='lovs'),
    path('lovs/plant-id/', PlantByIdLOV.as_view(), name='plant-id'),
    path('lovs/center-code/', CenterCodeByIdLOV.as_view(), name='plant-id'),
    path('lovs/supplier/', SupplierLOVAPI.as_view(), name='supplier-lovs'),
    path('lovs/available-consoles/', AvailableConsolesLOV.as_view(), name='available-consoles-lov'),
    path('lovs/<str:key>/', LovApiView.as_view(), name='lovs'),
    path('lovs/storer-key/by-hub/', StorerKeyByHubLOV.as_view(), name='storerkey-by-hub'),
    path('lovs/hub/by-storerkey/', HubByStorerKeyLOV.as_view(), name='storerkey-by-hub'),
    path('lovs/suppliers/by-storerkey/', SuppliersByStorerKeyLOV.as_view(), name='suppliers-by-storerkey'),
    path('address/<str:id>/', AddressBookView.as_view(), name='address-book'),
    path('mot/<str:id>/', MOTView.as_view(), name='mot'),
    path('freight-forwarder/<str:id>/', FreightForwarderView.as_view(), name='frieght-forwader'),
    path('packaging-type/<str:id>/', PackagingTypeView.as_view(), name='packaging-type'),
    path('gl-account/<str:id>/', GLAccountView.as_view(), name='gl-account'),
    path('cost-center-code/<str:id>/', CostCenterCodeView.as_view(), name='cost-center-code'),
    path('rejection-code/<str:id>/', RejectionView.as_view(), name='rejection'),
    path('notifications/<str:id>/', NotificationAPIView.as_view(), name='notifications'),
    # path('archive/notifications/', UserNotificationAPIView.as_view(), name='user-notifications'),


    
    
    
    # path('oracle-test/', OracleTestAPI.as_view(), name='oracle-test-db'),
]