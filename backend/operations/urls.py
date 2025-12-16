from django.urls import path
from .apis import (
    PurchaseOrderStatusCountAPI,
    PurchaseOrderBulkCreateAPI,
    PurchaseOrderCreateAPI,
    PurchaseOrderHeaderView,
    PurchaseOrderLineView,
    PurchaseOrderBulkUploadAPI,
    PurchaseOrderHeaderLineView,
    PurchaseOrderBulkUpdateAPI,
    PurchaseOrderLineCountAPI,
    ConsignmentStatusMonthlyCountAPI,
    ConsignmentStatusDonutChartAPI,
    ConsignmentStatusSummaryCountAPI,
    ConsignmentStatusBetweenDaysCountAPI,
    ConsignmentDashboardThisMonth,
    PurchaseOrderUploadHistoryAPI,
    ReceivePackages,AWBAPI,SupplierPurchaseOrders,
    ConsignmentEditCheckAPI,StorerKeyPurchaseOrders
    # AWBAPI,SupplierPurchaseOrders
)
from .consignment_apis import (
    # StageConsignmentDocumentAPI,
    # StageConsignmentAddressAPI,
    # StageConsignmentCreateAPI,
    # StageConsignmentPOLineAPI,
    # StagePackagingAllocationAPI,
    # StageConsignmentPackagingAPI,
    # StageConsignmentGetAPI,
    ConsignmentListAPI,
    ConsignmentStatusUpdateAPI,
    ConsignmentAuditTrailGetAPI,
    ConsignmentStatusCountAPI,
    ConsignmentAssignFFAPI,
    ConsignmentPackageListAPI,
    AdhocConsignmentCreateAPI,
    AdhocConsignmentPackageView,
    AdhocConsignmentAlloationAPI,
    AdhocConsignmentDocumentAPI,
    ConsignmentUpload,
    # StageConsignmentPackagingPOLineAPI,
    # StagePOLineAllocationAPI,
    UserGridPreferencesAPI
)

from .v2_apis.consignment_apis import(
    ConsignmentCreationAPI, ComplianceDetailsAPI, SelectedLinesAPI,
    ConsignmentPackagesAPI, PoLineAllocationAPI, ConsignmentFileUploadAPI, ConsignmentStepHandlerAPI,
    ConsignmentAddressAPI,ConsignmentSummaryCountsAPI, ConsignmentHoverAPI, ConsignmentDgItemsAPI,
    ConsignmentComprehensiveReport, CreateDraftConsignmentAPI, CheckProcessingPOs)
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'consignment-lines', SelectedLinesAPI, basename='consignment-lines')


urlpatterns = [
    
    ## PO Import/Upload APIs
    path("purchase-order/bulk-import/", PurchaseOrderBulkCreateAPI.as_view(), name="purchase-order-bulk-create"),
    path("purchase-order/bulk-upload/<str:id>/", PurchaseOrderBulkUploadAPI.as_view(), name="purchase-order-excel-upload"),
    path("purchase-order/bulk-update/", PurchaseOrderBulkUpdateAPI.as_view(), name="purchase-order-bulk-update"),
    path("purchase-order/bulkupload/history/", PurchaseOrderUploadHistoryAPI.as_view(), name="purchase-order-bulk-upload-history"),
    
    # Purchase Order Dashboard
    path("purchase-order/dashboard/", ConsignmentDashboardThisMonth.as_view(), name="purchase-order-dashboard"),
    path("purchase-order/status-count/", PurchaseOrderStatusCountAPI.as_view(), name="purchase-order-status-count"),
    path("purchase-order-line/status-count/<str:po_no>/", PurchaseOrderLineCountAPI.as_view(), name="po-line-status-count"),

    ## Purchase Order
    path("purchase-order/header/<str:ref_no>/", PurchaseOrderHeaderView.as_view(), name="purchase-order-header"),
    path("purchase-order/line/<str:ref_no>/", PurchaseOrderLineView.as_view(), name="purchase-order-line"),
    path("purchase-order/update/", PurchaseOrderHeaderLineView.as_view(), name="purchase-order-header-line"),
    path('purchase-orders/by-supplier/', SupplierPurchaseOrders.as_view(), name='supplier-purchase-orders'),
    path('purchase-orders/by-storerkey/', StorerKeyPurchaseOrders.as_view(), name='storerkey-purchase-orders'),

    ## Staging Consignment
    # path("stage/consignment-create/", StageConsignmentCreateAPI.as_view(), name="stage-consignment-create"),
    # path("stage/consignment-get/", StageConsignmentGetAPI.as_view(), name="stage-consignment-get"),
    # path("stage/consignment/address/", StageConsignmentAddressAPI.as_view(), name="stage-consignment-address"),
    # path("stage/consignment/po-lines/packages/", StageConsignmentPackagingPOLineAPI.as_view(), name="stage-consignment-packaging-po-line-"),
    # path("stage/consignment/po-lines/<str:id>/", StageConsignmentPOLineAPI.as_view(), name="stage-consignment-poline"),
    # path("stage/consignment/packages/<str:id>/", StageConsignmentPackagingAPI.as_view(), name="stage-consignment-packaging"),
    # path("stage/consignment/allocation/<str:id>/", StagePackagingAllocationAPI.as_view(), name="stage-consignment-allocation"),
    # path("stage/consignment/allocation-po-line/<str:id>/", StagePOLineAllocationAPI.as_view(), name="stage-consignment-allocation"),
    # path("stage/consignment/documents/<str:id>/", StageConsignmentDocumentAPI.as_view(), name="stage-consignment-document"),

    
    ## Consignment Dashboard
    path("consignment/monthly-count/", ConsignmentStatusMonthlyCountAPI.as_view(), name="consignment-monthly-count"),
    path("consignment/donut-chart/", ConsignmentStatusDonutChartAPI.as_view(), name="consignment-last-month-comparison"),
    path("consignment/status-summary/", ConsignmentStatusSummaryCountAPI.as_view(), name="consignment-status-summary"),
    # path("consignment-po/<str:poId>/", PurchaseorderConsignmentsAPI.as_view(), name="purchase-order-consignmnets"),
    
    ## Consignment
    path("consignment/create-draft/", CreateDraftConsignmentAPI.as_view(), name="create-draft-consignment"),
    path("consignment/check-processing-po/<str:id>/", CheckProcessingPOs.as_view(), name="check-processing-po"),
    path("consignment/create/", ConsignmentCreationAPI.as_view(), name="consignment-creation"),
    path("consignment/summary-counts/<str:id>/", ConsignmentSummaryCountsAPI.as_view(), name="consignment-summary-counts"),
    path("consignment/hover/<str:id>/", ConsignmentHoverAPI.as_view(), name="consignment-hover"),
    path("consignment/dg-items/<str:id>/", ConsignmentDgItemsAPI.as_view(), name="consignment-dg-items"),
    path("consignment/packages/<str:id>/", ConsignmentPackagesAPI.as_view(), name="consignment-packaging"),
    path("consignment/packages/allocation/<str:id>/", PoLineAllocationAPI.as_view(), name="consignment-package-allocation"),
    path("consignment/compliance-details/", ComplianceDetailsAPI.as_view(), name="consignment-compliance-details"),
    path("consignment/file-upload/<str:id>/",ConsignmentFileUploadAPI.as_view(),name = 'consignment-file-upload'),
    path("consignment/step-handler/<str:id>/",ConsignmentStepHandlerAPI.as_view(),name = 'consignment-step-handler'),
    path("consignment/address/<str:id>/",ConsignmentAddressAPI.as_view(),name = 'consignment-address'),
    path("consignment/days-between-status-list/", ConsignmentStatusBetweenDaysCountAPI.as_view(), name="consignment-days-between-status-list"),
    path("consignment/status-count/", ConsignmentStatusCountAPI.as_view(), name="consignment-status-count"),
    path("consignment/audit-trail/<str:id>/", ConsignmentAuditTrailGetAPI.as_view(), name="consignment-audit-trail-get"),
    path("consignment/status-update/", ConsignmentStatusUpdateAPI.as_view(), name="consignment-status-update"),
    path("consignment/assign-ff/", ConsignmentAssignFFAPI.as_view(), name="consignment-assign-ff"),
    path("consignment/packages-list/<str:id>/", ConsignmentPackageListAPI.as_view(), name="consignment-package-list-api"),
    # path("consignment/edit-check/<str:id>/",ConsignmentEditCheckAPI.as_view(),name = 'consignment-edit-check'),
    path("consignment/<str:id>/", ConsignmentListAPI.as_view(), name="consignment-create"),
    path("upload/consignment/", ConsignmentUpload.as_view(), name="consignment-upload"),
    path("consignment/<str:id>/awb-file/", AWBAPI.as_view(), name="consignment-awb-file"),

    ## Adhoc Consignment
    path("adhoc/consignment/<str:id>/", AdhocConsignmentCreateAPI.as_view(), name="adhoc-consignment-create"),
    path("adhoc/consignment/packages/<str:id>/", AdhocConsignmentPackageView.as_view(), name="adhoc-consignment-package"),
    path("adhoc/consignment/allocation/<str:package_id>/", AdhocConsignmentAlloationAPI.as_view(), name="adhoc-consignment-allocation"),
    path("adhoc/consignment/documents/<str:id>/", AdhocConsignmentDocumentAPI.as_view(), name="adhoc-consignment-document"),


    ## Services
    path("usergridpreferences/",UserGridPreferencesAPI.as_view(),name = 'user-grid-preferences'),
    path("receive/package/",ReceivePackages.as_view(),name = 'receive-packages'),


    ## Reports
    path("comprehensive-report/consignment/", ConsignmentComprehensiveReport.as_view(), name="consignment-comprehensive-report"),
    
    
    ## deprecated
    # path("purchase-order/create/", PurchaseOrderCreateAPI.as_view(), name="purchase-order-create"),
    
]

urlpatterns += router.urls
