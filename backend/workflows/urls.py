from django.urls import path
from .apis import *
from .bol_xml_files import *

urlpatterns = [
    path('get-free-consignments/<str:console_id>/', GetFreeConsignmentsAPI.as_view(), name='get-free-consignments'),
    path('console/add-consignments/', AddConsignmentsToConsoleAPI.as_view(), name='add-consignments'),
    path("console/assign-ff/", ConsoleFFAssignAPI.as_view(), name="console-assign-ff"),
    path("console/meta-data/<str:id>/", ConsoleMetaData.as_view(), name="console-meta-data"),
    path('console/pickup-reject/', ConsolePickupReject.as_view(), name='console-pickup-reject'),
    path('console/<str:id>/', ConsoleView.as_view(), name='console-view'),

    
    # path('generate-bol/<str:console_id>/', BOLGenerateAPI.as_view(), name='generate-bol'),
    path('generate-xml/<str:console_id>/', XMLGenerateAPI.as_view(), name='generate-xml'),
    path('generate-bol-html-v2/<str:id>/', BOLGenerateHTMLV2API.as_view(), name='generate-bol-html'),
    path('console-bol/<str:id>/', ConsoleBOL.as_view(), name='console-bol'),
    # path('generate-bol-html/<str:console_id>/', BOLGenerateHTMLAPI.as_view(), name='generate-bol-html'),
]