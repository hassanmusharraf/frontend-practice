from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static


urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/accounts/', include('accounts.urls')),
    # path('api/adhoc/', include('adhoc.urls')),
    path('api/entities/', include('entities.urls')),
    path('api/operations/', include('operations.urls')),
    path('api/portal/', include('portal.urls')),
    path('api/workflows/', include('workflows.urls')),
]


if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
