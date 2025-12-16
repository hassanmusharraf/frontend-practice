from django.urls import path
from .apis import LoginAPI, RecentlySearchView, GlobalSearchView, PreferenceAPI, ChangePasswordAPI


urlpatterns = [
    path("login/", LoginAPI.as_view(), name="login"),
    path("change-password/", ChangePasswordAPI.as_view(), name="change-password"),
    path("preference/", PreferenceAPI.as_view(), name="preference"),
    path("recently-search/<str:id>/", RecentlySearchView.as_view(), name="recently-search"),
    path("global-search/<str:id>/", GlobalSearchView.as_view(), name="global-search"),
    
]