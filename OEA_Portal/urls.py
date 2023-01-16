"""OEA_Portal URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from OEA_Portal.core.views import InstallationFormView, InstallationLogsView, MetadataAddView, \
            MetadataListView, HomeView, read_blob
from django.views.decorators.csrf import csrf_exempt

urlpatterns = [
    path('admin', admin.site.urls),
    path('', csrf_exempt(HomeView.as_view()), name='home'),
    path('home', csrf_exempt(HomeView.as_view()), name='home'),
    path('logs', csrf_exempt(InstallationLogsView.as_view()), name='logs'),
    path('install', csrf_exempt(InstallationFormView.as_view()), name='install'),
    path('metadata', csrf_exempt(MetadataAddView.as_view()), name='metadata'),
    path('metadata_list', csrf_exempt(MetadataListView.as_view()), name='metadata_list'),
    path('read_blob', csrf_exempt(read_blob), name='read_blob')
]
