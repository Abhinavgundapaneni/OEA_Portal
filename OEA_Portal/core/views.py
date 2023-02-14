import uuid
from .forms import InstallationForm, MetadataFormSet, ProfileForm
from OEA_Portal.core.services.AssetManagementService.asset import BaseOEAAsset
from OEA_Portal.core.services.AssetManagementService.operations import *
from OEA_Portal.auth.AzureClient import AzureClient
from django.http.response import HttpResponse
from OEA_Portal.core.services.utils import *
from django.views.generic.list import ListView
from django.views.generic import TemplateView
from django.shortcuts import redirect
from OEA_Portal.core.OEAInstaller import OEAInstaller

base_url = 'temp'

class HomeView(TemplateView):
    template_name = 'core/homepage.html'
    config = get_config_data()

    def get(self, *args, **kwargs):
        global base_url
        if('base_url' in self.request.GET):
            base_url = self.request.GET['base_url']
        subscriptions = get_all_subscriptions_in_tenant()
        workspaces = get_all_workspaces_in_subscription(AzureClient(self.config['SubscriptionId'], self.config['SubscriptionId']))
        return self.render_to_response({'base_url':self.config['BaseURL'],
        'tenants':['123', '456'],
        'subscriptions':subscriptions,
        'workspaces':workspaces
        })

class DashboardView(TemplateView):
    template_name = 'core/dashboard.html'
    config = get_config_data()

    def get(self, *args, **kwargs):
        azure_client = AzureClient(self.config['SubscriptionId'], self.config['SubscriptionId'])
        workspace = get_workspace_object(azure_client, self.config['WorkspaceName'])
        modules, packages, schemas, version = get_installed_assets_in_workspace(self.config['WorkspaceName'], azure_client)
        return self.render_to_response({'base_url':self.config['BaseURL'],
            'modules':modules,
            'packages':packages,
            'schemas':schemas,
            'oea_version': version,
            'storage_account': workspace.storage_account
        })

class InstallationFormView(TemplateView):
    template_name = 'core/installation_form.html'
    config = get_config_data()

    def get_context_data(self, **kwargs):
        context = super(InstallationFormView, self).get_context_data(**kwargs)
        context['base_url'] = base_url
        return context

    def get(self, *args, **kwargs):
        version_choices = [(x,x) for x in self.config['OEA_Versions']]
        form = InstallationForm(version_choices)
        return self.render_to_response({'form':form})

    def post(self, *args, **kwargs):
        tenant_id = self.config['TenantId']
        subscription_id = self.config['SubscriptionId']
        include_groups = self.request.POST.get('include_groups')
        oea_version = self.request.POST.get('oea_version')
        oea_suffix = self.request.POST.get('oea_suffix')
        location = self.request.POST.get('location')

        request_id = uuid.uuid4()
        oea_installer = OEAInstaller(tenant_id, subscription_id, oea_suffix,oea_version, location, include_groups)
        oea_installer.install(request_id)
        return redirect('home')

class ProfileView(TemplateView):
    template_name = 'core/profile.html'
    config = get_config_data()
    def get_context_data(self, **kwargs):
        context = super(ProfileView, self).get_context_data(**kwargs)
        context['base_url'] = base_url
        return context

    def get(self, *args, **kwargs):
        asset = BaseOEAAsset("Ed-Fi", "edfi", "0.2", "0.7", "module")
        azure_client = AzureClient(self.config['SubscriptionId'], self.config['SubscriptionId'])
        asset.install(azure_client, OEAInstance('syn-oea-abhinav4', 'rg-oea-abhinav4', 'kv-oea-abhinav4', 'stoeaabhinav4'))

    def post(self, *args, **kwargs):
        tenant_id = self.request.POST.get('tenant_id')
        subscription_id = self.request.POST.get('subscription_id')
        update_config_database({'TenantId':tenant_id, 'SubscriptionId':subscription_id})
        profile_form = ProfileForm(initial=({'tenant_id':tenant_id, 'subscription_id':subscription_id}))
        return self.render_to_response({'profile_form':profile_form, 'base_url':base_url})

class InstalledModulesView(TemplateView):
    template_name = "core/installed_modules.html"

    def get_context_data(self, **kwargs):
        context = super(InstalledModulesView, self).get_context_data(**kwargs)
        context['base_url'] = base_url
        return context

    def get(self, *args, **kwargs):
        data = get_installed_assets_in_workspace()
        return self.render_to_response({'data':data})


