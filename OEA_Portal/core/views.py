import uuid
from .forms import InstallationForm, MetadataFormSet, ProfileForm
#from OEA_Portal.settings import TENANT_ID, SUBSCRIPTION_ID
from OEA_Portal.core.services.BlobService import get_blob_contents
from OEA_Portal.auth.AzureClient import AzureClient
from django.http.response import HttpResponse
from django.views.generic.edit import FormView
from OEA_Portal.core.services.utils import get_config_data\
        , update_config_database, get_all_storage_accounts_in_subscription, get_all_workspaces_in_subscription, is_oea_installed_in_workspace
from OEA_Portal.core.services.ModuleManagementService import get_module_data_for_all_workspaces\
    , delete_module_from_workspace
from django.views.generic.list import ListView
from django.views.generic import TemplateView
from .models import InstallationLogs, TableMetadata
from django.shortcuts import redirect
from OEA_Portal.core.OEAInstaller import OEAInstaller

base_url = 'temp'

class HomeView(TemplateView):
    template_name = 'core/homepage.html'

    def get(self, *args, **kwargs):
        global base_url
        if('base_url' in self.request.GET):
            base_url = self.request.GET['base_url']
        return self.render_to_response({'base_url':base_url})

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

class MetadataAddView(TemplateView):
    template_name = "core/metadata_form.html"

    def get(self, *args, **kwargs):
        formset = MetadataFormSet()
        return self.render_to_response({'metadata_formset': formset, 'base_url':base_url})

    def post(self, *args, **kwargs):
        formset = MetadataFormSet(data=self.request.POST)
        if formset.is_valid():
            for form in formset:
                table_name = form.data.get('table-name')
                col_name = form.cleaned_data.get('column_name')
                col_type = form.cleaned_data.get('column_type')
                pseuodynimization = form.cleaned_data.get('pseuodynimization')
                constraint = form.cleaned_data.get('constraint')
                TableMetadata(
                    table_name=table_name,
                    column_name=col_name,
                    column_type=col_type,
                    constraint=constraint,
                    pseuodynimization=pseuodynimization).save()
            return self.render_to_response({'metadata_formset': MetadataFormSet()})

        return self.render_to_response({'metadata_formset': formset})

class ProfileView(TemplateView):
    template_name = 'core/profile.html'

    def get_context_data(self, **kwargs):
        context = super(ProfileView, self).get_context_data(**kwargs)
        context['base_url'] = base_url
        return context

    def get(self, *args, **kwargs):
        config = get_config_data()
        tenant_id = config['TenantId']
        subscription_id = config['SubscriptionId']
        profile_form = ProfileForm(initial=({'tenant_id':tenant_id, 'subscription_id':subscription_id}))
        return self.render_to_response({'profile_form':profile_form, 'base_url':base_url})

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
        data = get_module_data_for_all_workspaces()
        return self.render_to_response({'data':data})

def delete_module(request):
    config = get_config_data()
    tenant_id = config['TenantId']
    subscription_id = config['SubscriptionId']
    azure_client = AzureClient(tenant_id, subscription_id)
    print([i.name for i in get_all_workspaces_in_subscription(azure_client)])
    print([i.name for i in get_all_storage_accounts_in_subscription(azure_client)])
    print(is_oea_installed_in_workspace(azure_client, 'syn-oea-abhinav4', 'rg-oea-abhinav4'))
    return HttpResponse('hello')