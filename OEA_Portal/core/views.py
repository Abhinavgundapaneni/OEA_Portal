import uuid
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from .forms import InstallationForm, MetadataFormSet
from django.views.generic.edit import FormView
from django.views.generic.list import ListView
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
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

class InstallationLogsView(ListView):
    template_name = 'core/installation_logs.html'
    model = InstallationLogs

    def get_queryset(self):
        request_id = self.request.session['request_id']
        return InstallationLogs.objects.filter(request_id = request_id)

class InstallationFormView(FormView):
    form_class = InstallationForm
    template_name = 'core/installation_form.html'

    def get_context_data(self, **kwargs):
        context = super(InstallationFormView, self).get_context_data(**kwargs)
        context['base_url'] = base_url
        return context

    def form_valid(self, form):
        tenant_id = form.cleaned_data.get('tenant_id')
        subscription_id = form.cleaned_data.get('subscription_id')
        include_groups = form.cleaned_data.get('include_groups')
        oea_suffix = form.cleaned_data.get('oea_suffix')
        location = form.cleaned_data.get('location')

        request_id = uuid.uuid4()
        oea_installer = OEAInstaller(tenant_id, subscription_id, oea_suffix, location, include_groups)
        # oea_installer.install(request_id)
        #self.request.session['request_id'] = str(request_id)
        oea_installer.install_edfi_module()
        return redirect('logs')

class MetadataAddView(TemplateView):
    template_name = "core/metadata_form.html"

    def get(self, *args, **kwargs):
        formset = MetadataFormSet()
        return self.render_to_response({'metadata_formset': formset, 'base_url':base_url})

    @method_decorator(csrf_exempt)
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

class MetadataListView(ListView):
    model = TableMetadata
    template_name = 'core/metadata_list.html'

    def get(self, *args, **kwargs):
        return self.render_to_response({'base_url':base_url})
