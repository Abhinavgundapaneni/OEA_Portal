from django import forms
from django.forms import HiddenInput, formset_factory

CHOICES =(
    ("hash","hash"),
    ("hash-no-lookup","hash-no-lookup"),
    ("no-op","no-op"),
    ("partition-by","partition-by"),
    ("mask","mask"),
)

TYPES = (
    ("string", "string"),
    ("integer", "integer"),
    ("float", "float"),
    ("long", "long"),
    ("object", "object"),
    ("array", "array"),
)

CONSTRAINTS = (
    ("None", "None"),
    ("primary key", "primary key"),
    ("foreign key", "foreign key")
)

class InstallationForm(forms.Form):
    tenant_id = forms.CharField(max_length=50)
    subscription_id = forms.CharField(max_length=50)
    oea_suffix = forms.CharField(max_length=10)
    location = forms.CharField(max_length=15)
    include_groups = forms.BooleanField(required=False, label='Include Groups')

class ColumnMetadata(forms.Form):
    column_name = forms.CharField(max_length=100)
    column_type = forms.ChoiceField(choices=TYPES)
    pseuodynimization = forms.ChoiceField(choices=CHOICES)
    constraint = forms.ChoiceField(choices=CONSTRAINTS)

class BaseMetadataFormset(forms.BaseFormSet):
    def add_fields(self, form, index):
        """ hide ordering and deletion fields """
        super().add_fields(form, index)
        if 'ORDER' in form.fields:
            form.fields['ORDER'].widget = forms.HiddenInput()
        if 'DELETE' in form.fields:
            form.fields['DELETE'].widget = forms.HiddenInput()

MetadataFormSet = formset_factory(ColumnMetadata, formset=BaseMetadataFormset, extra=1, can_delete=True)