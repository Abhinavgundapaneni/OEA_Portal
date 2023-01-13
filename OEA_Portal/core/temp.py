import os
from base64 import b64encode
from OEA_Portal.settings import BASE_DIR
import uuid
import zipfile
import urllib.request
import secrets
from celery import Celery, shared_task, current_app, Task
from .models import InstallationLogs
from OEA_Portal.auth.AzureClient import AzureClient
from OEA_Portal.core.services.AzureResourceProvisionService import AzureResourceProvisionService
from OEA_Portal.core.services.SynapseManagementService import SynapseManagementService
import logging
"""tenant_id = tenant_id
subscription_id = subscription_id
location = location
tags = tags
include_groups = include_groups
framework_path_relative = f"{BASE_DIR}/temp/OEA_v0.7/framework/synapse".replace('\\', '/')
framework_zip_url = "https://github.com/microsoft/OpenEduAnalytics/releases/download/v0.7/OEA_v0.7.zip"
#todo: Find way to get signed-in user id using python sdk."""
user_object_id = '34b26f30-cbfc-47ec-9131-27fef4433705'
"""resource_group_name = f"rg-oea-{oea_suffix}"
storage_account_name = f"stoea{oea_suffix}"
keyvault_name = f"kv-oea-{oea_suffix}"
appinsights_name = f"appi-oea-{oea_suffix}"
synapse_workspace_name = f"syn-oea-{oea_suffix}""""
containers = ['oea', 'stage1', 'stage2', 'stage3']
dirs = ['stage1/Transactional','stage2/Ingested','stage2/Refined','oea/sandboxes/sandbox1/stage1/Transactional',\
    'oea/sandboxes/sandbox1/stage2/Ingested','oea/sandboxes/sandbox1/stage2/Refined','oea/sandboxes/sandbox1/stage3',\
        'oea/dev/stage1/Transactional','oea/dev/stage2/Ingested','oea/dev/stage2/Refined','oea/dev/stage3']
storage_account_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}"
global_admins_name = None
ds_group_name = None
eds_group_name = None
de_group_name = None
logger = logging.getLogger('OEAInstaller')

def log_entry_to_db(request_id, action, message):
    InstallationLogs.objects.create(request_id=request_id, action=action, message=message)


def replace_strings(file_path, storage_account_name, keyvault_name, synapse_workspace_name):
    with open(file_path) as f:
        data = f.read()
        data = data.replace('yourkeyvault', keyvault_name)\
                    .replace('yourstorageaccount', storage_account_name)\
                    .replace('yoursynapseworkspace', synapse_workspace_name)
    with open(file_path, 'wt') as f:
        f.write(data)

def verify_permissions(azure_client:AzureClient, resource_provision_service:AzureResourceProvisionService):
    """ Check if user has "Owner" Permission on the subscription, fail if not """
    owner_role_def = resource_provision_service.get_role('Owner', f"/subscriptions/{azure_client.subscription_id}")
    owner_role_assignments = [role_assignment for role_assignment in azure_client.get_authorization_client().role_assignments.list(filter=f'principalId eq \'{user_object_id}\'') if role_assignment.role_definition_id == owner_role_def.id]
    if(len(owner_role_assignments) == 0):
        logger.error("--> Setup failed! The user does not have the \"Owner\" Permission on the Azure subscription")
        raise PermissionError("User does not enough permissions.")

def get_container_resourceId(azure_client:AzureClient, resource_group_name, storage_account_name,  container):
    """ Returns the Resource Id of the given container """
    return f"/subscriptions/{azure_client.subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}/blobServices/default/containers/{container}"

def create_synapse_architecture(azure_resource_provision_service:AzureResourceProvisionService, synapse_management_service:SynapseManagementService, synapse_workspace_name, storage_account_name):
    synapse_workspace_object = azure_resource_provision_service.create_synapse_workspace(synapse_workspace_name, storage_account_name)
    azure_resource_provision_service.create_role_assignment('Storage Blob Data Contributor', storage_account_id, synapse_workspace_object.identity.principal_id)

    synapse_management_service.add_firewall_rule_for_synapse('allowAll', '0.0.0.0', '255.255.255.255', synapse_workspace_name)
    synapse_management_service.create_spark_pool(synapse_workspace_name, "spark3p2sm",
            {
                "node_size": "small",
                "max_node_count": 5
            }
        )
    synapse_management_service.create_spark_pool(synapse_workspace_name, "spark3p2med",
            {
                "node_size": "medium",
                "max_node_count": 10
            }
        )

def install_linked_services(request_id, synapse_management_service:SynapseManagementService, framework_path_relative, synapse_workspace_name):
    if(os.path.isdir(f'{framework_path_relative}/linkedService/') is False):
        log_entry_to_db(request_id, 'create', 'No Linked Service to Install.')
        return
    linked_services = os.listdir(f'{framework_path_relative}/linkedService/')
    for ls in linked_services:
        try:
            replace_strings(f'{framework_path_relative}/linkedService/{ls}')
            synapse_management_service.create_linked_service(synapse_workspace_name, ls.split('.')[0], f'{framework_path_relative}/linkedService/{ls}')
        except Exception as e:
            log_entry_to_db(request_id, 'error', f"Failed to install the Linked Service - {ls.split('.')[0]} : {str(e)}")

def install_datasets(request_id, synapse_management_service:SynapseManagementService, framework_path_relative, synapse_workspace_name):
    if(os.path.isdir(f'{framework_path_relative}/dataset/') is False):
        log_entry_to_db(request_id, 'create', 'No Dataset to Install.')
        return
    datasets = os.listdir(f'{framework_path_relative}/dataset/')
    for dataset in datasets:
        try:
            replace_strings(f'{framework_path_relative}/dataset/{dataset}')
            synapse_management_service.create_dataset(synapse_workspace_name, dataset.split('.')[0], f'{framework_path_relative}/dataset/{dataset}')
        except Exception as e:
            log_entry_to_db(request_id, 'error', f"Failed to install the Dataset - {dataset.split('.')[0]} : {str(e)}")

def install_notebooks(request_id, synapse_management_service:SynapseManagementService, framework_path_relative, synapse_workspace_name):
    if(os.path.isdir(f'{framework_path_relative}/notebook/') is False):
        log_entry_to_db(request_id, 'create', 'No Notebook to Install.')
        return
    notebooks = os.listdir(f'{framework_path_relative}/notebook/')
    for notebook in notebooks:
        try:
            replace_strings(f"{framework_path_relative}/notebook/{notebook}")
            synapse_management_service.create_notebook(f"{framework_path_relative}/notebook/{notebook}", synapse_workspace_name)
        except Exception as e:
            log_entry_to_db(request_id, 'error', f"Failed to install the Notebook - {notebook.split('.')[0]} : {str(e)}")

def install_pipelines(request_id, synapse_management_service:SynapseManagementService, framework_path_relative, synapse_workspace_name):
    if(os.path.isdir(f'{framework_path_relative}/pipeline/') is False):
        log_entry_to_db(request_id, 'create', 'No Pipelines to Install.')
        return
    pipelines = [item for item in os.listdir(f'{framework_path_relative}/pipeline/') if os.path.isfile(f'{framework_path_relative}/pipeline/{item}')]
    for pipeline in pipelines:
        try:
            replace_strings(f'{framework_path_relative}/pipeline/{pipeline}')
            synapse_management_service.create_or_update_pipeline(synapse_workspace_name, f'{framework_path_relative}/pipeline/{pipeline}', pipeline.split('.')[0])
        except Exception as e:
            log_entry_to_db(request_id, 'error', f"Failed to install the Pipeline - {pipeline.split('.')[0]} : {str(e)}")

def install_dataflows(request_id, synapse_management_service:SynapseManagementService, framework_path_relative, synapse_workspace_name):
    if(os.path.isdir(f'{framework_path_relative}/dataflow/') is False):
        log_entry_to_db(request_id, 'create', 'No Dataflows to Install.')
        return
    dataflows = [item for item in os.listdir(f'{framework_path_relative}/dataflow/') if os.path.isfile(f'{framework_path_relative}/pipeline/{item}')]
    for dataflow in dataflows:
        try:
            replace_strings(f'{framework_path_relative}/dataflow/{dataflow}')
            synapse_management_service.create_or_update_dataflow(synapse_workspace_name, f'{framework_path_relative}/dataflow/{dataflow}', dataflow.split('.')[0])
        except Exception as e:
            log_entry_to_db(request_id, 'error', f"Failed to install the Dataflow - {dataflow.split('.')[0]} : {str(e)}")


def create_aad_groups(provision_resource_service:AzureResourceProvisionService, resource_group_name):
    #todo: Migrate this step to use Python SDK.
    os.system(f"az ad group create --display-name \"{global_admins_name}\" --mail-nickname 'EduAnalyticsGlobalAdmins'")
    os.system(f"az ad group owner add --group \"{global_admins_name}\" --owner-object-id {user_object_id}")
    global_admins_id = os.popen(f"az ad group show --group \"{global_admins_name}\" --query id --output tsv").read()[:-1]

    os.system(f"az ad group create --display-name \"{ds_group_name}\" --mail-nickname 'EduAnalyticsDataScientists'")
    os.system(f"az ad group owner add --group \"{ds_group_name}\" --owner-object-id {user_object_id}")
    data_scientists_id = os.popen(f"az ad group show --group \"{ds_group_name}\" --query id --output tsv").read()[:-1]

    os.system(f"az ad group create --display-name \"{de_group_name}\" --mail-nickname 'EduAnalyticsDataEngineers' -o none")
    os.system(f"az ad group owner add --group \"{de_group_name}\" --owner-object-id {user_object_id} -o none")
    data_engineers_id = os.popen(f"az ad group show --group \"{de_group_name}\" --query id --output tsv").read()[:-1]

    os.system(f"az ad group create --display-name \"{eds_group_name}\" --mail-nickname 'EduAnalyticsExternalDataScientists' -o none")
    os.system(f"az ad group owner add --group \"{eds_group_name}\" --owner-object-id {user_object_id} -o none")
    external_data_scientists_id = os.popen(f"az ad group show --group \"{eds_group_name}\" --query id --output tsv").read()[:-1]

    create_role_assignments_to_groups(provision_resource_service, resource_group_name, global_admins_id, data_scientists_id, external_data_scientists_id, data_engineers_id)

def create_role_assignments_to_groups(provision_resource_service:AzureResourceProvisionService, resource_group_name, global_admins_id, data_scientists_id, external_data_scientists_id, data_engineers_id):
    provision_resource_service.create_role_assignment('Owner', f"/subscriptions/{provision_resource_service.azure_client.subscription_id}/resourceGroups/{resource_group_name}/", global_admins_id)

    # Assign "Storage Blob Data Contributor" to security groups to allow users to query data via Synapse studio
    provision_resource_service.create_role_assignment('Storage Blob Data Contributor', storage_account_id, global_admins_id)
    provision_resource_service.create_role_assignment('Storage Blob Data Contributor', storage_account_id, data_scientists_id)
    provision_resource_service.create_role_assignment('Storage Blob Data Contributor', storage_account_id, data_engineers_id)

    # Assign limited access to specific containers for the external data scientists
    provision_resource_service.create_role_assignment('Storage Blob Data Contributor', get_container_resourceId('stage2'), external_data_scientists_id)
    provision_resource_service.create_role_assignment('Storage Blob Data Contributor', get_container_resourceId('stage3'), external_data_scientists_id)
    provision_resource_service.create_role_assignment('Storage Blob Data Contributor', get_container_resourceId('oea'), external_data_scientists_id)
    provision_resource_service.create_role_assignment('Reader', storage_account_id, data_engineers_id)

def download_and_extract_framework(framework_zip_url):
    zip_path, _ = urllib.request.urlretrieve(framework_zip_url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(f"{BASE_DIR}/temp")

def run(request_id=None):
    if request_id is None:
        request_id = uuid.uuid4()
    download_and_extract_framework()

    azure_client = AzureClient(tenant_id, subscription_id, location=location, default_tags=tags)
    azure_resource_provision_service = AzureResourceProvisionService(azure_client)
    synapse_management_service = SynapseManagementService(azure_client, synapse_workspace_name, resource_group_name)

    log_entry_to_db(request_id, 'verify', 'Verifying if the user has Owner permissions or not.')
    verify_permissions(azure_client, azure_resource_provision_service)

    log_entry_to_db(request_id, 'create', f'Creating Resource group in Azure resource group - {resource_group_name}')
    azure_resource_provision_service.create_resource_group(resource_group_name)

    log_entry_to_db(request_id, 'create', f'Creating storage accounts and containers (Along with directories) - {storage_account_name}')
    storage_account_object = azure_resource_provision_service.create_storage_account(storage_account_name)
    azure_resource_provision_service.create_containers_and_directories(storage_account_name, containers, dirs)

    log_entry_to_db(request_id, 'create', f'Creating synapse architecture in Azure subscription - {synapse_workspace_name}')
    create_synapse_architecture(azure_resource_provision_service, synapse_management_service)

    access_policy_for_synapse = { 'tenant_id': tenant_id, 'object_id': synapse_workspace_object.identity.principal_id,
                                        'permissions': { 'secrets': ['get'] }
                                    }
    access_policy_for_user = { 'tenant_id': tenant_id, 'object_id': user_object_id,
                                'permissions': { 'keys': ['all'], 'secrets': ['all'] }
                            }

    log_entry_to_db(request_id, 'create', f'Create azure keyvault and secrets - {keyvault_name}')
    azure_resource_provision_service.create_key_vault(keyvault_name, [access_policy_for_synapse, access_policy_for_user])
    azure_resource_provision_service.create_secret_in_keyvault(keyvault_name, 'oeaSalt', b64encode(secrets.token_bytes(16)).decode())
    #todo: Migrate this step to use Python SDK.
    # os.system(f"az monitor app-insights component create --app {appinsights_name} --resource-group {resource_group_name} --location {location} --tags {tags} -o none")

    if include_groups is True:
        log_entry_to_db(request_id, 'create', 'Creating AAD groups and role assignments.')
        create_aad_groups()
        create_role_assignments_to_groups()
    else:
        log_entry_to_db(request_id, 'create', 'Adding Storage Blob Data Contributor role assignment to the user')
        azure_resource_provision_service.create_role_assignment('Storage Blob Data Contributor', storage_account_id, user_object_id)

    log_entry_to_db(request_id, 'create','Installing Linked Services.')
    install_linked_services(request_id, synapse_management_service)

    log_entry_to_db(request_id, 'create', 'Installing Datasets')
    install_datasets(request_id, synapse_management_service)

    log_entry_to_db(request_id, 'create', 'Installing Notebooks')
    install_notebooks(request_id, synapse_management_service)

    log_entry_to_db(request_id, 'create', 'Installing Pipelines')
    install_pipelines(request_id, synapse_management_service)

    log_entry_to_db(request_id, 'create', 'Installing Dataflows')
    install_dataflows(request_id, synapse_management_service)