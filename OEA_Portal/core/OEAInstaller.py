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

class OEAInstaller():
    #todo: Add class description.
    def __init__(self, tenant_id, subscription_id, oea_suffix, location='eastus', tags=None, include_groups=False):
        self.tenant_id = tenant_id
        self.subscription_id = subscription_id
        self.location = location
        self.tags = tags
        self.include_groups = include_groups
        self.framework_path_relative = f"{BASE_DIR}/temp/OEA_v0.7/framework/synapse".replace('\\', '/')
        self.framework_zip_url = "https://github.com/microsoft/OpenEduAnalytics/releases/download/v0.7/OEA_v0.7.zip"
        #todo: Find way to get signed-in user id using python sdk.
        self.user_object_id = '34b26f30-cbfc-47ec-9131-27fef4433705'
        self.resource_group_name = f"rg-oea-{oea_suffix}"
        self.storage_account_name = f"stoea{oea_suffix}"
        self.keyvault_name = f"kv-oea-{oea_suffix}"
        self.appinsights_name = f"appi-oea-{oea_suffix}"
        self.synapse_workspace_name = f"syn-oea-{oea_suffix}"
        self.containers = ['oea', 'stage1', 'stage2', 'stage3']
        self.dirs = ['stage1/Transactional','stage2/Ingested','stage2/Refined','oea/sandboxes/sandbox1/stage1/Transactional',\
            'oea/sandboxes/sandbox1/stage2/Ingested','oea/sandboxes/sandbox1/stage2/Refined','oea/sandboxes/sandbox1/stage3',\
                'oea/dev/stage1/Transactional','oea/dev/stage2/Ingested','oea/dev/stage2/Refined','oea/dev/stage3']
        self.storage_account_id = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group_name}/providers/Microsoft.Storage/storageAccounts/{self.storage_account_name}"
        self.global_admins_name = None
        self.ds_group_name = None
        self.eds_group_name = None
        self.de_group_name = None
        # self.celery_app = Celery('OEAInstaller', broker='amqp://localhost')

        self.logger = logging.getLogger('OEAInstaller')

    def log_entry_to_db(request_id, action, message):
        InstallationLogs.objects.create(request_id=request_id, action=action, message=message)

    def replace_strings(self, file_path):
        with open(file_path) as f:
            data = f.read()
            data = data.replace('yourkeyvault', self.keyvault_name)\
                        .replace('yourstorageaccount', self.storage_account_name)\
                        .replace('yoursynapseworkspace', self.synapse_workspace_name)
        with open(file_path, 'wt') as f:
            f.write(data)

    def verify_permissions(self, azure_client, resouce_provision_service):
        """ Check if user has "Owner" Permission on the subscription, fail if not """
        owner_role_def = resouce_provision_service.get_role('Owner', f"/subscriptions/{self.subscription_id}")
        owner_role_assignments = [role_assignment for role_assignment in azure_client.get_authorization_client().role_assignments.list(filter=f'principalId eq \'{self.user_object_id}\'') if role_assignment.role_definition_id == owner_role_def.id]
        if(len(owner_role_assignments) == 0):
            self.logger.error("--> Setup failed! The user does not have the \"Owner\" Permission on the Azure subscription")
            raise PermissionError("User does not enough permissions.")

    def get_container_resourceId(self, container):
        """ Returns the Resource Id of the given container """
        return f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group_name}/providers/Microsoft.Storage/storageAccounts/{self.storage_account_name}/blobServices/default/containers/{container}"

    def create_synapse_architecture(self, azure_resource_provision_service, synapse_management_service):
        self.synapse_workspace_object = azure_resource_provision_service.create_synapse_workspace(self.synapse_workspace_name, self.storage_account_name)
        azure_resource_provision_service.create_role_assignment('Storage Blob Data Contributor', self.storage_account_object.id, self.synapse_workspace_object.identity.principal_id)

        synapse_management_service.add_firewall_rule_for_synapse('allowAll', '0.0.0.0', '255.255.255.255', self.synapse_workspace_name)
        synapse_management_service.create_spark_pool(self.synapse_workspace_name, "spark3p2sm",
                {
                    "node_size": "small",
                    "max_node_count": 5
                }
            )
        synapse_management_service.create_spark_pool(self.synapse_workspace_name, "spark3p2med",
                {
                    "node_size": "medium",
                    "max_node_count": 10
                }
            )

    def install_linked_services(self, request_id, synapse_management_service:SynapseManagementService):
        if(os.path.isdir(f'{self.framework_path_relative}/linkedService/') is False):
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'No Linked Service to Install.')
            return
        linked_services = os.listdir(f'{self.framework_path_relative}/linkedService/')
        for ls in linked_services:
            try:
                self.replace_strings(f'{self.framework_path_relative}/linkedService/{ls}')
                synapse_management_service.create_linked_service(self.synapse_workspace_name, ls.split('.')[0], f'{self.framework_path_relative}/linkedService/{ls}')
            except Exception as e:
                pass
                # OEAInstaller.log_entry_to_db(request_id, 'error', f"Failed to install the Linked Service - {ls.split('.')[0]} : {str(e)}")

    def install_datasets(self, request_id, synapse_management_service:SynapseManagementService):
        if(os.path.isdir(f'{self.framework_path_relative}/dataset/') is False):
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'No Dataset to Install.')
            return
        datasets = os.listdir(f'{self.framework_path_relative}/dataset/')
        for dataset in datasets:
            try:
                self.replace_strings(f'{self.framework_path_relative}/dataset/{dataset}')
                synapse_management_service.create_dataset(self.synapse_workspace_name, dataset.split('.')[0], f'{self.framework_path_relative}/dataset/{dataset}')
            except Exception as e:
                pass
                # OEAInstaller.log_entry_to_db(request_id, 'error', f"Failed to install the Dataset - {dataset.split('.')[0]} : {str(e)}")

    def install_notebooks(self, request_id, synapse_management_service:SynapseManagementService):
        if(os.path.isdir(f'{self.framework_path_relative}/notebook/') is False):
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'No Notebook to Install.')
            return
        notebooks = os.listdir(f'{self.framework_path_relative}/notebook/')
        for notebook in notebooks:
            try:
                self.replace_strings(f"{self.framework_path_relative}/notebook/{notebook}")
                synapse_management_service.create_notebook(f"{self.framework_path_relative}/notebook/{notebook}", self.synapse_workspace_name)
            except Exception as e:
                pass
                # OEAInstaller.log_entry_to_db(request_id, 'error', f"Failed to install the Notebook - {notebook.split('.')[0]} : {str(e)}")

    def install_pipelines(self, request_id, synapse_management_service:SynapseManagementService):
        if(os.path.isdir(f'{self.framework_path_relative}/pipeline/') is False):
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'No Pipelines to Install.')
            return
        pipelines = [item for item in os.listdir(f'{self.framework_path_relative}/pipeline/') if os.path.isfile(f'{self.framework_path_relative}/pipeline/{item}')]
        for pipeline in pipelines:
            try:
                self.replace_strings(f'{self.framework_path_relative}/pipeline/{pipeline}')
                synapse_management_service.create_or_update_pipeline(self.synapse_workspace_name, f'{self.framework_path_relative}/pipeline/{pipeline}', pipeline.split('.')[0])
            except Exception as e:
                pass
                # OEAInstaller.log_entry_to_db(request_id, 'error', f"Failed to install the Pipeline - {pipeline.split('.')[0]} : {str(e)}")

    def install_dataflows(self, request_id, synapse_management_service:SynapseManagementService):
        if(os.path.isdir(f'{self.framework_path_relative}/dataflow/') is False):
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'No Dataflows to Install.')
            return
        dataflows = [item for item in os.listdir(f'{self.framework_path_relative}/dataflow/') if os.path.isfile(f'{self.framework_path_relative}/pipeline/{item}')]
        for dataflow in dataflows:
            try:
                self.replace_strings(f'{self.framework_path_relative}/dataflow/{dataflow}')
                synapse_management_service.create_or_update_dataflow(self.synapse_workspace_name, f'{self.framework_path_relative}/dataflow/{dataflow}', dataflow.split('.')[0])
            except Exception as e:
                pass
                # OEAInstaller.log_entry_to_db(request_id, 'error', f"Failed to install the Dataflow - {dataflow.split('.')[0]} : {str(e)}")


    def create_aad_groups(self):
        #todo: Migrate this step to use Python SDK.
        os.system(f"az ad group create --display-name \"{self.global_admins_name}\" --mail-nickname 'EduAnalyticsGlobalAdmins'")
        os.system(f"az ad group owner add --group \"{self.global_admins_name}\" --owner-object-id {self.user_object_id}")
        self.global_admins_id = os.popen(f"az ad group show --group \"{self.global_admins_name}\" --query id --output tsv").read()[:-1]

        os.system(f"az ad group create --display-name \"{self.ds_group_name}\" --mail-nickname 'EduAnalyticsDataScientists'")
        os.system(f"az ad group owner add --group \"{self.ds_group_name}\" --owner-object-id {self.user_object_id}")
        self.data_scientists_id = os.popen(f"az ad group show --group \"{self.ds_group_name}\" --query id --output tsv").read()[:-1]

        os.system(f"az ad group create --display-name \"{self.de_group_name}\" --mail-nickname 'EduAnalyticsDataEngineers' -o none")
        os.system(f"az ad group owner add --group \"{self.de_group_name}\" --owner-object-id {self.user_object_id} -o none")
        self.data_engineers_id = os.popen(f"az ad group show --group \"{self.de_group_name}\" --query id --output tsv").read()[:-1]

        os.system(f"az ad group create --display-name \"{self.eds_group_name}\" --mail-nickname 'EduAnalyticsExternalDataScientists' -o none")
        os.system(f"az ad group owner add --group \"{self.eds_group_name}\" --owner-object-id {self.user_object_id} -o none")
        self.external_data_scientists_id = os.popen(f"az ad group show --group \"{self.eds_group_name}\" --query id --output tsv").read()[:-1]

    def create_role_assignments_to_groups(self, provision_resource_service):
        provision_resource_service.create_role_assignment('Owner', f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group_name}/", self.global_admins_id)

        # Assign "Storage Blob Data Contributor" to security groups to allow users to query data via Synapse studio
        provision_resource_service.create_role_assignment('Storage Blob Data Contributor', self.storage_account_id, self.global_admins_id)
        provision_resource_service.create_role_assignment('Storage Blob Data Contributor', self.storage_account_id, self.data_scientists_id)
        provision_resource_service.create_role_assignment('Storage Blob Data Contributor', self.storage_account_id, self.data_engineers_id)

        # Assign limited access to specific containers for the external data scientists
        provision_resource_service.create_role_assignment('Storage Blob Data Contributor', self.get_container_resourceId('stage2'), self.external_data_scientists_id)
        provision_resource_service.create_role_assignment('Storage Blob Data Contributor', self.get_container_resourceId('stage3'), self.external_data_scientists_id)
        provision_resource_service.create_role_assignment('Storage Blob Data Contributor', self.get_container_resourceId('oea'), self.external_data_scientists_id)
        provision_resource_service.create_role_assignment('Reader', self.storage_account_id, self.data_engineers_id)

    def download_and_extract_framework(self):
        zip_path, _ = urllib.request.urlretrieve(self.framework_zip_url)
        with zipfile.ZipFile(zip_path, "r") as f:
            f.extractall(f"{BASE_DIR}/temp")

    def install(self, request_id=None):
        if request_id is None:
            request_id = uuid.uuid4()
        self.download_and_extract_framework()

        azure_client = AzureClient(self.tenant_id, self.subscription_id, location=self.location, default_tags=self.tags)
        azure_resource_provision_service = AzureResourceProvisionService(azure_client)
        synapse_management_service = SynapseManagementService(azure_client, self.synapse_workspace_name, self.resource_group_name)

        # OEAInstaller.log_entry_to_db(request_id, 'verify', 'Verifying if the user has Owner permissions or not.')
        self.verify_permissions(azure_client, azure_resource_provision_service)

        # OEAInstaller.log_entry_to_db(request_id, 'create', f'Creating Resource group in Azure resource group - {self.resource_group_name}')
        azure_resource_provision_service.create_resource_group(self.resource_group_name)

        # OEAInstaller.log_entry_to_db(request_id, 'create', f'Creating storage accounts and containers (Along with directories) - {self.storage_account_name}')
        self.storage_account_object = azure_resource_provision_service.create_storage_account(self.storage_account_name)
        azure_resource_provision_service.create_containers_and_directories(self.storage_account_name, self.containers, self.dirs)

        # OEAInstaller.log_entry_to_db(request_id, 'create', f'Creating synapse architecture in Azure subscription - {self.synapse_workspace_name}')
        self.create_synapse_architecture(azure_resource_provision_service, synapse_management_service)

        access_policy_for_synapse = { 'tenant_id': self.tenant_id, 'object_id': self.synapse_workspace_object.identity.principal_id,
                                            'permissions': { 'secrets': ['get'] }
                                        }
        access_policy_for_user = { 'tenant_id': self.tenant_id, 'object_id': self.user_object_id,
                                    'permissions': { 'keys': ['all'], 'secrets': ['all'] }
                                }

        # OEAInstaller.log_entry_to_db(request_id, 'create', f'Create azure keyvault and secrets - {self.keyvault_name}')
        azure_resource_provision_service.create_key_vault(self.keyvault_name, [access_policy_for_synapse, access_policy_for_user])
        azure_resource_provision_service.create_secret_in_keyvault(self.keyvault_name, 'oeaSalt', b64encode(secrets.token_bytes(16)).decode())
        #todo: Migrate this step to use Python SDK.
        # os.system(f"az monitor app-insights component create --app {self.appinsights_name} --resource-group {self.resource_group_name} --location {self.location} --tags {self.tags} -o none")

        if self.include_groups is True:
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'Creating AAD groups and role assignments.')
            self.create_aad_groups()
            self.create_role_assignments_to_groups()
        else:
            # OEAInstaller.log_entry_to_db(request_id, 'create', 'Adding Storage Blob Data Contributor role assignment to the user')
            azure_resource_provision_service.create_role_assignment('Storage Blob Data Contributor', self.storage_account_id, self.user_object_id)

        # OEAInstaller.log_entry_to_db(request_id, 'create','Installing Linked Services.')
        self.install_linked_services(request_id, synapse_management_service)

        # OEAInstaller.log_entry_to_db(request_id, 'create', 'Installing Datasets')
        self.install_datasets(request_id, synapse_management_service)

        # OEAInstaller.log_entry_to_db(request_id, 'create', 'Installing Notebooks')
        self.install_notebooks(request_id, synapse_management_service)

        # OEAInstaller.log_entry_to_db(request_id, 'create', 'Installing Pipelines')
        self.install_pipelines(request_id, synapse_management_service)

        # OEAInstaller.log_entry_to_db(request_id, 'create', 'Installing Dataflows')
        self.install_dataflows(request_id, synapse_management_service)