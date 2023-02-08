import json
import os
from OEA_Portal.settings import CONFIG_DATABASE, WORKSPACE_DB_ROOT_PATH
from OEA_Portal.auth.AzureClient import AzureClient
from OEA_Portal.core.models import *
from azure.identity import DefaultAzureCredential
from azure.mgmt.subscription import SubscriptionClient

def get_config_data():
    """
    Returns the Tenant ID and Subscription ID of the given azure account from the config JSON.
    """
    with open(CONFIG_DATABASE) as f:
        data = json.load(f)
    return data

def update_config_database(target_data):
    """
    Updates the Tenant ID and Subscription ID in the config JSON.
    """
    with open(CONFIG_DATABASE) as f:
        data = json.load(f)
        for key in target_data:
            if(key in data): data[key] = target_data[key]
    with open(CONFIG_DATABASE, 'w') as f:
        f.write(json.dumps(data))

def get_blob_contents(azure_client:AzureClient, storage_account_name, blob_path):
    """
    Downloads and returns the contents of a blob in a given storage account.
    """
    container_name = blob_path.split('/')[0].replace('/', '')
    blob_name = ''.join(blob_path.split('/')[1:], '/')
    try:
        data = azure_client.get_blob_client(storage_account_name, container_name, blob_name).download_blob()
    except:
        raise Exception(f'Unable to download blob from Storage account - {storage_account_name}')
    return data

def get_all_subscriptions_in_tenant():
    """
    Returns list of subscriptions in a given tenant containing the id and name.
    """
    subscription_models = []
    credential = DefaultAzureCredential()
    subscriptions = SubscriptionClient(credential).subscriptions.list()
    for subscription in subscriptions:
        subscription_models.append(AzureSubscription(subscription.display_name, subscription.id))
    return subscription_models

def get_storage_account_from_url(account_url):
    return account_url.replace('.dfs.core.windows.net', '').replace('https://', '')

def get_all_workspaces_in_subscription(azure_client:AzureClient):
    """
    Returns the list of all workspaces in a given subscription.
    """
    workspace_models = []
    workspaces = azure_client.get_synapse_client().workspaces.list()
    for workspace in workspaces:
        resource_group = workspace.id.split('/')[4]
        storage_account = get_storage_account_from_url(workspace.default_data_lake_storage)
        workspace_models.append(SynapseWorkspace(workspace.name, resource_group, azure_client.subscription_id, storage_account))
    return workspace_models

def get_workspace_object(azure_client:AzureClient, workspace_name):
    """
    Returns the "SynapseWorkspace" model for a given workspace.
    """
    workspaces = azure_client.get_synapse_client().workspaces.list()
    workspace = any(workspace for workspace in workspaces if workspace.name == workspace_name)
    resource_group = workspace.id.split('/')[4]
    storage_account = get_storage_account_from_url(workspace.default_data_lake_storage)
    return SynapseWorkspace(workspace_name, resource_group, azure_client.subscription_id, storage_account)

def is_oea_installed_in_workspace(azure_client:AzureClient, workspace:SynapseWorkspace):
    """
    Returns True if OEA is installed in the workspace, else False.
    """
    linked_storage_account = azure_client.get_synapse_client().workspaces.get(resource_group_name=workspace.resource_group, workspace_name=workspace.workspace_name).default_data_lake_storage.account_url.replace('.dfs.core.windows.net', '').replace('https://', '')
    keys = azure_client.get_storage_client().storage_accounts.list_keys(workspace.resource_group, linked_storage_account)
    return azure_client.get_datalake_client(linked_storage_account, keys.keys[0].value).get_directory_client(file_system='oea', directory=f'admin/workspaces/{workspace.workspace_name}').exists()

def get_all_storage_accounts_in_subscription(azure_client:AzureClient):
    """
    Returns the list of all storage accounts in a given subscription.
    """
    return [x.name for x in azure_client.get_storage_client().storage_accounts.list()]



