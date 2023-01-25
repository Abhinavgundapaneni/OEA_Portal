import json
import os
from OEA_Portal.settings import CONFIG_DATABASE, WORKSPACE_DB_ROOT_PATH
from OEA_Portal.auth.AzureClient import AzureClient

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

def get_all_workspaces_in_subscription(azure_client:AzureClient):
    """
    Returns the list of all workspaces in a given subscription.
    """
    return [x.name for x in azure_client.get_synapse_client().workspaces.list()]

def is_oea_installed_in_workspace(azure_client:AzureClient, workspace_name, resource_group_name):
    linked_storage_account = azure_client.get_synapse_client().workspaces.get(resource_group_name=resource_group_name, workspace_name=workspace_name).default_data_lake_storage.account_url.replace('.dfs.core.windows.net', '').replace('https://', '')
    keys = azure_client.get_storage_client().storage_accounts.list_keys(resource_group_name, linked_storage_account)
    blobs = azure_client.get_datalake_client(linked_storage_account, keys.keys[0].value).list_file_systems(name_starts_with='oea')
    blob_names = [i.name for i in blobs]
    print(blob_names)
    return False if (blob_names is None or len(blob_names) == 0) else True

def get_all_storage_accounts_in_subscription(azure_client:AzureClient):
    """
    Returns the list of all storage accounts in a given subscription.
    """
    return [x.name for x in azure_client.get_storage_client().storage_accounts.list()]



