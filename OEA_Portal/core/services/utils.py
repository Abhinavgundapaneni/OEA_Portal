import json
import os
from OEA_Portal.settings import BASE_DIR, CONFIG_DATABASE
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
    return azure_client.get_synapse_client().workspaces.list()

def get_all_workspaces_with_oea_installed(azure_client:AzureClient):
    """
    Returns the list of all workspaces with OEA installed in a given subscription.
    """
    azure_client.get_datalake_client().list_file_systems(name_starts_with='oea')

def get_all_storage_accounts_in_subscription(azure_client:AzureClient):
    """
    Returns the list of all storage accounts in a given subscription.
    """
    return azure_client.get_storage_client().storage_accounts.list()



