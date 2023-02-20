from OEA_Portal.settings import OEA_ASSET_TYPES, BASE_DIR
from .. import SynapseManagementService
from ..utils import get_blob_contents, get_storage_account_from_url, download_and_extract_zip_from_url
from OEA_Portal.auth.AzureClient import AzureClient
from OEA_Portal.core.models import OEAInstalledAsset
from azure.mgmt.resource.resources.models import Deployment, DeploymentProperties
import urllib.request
import os
import json


def get_oea_assets(asset_type:str):
    """
    Returns a list of names of all available OEA assets of a given type.
    """
    if(asset_type not in OEA_ASSET_TYPES):
        raise Exception(f"{asset_type} is not an OEA supported Asset type.")
    url = f"https://api.github.com/repos/microsoft/OpenEduAnalytics/contents/{asset_type}s/{asset_type}_catalog?ref=main"
    request = urllib.request.Request(url)
    response = json.loads(urllib.request.urlopen(request))
    return [asset["name"] for asset in response]

def get_installed_assets_in_workspace(workspace_name, azure_client:AzureClient):
    """
    Returns the list of Installed modules, packages and assets in the given workspace.
    """
    workspace_object = next(ws for ws in azure_client.synapse_client.workspaces.list() if ws.name == workspace_name)
    storage_account = get_storage_account_from_url(workspace_object.default_data_lake_storage.account_url)
    data = get_blob_contents(azure_client, storage_account, f'oea/admin/workspaces/{workspace_name}/status.json')
    modules = [OEAInstalledAsset(asset['Name'], asset['Version'], asset['LastUpdatedTime']) for asset in data['Modules']]
    packages = [OEAInstalledAsset(asset['Name'], asset['Version'], asset['LastUpdatedTime']) for asset in data['Packages']]
    schemas = [OEAInstalledAsset(asset['Name'], asset['Version'], asset['LastUpdatedTime']) for asset in data['Schemas']]
    return modules, packages, schemas, data['OEA_Version']

def deploy_template_to_resource_group(azure_client:AzureClient):
    with open(f"{BASE_DIR}/downloads/temp.json") as f : template_json = json.load(f)
    poller = azure_client.get_resource_client().deployments.begin_create_or_update(
        resource_group_name='rg-oea-abhinav4',
        deployment_name='deployment-001',
        parameters=Deployment(
            location='eastus',
            properties=DeploymentProperties(
                mode='Incremental',
                template=template_json,
                parameters={
                    "workspaceName": "syn-oea-abhinav4",
                    "LS_SQL_Serverless_OEA": "LS_SQL_Serverless",
                    "LS_HTTP": "LS_HTTP",
                    "LS_ADLS_OEA": "LS_DataLake"
                }
            )
        )
    )
    print(poller.result())