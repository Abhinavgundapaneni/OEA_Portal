from OEA_Portal.settings import OEA_ASSET_TYPES, BASE_DIR
from ..SynapseManagementService import SynapseManagementService
from ..utils import get_blob_contents, get_storage_account_from_url, download_and_extract_zip_from_url
from OEA_Portal.auth.AzureClient import AzureClient
from OEA_Portal.core.models import OEAInstalledAsset, OEAInstance
from azure.mgmt.resource.resources.models import Deployment, DeploymentProperties
import urllib.request
import os
import re
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

#todo: Tried to using deployments to install pipeline. Delete if not working.
def deploy_template_to_resource_group(azure_client:AzureClient):
    with open(f"{BASE_DIR}/downloads/temp.json") as f : template_json = json.load(f)
    with open(f"{BASE_DIR}/downloads/parameters.json") as f : param_json = json.load(f)
    poller = azure_client.get_resource_client().deployments.begin_create_or_update(
        resource_group_name='rg-oea-abhinav4',
        deployment_name='deployment-003',
        parameters=Deployment(
            properties=DeploymentProperties(
                mode='Incremental',
                template=template_json,
                parameters=param_json
            )
        )
    )
    print(poller.result())

def parse_deployment_template_and_install_artifacts(file_path:str, azure_client:AzureClient):
    with open(file_path) as f:
        template_str = f.read()
        template_json = json.loads(template_str)

    parameters = template_json["parameters"]
    sms = SynapseManagementService(azure_client, 'rg-oea-abhinav4')
    for param in list(parameters.keys())[1:]:
        template_str = template_str.replace(f"[parameters('{param}')]", param)
    # todo: Figure out how to get target OEA instance.
    target_oea_instance = OEAInstance('syn-oea-abhinav4', 'rg-oea-abhinav4', 'kv-oea-abhinav4', 'stoeaabhinav4')
    template_json = json.loads(template_str)

    for resource in template_json["resources"]:
        resource["name"] = re.sub('[^a-zA-Z0-9_]', '', resource["name"].split(",")[-1])
        if resource["type"] == "Microsoft.Synapse/workspaces/datasets":
            sms.create_or_update_dataset(target_oea_instance, dataset_dict=resource, wait_till_completion=True)

    for resource in template_json["resources"]:
        if resource["type"] == "Microsoft.Synapse/workspaces/dataflows":
            sms.create_or_update_dataflow(target_oea_instance, dataflow_dict=resource, wait_till_completion=True)
        elif resource["type"] == "Microsoft.Synapse/workspaces/notebooks":
            sms.create_or_update_notebook(target_oea_instance, notebook_dict=resource, wait_till_completion=True)

    for resource in template_json["resources"]:
        if resource["type"] == "Microsoft.Synapse/workspaces/pipelines":
            sms.create_or_update_pipeline()