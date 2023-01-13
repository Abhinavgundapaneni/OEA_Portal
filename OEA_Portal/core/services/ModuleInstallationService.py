import os
import json
import logging
from settings import BASE_DIR
from . import SynapseManagementService
import urllib.request
import zipfile
from OEA_Portal.auth.AzureClient import AzureClient

def download_and_extract_module(url, path=f"{BASE_DIR}/temp"):
    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(path)
    return path

def install_edfi_module(sms:SynapseManagementService, azure_client: AzureClient, workspace_name, version='0.2'):
    """
    Installs the Ed-Fi Module on your Workspace. Default version : 0.2
    """
    module_release_url = f'https://github.com/microsoft/OpenEduAnalytics/releases/download/module_edfi_v{version}/module_edfi_v{version}.zip'
    try:
        path = download_and_extract_module(module_release_url)
    except:
        raise Exception(f'Unable to download and extract Module from - {module_release_url}. Check if the Version you are trying to install is valid.')
    module_root_path = f'{path}/module_edfi_v{version}'


    dataflows = [item for item in os.listdir(f'{module_root_path}/dataflow/')]
    for dataflow in dataflows:
        try:
            sms.create_or_update_dataflow(workspace_name, f'{module_root_path}/dataflow/{dataflow}', dataflow.split('.')[0])
        except Exception as e:
                pass


