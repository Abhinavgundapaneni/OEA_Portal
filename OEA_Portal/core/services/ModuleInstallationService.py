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

def install_edfi_module(sms:SynapseManagementService, azure_client: AzureClient, config, version='0.2'):
    """
    Installs the Ed-Fi Module on your Workspace. Default version : 0.2
    """
    module_release_url = f'https://github.com/microsoft/OpenEduAnalytics/releases/download/module_edfi_v{version}/module_edfi_v{version}.zip'
    try:
        path = download_and_extract_module(module_release_url)
    except:
        raise Exception(f'Unable to download and extract Module from - {module_release_url}. Check if the Version you are trying to install is valid.')

    module_root_path = f'{path}/module_edfi_v{version}'
    sms.install_all_datasets(config, f'{module_root_path}/dataset', wait_till_completion=False)
    sms.install_all_dataflows(config, f'{module_root_path}/dataflow', wait_till_completion=False)
    sms.install_all_notebooks(config, f'{module_root_path}/notebook', wait_till_completion=False)
    pipelines = ['Copy_from_REST_Keyset_Parallel.json', 'Copy_from_REST_Anonymous_to_ADLS.json', 'Copy_EdFi_Entities_to_Stage1.json', 'Copy_Stage1_To_Stage2.json', 'Master_Pipeline.json']
    sms.install_all_pipelines(config, f'{module_root_path}/pipeline', pipelines=pipelines)


