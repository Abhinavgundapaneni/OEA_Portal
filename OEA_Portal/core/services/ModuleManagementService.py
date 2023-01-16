from OEA_Portal.settings import BASE_DIR
from . import SynapseManagementService
import urllib.request
import zipfile
import os
import json

def download_and_extract_module(url, path=f'{BASE_DIR}/temp'.replace("\\", "/")):
    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(path)
    return path

def get_module_data_for_all_workspaces():
    """
    Creates a Dictionary with information related to all the installed modules in all workspaces.
    """
    data_dict = {}
    if(os.path.isdir(f'{BASE_DIR}/temp/workspaces'.replace("\\", "/"))):
        workspaces = os.listdir(f'{BASE_DIR}/temp/workspaces'.replace("\\", "/"))
        for workspace in workspaces:
            with open(f'{BASE_DIR}/temp/workspaces/{workspace}/module_status.json'.replace("\\", "/")) as f:
                workspace_data = json.load(f)
            data_dict[workspace] = workspace_data
    return data_dict

def install_edfi_module(sms:SynapseManagementService, config, version='0.2'):
    """
    Installs the Ed-Fi Module on your Workspace. Default version : 0.2
    """
    module_release_url = f'https://github.com/microsoft/OpenEduAnalytics/releases/download/module_edfi_v{version}/module_edfi_v{version}.zip'
    try:
        path = download_and_extract_module(module_release_url)
    except:
        raise Exception(f'Unable to download and extract Module from - {module_release_url}. Check if the Version you are trying to install is valid.')

    module_root_path = f'{path}/module_edfi_v{version}'
    try:
        sms.install_all_datasets(config, f'{module_root_path}/dataset', wait_till_completion=False)
        sms.install_all_dataflows(config, f'{module_root_path}/dataflow', wait_till_completion=False)
        sms.install_all_notebooks(config, f'{module_root_path}/notebook', wait_till_completion=False)
        pipelines = ['Copy_from_REST_Keyset_Parallel.json', 'Copy_from_REST_Anonymous_to_ADLS.json', 'Copy_EdFi_Entities_to_Stage1.json', 'Copy_Stage1_To_Stage2.json', 'Master_Pipeline.json']
        sms.install_all_pipelines(config, f'{module_root_path}/pipeline', pipelines=pipelines)
    except:
        raise Exception('Error Installing Ed-Fi Module.')