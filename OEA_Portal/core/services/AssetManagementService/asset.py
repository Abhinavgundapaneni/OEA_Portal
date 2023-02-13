import os
import json
from OEA_Portal.settings import OEA_ASSET_TYPES, BASE_DIR
from ..utils import download_and_extract_zip_from_url

class BaseOEAAsset:
    def __init__(self, asset_name:str, release_keyword:str, version:str, supported_oea_versions:list, asset_type:str):
        if(asset_type not in OEA_ASSET_TYPES):
            raise Exception(f"{asset_type} is not an OEA supported Asset type.")
        self.asset_display_name = asset_name
        self.version = version
        self.supported_oea_versions = supported_oea_versions
        self.asset_type = asset_type
        self.release_keyword = release_keyword
        self.local_asset_download_path = f"{BASE_DIR}/downloads/{asset_type}"
        self.local_asset_root_path =f"{BASE_DIR}/downloads/{asset_type}/{self.asset_type}_{self.release_keyword}_v{self.version}"

        # Synapse Artifacts associated with the Asset.
        self.pipelines = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/pipeline"))) if os.path.isdir(f"{self.local_asset_root_path}/pipeline") else []
        self.dataflows = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/dataflow"))) if os.path.isdir(f"{self.local_asset_root_path}/dataflow") else []
        self.datasets = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/dataset"))) if os.path.isdir(f"{self.local_asset_root_path}/dataset") else []
        self.notebooks = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/notebook"))) if os.path.isdir(f"{self.local_asset_root_path}/notebook") else []
        self.linked_services = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/linkedService"))) if os.path.isdir(f"{self.local_asset_root_path}/linkedService") else []
        self.integration_runtimes = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/integrationRuntime"))) if os.path.isdir(f"{self.local_asset_root_path}/integrationRuntime") else []
        self.sql_scripts = list(filter( lambda x: '.md' not in x, os.listdir(f"{self.local_asset_root_path}/sqlScript"))) if os.path.isdir(f"{self.local_asset_root_path}/sqlScript") else []

        self.dependency_dict = {}

        self.asset_url = f"https://github.com/microsoft/OpenEduAnalytics/releases/download/{asset_type}_{release_keyword}_v{version}/{asset_type}_{release_keyword}_v{version}.zip"
        download_and_extract_zip_from_url(self.asset_url, self.local_asset_download_path)
        self.create_dependency_matrix()
        self.pipelines_dependency_order = self.create_pipeline_dependency_order()

    def dfs(self, table_name, visited, dependency_dict, dependency_order):
        visited[table_name] = True
        if dependency_dict[table_name] == []:
            dependency_order.append(table_name)
            return
        for dependent_table in dependency_dict[table_name]:
            if(visited[dependent_table] is False):
                self.dfs(dependent_table, visited, dependency_dict, dependency_order)
            if(visited[dependent_table] is False):
                dependency_order.append(dependent_table)
        dependency_order.append(table_name)

    def create_pipeline_dependency_order(self):
        visited = {}
        dependency_order = []
        for pipeline in self.pipelines:
            visited[pipeline.split('.')[0]] = False
        for pipeline in self.pipelines:
            if(visited[pipeline.split('.')[0]] is False):
                self.dfs(pipeline.split('.')[0], visited, self.dependency_dict, dependency_order)
        return dependency_order

    def create_dependency_matrix(self):
        files = os.listdir(f"{self.local_asset_root_path}/pipeline")
        for file in files:
            if '.json' in file:
                key = file.split('.')[0]
                self.dependency_dict[key] = []
                with open(f"{self.local_asset_root_path}/pipeline/{file}") as f:
                    file_json = json.load(f)
                for x in get_values_from_json(file_json, 'pipeline'):
                    value = x['referenceName']
                    self.dependency_dict[key].append(value)

    def install(self):
        print(self.pipelines_dependency_order)

    def uninstall(self):
        pass

def get_values_from_json(file_json, target_field):
    for k,v in file_json.items():
        if k == target_field:
            yield v
        elif isinstance(v, dict):
            for item in get_values_from_json(v, target_field):
                yield item
        elif isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    for item in get_values_from_json(x, target_field):
                        yield item
