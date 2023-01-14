import os
import json
import logging
from OEA_Portal.auth.AzureClient import AzureClient
from azure.mgmt.synapse.models import Workspace, DataLakeStorageAccountDetails, ManagedIdentity, IpFirewallRuleInfo
from azure.mgmt.synapse.models import BigDataPoolResourceInfo, AutoScaleProperties, AutoPauseProperties, LibraryRequirements,\
     NodeSizeFamily, NodeSize, BigDataPoolPatchInfo, IpFirewallRuleInfo
from OEA_Portal.settings import BASE_DIR

logger = logging.getLogger('SynapseManagementService')

class SynapseManagementService:
    def __init__(self, azure_client:AzureClient, workspace_name, resource_group_name):
        self.azure_client = azure_client
        self.workspace_name = workspace_name
        self.resource_group_name = resource_group_name

    def create_or_update_pipeline(self, synapse_workspace, pipeline_file_path, pipeline_name):
        """ Creates or updates the Pipeline in the given Synapse studio.
            Expects the pipeline configuration file in JSON format.
        """
        with open(pipeline_file_path) as f: pipeline_dict = json.load(f)
        if '$schema' not in pipeline_dict.keys():
            poller = self.azure_client.get_artifacts_client(synapse_workspace).pipeline.create_or_update_pipeline(pipeline_name, pipeline_dict)
            return poller

    def create_or_update_dataflow(self, synapse_workspace, dataflow_file_path):
        """ Creates or updates the Dataflow in the given Synapse studio.
            Expects the dataflow configuration file in JSON format.
        """
        with open(dataflow_file_path) as f: dataflow_dict = json.load(f)
        poller = self.azure_client.get_artifacts_client(synapse_workspace).data_flow.create_or_update_dataflow(dataflow_dict['name'], dataflow_dict)
        return poller

    def create_notebook(self, notebook_filename, synapse_workspace_name):
        """ Creates or updates the Notebook in the given Synapse studio.
            Expects the dataflow configuration file in JSON or ipynb format.
        """
        artifacts_client = self.azure_client.get_artifacts_client(synapse_workspace_name)
        with open(notebook_filename) as f:
            if(notebook_filename.split('.')[-1] == 'json'):
                notebook_dict = json.load(f)
                notebook_name = notebook_dict['name']
            elif(notebook_filename.split('.')[-1] == 'ipynb'):
                notebook_dict = json.loads(f.read())
                notebook_name = notebook_filename.split('/')[-1].split('.')[0]
            else:
                raise ValueError('Notebook format not supported.')
        # self.validate_notebook_json(notebook_dict)
        logger.info(f"Creating notebook: {notebook_name}")
        poller = artifacts_client.notebook.begin_create_or_update_notebook(notebook_name, notebook_dict)
        return poller.result() #AzureOperationPoller

    def create_linked_service(self, workspace_name, linked_service_name, file_path):
        """ Creates a linked service in the Synapse studio.
            Expects a linked service configuration file in JSON format
        """
        # todo: modify this to use Python SDK
        os.system(f"az synapse linked-service create --workspace-name {workspace_name} --name {linked_service_name} --file @{file_path} -o none")

    def create_dataset(self, workspace_name, dataset_name, file_path):
        """ Creates a dataset in the Synapse studio.
            Expects a dataset configuration file in JSON format
        """
        # todo: modify this to use Python SDK
        os.system(f"az synapse dataset create --workspace-name {workspace_name} --name {dataset_name} --file @{file_path} -o none")

    def add_firewall_rule_for_synapse(self, rule_name, start_ip_address, end_ip_address, synapse_workspace_name):
        """ Create a Firewall rule for the Azure Synapse Studio """
        poller = self.azure_client.get_synapse_client().ip_firewall_rules.begin_create_or_update(self.resource_group_name, synapse_workspace_name, rule_name,
            {
                "name" : rule_name,
                "start_ip_address" : start_ip_address,
                "end_ip_address" : end_ip_address
            }
        )
        return poller.result()

    def create_spark_pool(self, synapse_workspace_name, spark_pool_name, options=None):
        """ Creates the Spark Pool based on the options parameter and updates the pool with the required library requirements.

            :param node_size: size of the spark node. Defaulted to small
            :type node_size: str
            :param min_node_count: minimum node count for the spark pool
            :param min_node_count: int
            :param max_node_count: minimum node count for the spark pool
            :param max_node_count: int
            https://docs.microsoft.com/en-us/python/api/azure-mgmt-synapse/azure.mgmt.synapse.aio.operations.bigdatapoolsoperations?view=azure-python
        """
        if not options: options = {}
        min_node_count = options.pop('min_node_count', 3)
        max_node_count = options.pop('max_node_count', 5)
        node_size = options.pop('node_size', 'small')
        if node_size == 'small': node_size = NodeSize.SMALL
        elif node_size == 'medium': node_size = NodeSize.MEDIUM
        elif node_size == 'large': node_size = NodeSize.LARGE
        elif node_size == 'xlarge': node_size = NodeSize.X_LARGE
        elif node_size == 'xxlarge': node_size = NodeSize.XX_LARGE
        else: raise ValueError('Invalid Node Size.')

        poller = self.azure_client.get_synapse_client().big_data_pools.begin_create_or_update(self.resource_group_name, synapse_workspace_name, spark_pool_name,
            BigDataPoolResourceInfo(
                tags = self.azure_client.tags,
                location = self.azure_client.location,
                auto_scale = AutoScaleProperties(enabled=True, min_node_count=min_node_count, max_node_count=max_node_count),
                auto_pause = AutoPauseProperties(delay_in_minutes=15, enabled=True),
                spark_version = '3.2',
                node_size = node_size,
                node_size_family = NodeSizeFamily.MEMORY_OPTIMIZED,
            )
        )
        result = poller.result() # wait for completion of spark pool
        library_requirements = f"{os.path.dirname(__file__)}/requirements.txt"
        self.update_spark_pool_with_requirements(synapse_workspace_name, spark_pool_name, library_requirements)
        return result

    def update_spark_pool_with_requirements(self, synapse_workspace_name, spark_pool_name, library_requirements_path_and_filename):
        """ Update the existing Spark pool by installing the required library requirements.
            Expects a path to the text file containing the list of library requirements"""
        with open(library_requirements_path_and_filename, 'r') as f: lib_contents = f.read()
        poller = self.azure_client.get_synapse_client().big_data_pools.update(self.resource_group_name, synapse_workspace_name, spark_pool_name,
            BigDataPoolPatchInfo (
                library_requirements = LibraryRequirements(filename=os.path.basename(library_requirements_path_and_filename), content=lib_contents)
            )
        )
        return poller

    def validate_notebook_json(self, nb_json):
        """ These attributes must exist for the call to begin_create_or_update_notebook to pass validation """
        if not 'nbformat' in nb_json['properties']: nb_json['properties']['nbformat'] = 4
        if not 'nbformat_minor' in nb_json['properties']: nb_json['properties']['nbformat_minor'] = 2
        for cell in nb_json['properties']['cells']:
            if not 'metadata' in cell: cell['metadata'] = {}
        if 'bigDataPool' in nb_json['properties']:
            nb_json['properties'].pop('bigDataPool', None) #Remove bigDataPool if it's there

    def install_all_datasets(self, synapse_workspace_name, root_path, datasets=None):
        """
        Installs all dataflows from the given path on the Synapse workspace.
        If order of installation is important or you want to install only selected assets in the path,
        pass the datasets parameter with the required assets in the correct order.
        If not passed, it will install all the assets in the path.
        """

        if(os.path.isdir(f'{root_path}/') is True):
            if datasets is None:
                datasets = os.listdir(f'{root_path}/')
            for dataset in datasets:
                try:
                    self.create_dataset(synapse_workspace_name, dataset.split('.')[0], f'{root_path}/{dataset}')
                except Exception as e:
                        #todo: Handle the error
                        raise Exception(str(e))

    def install_all_dataflows(self, synapse_workspace_name, root_path, dataflows=None):
        """
        Installs all dataflows from the given path on the Synapse workspace.
        If order of installation is important or you want to install only selected assets in the path,
        pass the dataflows parameter with the required assets in the correct order.
        If not passed, it will install all the assets in the path.
        """

        if(os.path.isdir(f'{root_path}/') is True):
            if(dataflows is None):
                dataflows = [item for item in os.listdir(f'{root_path}/')]
            for dataflow in dataflows:
                try:
                    poller = self.create_or_update_dataflow(synapse_workspace_name, f'{root_path}/{dataflow}', dataflow.split('.')[0])
                except Exception as e:
                    raise Exception(str(e))

    def install_all_notebooks(self, synapse_workspace_name, root_path, notebooks=None):
        """
        Installs all notebooks from the given path on the Synapse workspace.
        If order of installation is important or you want to install only selected assets in the path,
        pass the notebooks parameter with the required assets in the correct order.
        If not passed, it will install all the assets in the path.
        """

        if(os.path.isdir(f'{root_path}/') is True):
            if(notebooks is None):
                notebooks = os.listdir(f'{root_path}/')
            for notebook in notebooks:
                try:
                    poller = self.create_notebook(f"{root_path}/{notebook}", synapse_workspace_name)
                except Exception as e:
                    raise Exception(str(e))

    def install_all_pipelines(self, synapse_workspace_name, root_path, pipelines=None):
        """
        Installs all pipelines from the given path on the Synapse workspace.
        If order of installation is important or you want to install only selected assets in the path,
        pass the pipelines parameter with the required assets in the correct order.
        If not passed, it will install all the assets in the path.
        """

        if(os.path.isdir(f'{root_path}/') is True):
            if(pipelines is None):
                pipelines = [item for item in os.listdir(f'{root_path}/')]
            for pipeline in pipelines:
                try:
                    poller = self.create_or_update_pipeline(synapse_workspace_name, f'{root_path}/{pipeline}', pipeline.split('.')[0])
                except Exception as e:
                    raise Exception(str(e))

    def install_all_linked_services(self, synapse_workspace_name, root_path, linked_services=None):
        """
        Installs all linked services from the given path on the Synapse workspace.
        If order of installation is important or you want to install only selected assets in the path,
        pass the linked services parameter with the required assets in the correct order.
        If not passed, it will install all the assets in the path.
        """

        if(os.path.isdir(f'{root_path}/') is True):
            if(linked_services is None):
                linked_services = os.listdir(f'{root_path}/')
            for ls in linked_services:
                try:
                    poller = self.create_linked_service(synapse_workspace_name, ls.split('.')[0], f'{root_path}/{ls}')
                except Exception as e:
                    raise Exception(str(e))
