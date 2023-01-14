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

    def replace_strings(self, data, config):
        data = data.replace('yourkeyvault', config['keyvault'])\
                        .replace('yourstorageaccount', config['storage_account'])\
                        .replace('yoursynapseworkspace', config['workspace'])
        return data

    def create_or_update_pipeline(self, config, pipeline_file_path, pipeline_name, wait_till_completion):
        """ Creates or updates the Pipeline in the given Synapse studio.
            Expects the pipeline configuration file in JSON format.
        """
        with open(pipeline_file_path) as f: pipeline_dict = json.loads(self.replace_strings(f.read(), config))
        if '$schema' not in pipeline_dict.keys():
            poller = self.azure_client.get_artifacts_client(config['workspace']).pipeline.begin_create_or_update_pipeline(pipeline_name, pipeline_dict)
            if(wait_till_completion):
                return poller.result() #AzureOperationPoller
            else:
                return poller

    def create_or_update_dataflow(self, config, dataflow_file_path, wait_till_completion):
        """ Creates or updates the Dataflow in the given Synapse studio.
            Expects the dataflow configuration file in JSON format.
        """
        with open(dataflow_file_path) as f: dataflow_dict = json.loads(self.replace_strings(f.read(), config))
        poller = self.azure_client.get_artifacts_client(config['workspace']).data_flow.begin_create_or_update_dataflow(dataflow_dict['name'], dataflow_dict)
        if(wait_till_completion):
            return poller.result() #AzureOperationPoller
        else:
            return poller

    def create_notebook(self, notebook_filename, config, wait_till_completion):
        """ Creates or updates the Notebook in the given Synapse studio.
            Expects the dataflow configuration file in JSON or ipynb format.
        """
        artifacts_client = self.azure_client.get_artifacts_client(config['workspace'])
        with open(notebook_filename) as f:
            if(notebook_filename.split('.')[-1] == 'json'):
                notebook_dict = json.loads(self.replace_strings(f.read(), config))
                notebook_name = notebook_dict['name']
            elif(notebook_filename.split('.')[-1] == 'ipynb'):
                properties = json.loads(self.replace_strings(f.read(), config))
                notebook_name = notebook_filename.split('/')[-1].split('.')[0]
                notebook_dict = {"name": notebook_name, "properties": properties}
            else:
                raise ValueError('Notebook format not supported.')
        self.validate_notebook_json(notebook_dict)
        logger.info(f"Creating notebook: {notebook_name}")
        poller = artifacts_client.notebook.begin_create_or_update_notebook(notebook_name, notebook_dict)
        if(wait_till_completion):
            return poller.result() #AzureOperationPoller
        else:
            return poller

    def create_linked_service(self, config, linked_service_name, file_path, wait_till_completion):
        """ Creates a linked service in the Synapse studio.
            Expects a linked service configuration file in JSON format
        """
        # todo: modify this to use Python SDK
        with open(file_path, 'r') as f: data = self.replace_strings(f.read(), config)
        with open(file_path, 'wt') as f: f.write(data)

        os.system(f"az synapse linked-service create --workspace-name {config['workspace']} --name {linked_service_name} --file @{file_path} -o none")

    def create_dataset(self, config, dataset_name, file_path, wait_till_completion):
        """ Creates a dataset in the Synapse studio.
            Expects a dataset configuration file in JSON format
        """
        # todo: modify this to use Python SDK
        with open(file_path, 'r') as f: data = self.replace_strings(f.read(), config)
        with open(file_path, 'wt') as f: f.write(data)

        os.system(f"az synapse dataset create --workspace-name {config['workspace']} --name {dataset_name} --file @{file_path} -o none")

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

    def install_all_datasets(self, config, root_path, datasets=None, wait_till_completion=True):
        """
        Installs all datasets from the given path on the Synapse workspace.
        If order of installation is important or you want to install only selected assets in the path,
        pass the datasets parameter with the required assets in the correct order.
        If not passed, it will install all the assets in the path.
        """

        if(os.path.isdir(f'{root_path}/') is True):
            if datasets is None:
                datasets = os.listdir(f'{root_path}/')
            for dataset in datasets:
                try:
                    self.create_dataset(config, dataset.split('.')[0], f'{root_path}/{dataset}', wait_till_completion)
                except Exception as e:
                        #todo: Handle the error
                        raise Exception(str(e))

    def install_all_dataflows(self, config, root_path, dataflows=None, wait_till_completion=True):
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
                    self.create_or_update_dataflow(config, f'{root_path}/{dataflow}', wait_till_completion)
                except Exception as e:
                    raise Exception(str(e))

    def install_all_notebooks(self, config, root_path, notebooks=None, wait_till_completion=True):
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
                    self.create_notebook(f"{root_path}/{notebook}", config, wait_till_completion)
                except Exception as e:
                    raise Exception(str(e))

    def install_all_pipelines(self, config, root_path, pipelines=None, wait_till_completion=True):
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
                    self.create_or_update_pipeline(config, f'{root_path}/{pipeline}', pipeline.split('.')[0], wait_till_completion)
                except Exception as e:
                    raise Exception(str(e))

    def install_all_linked_services(self, config, root_path, linked_services=None, wait_till_completion=True):
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
                    self.create_linked_service(config, ls.split('.')[0], f'{root_path}/{ls}', wait_till_completion)
                except Exception as e:
                    raise Exception(str(e))
