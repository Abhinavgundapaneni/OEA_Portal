import uuid
from django.db import models

class SynapseWorkspace:
    def __init__(self, workspace_name, resource_group, subscription) -> None:
        self.workspace_name = workspace_name
        self.resource_group = resource_group
        self.subscription = subscription

class AzureSubscription:
    def __init__(self, subscription_name, subscription_id) -> None:
        self.subscription_name = subscription_name
        self.subscription_id = subscription_id

#todo: Find a better name for this
class InstallationConfig:
    def __init__(self, workspace_name, resource_group, keyvault, linked_storage_account) -> None:
        self.workspace_name = workspace_name
        self.resource_group = resource_group
        self.keyvault = keyvault
        self.storage_account = linked_storage_account