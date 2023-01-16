import json
import os
from OEA_Portal.settings import BASE_DIR

def get_tenant_and_subscription_values_from_config():
    """
    Returns the Tenant ID and Subscription ID of the given azure account from the config JSON.
    """
    with open(f'{BASE_DIR}/temp/config.json'.replace("\\", "/")) as f:
        data = json.load(f)
        tenant_id = data['TenantId']
        subscription_id = data['SubscriptionId']
    return tenant_id, subscription_id

def update_tenant_and_subscription_values_from_config(tenant_id, subscription_id):
    """
    Updates the Tenant ID and Subscription ID in the config JSON.
    """
    with open(f'{BASE_DIR}/temp/config.json'.replace("\\", "/")) as f:
        data = json.load(f)
        data['TenantId'] = tenant_id
        data['SubscriptionId'] = subscription_id
    with open(f'{BASE_DIR}/temp/config.json', 'w') as f:
        f.write(json.dumps(data))

