from OEA_Portal.auth.AzureClient import AzureClient

def get_blob_contents(azure_client:AzureClient, storage_account_name, container_name, blob_name):
    """
    Downloads and returns the contents of a blob in a given storage account.
    """
    try:
        data = azure_client.get_blob_client(storage_account_name, container_name, blob_name).download_blob()
    except:
        raise Exception(f'Unable to download blob from Storage account - {storage_account_name}')
    return data