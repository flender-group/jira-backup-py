import json
import yaml
import time
import os
import argparse
import requests
import boto3
from boto3.s3.transfer import TransferConfig
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
import wizard
import platform
import subprocess
import sys
from app_logger import get_logger
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError

def read_config(path=''):
    if path == '':
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    with open(path, 'r') as config_file:
        return yaml.full_load(config_file)

def az_login():
    azure_client_id = os.getenv("AZURE_CLIENT_ID")
    if azure_client_id:
        credential = ManagedIdentityCredential(client_id=azure_client_id)
    else:
        credential = DefaultAzureCredential()
    return credential

def get_secret_from_keyvault(kv_url, secret_name='api-token'):
    try:
        secret_client = SecretClient(vault_url=kv_url, credential=az_login())
        secret = secret_client.get_secret(secret_name)
        logging.info(f"Successfully retrieved secret '{secret_name}' from KeyVault")
        return secret.value
    except AzureError as e:
        logging.error(f"Error retrieving secret from KeyVault: {e}")
        raise e

def retry_with_exponential_backoff(func, delay=300, max_retries=5, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed with error: {e.with_traceback(None)}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logging.error(f"Max retries reached. Operation failed. Error: {e.with_traceback(None)}")
                return func()

class Atlassian:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.auth = (config['USER_EMAIL'], config['API_TOKEN'])
        self.session.headers.update({'Content-Type': 'application/json', 'Accept': 'application/json'})
        self.payload = {"cbAttachments": self.config['INCLUDE_ATTACHMENTS'], "exportToCloud": "true"}
        self.start_confluence_backup = 'https://{}/wiki/rest/obm/1.0/runbackup'.format(self.config['HOST_URL'])
        self.start_jira_backup = 'https://{}/rest/backup/1/export/runbackup'.format(self.config['HOST_URL'])
        self.backup_status = {}
        self.wait = 60

    def stream_to_azure(self, url, remote_filename):
        logging.info(f'Streaming Backup {remote_filename} to Azure Blob Storage')
        logging.debug('Azure upload configuration: {}'.format(self.config['UPLOAD_TO_AZURE']))
        
        if self.config['UPLOAD_TO_AZURE']['AZURE_CONNECTION_STRING']:
            blob_service_client = BlobServiceClient.from_connection_string(
                self.config['UPLOAD_TO_AZURE']['AZURE_CONNECTION_STRING']
            )
        elif self.config['UPLOAD_TO_AZURE']['AZURE_ACCOUNT_KEY']:
            account_url = f"https://{self.config['UPLOAD_TO_AZURE']['AZURE_ACCOUNT_NAME']}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.config['UPLOAD_TO_AZURE']['AZURE_ACCOUNT_KEY']
            )
        elif self.config['UPLOAD_TO_AZURE']['AZURE_MANAGED_SYSTEM_IDENTITY']:
            account_url = f"https://{self.config['UPLOAD_TO_AZURE']['AZURE_ACCOUNT_NAME']}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url, credential=az_login())
        else:
            logging.error('No valid Azure authentication method found in configuration')
            raise Exception('Unsupported authentication configuration')
        
        container_name = self.config['UPLOAD_TO_AZURE']['AZURE_CONTAINER']
        logging.debug('Using Azure container: {}'.format(container_name))

        blob_name = f"{self.config['UPLOAD_TO_AZURE']['AZURE_DIR']}{remote_filename}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        def do_upload():
            r = self.session.get(url, stream=True, timeout=60)
            r.raise_for_status()

            content_length = int(r.headers.get('Content-Length', 0)) or None
            chunk_size = 4 * 1024 * 1024  # 4 MB

            def body():
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        yield chunk

            blob_client.upload_blob(
                data=body(),
                overwrite=True,
                length=content_length,
                content_type=r.headers.get('content-type', 'application/zip'),
            )

        retry_with_exponential_backoff(do_upload, max_retries=10)
        logging.info(f'Successfully uploaded backup blob {blob_name} to Azure storage container: {container_name}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-C', type=str, dest='config_file', default='', help='path to config file')
    parser.add_argument('-d', type=str, dest='backup_url', default='', help='URL to download into the storage configuration')
    parser.add_argument('-w', action='store_true', dest='wizard', help='activate config wizard')
    parser.add_argument('-c', action='store_true', dest='confluence', help='activate confluence backup')
    parser.add_argument('-j', action='store_true', dest='jira', help='activate jira backup')
    parser.add_argument('-s', '--schedule', action='store_true', dest='schedule', help='setup automated scheduled backup')
    parser.add_argument('--schedule-days', type=int, default=4, help='frequency in days for scheduled backup (default: 4)')
    parser.add_argument('--schedule-time', type=str, default='10:00', help='time for scheduled backup in HH:MM format (default: 10:00)')
    parser.add_argument('--schedule-service', type=str, choices=['jira', 'confluence'], default='jira', help='service type for scheduled backup (default: jira)')
    parser.add_argument('--kv-url', type=str, help='KeyVault URL (Full http URL) for API Token Secret')
    parser.add_argument('--kv-secret-name', type=str, default='api-token', help='KeyVault secret name for API Token')
    parser.add_argument('--verbose', action='store_true', help='enable verbose logging')
    parser.add_argument('--url', type=str, dest='backup_url', default='', help='URL to download into the storage configuration')
    args = parser.parse_args()
    # print('debug command-line: {}'.format(args))

    if args.verbose:
        logging = get_logger(name="backup", level=10)
    else:
        logging = get_logger(name="backup", level=20)
    
    if args.wizard:
        wizard.create_config()
    
    config = read_config(args.config_file)

    if args.kv_url and args.kv_secret_name:
        try:
            api_token = get_secret_from_keyvault(args.kv_url, args.kv_secret_name)
            config['API_TOKEN'] = api_token
            logging.debug('Retrieved API token from KeyVault and set as environment variable')
        except Exception as e:
            logging.error(f"Failed to retrieve API token from KeyVault: {e}")
            print(f"Error: Failed to retrieve API token from KeyVault: {e}")
            exit(1) 

    if config['HOST_URL'] == 'something.atlassian.net':
        logging.error('Configuration file not set up properly')
        raise ValueError('You forgot to edit config.yaml or to run the backup script with "-w" flag')

    atlass = Atlassian(config)
    backup_url = args.backup_url
    file_name = '{timestemp}_{uuid}.zip'.format(
        timestemp=time.strftime('%d%m%Y_%H%M'), uuid=backup_url.split('/')[-1].replace('?fileId=', ''))

    if 'UPLOAD_TO_AZURE' in config and config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', '') != '':
        atlass.stream_to_azure(backup_url, file_name)
        if args.confluence:
            logging.debug('Successfully uploaded Confluence backup to Azure container: {}'.format(config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', '')))
        elif args.jira:
            logging.debug('Successfully uploaded Jira backup to Azure container: {}'.format(config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', '')))
