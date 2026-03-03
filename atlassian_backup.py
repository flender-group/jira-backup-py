import json
import sys
import subprocess
import os
import time
import argparse
import yaml
import requests
from app_logger import get_logger
from urllib3.util import Retry
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError

logging = get_logger(__name__)
# Default backup directory
BACKUP_DIR = "/backups"

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
        return secret.value
    except AzureError as e:
        raise AzureError(f"Error retrieving secret '{secret_name}' from KeyVault: {e}") from e

def remove_local_file(filename):
    try:
        os.remove(filename)
        logging.info('Local backup file %s removed after successful upload', filename)
    except FileNotFoundError as e:
        logging.warning('Local backup file %s not found for removal: %s', filename, e)
    except OSError as e:
        logging.error('Provided path is a directory: %s: %s', filename, e)

class Atlassian:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=300,
            allowed_methods=frozenset({'GET', 'POST'}),
            status_forcelist=[412, 429, 500, 502, 503, 504],
            raise_on_status=True
        )
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retries))
        self.session.auth = (config['USER_EMAIL'], config['API_TOKEN'])
        self.session.headers.update({'Content-Type': 'application/json', 'Accept': 'application/json'})
        self.payload = {"cbAttachments": self.config['INCLUDE_ATTACHMENTS'], "exportToCloud": "true"}
        self.start_confluence_backup = 'https://{}/wiki/rest/obm/1.0/runbackup'.format(self.config['HOST_URL'])
        self.start_jira_backup = 'https://{}/rest/backup/1/export/runbackup'.format(self.config['HOST_URL'])
        self.backup_status = {}
        self.wait = 60

    def create_confluence_backup(self):
        self.session.post(self.start_confluence_backup, data=json.dumps(self.payload))
        logging.info('Confluence backup process successfully started')
        confluence_backup_status = 'https://{}/wiki/rest/obm/1.0/getprogress'.format(self.config['HOST_URL'])
        time.sleep(self.wait)
        while 'fileName' not in self.backup_status.keys():
            self.backup_status = json.loads(self.session.get(confluence_backup_status).text)
            print('Current status: {progress}; {description}'.format(
                progress=self.backup_status['alternativePercentage'],
                description=self.backup_status['currentStatus']))
            logging.info("Current Confluence backup status: %s; %s", self.backup_status['alternativePercentage'], self.backup_status['currentStatus'])
            logging.info("Waiting %s seconds for next status check...", self.wait)
            time.sleep(self.wait)
        return 'https://{url}/wiki/download/{file_name}'.format(
            url=self.config['HOST_URL'], file_name=self.backup_status['fileName'])

    def create_jira_backup(self):
        backup = self.session.post(self.start_jira_backup, data=json.dumps(self.payload))
        task_id = json.loads(backup.text)['taskId']
        logging.info("Jira backup task started with taskId=%s", task_id)
        print('Jira backup process successfully started: taskId={}'.format(task_id))
        jira_backup_status = 'https://{jira_host}/rest/backup/1/export/getProgress?taskId={task_id}'.format(
            jira_host=self.config['HOST_URL'], task_id=task_id)
        time.sleep(self.wait)
        while 'result' not in self.backup_status.keys():
            self.backup_status = json.loads(self.session.get(jira_backup_status).text)
            print('Current status: {status} {progress}; {description}'.format(
                status=self.backup_status['status'],
                progress=self.backup_status['progress'],
                description=self.backup_status['description']))
            logging.info("Current Jira backup status: %s %s; %s", self.backup_status['status'], self.backup_status['progress'], self.backup_status['description'])
            logging.info("Waiting %s seconds for next status check...", self.wait)
            time.sleep(self.wait)
        return '{prefix}/{result_id}'.format(
            prefix='https://' + self.config['HOST_URL'] + '/plugins/servlet', result_id=self.backup_status['result'])

    def download_file(self, url, local_filename):
        logging.info('Downloading file from URL: %s', url)

        if not os.path.ismount(BACKUP_DIR):
            logging.error('Backup directory %s is not a mount point, cannot save backup file', BACKUP_DIR)
            raise OSError(f"Backup directory {BACKUP_DIR} is not a mount point, cannot save backup file")

        file_path = os.path.join(BACKUP_DIR, local_filename)

        cmd = [
            'wget',
            '--progress=dot:mega',
            '--auth-no-challenge',
            f'--user={self.session.auth[0]}',
            f'--password={self.session.auth[1]}',
            f'--output-document={file_path}',
            url
        ]

        logging.debug('Running wget command for file: %s', file_path)

        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True                    # decodes bytes to str
            )
            # wget writes progress to stderr
            if result.stderr:
                logging.debug('wget output: %s', result.stderr.strip())

            logging.info('Download complete. File saved to: %s', file_path)

        except subprocess.CalledProcessError as e:
            logging.error('wget failed with exit code %d: %s', e.returncode, e.stderr.strip())
            # remove partial file if download failed
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.warning('Removed incomplete file: %s', file_path)
            raise
        except FileNotFoundError:
            logging.error('wget is not installed or not found in PATH')
            raise

    def upload_to_azure(self, blob_name, local_filename):
        logging.info('Uploading Backup %s to Azure Blob Storage', blob_name)
        logging.debug("Azure upload configuration: %s", self.config['UPLOAD_TO_AZURE'])
        
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
        logging.debug('Using Azure container: %s', container_name)
        
        blob_name = f"{self.config['UPLOAD_TO_AZURE']['AZURE_DIR']}{blob_name}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        block_list = []
        block_size = 4 * 1024 * 1024  # 4MB chunks
        uploaded = 0
        file_size = os.path.getsize(local_filename)
        with open(local_filename, 'rb') as file:
            block_id = 0
            while True:
                chunk = file.read(block_size)
                if not chunk:
                    break
                
                block_id_str = f"{block_id:08d}".encode()
                blob_client.stage_block(block_id_str, chunk)
                block_list.append(block_id_str)
                uploaded += len(chunk)
                if file_size:
                    pct = uploaded * 100.0 / file_size
                    logging.info(
                        'Uploaded block %d: %d/%d bytes (%.2f%%) for %s',
                        block_id, uploaded, file_size, pct, blob_name
                    )
                else:
                    logging.info(
                        'Uploaded block %d: %d bytes so far for %s',
                        block_id, uploaded, blob_name
                    )
                block_id += 1

        blob_client.commit_block_list(block_list)
        logging.info('Successfully uploaded backup blob %s to Azure storage container: %s', blob_name, container_name)
        remove_local_file(local_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-C', type=str, dest='config_file', default='', help='path to config file')
    parser.add_argument('-d', type=str, dest='backup_url', default='', help='atlassian backup url')
    parser.add_argument('-c', action='store_true', dest='confluence', help='activate confluence backup')
    parser.add_argument('-j', action='store_true', dest='jira', help='activate jira backup')
    parser.add_argument('--kv-url', type=str, help='KeyVault URL (Full http URL) for API Token Secret')
    parser.add_argument('--kv-secret-name', type=str, default='api-token', help='KeyVault secret name for API Token')
    parser.add_argument('--verbose', action='store_true', help='enable verbose logging')
    args = parser.parse_args()

    if args.verbose:
        logging = get_logger(name="backup", level=10)
    else:
        logging = get_logger(name="backup", level=20)

    config = read_config(args.config_file)

    if args.kv_url and args.kv_secret_name:
        try:
            api_token = get_secret_from_keyvault(args.kv_url, args.kv_secret_name)
            config['API_TOKEN'] = api_token
            logging.debug('Retrieved API token from KeyVault and updated config')
        except AzureError as e:
            logging.error("Failed to retrieve API token from KeyVault: %s", e)
            print(f"Error: Failed to retrieve API token from KeyVault: {e}")
            sys.exit(1) 

    if config['HOST_URL'] == 'something.atlassian.net':
        logging.error('Configuration file not set up properly')
        raise ValueError('You forgot to edit config.yaml or to run the backup script with "-w" flag')

    atlass = Atlassian(config)
    if not args.backup_url:
        logging.debug('No backup URL provided, initiating backup process')
        logging.info('Starting backup; include attachments: %s', config['INCLUDE_ATTACHMENTS'])
        if args.confluence: 
            backup_url = atlass.create_confluence_backup()
            logging.debug('Confluence backup URL obtained: %s', backup_url)
        else: 
            backup_url = atlass.create_jira_backup()
            logging.debug('Jira backup URL obtained: %s', backup_url)
    else:
        backup_url = args.backup_url

    logging.info('Backup URL: %s', backup_url)
    file_name = '{timestemp}_{uuid}.zip'.format(
        timestemp=time.strftime('%d%m%Y_%H%M'), uuid=backup_url.split('/')[-1].replace('?fileId=', ''))
    full_path = os.path.join(BACKUP_DIR, file_name)

    atlass.download_file(backup_url, file_name)
    logging.debug('Downloaded backup file locally: %s', file_name)

    if 'UPLOAD_TO_AZURE' in config and config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', '') != '':
        atlass.upload_to_azure(file_name, full_path)
        if args.confluence:
            logging.debug("Successfully uploaded Confluence backup to Azure container: %s", config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', ''))
        elif args.jira:
            logging.debug("Successfully uploaded Jira backup to Azure container: %s", config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', ''))
