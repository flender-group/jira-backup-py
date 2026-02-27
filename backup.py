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

logging = get_logger(__name__)

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

def retry_with_exponential_backoff(func, delay=300, max_retries=5, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            response = func()
            if response is not None:
                response.raise_for_status()
            return response
        except Exception as e:
            logging.warning("Attempt %d failed with error: %s", attempt + 1, e, exc_info=True)
            if attempt < max_retries - 1:
                logging.info("Retrying in %s seconds...", delay)
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logging.error("Max retries reached. Operation failed. Error: %s", e, exc_info=True)
                sys.exit(1)

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

    def create_confluence_backup(self):
        backup = retry_with_exponential_backoff(lambda: self.session.post(self.start_confluence_backup, data=json.dumps(self.payload)))
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
        backup = retry_with_exponential_backoff(lambda: self.session.post(self.start_jira_backup, data=json.dumps(self.payload)))
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
        print('Downloading file from URL: {}'.format(url))
        r = self.session.get(url, stream=True)
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups', local_filename)
        with open(file_path, 'wb') as file_:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    file_.write(chunk)
        print(file_path)

    def stream_to_s3(self, url, remote_filename):
        print('Streaming to S3')

        if self.config['UPLOAD_TO_S3']['AWS_ACCESS_KEY'] == '':
            s3_client = boto3.client('s3')
        else:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['UPLOAD_TO_S3']['AWS_ACCESS_KEY'],
                aws_secret_access_key=self.config['UPLOAD_TO_S3']['AWS_SECRET_KEY'],
                region_name=self.config['UPLOAD_TO_S3']['AWS_REGION'],
                endpoint_url=self.config['UPLOAD_TO_S3']['AWS_ENDPOINT_URL'],
                use_ssl=self.config['UPLOAD_TO_S3']['AWS_IS_SECURE']
            )

        bucket_name = self.config['UPLOAD_TO_S3']['S3_BUCKET']
        r = self.session.get(url, stream=True)
        if r.status_code == 200:
            key = "{s3_bucket}{s3_filename}".format(
                s3_bucket=self.config['UPLOAD_TO_S3']['S3_DIR'],
                s3_filename=remote_filename
            )

            content_length = int(r.headers.get('Content-Length', 0))

            config = TransferConfig(
                multipart_threshold=content_length + 1,
                max_concurrency=1,
                use_threads=False
            )

            s3_client.upload_fileobj(
                Fileobj=r.raw,
                Bucket=bucket_name,
                Key=key,
                ExtraArgs={'ContentType': r.headers['content-type']},
                Config=config
            )

    def stream_to_gcs(self, url, remote_filename):
        print('Streaming to GCS')
        
        if self.config['UPLOAD_TO_GCP']['GCP_SERVICE_ACCOUNT_KEY']:
            client = storage.Client.from_service_account_json(
                self.config['UPLOAD_TO_GCP']['GCP_SERVICE_ACCOUNT_KEY'],
                project=self.config['UPLOAD_TO_GCP']['GCP_PROJECT_ID']
            )
        else:
            client = storage.Client(project=self.config['UPLOAD_TO_GCP']['GCP_PROJECT_ID'])
        
        bucket_name = self.config['UPLOAD_TO_GCP']['GCS_BUCKET']
        bucket = client.bucket(bucket_name)
        
        r = self.session.get(url, stream=True)
        if r.status_code == 200:
            blob_name = "{gcs_dir}{filename}".format(
                gcs_dir=self.config['UPLOAD_TO_GCP']['GCS_DIR'],
                filename=remote_filename
            )
            
            blob = bucket.blob(blob_name)
            blob.content_type = r.headers.get('content-type', 'application/zip')
            
            blob.upload_from_file(r.raw, content_type=blob.content_type)

    def stream_to_azure(self, url, remote_filename):
        logging.info('Streaming Backup %s to Azure Blob Storage', remote_filename)
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
        
        blob_name = f"{self.config['UPLOAD_TO_AZURE']['AZURE_DIR']}{remote_filename}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        def do_upload():
            r = self.session.get(url, stream=True, timeout=60)
            r.raise_for_status()

            content_length = int(r.headers.get('Content-Length', 0)) or None
            chunk_size = 4 * 1024 * 1024  # 4 MB
            uploaded = 0

            def body():
                nonlocal uploaded
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    uploaded += len(chunk)
                    if content_length:
                        pct = uploaded * 100.0 / content_length
                        logging.info(
                            "Uploaded %d/%d bytes (%.2f%%)...", uploaded, content_length, pct
                        )
                    else:
                        logging.info("Uploaded %d bytes so far...", uploaded)
                    yield chunk

            blob_client.upload_blob(
                data=body(),
                overwrite=True,
                length=content_length,
                content_type=r.headers.get('content-type', 'application/zip'),
            )

        retry_with_exponential_backoff(do_upload, max_retries=10)
        logging.info('Successfully uploaded backup blob %s to Azure storage container: %s', blob_name, container_name)

def setup_scheduled_task(frequency_days=4, time_hour=10, time_minute=0, service_type='jira'):
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    
    system = platform.system().lower()
    
    if system in ['linux', 'darwin']:
        return setup_cron_task(script_path, script_dir, frequency_days, time_hour, time_minute, service_type)
    elif system == 'windows':
        return setup_windows_task(script_path, script_dir, frequency_days, time_hour, time_minute, service_type)
    else:
        raise Exception(f"Unsupported operating system: {system}")


def setup_cron_task(script_path, script_dir, frequency_days, time_hour, time_minute, service_type):
    python_path = sys.executable
    service_flag = '-j' if service_type == 'jira' else '-c'
    
    cron_command = f"{time_minute} {time_hour} */{frequency_days} * * cd {script_dir} && {python_path} {script_path} {service_flag}"
    
    try:
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        existing_cron = result.stdout if result.returncode == 0 else ""
        
        # Remove only the cron entry for the same service type
        lines = existing_cron.strip().split('\n') if existing_cron.strip() else []
        updated_lines = []
        skip_next = False
        
        for i, line in enumerate(lines):
            if skip_next:
                skip_next = False
                continue
            
            # Check if this is a comment line for jira-backup-py
            if 'jira-backup-py automated backup' in line and f'({service_type})' in line:
                # Check if the next line contains the cron command for this service
                if i + 1 < len(lines) and service_flag in lines[i + 1]:
                    skip_next = True  # Skip both the comment and the command
                    print(f"Updating existing {service_type} backup schedule...")
                    continue
            
            updated_lines.append(line)
        
        existing_cron = '\n'.join(updated_lines) + '\n' if updated_lines else ""
        new_cron = existing_cron + f"# jira-backup-py automated backup ({service_type})\n{cron_command}\n"
        
        process = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, text=True)
        process.communicate(input=new_cron)
        
        if process.returncode == 0:
            print(f"Successfully scheduled {service_type} backup to run every {frequency_days} days at {time_hour:02d}:{time_minute:02d}")
            return True
        else:
            print("Failed to create cron job")
            return False
            
    except Exception as e:
        print(f"Error setting up cron job: {e}")
        return False


def setup_windows_task(script_path, script_dir, frequency_days, time_hour, time_minute, service_type):
    python_path = sys.executable
    service_flag = '-j' if service_type == 'jira' else '-c'
    task_name = f"jira-backup-py-{service_type}"
    
    cmd = [
        'schtasks', '/create',
        '/tn', task_name,
        '/sc', 'DAILY',
        '/mo', str(frequency_days),
        '/tr', f'"{python_path}" "{script_path}" {service_flag}',
        '/st', f'{time_hour:02d}:{time_minute:02d}',
        '/f'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Successfully scheduled {service_type} backup to run every {frequency_days} days at {time_hour:02d}:{time_minute:02d}")
            return True
        else:
            print(f"Failed to create scheduled task: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error setting up scheduled task: {e}")
        return False

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
    args = parser.parse_args()
    # print('debug command-line: {}'.format(args))

    if args.verbose:
        logging = get_logger(name="backup", level=10)
    else:
        logging = get_logger(name="backup", level=20)
    
    if args.wizard:
        wizard.create_config()

    if args.schedule:
        try:
            time_parts = args.schedule_time.split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            
            if not (0 <= hour <= 23) or not (0 <= minute <= 59):
                raise ValueError("Invalid time format")
                
            setup_scheduled_task(
                frequency_days=args.schedule_days,
                time_hour=hour,
                time_minute=minute,
                service_type=args.schedule_service
            )
            print("Scheduled task setup completed")
            sys.exit(0)
        except ValueError as e:
            logging.error("Invalid time format: %s", e)
            print(f"Error: Invalid time format. Use HH:MM format (e.g., 10:30)")
            sys.exit(1)
        except Exception as e:
            logging.error("Error setting up scheduled task: %s", e)
            print(f"Error setting up scheduled task: {e}")
            sys.exit(1)
    
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

    if config['DOWNLOAD_LOCALLY'] == 'true':
        atlass.download_file(backup_url, file_name)
        logging.debug('Downloaded backup file locally: %s', file_name)

    if 'UPLOAD_TO_S3' in config and config['UPLOAD_TO_S3'].get('S3_BUCKET', '') != '':
        atlass.stream_to_s3(backup_url, file_name)
        logging.debug("Uploaded to S3 bucket: %s", config['UPLOAD_TO_S3'].get('S3_BUCKET', ''))
    
    if 'UPLOAD_TO_GCP' in config and config['UPLOAD_TO_GCP'].get('GCS_BUCKET', '') != '':
        atlass.stream_to_gcs(backup_url, file_name)
        logging.debug("Uploaded to GCS bucket: %s", config['UPLOAD_TO_GCP'].get('GCS_BUCKET', ''))
    
    if 'UPLOAD_TO_AZURE' in config and config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', '') != '':
        atlass.stream_to_azure(backup_url, file_name)
        if args.confluence:
            logging.debug("Successfully uploaded Confluence backup to Azure container: %s", config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', ''))
        elif args.jira:
            logging.debug("Successfully uploaded Jira backup to Azure container: %s", config['UPLOAD_TO_AZURE'].get('AZURE_CONTAINER', ''))
