import os
import json
import hashlib
import logging
import threading
from queue import Queue
from datetime import datetime
from dotenv import load_dotenv
import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from colorama import init, Fore, Style
import pyfiglet
from tabulate import tabulate

# Initialize colorama for colored terminal output
init()

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gcp-keys.json"

# --- Configuration Manager ---
class ConfigManager:
    def __init__(self):
        load_dotenv()
        self.config = {
            "aws_access_key": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "aws_bucket": os.getenv("AWS_BUCKET_NAME", "my-backup-bucket"),
            "gcp_project": os.getenv("GCP_PROJECT_ID"),
            "gcp_bucket": os.getenv("GCP_BUCKET_NAME", "my-backup-bucket"),
            "azure_connection_string": os.getenv("AZURE_CONNECTION_STRING"),
            "azure_container": os.getenv("AZURE_CONTAINER_NAME", "mybackupcontainer"),
            "backup_file_list": os.getenv("BACKUP_FILE_LIST", "backup_files.txt"),
            "log_file": os.getenv("LOG_FILE", "logs.json"),
        }

    def get(self, key):
        return self.config.get(key)

# --- Storage Abstraction Layer ---
class CloudStorage:
    def upload_file(self, file_path, destination):
        raise NotImplementedError

    def file_exists(self, file_path, destination):
        raise NotImplementedError

    def list_files(self):
        raise NotImplementedError

    def download_file(self, destination, local_path):
        raise NotImplementedError

    @property
    def name(self):
        raise NotImplementedError

class S3Storage(CloudStorage):
    def __init__(self, access_key, secret_key, bucket):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self._name = "AWS S3"

    @property
    def name(self):
        return self._name

    def upload_file(self, file_path, destination):
        try:
            print(f"{Fore.YELLOW}Uploading {file_path} to AWS S3...{Style.RESET_ALL}")
            self.client.upload_file(file_path, self.bucket, destination)
            print(f"{Fore.GREEN}Successfully uploaded {file_path} to AWS S3{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}AWS S3 upload failed: {str(e)}{Style.RESET_ALL}")
            logging.error(f"AWS S3 upload failed: {str(e)}")
            return False

    def file_exists(self, file_path, destination):
        try:
            self.client.head_object(Bucket=self.bucket, Key=destination)
            return True
        except self.client.exceptions.ClientError:
            return False

    def list_files(self):
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            logging.error(f"Failed to list files in AWS S3: {str(e)}")
            return []

    def download_file(self, destination, local_path):
        try:
            self.client.download_file(self.bucket, destination, local_path)
            print(f"{Fore.GREEN}Downloaded {destination} from AWS S3 to {local_path}{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}AWS S3 download failed: {str(e)}{Style.RESET_ALL}")
            logging.error(f"AWS S3 download failed: {str(e)}")
            return False

class GCPStorage(CloudStorage):
    def __init__(self, project_id, bucket):
        self.bucket_name = bucket
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket)
        self._name = "Google Cloud Storage"

    @property
    def name(self):
        return self._name

    def upload_file(self, file_path, destination):
        try:
            print(f"{Fore.YELLOW}Uploading {file_path} to Google Cloud Storage...{Style.RESET_ALL}")
            blob = self.bucket.blob(destination)
            blob.upload_from_filename(file_path)
            print(f"{Fore.GREEN}Successfully uploaded {file_path} to Google Cloud Storage{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}GCP Storage upload failed: {str(e)}{Style.RESET_ALL}")
            logging.error(f"GCP Storage upload failed: {str(e)}")
            return False

    def file_exists(self, file_path, destination):
        blob = self.bucket.blob(destination)
        return blob.exists()

    def list_files(self):
        try:
            blobs = self.bucket.list_blobs()
            return [blob.name for blob in blobs]
        except Exception as e:
            logging.error(f"Failed to list files in GCP Storage: {str(e)}")
            return []

    def download_file(self, destination, local_path):
        try:
            blob = self.bucket.blob(destination)
            blob.download_to_filename(local_path)
            print(f"{Fore.GREEN}Downloaded {destination} from Google Cloud Storage to {local_path}{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}GCP Storage download failed: {str(e)}{Style.RESET_ALL}")
            logging.error(f"GCP Storage download failed: {str(e)}")
            return False

class AzureStorage(CloudStorage):
    def __init__(self, connection_string, container):
        self.container = container
        self.client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.client.get_container_client(container)
        self._name = "Azure Blob Storage"

    @property
    def name(self):
        return self._name

    def upload_file(self, file_path, destination):
        try:
            print(f"{Fore.YELLOW}Uploading {file_path} to Azure Blob Storage...{Style.RESET_ALL}")
            blob_client = self.container_client.get_blob_client(destination)
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"{Fore.GREEN}Successfully uploaded {file_path} to Azure Blob Storage{Style.RESET_ALL}")
            return True
        except ResourceExistsError:
            return True
        except Exception as e:
            print(f"{Fore.RED}Azure Blob upload failed: {str(e)}{Style.RESET_ALL}")
            logging.error(f"Azure Blob upload failed: {str(e)}")
            return False

    def file_exists(self, file_path, destination):
        try:
            blob_client = self.container_client.get_blob_client(destination)
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    def list_files(self):
        try:
            blobs = self.container_client.list_blobs()
            return [blob.name for blob in blobs]
        except Exception as e:
            logging.error(f"Failed to list files in Azure Storage: {str(e)}")
            return []

    def download_file(self, destination, local_path):
        try:
            blob_client = self.container_client.get_blob_client(destination)
            with open(local_path, "wb") as f:
                blob_data = blob_client.download_blob()
                f.write(blob_data.readall())
            print(f"{Fore.GREEN}Downloaded {destination} from Azure Blob Storage to {local_path}{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}Azure Blob download failed: {str(e)}{Style.RESET_ALL}")
            logging.error(f"Azure Blob download failed: {str(e)}")
            return False

# --- Sync Manager ---
class SyncManager:
    def __init__(self, storage_providers, log_file):
        self.storage_providers = storage_providers
        self.log_file = log_file
        self.uploaded_files = self.load_uploaded_files()

    def load_uploaded_files(self):
        """Load previously uploaded files' hashes from log."""
        try:
            with open(self.log_file, "r") as f:
                return json.load(f).get("uploaded_files", {})
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_uploaded_files(self):
        """Save uploaded files' hashes to log."""
        with open(self.log_file, "w") as f:
            json.dump({"uploaded_files": self.uploaded_files}, f, indent=4)

    def get_file_hash(self, file_path):
        """Calculate SHA-256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def distribute_files(self, file_paths):
        """Distribute files equally across storage providers."""
        n_providers = len(self.storage_providers)
        if not file_paths:
            return []

        base_count = len(file_paths) // n_providers
        remainder = len(file_paths) % n_providers
        distribution = []

        start = 0
        for i, provider in enumerate(self.storage_providers):
            count = base_count + (1 if i < remainder else 0)
            distribution.append((provider, file_paths[start:start + count]))
            start += count

        return distribution

    def sync_files(self, file_paths):
        """Sync files to assigned cloud providers, checking for duplicates."""
        if not file_paths:
            print("No files to upload.")
            return

        # Load already backed-up files
        backed_up = self.uploaded_files
        files_to_upload = []

        # Check which files need uploading
        for file_path in file_paths:
            if not os.path.exists(file_path):
                print(f"{Fore.RED}File does not exist: {file_path}{Style.RESET_ALL}")
                continue
            file_hash = self.get_file_hash(file_path)
            if file_path in backed_up and backed_up[file_path].get("hash") == file_hash:
                print(f"Skipping already backed-up file: {file_path}")
                continue
            files_to_upload.append(file_path)

        if not files_to_upload:
            print("All files are already backed up.")
            return

        # Distribute files across providers
        distribution = self.distribute_files(files_to_upload)
        upload_queue = Queue()

        # Enqueue uploads
        for provider, assigned_files in distribution:
            for file_path in assigned_files:
                destination = os.path.basename(file_path)
                if provider.file_exists(file_path, destination):
                    print(f"File already exists in {provider.name}: {destination}")
                    continue
                upload_queue.put((provider, file_path, destination, self.get_file_hash(file_path)))

        # Worker function for uploads
        def upload_worker():
            while True:
                try:
                    provider, file_path, destination, file_hash = upload_queue.get_nowait()
                except Queue.Empty:
                    break

                success = provider.upload_file(file_path, destination)
                if success:
                    self.uploaded_files[file_path] = {
                        "hash": file_hash,
                        "last_uploaded": datetime.now().isoformat(),
                        "service": provider.name,
                        "destination": destination
                    }
                    self.save_uploaded_files()
                upload_queue.task_done()

        # Start upload workers
        threads = []
        for _ in range(min(3, upload_queue.qsize())):
            t = threading.Thread(target=upload_worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def list_backed_up_files(self):
        """List all files stored across cloud providers."""
        file_list = []
        serial_no = 1
        seen_files = set()

        # First, include files from logs
        for file_path, data in self.uploaded_files.items():
            file_name = os.path.basename(file_path)
            if file_name not in seen_files:
                file_list.append([
                    serial_no,
                    file_name[:10],
                    data.get("service", "Unknown")
                ])
                seen_files.add(file_name)
                serial_no += 1

        # Then, check clouds for additional files not in logs
        for provider in self.storage_providers:
            cloud_files = provider.list_files()
            for file_name in cloud_files:
                if file_name not in seen_files:
                    file_list.append([
                        serial_no,
                        file_name[:10],
                        provider.name
                    ])
                    seen_files.add(file_name)
                    serial_no += 1

        if not file_list:
            print("No files found in backup storage.")
            return []

        # Print table
        headers = ["Sr No", "File Name (First 10)", "Source Service"]
        print("\n" + tabulate(file_list, headers=headers, tablefmt="grid"))
        return file_list

    def download_file(self, serial_no, file_list):
        """Download a file based on serial number."""
        if not (1 <= serial_no <= len(file_list)):
            print(f"{Fore.RED}Invalid serial number{Style.RESET_ALL}")
            return False

        file_entry = file_list[serial_no - 1]
        file_name_prefix = file_entry[1]
        service_name = file_entry[2]

        # Try to find the full file name and service from logs
        full_file_name = None
        matched_service = None
        for file_path, data in self.uploaded_files.items():
            file_name = os.path.basename(file_path)
            if file_name.startswith(file_name_prefix):
                log_service = data.get("service", "Unknown")
                if service_name == "Unknown" or log_service == service_name:
                    full_file_name = file_name
                    matched_service = log_service
                    break

        # If not found in logs or service is Unknown, query clouds
        if not full_file_name or matched_service == "Unknown":
            for provider in self.storage_providers:
                cloud_files = provider.list_files()
                for fname in cloud_files:
                    if fname.startswith(file_name_prefix):
                        if service_name == "Unknown" or provider.name == service_name:
                            full_file_name = fname
                            matched_service = provider.name
                            break
                if full_file_name:
                    break

        if not full_file_name:
            print(f"{Fore.RED}File not found: {file_name_prefix}{Style.RESET_ALL}")
            return False

        # Determine provider
        provider = next((p for p in self.storage_providers if p.name == matched_service), None)
        if not provider:
            for p in self.storage_providers:
                if p.file_exists(None, full_file_name):
                    provider = p
                    break

        if not provider:
            print(f"{Fore.RED}Service not found for file: {full_file_name}{Style.RESET_ALL}")
            return False

        # Download to current directory
        local_path = os.path.join(os.getcwd(), full_file_name)
        return provider.download_file(full_file_name, local_path)

# --- Main Application ---
def load_file_list(file_list_path):
    """Load list of files to back up from txt file."""
    try:
        with open(file_list_path, "r") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"{Fore.RED}File list not found: {file_list_path}{Style.RESET_ALL}")
        logging.error(f"File list not found: {file_list_path}")
        return []

def display_menu():
    """Display the interactive menu."""
    print("\n" + pyfiglet.figlet_format("Multi-Cloud Backup System"))
    print("Select an option:")
    print("1. List backed-up files")
    print("2. Upload files from backup_files.txt")
    print("3. Download backed-up files")
    print("4. Exit")

def main():
    # Initialize logging and suppress Azure SDK verbosity
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("backup.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger("azure").setLevel(logging.WARNING)

    # Load configuration
    config = ConfigManager()

    # Initialize storage providers
    storage_providers = [
        S3Storage(
            config.get("aws_access_key"),
            config.get("aws_secret_key"),
            config.get("aws_bucket")
        ),
        GCPStorage(
            config.get("gcp_project"),
            config.get("gcp_bucket")
        ),
        AzureStorage(
            config.get("azure_connection_string"),
            config.get("azure_container")
        )
    ]

    # Initialize sync manager
    sync_manager = SyncManager(storage_providers, config.get("log_file"))

    while True:
        display_menu()
        choice = input("\nEnter your choice (1-4): ").strip()

        if choice == "1":
            sync_manager.list_backed_up_files()

        elif choice == "2":
            file_paths = load_file_list(config.get("backup_file_list"))
            if file_paths:
                print("\nChecking files to upload...")
                sync_manager.sync_files(file_paths)
            else:
                print("No files to upload.")

        elif choice == "3":
            file_list = sync_manager.list_backed_up_files()
            if file_list:
                serial_input = input("\nEnter serial number(s) to download (e.g., 1 or 1,2,3): ").strip()
                try:
                    # Parse serial numbers (comma-separated)
                    serial_numbers = [int(x) for x in serial_input.replace(" ", "").split(",")]
                    if not serial_numbers:
                        print(f"{Fore.RED}No serial numbers provided{Style.RESET_ALL}")
                        continue

                    # Validate all serial numbers
                    invalid = [sn for sn in serial_numbers if not (1 <= sn <= len(file_list))]
                    if invalid:
                        print(f"{Fore.RED}Invalid serial number(s): {', '.join(map(str, invalid))}{Style.RESET_ALL}")
                        continue

                    # Download each file
                    for sn in serial_numbers:
                        print(f"\nDownloading file with serial number {sn}...")
                        success = sync_manager.download_file(sn, file_list)
                        if not success:
                            print(f"{Fore.RED}Failed to download file with serial number {sn}{Style.RESET_ALL}")

                except ValueError:
                    print(f"{Fore.RED}Please enter valid numbers (e.g., 1 or 1,2,3){Style.RESET_ALL}")
            else:
                print("No files available to download.")

        elif choice == "4":
            print("Exiting Multi-Cloud Backup System.")
            break

        else:
            print(f"{Fore.RED}Invalid choice. Please select 1, 2, 3, or 4.{Style.RESET_ALL}")

if __name__ == "__main__":
    main()