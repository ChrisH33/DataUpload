import os
import logging
import time
import platform
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import shutil

# >>>>>>>>>> CONFIG <<<<<<<<<<
windows_prefix = "W:/"
linux_prefix = "/mnt/dna_pipelines/"
network_subdir = "0.253 Short Read (SR)/1. Short Read Library creation/8. MAVE_SGE/SGE Upload"

# Absolute directories
if platform.system() == 'Linux':
    network_dir = os.path.join(linux_prefix, network_subdir)
else:
    network_dir = os.path.join(windows_prefix, network_subdir)

processed_dir = os.path.join(network_dir, "Processed")
os.makedirs(processed_dir, exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/drive.file']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')

# OAuth2 login (opens browser once)
flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
creds = flow.run_local_server(port=0)

WAIT_SECONDS = 45

# >>>>>>>>>> LOGGING <<<<<<<<<<
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# >>>>>>>>>> FUNCTIONS <<<<<<<<<<
def script_wait(seconds=WAIT_SECONDS):
    logger.info(f"Waiting for {seconds} seconds...")
    time.sleep(seconds)

def move_to_processed(file_path):
    try:
        dest_path = os.path.join(processed_dir, os.path.basename(file_path))
        shutil.move(file_path, dest_path)
        logger.info(f"Moved file to processed: {dest_path}")
    except Exception as e:
        logger.error(f"Failed to move file {file_path} to processed: {e}")

# >>>>>>>>>> MAIN LOOP <<<<<<<<<<
while True:
    # Check the upload folder is accessible
    if not os.path.isdir(network_dir):
        logger.error(f"Cannot access the network directory: {network_dir}")
        script_wait()
        continue

    # Find CSV files
    try:
        files = [f for f in os.listdir(network_dir) if f.lower().endswith(".csv")]
        if not files:
            logger.info("No CSV files found.")
            script_wait()
            continue
    except Exception as e:
        logger.error(f"Error accessing files: {e}")
        script_wait()
        continue

    # Upload each file and move to processed
    for file_name in files:
        full_path = os.path.join(network_dir, file_name)

    # Wait a bit before next check (Linux runs continuously, Windows breaks)
    if platform.system() != 'Linux':
        break

    script_wait()
