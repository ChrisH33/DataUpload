import os
import logging
import time
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

## Define Network Locations
network_dir = "W:/0.253 Short Read (SR)/1. Short Read Library creation/8. MAVE_SGE/SGE Upload"
processed_dir = os.path.join(network_dir, "Processed")  # move uploaded files here

# ---------- CONFIG ----------
SERVICE_ACCOUNT_FILE = "path/to/service_account.json"  # downloaded JSON
SCOPES = ['https://www.googleapis.com/auth/drive.file']
LOCAL_FILE_PATH = "C:/Users/ch33/Documents/MyFile.csv"  # file you want to upload
FOLDER_ID = "YOUR_FOLDER_ID_HERE"  # see below

## Declare Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def script_wait(seconds=45):
    logger.info(f"Waiting for {seconds} seconds...")
    time.sleep(seconds)

while True:
    if not os.path.isdir(network_dir):
        logger.error(f"Cannot access the network directory: {network_dir}")
        script_wait()
        continue
    os.makedirs(processed_dir, exist_ok=True)

    try:
        files = [os.path.join(network_dir, f) for f in os.listdir(network_dir) if f.endswith(".csv")]
    except Exception as e:
        logger.error(f"Error accessing files: {e}")
        script_wait()
        continue

    if not files:
        logger.info("No CSV files found.")
        script_wait()
        continue



    script_wait()  # small delay before next check
    break