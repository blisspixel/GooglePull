import io
import hashlib
import os
import time
import configparser
import pickle
import logging
import concurrent.futures
import traceback
from tqdm import tqdm
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
from functools import lru_cache

def setup_logging(config):
    """
    Configure the logging settings with timestamps.
    """
    log_format = '%(asctime)s - %(levelname)s - %(message)s'  # Added timestamps
    level = config.get('log_level', 'INFO')
    filename = config.get('log_file', 'debug.log')
    encoding = config.get('log_encoding', 'utf-8')
    logging.basicConfig(level=logging.getLevelName(level),
                        handlers=[logging.FileHandler(filename, 'w', encoding)],
                        format=log_format)  # Added format with timestamps
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

def read_config(file='config.ini'):
    config = configparser.ConfigParser()
    config.read(file)
    return config['DEFAULT']

def handle_http_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HttpError as error:
            logging.error(f"An error occurred: {error}")
            return []
    return wrapper

def handle_http_error_with_exponential_backoff(func):
    """
    Wrapper for functions that make Google API calls to handle HttpError and perform exponential backoff.
    """
    max_retry = read_config().get('max_retry', 5)

    def wrapper(*args, **kwargs):
        for retry in range(max_retry):
            try:
                return func(*args, **kwargs)
            except HttpError as error:
                if error.resp.status in [403, 429, 500, 503]:  # errors indicating rate limit
                    sleep_time = 2**retry  # exponential backoff
                    logging.warning(f"Rate limit exceeded, retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise  # if the HttpError is not due to rate limit, re-raise the exception
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
        logging.error("Exceeded maximum retry attempts.")
    return wrapper

def list_sources(service, parent_id=None, resource_key=None):
    logging.debug('Listing sources...')
    try:
        page_token = None
        items = []
        while True:
            results = service.files().list(
                q=(f"'{parent_id}' in parents and trashed=false" if parent_id else "trashed=false"),
                pageSize=100,
                fields="nextPageToken, files(id, name, md5Checksum, size, mimeType)",
                pageToken=page_token
            ).execute()

            new_items = results.get('files', [])
            for item in new_items:
                item['resourceKey'] = resource_key
            items.extend(new_items)
            logging.debug(f'Current items: {new_items}')

            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break
    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        return []

    logging.debug('Finished listing sources.')
    return items

def get_total_files(service, parent_id):
    items = list_sources(service, parent_id)
    total_files = len(items)
    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            total_files += get_total_files(service, item['id'])
    return total_files

def get_total_size(service, parent_id):
    items = list_sources(service, parent_id)
    total_size = sum(int(item.get('size', 0)) for item in items if item['mimeType'] != 'application/vnd.google-apps.folder')
    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            total_size += get_total_size(service, item['id'])
    return total_size

def get_destination(attempts=3):
    """
    Get the destination folder, limited to a certain number of attempts.
    """
    for _ in range(attempts):
        dest_folder = Path(input("Enter the destination folder (e.g., 'G:\\Example'): "))
        if dest_folder.is_dir():
            return dest_folder
        else:
            logging.error("Invalid path, please try again.")
    raise ValueError("Maximum number of attempts reached. Please check your destination path.")

def download_file(service, item, dest_folder: Path, pbar, max_retry=5):
    print('Processing file:', item['name'])  # debug print
    logging.debug(f"Processing file: {item['name']} ({item['id']})")

    try:
        dest_file_path = dest_folder / item['name']
        dest_folder.mkdir(parents=True, exist_ok=True)

        if dest_file_path.exists():
            with open(dest_file_path, 'rb') as f_in:
                m = hashlib.md5()
                m.update(f_in.read())
                computed_md5 = m.hexdigest()

            if computed_md5 == item.get('md5Checksum', None):
                logging.debug(f"File {item['name']} already exists and matches source. Skipping download.")
                return

        logging.debug("Downloading file...")
        request = None
        mimeType = item['mimeType']
        if mimeType == 'application/vnd.google-apps.document':
            request = service.files().export_media(fileId=item['id'], mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            dest_file_path = dest_file_path.with_suffix('.docx')
        elif mimeType == 'application/vnd.google-apps.spreadsheet':
            request = service.files().export_media(fileId=item['id'], mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            dest_file_path = dest_file_path.with_suffix('.xlsx')
        elif mimeType == 'application/vnd.google-apps.presentation':
            request = service.files().export_media(fileId=item['id'], mimeType='application/vnd.openxmlformats-officedocument.presentationml.presentation')
            dest_file_path = dest_file_path.with_suffix('.pptx')
        else:
            request = service.files().get_media(fileId=item['id'])

        # Check if the item has a resource key
        if 'resourceKey' in item:
            # Set the resource key for accessing link-shared files
            request.headers['X-Goog-Drive-Resource-Keys'] = f"{item['id']}/{item['resourceKey']}"

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        for _ in range(max_retry):
            try:
                while done is False:
                    status, done = downloader.next_chunk()
                    pbar.update(int(status.resumable_progress))
                break
            except HttpError as error:
                if error.resp.status in [403, 429, 500, 503]:
                    sleep_time = 2**_  # exponential backoff
                    logging.warning(f"Rate limit exceeded, retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise
        else:
            logging.error("Exceeded maximum retry attempts.")

        logging.debug("Download complete. Writing to file...")
        with open(dest_file_path, 'wb') as f_out:
            f_out.write(fh.getvalue())
        logging.debug("File written.")

        # Delete the file from Google Drive
        try:
            service.files().delete(fileId=item['id']).execute()
            logging.debug(f"File {item['name']} deleted from Google Drive.")
        except HttpError as error:
            logging.error(f"An error occurred: {error}")

    except HttpError as error:
        logging.error(f"An error occurred: {error}")

def download_files(service, source, dest_folder, is_root=False):
    """
    Download all files from a source to the destination folder.
    """
    if not is_root:  # only create subdirectories if not root
        dest_folder = dest_folder / source['name']
    dest_folder.mkdir(parents=True, exist_ok=True)

    items = list_sources(service, source['id'])
    logging.debug(f"Items to download: {items}")  # <-- Add this debug line

    with tqdm(total=get_total_size(service, source['id']), desc="Downloading files", unit="B", unit_scale=True) as pbar:
        # ...
        # Single-threaded download
        for item in items:
            if item['mimeType'] != 'application/vnd.google-apps.folder':
                download_file(service, item, dest_folder, pbar)

        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                download_files(service, {'id': item['id'], 'name': item['name']}, dest_folder)

    # Delete empty folders after all files have been downloaded
    delete_empty_folders(service, source['id'])

def delete_empty_folders(service, folder_id, max_retry=5):
    """
    Deletes the empty folders in Google Drive.
    """
    items = list_sources(service, folder_id)
    if not items:  # the folder is empty
        for _ in range(max_retry):
            try:
                service.files().delete(fileId=folder_id).execute()
                logging.debug(f"Folder {folder_id} deleted from Google Drive.")
                break
            except HttpError as error:
                if error.resp.status in [403, 429, 500, 503]:
                    sleep_time = 2**_  # exponential backoff
                    logging.warning(f"Rate limit exceeded, retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise
        else:
            logging.error("Exceeded maximum retry attempts.")
    else:
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                delete_empty_folders(service, item['id'])

def generate_token(config):
    SCOPES = ['https://www.googleapis.com/auth/drive']

    flow = InstalledAppFlow.from_client_secrets_file(config.get('credentials_file', 'credentials.json'), SCOPES)
    creds = flow.run_local_server(port=0)

    with open(config.get('token_file', 'token.pickle'), 'wb') as token_file:
        pickle.dump(creds, token_file)
def main():
    try:
        config = read_config()
        setup_logging(config)

        token_file = Path(config.get('token_file', 'token.pickle'))
        if not token_file.exists():
            generate_token(config)

        with token_file.open('rb') as token_file:
            creds = pickle.load(token_file)

        service = build('drive', 'v3', credentials=creds)

        folder_id = input("Please enter the folder ID: ")
        resource_key = input("Please enter the resource key: ")
        logging.debug(f'Getting files from the folder with ID {folder_id}...')
        all_sources = list_sources(service, folder_id, resource_key)
        logging.debug('Got all files.')
    
        if not all_sources:
            logging.info("No sources available.")
            return

        source = {'id': folder_id, 'name': 'Folder to Download'}
        dest_folder = get_destination()

        warning_message = "\nWARNING: This operation will DELETE files from Google Drive once they are downloaded. "
        warning_message += "This is a DESTRUCTIVE action. To proceed, type 'Yes' or 'Y': "
        confirmation = input(warning_message)

        # Converts the user's input to lowercase and trims any spaces
        confirmation = confirmation.lower().strip()

        if confirmation not in ["yes", "y"]:
            logging.info("Operation cancelled by the user.")
            return

        logging.info(f"Downloading files from {source['name']} to {dest_folder}...")
        download_files(service, source, dest_folder, is_root=True)
        logging.info("Operation completed.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error(traceback.format_exc())

if __name__ == '__main__':
    main()