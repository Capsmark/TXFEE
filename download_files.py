import time
import os
import requests
import gzip
import shutil
from datetime import datetime, timedelta

base_url = "https://public.bybit.com/trading/BTCUSD/"
#  = datetime(2019, 10, 1)
start_date = datetime(2019, 10, 1)
end_date = datetime(2023, 8, 29)
save_dir = "/mnt/disks/csv"  # Replace with your directory


def download_file(url, save_path):
    """Download file from url and save it to save_path."""
    try:
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise Exception(f"Failed to download the file: {url}")
        with open(save_path, "wb") as f:
            f.write(response.content)
    except Exception as e:
        print(f"Error: {e}")
        print(f"File {url} encounter an error. Continuing to the next file...")
        return False
    return True


def extract_file(save_path):
    """Extract .gz file to csv."""
    extracted_path = save_path[:-3]  # Remove the '.gz'
    with gzip.open(save_path, 'rb') as f_in:
        with open(extracted_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return extracted_path


def remove_file(file_path):
    """Remove file."""
    if os.path.exists(file_path):
        os.remove(file_path)


def download_and_extract_file(url, save_path):
    """Download and extract file from url and save it to save_path."""
    # If the extracted file already exists, skip download and extraction
    if os.path.exists(save_path[:-3]):
        print(f"File {save_path[:-3]} already exists, skipping...")
        return

    print(f'\nDownloading File {url}')

    success = download_file(url, save_path)
    if not success:
        return

    # Added a delay to be soft on the API
    time.sleep(5)

    print('Extracting ...')
    extract_file(save_path)

    print('Deleting the archive ...')
    remove_file(save_path)

    print('-------------------------')


# Loop over the dates from start_date to end_date
date = start_date
while date <= end_date:
    # Construct the filename
    filename = f"BTCUSD{date.strftime('%Y-%m-%d')}.csv.gz"

    # Construct the URL
    url = base_url + filename

    # Path to save the file
    save_path = os.path.join(save_dir, filename)

    # Download and extract the file
    download_and_extract_file(url, save_path)

    # Move to the next day
    date += timedelta(days=1)

print("All files have been downloaded and extracted.")
