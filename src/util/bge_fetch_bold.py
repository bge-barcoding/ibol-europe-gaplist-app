import os
import requests
import wget
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime

# TODO: probably kill me because retrieve_latest_bold_datapackage.py already exists

# Base URL for BOLD API datapackage retrieval
url_base = 'https://v4.boldsystems.org/index.php/API_Datapackage?id='
# URL to fetch the latest version of BOLD data packages
url_latest = 'https://v4.boldsystems.org/index.php/datapackages/Latest'
# Output directory for the BOLD data packages
output_dir = './data/input_files'

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to retrieve the latest BOLD public datapackage ID
def get_latest_datapackage():
    try:
        # Send request to get the latest datapackage page
        response = requests.get(url_latest)
        response.raise_for_status()  # Raise an error for bad HTTP responses
    except requests.exceptions.RequestException as e:
        print(f"Error while fetching the latest datapackage: {e}")
        return None

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    all_text = soup.get_text()

    # Use regex to find datapackage IDs in the text
    pattern = r'BOLD_Public\.\S+'
    matches = re.findall(pattern, all_text)

    if not matches:
        print("No matching datapackage found.")
        return None
    else:
        # Return the first match found (assuming we're only interested in the first one)
        return matches[0]

# Function to check if the datapackage already exists locally
def file_exists(datapackage_id):
    filename = os.path.join(output_dir, f"{datapackage_id}.zip")
    return os.path.exists(filename)

# Function to download the BOLD data package
def download_datapackage(datapackage_id):
    try:
        # Form the complete URL for the API request
        url = url_base + datapackage_id
        uid = requests.get(url).text.strip().replace('"', '')
        download_url = f"{url}&uid={uid}"

        # Set the filename for the downloaded file
        filename = os.path.join(output_dir, f"{datapackage_id}.zip")

        # Check if the file already exists
        if file_exists(datapackage_id):
            print(f"File {filename} already exists. Skipping download.")
        else:
            print(f"Downloading datapackage from: {download_url}")
            wget.download(download_url, filename)
            print(f"\nDownload completed and saved to {filename}.")

    except requests.exceptions.RequestException as e:
        print(f"Error while downloading the datapackage: {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")

# Function to execute the download process
def run_download():
    datapackage_id = get_latest_datapackage()

    if datapackage_id:
        print(f"Found datapackage ID: {datapackage_id}")
        download_datapackage(datapackage_id)
    else:
        print("No datapackage to download.")

# Main execution block
if __name__ == "__main__":
    print(f"Script started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
    run_download()
