import os
import argparse
import requests
import wget
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime

# Base URL for BOLD API datapackage retrieval
url_base = 'https://v4.boldsystems.org/index.php/API_Datapackage?id='
# URL to fetch the latest version of BOLD data packages
url_latest = 'https://v4.boldsystems.org/index.php/datapackages/Latest'


def get_latest_datapackage(retries=3):
    """
    Retrieve the latest BOLD public datapackage ID.

    :param retries: Number of retry attempts in case of failure (default is 3).
    :return: Latest datapackage ID or None if not found.
    """
    for attempt in range(retries):
        try:
            print("Fetching the latest datapackage...")
            response = requests.get(url_latest)
            response.raise_for_status()
            print("Successfully fetched the latest datapackage page.")
            break
        except requests.exceptions.RequestException as e:
            print(f"Error while fetching the latest datapackage: {e}")
            print(f"Retrying... ({attempt + 1}/{retries})")
            time.sleep(5)

    else:
        print("Failed to fetch the latest datapackage after multiple attempts.")
        return None

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    all_text = soup.get_text()

    # Use regex to find datapackage IDs in the text
    matches = re.findall(r'BOLD_Public\.\S+', all_text)

    if not matches:
        print("No matching datapackage found.")
        return None
    else:
        latest_id = matches[0]  # Assume the first match is the latest
        print(f"Found datapackage ID: {latest_id}")
        return latest_id


def download_datapackage(output_dir, datapackage_id):
    """
    Download the BOLD data package if it does not exist locally.

    :param output_dir: Download folder.
    :param datapackage_id: The ID of the datapackage to download.
    """
    try:
        url = url_base + datapackage_id
        print(f"Constructed URL for downloading: {url}")

        # Fetch the UID for download
        uid_response = requests.get(url)
        uid_response.raise_for_status()
        uid = uid_response.text.strip().replace('"', '')
        download_url = f"{url}&uid={uid}"
        print(f"Download URL: {download_url}")

        # Check if the latest datapackage file already exists
        gz_file = os.path.join(output_dir, f"{datapackage_id}.gz")

        exists = os.path.exists(gz_file)
        print(f"Checking if the latest datapackage exists: {datapackage_id} -> {'Found' if exists else 'Not found'}")

        if exists:
            print(f"The latest datapackage {datapackage_id} already exists. Skipping download.")
        else:
            print(f"Downloading datapackage from: {download_url}")
            wget.download(download_url, gz_file)
            print(f"\nDownload completed and saved to {gz_file}.")
            # unzip_and_extract(output_dir, filename)

        return gz_file

    except requests.exceptions.RequestException as e:
        print(f"Error while downloading the datapackage: {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")


# def unzip_and_extract(output_dir, gz_filename):
#     """
#     Unzip and extract the downloaded .gz file.
#
#     :param gz_filename: The name of the .gz file to unzip.
#     """
#     try:
#         print(f"Unzipping the file: {gz_filename}")
#         with gzip.open(gz_filename, 'rb') as f_in:
#             uncompressed_filename = gz_filename[:-3]  # Remove .gz extension
#             with open(uncompressed_filename, 'wb') as f_out:
#                 f_out.write(f_in.read())
#         print(f"Unzipping completed. File saved as: {uncompressed_filename}")
#
#         print(f"Extracting the contents of the tar file: {uncompressed_filename}")
#         with tarfile.open(uncompressed_filename, 'r:') as tar:
#             tar.extractall(path=output_dir)
#         print(f"Extraction completed. Files extracted to: {output_dir}")
#
#         # Clean up by removing the .gz and tar files after extraction
#         os.remove(gz_filename)
#         os.remove(uncompressed_filename)
#         print("Cleaned up: removed the downloaded .gz and uncompressed tar files.")
#
#     except Exception as e:
#         print(f"Error during unzipping and extraction: {e}")


def run_download(output_dir):
    """
    Execute the download process for the latest datapackage.
    """
    print("Starting download process...")
    datapackage_id = get_latest_datapackage()

    if datapackage_id:
        print(f"Found datapackage ID: {datapackage_id}")
        download_datapackage(output_dir, datapackage_id)
    else:
        print("No datapackage to download.")


# Main execution block
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('output_dir', metavar='output-dir', help="save location of the datapackage file")

    args = parser.parse_args()

    try:
        os.makedirs(args.output_dir, exist_ok=True)
    except:
        print(f"Could not create output directory '{args.output_dir}'")
        exit(1)

    print(f"Script started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
    run_download(args.output_dir)
