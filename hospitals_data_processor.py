"""
File Name: hospitals_data_processor.py
Author: Jose Rodriguez
Description:
    This script downloads and processes CMS hospital-related datasets.
    It filters datasets by the "Hospitals" theme, checks if they're new or updated
    since the last run, and saves them locally with column headers standardized to snake_case.
    Unless --force-refresh is used, then it will download all datasets regardless of last run time.

Usage:
    python hospitals_data_processor.py [--force-refresh]
"""
import os
import csv
import json
import requests
import argparse
import sys
import inflection 
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor

# Path to backup CMS metadata JSON file
CMS_DATA_FILE = "CMS_BU_DATA.json"
# Directory to save downloaded CSV files
HOSPITAL_DOWNLOAD_DIR = "hospital_download_data"
# Path to track runs
RUN_LOG = "run_log.txt"

def load_hospital_data():
    """
    Fetch metadata from CMS API. Fall back to local CMS_BU_DATA.json.
    """
    url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
    try:
        print("Fetching metadata from CMS API")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "dataset" in data:
            return data["dataset"]
        else:
            raise ValueError("Unexpected metadata format.")
    except (requests.RequestException, ValueError, json.JSONDecodeError) as e:
        print(f"Failed to fetch metadata from CMS: {e}")
        print("Falling back to local metadata file.")
        try:
            with open(CMS_DATA_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load fallback metadata: {e}")
            return []

def download_and_process(dataset):
    """
    Download CSV file and processes it:
    - Save to disk
    - !Important: Convert the column names to snake_case using inflection
    """
    url = dataset["distribution"][0]["downloadURL"]
    modified_str = dataset["modified"]
    modified_date = datetime.fromisoformat(modified_str)

    filename = url.split("/")[-1]
    save_path = os.path.join(HOSPITAL_DOWNLOAD_DIR, filename)

    print(f"Downloading: {filename}")

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to download {filename}: {e}")
        return

    decoded_lines = response.content.decode("utf-8").splitlines()
    reader = csv.reader(decoded_lines)
    rows = list(reader)

    if not rows:
        print(f"Empty CSV: {filename}")
        return

    headers = [inflection.underscore(col.strip()) for col in rows[0]]
    rows[0] = headers

    os.makedirs(HOSPITAL_DOWNLOAD_DIR, exist_ok=True)

    with open(save_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"Saved to: {save_path}")

def get_last_run_time():
    """
    Return the most recent timestamp from the RUN_LOG txt file.
    """
    if not os.path.exists(RUN_LOG):
        return datetime.min.replace(tzinfo=UTC)

    with open(RUN_LOG, "r") as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        print("Warning: run_log.txt is empty. Using fallback date.")
        return datetime.min.replace(tzinfo=UTC)

    last_line = lines[-1]
    try:
        timestamp_str = last_line.split(" | ")[0]
        return datetime.fromisoformat(timestamp_str)
    except (IndexError, ValueError):
        print("Warning: Could not parse timestamp from run log. Using fallback date.")
        return datetime.min.replace(tzinfo=UTC)

def update_run_log(message):
    """
    Append timestamped run entry to the RUN_LOG with a message and args.
    Example:
    2025-06-23T03:19:12.400936+00:00 | Downloaded 3 files | python hospitals_data_processor.py --force-refresh
    """
    timestamp = datetime.now(UTC).isoformat()
    command = "python " + " ".join(sys.argv)
    log_entry = f"{timestamp} | {message} | {command}\n"

    with open(RUN_LOG, "a") as f:
        f.write(log_entry)

def main():
    """
    Main function:
    - Parse arguments
    - Load metadata
    - Filter datasets by theme "Hospitals"
    - !Important Download and process datasets modified since last run
    """
    parser = argparse.ArgumentParser(description="Download CMS Hospital Datasets")
    parser.add_argument("--force-refresh", action="store_true", help="Download all Hospitals themed datasets regardless of last run time")
    args = parser.parse_args()

    print("Begin Processing")

    last_run = datetime.min.replace(tzinfo=UTC) if args.force_refresh else get_last_run_time()

    data = load_hospital_data()

    hospital_datasets = [
        dataset for dataset in data
        if "Hospitals" in dataset.get("theme", [])
        and datetime.fromisoformat(dataset["modified"]).replace(tzinfo=UTC) > last_run
    ]

    if not hospital_datasets:
        print("No new or updated 'Hospitals' datasets found.")
        print("To force a data refresh, add the --force-refresh argument.")
        update_run_log("Nothing to update")
    else:
        print(f"Found {len(hospital_datasets)} 'Hospitals' datasets to process.")
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(download_and_process, hospital_datasets)
        update_run_log(f"Downloaded {len(hospital_datasets)} file(s)")

    print("Complete Processing")

if __name__ == "__main__":
    main()