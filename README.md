# Hospital Data Processor

This script downloads and processes hospital-related datasets from the CMS (Centers for Medicare & Medicaid Services) provider data API.

## Features

- Downloads only datasets with the theme "Hospitals"
- Skips datasets that have not changed since the last run
- Converts CSV column headers to snake_case
- Uses multithreading for faster downloads
- Falls back to local metadata file if online metadata is unavailable

## Requirements

- Python 3.9 or later
- requests library

Install dependencies using:

pip install -r requirements.txt

## Usage

python hospitals_data_processor.py

To force a full refresh of all hospital datasets regardless of the last run:

python hospitals_data_processor.py --force-refresh


## Files

- `hospitals_data_processor.py`: Main script
- `CMS_BU_DATA.json`: Backup metadata file (used if online metadata fails)
- `run_log.txt`: Log file tracking script executions
- `hospital_download_data/`: Folder where downloaded CSV files are saved