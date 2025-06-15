import requests
import datetime
import os
import time

# --- Configuration ---
BASE_URL = "https://data.ecmwf.int/forecasts"
MODEL = "aifs-single" # Or "ifs" if you find the structure
RESOLUTION = "0p25"
STREAM = "oper"
TYPE = "fc" # forecast
FORMAT = "grib2"

# Forecast hours to download (e.g., 0h, 6h, 12h)
# The files are named like "20250506000000-0h-oper-fc.grib2" where "0h" is the step
FORECAST_STEPS = ["0h", "6h", "12h"] # A few sample steps

# Reference times (UTC) for the forecast runs
# Typically 00z, 06z, 12z, 18z
# Let's aim for 00z and 06z runs for "today"
REFERENCE_TIMES_UTC = ["00", "06"]

# --- End Configuration ---

DOWNLOAD_DIR = "downloaded_grib_files"

def download_file(url, local_filename):
    """Downloads a file from a URL to a local path."""
    print(f"Attempting to download: {url}")
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)

        with requests.get(url, stream=True, timeout=60) as r: # Increased timeout
            r.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
            file_size = int(r.headers.get('content-length', 0))
            print(f"Downloading to: {local_filename} (Size: {file_size / (1024*1024):.2f} MB)")
            downloaded_size = 0
            start_time = time.time()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    # Basic progress (optional)
                    # percent_done = (downloaded_size / file_size) * 100 if file_size > 0 else 0
                    # print(f"\rProgress: {percent_done:.1f}%", end="")
            # print() # Newline after progress
            end_time = time.time()
            print(f"Downloaded {local_filename} successfully in {end_time - start_time:.2f} seconds.")
            return True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - URL: {url}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err} - URL: {url}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err} - URL: {url}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during request: {req_err} - URL: {url}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return False

def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Created download directory: {DOWNLOAD_DIR}")

    # Get today's date in yyyymmdd format (UTC)
    # Or you can specify a date: target_date = datetime.date(2025, 5, 6)
    target_date = datetime.datetime.utcnow().date()
    date_str = target_date.strftime("%Y%m%d")
    date_str = 20250507 # For testing, set a specific date
    print(f"Targeting forecast date: {date_str}")

    download_count = 0

    for hh in REFERENCE_TIMES_UTC: # For 00z, 06z runs etc.
        base_run_url_part = f"{date_str}/{hh}z/{MODEL}/{RESOLUTION}/{STREAM}"

        for step in FORECAST_STEPS: # For each forecast step like 0h, 6h, etc.
            # File name format: [yyyymmdd][HH]0000-[step]-[stream]-[type].[format]
            # Example: 20250506000000-0h-oper-fc.grib2
            filename = f"{date_str}{hh}0000-{step}-{STREAM}-{TYPE}.{FORMAT}"
            file_url = f"{BASE_URL}/{base_run_url_part}/{filename}"

            local_filepath = os.path.join(DOWNLOAD_DIR, filename)

            if os.path.exists(local_filepath) and os.path.getsize(local_filepath) > 1000: # Basic check if already downloaded
                print(f"File already exists and seems valid: {local_filepath}")
                download_count +=1
                continue

            if download_file(file_url, local_filepath):
                download_count += 1
            else:
                print(f"Failed to download {filename}. Skipping.")
            time.sleep(1) # Be polite to the server

    if download_count > 0:
        print(f"\nFinished. Downloaded {download_count} GRIB files to '{DOWNLOAD_DIR}'.")
    else:
        print("\nFinished. No new files were downloaded (either already exist or failed).")
    print("Make sure you have 'eccodes' or 'cfgrib' installed if you plan to read these GRIB files.")

if __name__ == "__main__":
    main()