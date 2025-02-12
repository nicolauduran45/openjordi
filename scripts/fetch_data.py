import os
import requests
import pandas as pd
import sys

# Add the 'config' directory to the system path (from the perspective of the 'scripts' folder)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config')))
from config import DATA_SOURCES

# Define the local storage folder for raw data
DATA_FOLDER = "data"
RAW_FOLDER = os.path.join(DATA_FOLDER, "raw")

# Ensure the raw folder exists
os.makedirs(RAW_FOLDER, exist_ok=True)

def download_file(url, filepath):
    """Download a file from a given URL and save it locally."""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()  # Raise an error for failed requests
        
        with open(filepath, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"✅ Downloaded: {filepath}")
        return filepath

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to download {url}: {e}")
        return None

def fetch_data():
    """Fetch data from all sources and save them in the raw data folder."""
    for source, details in DATA_SOURCES.items():
        print(f"Fetching data from {source}...")

        url = details["url"]
        file_format = details["format"]
        file_extension = "csv" if file_format == "csv" else "xlsx"
        filename = f"{source.replace(' ', '_').lower()}.{file_extension}"
        SOURCE_FOLDER = os.path.join(RAW_FOLDER, source)
        # Ensure the raw folder exists
        os.makedirs(SOURCE_FOLDER, exist_ok=True)
        filepath = os.path.join(SOURCE_FOLDER, filename)  # Save in raw folder

        # Download and save the file
        downloaded_file = download_file(url, filepath)

        if downloaded_file and file_format in ["csv", "excel"]:
            # Verify and load the file into a DataFrame
            try:
                if file_format == "csv":
                    df = pd.read_csv(downloaded_file)
                elif file_format == "excel":
                    df = pd.read_excel(downloaded_file)

                print(f"🗂️ {source}: {df.shape[0]} rows saved")

            except Exception as e:
                print(f"❌ Error reading {downloaded_file}: {e}")

if __name__ == "__main__":
    fetch_data()
