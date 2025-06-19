#!/usr/bin/env python3
"""
OpenJordi Download Status Checker

This script checks the download status of all configured data sources
and provides a summary of which sources have been downloaded, when they
were last updated, and which sources need to be downloaded.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from tabulate import tabulate

# Add project root to path to ensure imports work correctly
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Import configuration
from config import DATA_SOURCES, RAW_DATA_DIR


def check_source_status(source_id):
    """
    Check the download status of a specific source.
    
    Args:
        source_id (str): The source ID to check
        
    Returns:
        dict: Status information for the source
    """
    source_config = DATA_SOURCES.get(source_id, {})
    source_dir = os.path.join(RAW_DATA_DIR, source_id)
    last_download_file = os.path.join(source_dir, "last_download.json")
    
    status = {
        "source_id": source_id,
        "funder": source_config.get("funder", ""),
        "format": source_config.get("format", ""),
        "downloaded": False,
        "last_download": None,
        "age_days": None,
        "download_path": None,
        "file_count": 0,
        "size_mb": 0
    }
    
    # Check if the source directory exists
    if not os.path.exists(source_dir):
        return status
    
    # Check if the last_download.json file exists
    if os.path.exists(last_download_file):
        try:
            with open(last_download_file, "r", encoding="utf-8") as f:
                last_download = json.load(f)
            
            # Get download timestamp
            download_time = datetime.fromisoformat(last_download.get("timestamp", ""))
            current_time = datetime.now()
            age_days = (current_time - download_time).total_seconds() / (60 * 60 * 24)
            
            status["downloaded"] = True
            status["last_download"] = download_time.strftime("%Y-%m-%d %H:%M:%S")
            status["age_days"] = round(age_days, 1)
            
            # Get the download directory
            download_dir = last_download.get("directory")
            if download_dir:
                full_download_path = os.path.join(source_dir, download_dir)
                status["download_path"] = full_download_path
                
                # Count files and calculate total size
                if os.path.exists(full_download_path):
                    file_count = 0
                    total_size = 0
                    for root, _, files in os.walk(full_download_path):
                        file_count += len(files)
                        total_size += sum(os.path.getsize(os.path.join(root, file)) for file in files)
                    
                    status["file_count"] = file_count
                    status["size_mb"] = round(total_size / (1024 * 1024), 2)  # Convert to MB
                
        except Exception as e:
            print(f"Error reading last download info for {source_id}: {str(e)}")
    
    return status


def check_all_sources():
    """
    Check the download status of all configured sources.
    
    Returns:
        list: Status information for all sources
    """
    all_status = []
    
    for source_id in DATA_SOURCES:
        status = check_source_status(source_id)
        all_status.append(status)
    
    return all_status


def display_status_table(statuses):
    """
    Display a formatted table of source statuses.
    
    Args:
        statuses (list): List of status dictionaries
    """
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(statuses)
    
    # Sort by downloaded status (not downloaded first), then by age (oldest first)
    df = df.sort_values(by=["downloaded", "age_days"], ascending=[True, False])
    
    # Select and rename columns for display
    display_df = df[["source_id", "funder", "format", "downloaded", "last_download", "age_days", "file_count", "size_mb"]]
    display_df.columns = ["Source ID", "Funder", "Format", "Downloaded", "Last Download", "Age (days)", "Files", "Size (MB)"]
    
    # Format the table
    table = tabulate(display_df, headers="keys", tablefmt="pipe", showindex=False)
    print("\n=== OpenJordi Data Source Status ===\n")
    print(table)
    
    # Summary statistics
    downloaded_count = df["downloaded"].sum()
    total_size_mb = df["size_mb"].sum()
    print(f"\nSummary: {downloaded_count}/{len(df)} sources downloaded, total size: {total_size_mb:.2f} MB")


def main():
    """Main function to run the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check OpenJordi download status")
    parser.add_argument("--json", action="store_true", help="Output in JSON format")
    parser.add_argument("--sources", nargs="+", help="Check specific source IDs")
    
    args = parser.parse_args()
    
    # Get status for all or specified sources
    if args.sources:
        # Check only specified sources
        statuses = []
        for source_id in args.sources:
            if source_id in DATA_SOURCES:
                status = check_source_status(source_id)
                statuses.append(status)
            else:
                print(f"Warning: Source '{source_id}' not found in configuration")
    else:
        # Check all sources
        statuses = check_all_sources()
    
    # Output in requested format
    if args.json:
        print(json.dumps(statuses, indent=2))
    else:
        display_status_table(statuses)


if __name__ == "__main__":
    main()