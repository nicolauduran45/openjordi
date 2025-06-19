#!/usr/bin/env python3
"""
OpenJordi Data Fetcher

This script handles downloading grant data from various sources and storing it in the raw data folder.
It supports multiple data formats including CSV, Excel, API, and HTML scraping.

Usage:
    python fetch_data.py                          # Fetch all sources (skips if downloaded in last 7 days)
    python fetch_data.py --force                  # Force fetch all sources even if recently downloaded
    python fetch_data.py --max-age 14             # Only fetch sources older than 14 days
    python fetch_data.py --sources SOURCE1 SOURCE2 # Fetch only specified sources
    python fetch_data.py --init-from-csv file.csv # Initialize sources from a CSV file

The script creates timestamped directories for each download and tracks the last successful
download of each source to avoid redundant downloads.
"""

import os
import sys
import logging
import requests
import pandas as pd
import json
import time
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
import shutil

# Add project root to path to ensure imports work correctly
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from dotenv import load_dotenv
from pathlib import Path

# Import configuration
from config import (
    DATA_SOURCES, PARSER_CONFIGS, 
    RAW_DATA_DIR, LOG_LEVEL, LOG_FILE,
    REQUEST_TIMEOUT, REQUEST_RETRIES, USER_AGENT
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("openjordi.fetch_data")


class DataFetcher:
    """Class to handle data fetching from various sources."""
    
    def __init__(self):
        """Initialize the data fetcher."""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT
        })
    
    def fetch_all_sources(self, sources_to_fetch=None, force_refresh=False, max_age_days=7):
        """
        Fetch data from specified sources.
        
        Args:
            sources_to_fetch (dict): Dictionary of sources to fetch (defaults to DATA_SOURCES)
            force_refresh (bool): If True, download all sources regardless of cache status
            max_age_days (int): Maximum age in days to consider a cached source as valid
            
        Returns:
            dict: Results of fetch operations by source ID
        """
        if sources_to_fetch is None:
            sources_to_fetch = DATA_SOURCES
            
        logger.info(f"Starting data fetch for {len(sources_to_fetch)} sources (force_refresh={force_refresh}, max_age_days={max_age_days})")
        
        results = {}
        skipped = []
        
        for source_id, source_config in sources_to_fetch.items():
            try:
                logger.info(f"Processing source: {source_id}")
                
                # Get source name for directory structure
                source_name = source_config.get("source_name", source_id)
                
                # Get the action directly from the source config (for individual sources like Marató)
                single_action = source_config.get("action", "")
                
                # Handle multiple actions if applicable (for grouped sources in future)
                actions = self._get_actions_for_source(source_config)
                
                # If we have a single action in the main config, use that
                if single_action and not actions:
                    actions = [{"action": single_action, "data_link": source_config.get("data_link")}]
                # If no specific actions, we'll process as a single source
                elif not actions:
                    actions = [{"action": "", "data_link": source_config.get("data_link")}]
                
                # Process each action for this source
                for action_config in actions:
                    action_name = action_config.get("action", "")
                    data_link = action_config.get("data_link")
                    
                    # Create a unique identifier for this source + action combination
                    action_id = f"{source_id}_{action_name}" if action_name else source_id
                    
                    # Get the path for checking if recently downloaded
                    source_path = self._get_source_path(source_name, action_name)
                    
                    # Check if we should skip this source+action (if not force_refresh)
                    if not force_refresh and self._is_recently_downloaded(source_path, max_age_days):
                        logger.info(f"Skipping {action_id} - already downloaded within the last {max_age_days} days")
                        results[action_id] = "skipped"
                        skipped.append(action_id)
                        continue
                    
                    # Update source_config with the specific action data link
                    current_config = source_config.copy()
                    if data_link:
                        current_config["data_link"] = data_link
                    
                    # Prepare directory for this source+action
                    source_dir = self._prepare_source_directory(source_name, action_name)
                    
                    # Determine the data format and call appropriate fetcher
                    data_format = source_config.get("format", "").lower()
                    if data_format in ["csv", "excel", "xlsx"]:
                        result = self._fetch_file(source_id, current_config, source_dir, action_name)
                    elif data_format == "api":
                        result = self._fetch_api(source_id, current_config, source_dir, action_name)
                    elif data_format == "html":
                        result = self._fetch_html(source_id, current_config, source_dir, action_name)
                    else:
                        logger.warning(f"Unsupported format '{data_format}' for source {source_id}")
                        result = False
                    
                    results[action_id] = result
                
            except Exception as e:
                logger.error(f"Error processing source {source_id}: {str(e)}", exc_info=True)
                results[source_id] = False
        
        # Summarize results
        success_count = sum(1 for result in results.values() if result is True)
        logger.info(f"Data fetch completed. Successfully processed {success_count}/{len(results)} sources.")
        
        return results
    
    def _get_actions_for_source(self, source_config):
        """
        Get the list of actions for a source if it has multiple actions.
        
        Args:
            source_config (dict): The source configuration
            
        Returns:
            list: List of action configurations, each with action name and data link
        """
        actions = []
        
        # Check if source has multiple actions defined
        if "actions" in source_config and isinstance(source_config["actions"], list):
            for action in source_config["actions"]:
                if isinstance(action, dict) and "name" in action and "data_link" in action:
                    actions.append({
                        "action": action["name"],
                        "data_link": action["data_link"]
                    })
        
        return actions
    
    def _get_source_path(self, source_name, action_name=None):
        """
        Get the path for a source and optional action.
        
        Args:
            source_name (str): The name of the source
            action_name (str, optional): The name of the action
            
        Returns:
            str: The path to the source directory
        """
        source_path = os.path.join(RAW_DATA_DIR, source_name)
        
        # Only create action subfolder if action name exists and is not empty
        if action_name and action_name.strip():
            source_path = os.path.join(source_path, action_name)
        
        return source_path
    
    def _prepare_source_directory(self, source_name, action_name=None):
        """
        Create and return path to source directory.
        
        Args:
            source_name (str): The name of the source
            action_name (str, optional): The name of the action
            
        Returns:
            str: The path to the timestamped directory for this fetch
        """
        # Build base path: data/raw/source_name/[action_name]
        source_path = self._get_source_path(source_name, action_name)
        os.makedirs(source_path, exist_ok=True)
        
        # Create a timestamped folder for this fetch run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fetch_dir = os.path.join(source_path, timestamp)
        os.makedirs(fetch_dir, exist_ok=True)
        
        # Create a symlink or copy to 'latest'
        latest_dir = os.path.join(source_path, "latest")
        if os.path.exists(latest_dir):
            if os.path.islink(latest_dir):
                os.unlink(latest_dir)
            else:
                shutil.rmtree(latest_dir)
        
        # Create symlink where supported, otherwise copy
        try:
            os.symlink(fetch_dir, latest_dir)
        except (OSError, AttributeError):
            # Windows may not support symlinks
            os.makedirs(latest_dir, exist_ok=True)
            # Copy content later when files are created
        
        return fetch_dir
    
    
        """
        Special handler for the OpenAIRE API.
        
        Args:
            source_id (str): The source identifier
            source_config (dict): The source configuration
            target_dir (str): The directory to save the API data
            action_name (str, optional): The name of the action
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use specific OpenAIRE API endpoint
            base_url = "http://api.openaire.eu/search/projects"
            logger.info(f"Fetching OpenAIRE projects data")
            
            # Initialize variables
            all_projects = []
            page = 1
            size = 100  # Max size allowed by API
            total_pages = None
            
            # Fetch projects with pagination
            while True:
                logger.info(f"Fetching OpenAIRE API page {page}" + 
                        (f" of {total_pages}" if total_pages else ""))
                
                params = {
                    "format": "json",
                    "page": page,
                    "size": size
                }
                
                try:
                    response = self.session.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Get project list
                    projects = data.get("response", {}).get("results", [])
                    
                    # Get pagination info if not already set
                    if total_pages is None and "response" in data:
                        total_results = int(data["response"].get("header", {}).get("total", 0))
                        total_pages = (total_results + size - 1) // size
                        logger.info(f"Total OpenAIRE projects: {total_results} (approx. {total_pages} pages)")
                    
                    if not projects:
                        logger.info("No more projects. Stopping.")
                        break
                    
                    # Add projects to our collection
                    all_projects.extend(projects)
                    logger.info(f"Downloaded {len(projects)} projects from page {page}")
                    
                    # Move to next page
                    page += 1
                    time.sleep(1)  # Be nice to the API
                    
                    # Safety check - if we've processed more pages than expected
                    if total_pages and page > total_pages + 5:
                        logger.warning(f"Exceeded expected number of pages ({total_pages}). Stopping.")
                        break
                    
                except Exception as e:
                    logger.error(f"Error on page {page}: {str(e)}")
                    break
            
            # Save all data
            base_name = f"{source_id}_{action_name}" if action_name else source_id
            output_file = os.path.join(target_dir, f"{base_name}.json")
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_projects, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Downloaded {len(all_projects)} OpenAIRE projects and saved to {output_file}")
            
            # Create metadata
            self._create_metadata(target_dir, source_id, source_config, {
                "api_url": base_url,
                "record_count": len(all_projects),
                "download_timestamp": datetime.now().isoformat(),
                "action": action_name,
                "pages_processed": page - 1
            })
            
            # Try to convert to CSV
            try:
                # Normalize the complex nested structure
                flat_projects = []
                for project in all_projects:
                    flat_project = {}
                    
                    # Extract basic project info
                    result = project.get("result", {})
                    metadata = result.get("metadata", {}).get("oaf:entity", {}).get("oaf:project", {})
                    
                    # Extract common fields
                    flat_project["code"] = metadata.get("code", {}).get("$", "")
                    flat_project["acronym"] = metadata.get("acronym", {}).get("$", "")
                    flat_project["title"] = metadata.get("title", {}).get("$", "")
                    
                    # Handle dates
                    if "startdate" in metadata:
                        flat_project["start_date"] = metadata["startdate"].get("$", "")
                    if "enddate" in metadata:
                        flat_project["end_date"] = metadata["enddate"].get("$", "")
                    
                    # Handle funding
                    if "fundingtree" in metadata and "funder" in metadata["fundingtree"]:
                        funders = metadata["fundingtree"]["funder"]
                        if isinstance(funders, list):
                            flat_project["funder"] = "; ".join([f.get("shortname", {}).get("$", "") for f in funders])
                        else:
                            flat_project["funder"] = funders.get("shortname", {}).get("$", "")
                    
                    # Add to flat projects
                    flat_projects.append(flat_project)
                
                # Convert to DataFrame and save as CSV
                df = pd.DataFrame(flat_projects)
                csv_file = os.path.join(target_dir, f"{base_name}.csv")
                df.to_csv(csv_file, index=False)
                logger.info(f"Converted OpenAIRE data to CSV: {csv_file}")
                
            except Exception as e:
                logger.warning(f"Could not convert OpenAIRE data to CSV: {str(e)}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error processing OpenAIRE API: {str(e)}", exc_info=True)
            return False
    
    

    def _fetch_api(self, source_id, source_config, target_dir, action_name=None):
        """
        Fetch data from an API endpoint.
        
        Args:
            source_id (str): The source identifier
            source_config (dict): The source configuration
            target_dir (str): The directory to save the API data
            action_name (str, optional): The name of the action
            
        Returns:
            bool: True if successful, False otherwise
        """
        url = source_config.get("data_link")
        if not url:
            logger.error(f"No API endpoint provided for source {source_id}" + 
                        (f" action {action_name}" if action_name else ""))
            return False
        
        # Special handling for OpenAIRE API
        if "openaire" in source_id.lower() or "openaire" in url.lower():
            return self._fetch_openaire_api(source_id, source_config, target_dir, action_name)
        
        # Regular API handling for other sources
        parser_type = source_config.get("parser", "api")
        parser_config = PARSER_CONFIGS.get(parser_type, {})
        
        try:
            # Handle authentication if needed
            if parser_config.get("auth_required"):
                # Add authentication logic here
                logger.info(f"API authentication required for {source_id}")
            
            # Handle pagination
            all_data = []
            page = 1
            has_more = True
            
            while has_more:
                paginated_url = f"{url}?page={page}" if parser_config.get("pagination") else url
                logger.info(f"Fetching API data from {paginated_url}")
                
                response = self.session.get(paginated_url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                
                data = response.json()
                
                # Extract data based on parser configuration
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    # Handle common API response patterns
                    if "data" in data and isinstance(data["data"], list):
                        all_data.extend(data["data"])
                    elif "results" in data and isinstance(data["results"], list):
                        all_data.extend(data["results"])
                    else:
                        all_data.append(data)
                
                # Check if we need to paginate
                if parser_config.get("pagination"):
                    # Determine if there's more data based on API response
                    # This logic might need to be customized per API
                    has_more = False
                    if isinstance(data, dict):
                        if "next" in data and data["next"]:
                            has_more = True
                        elif "has_more" in data and data["has_more"]:
                            has_more = True
                    
                    page += 1
                    # Respect rate limits
                    if has_more and "rate_limit" in parser_config:
                        time.sleep(60 / parser_config["rate_limit"])
                else:
                    has_more = False
            
            # Save the API response as JSON
            # Use action name in filename if provided
            base_name = f"{source_id}_{action_name}" if action_name else source_id
            output_file = os.path.join(target_dir, f"{base_name}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_data, f, indent=2)
            
            logger.info(f"Successfully saved API data to {output_file}")
            
            # Create metadata
            self._create_metadata(target_dir, source_id, source_config, {
                "api_url": url,
                "record_count": len(all_data),
                "download_timestamp": datetime.now().isoformat(),
                "action": action_name
            })
            
            # Optionally convert to CSV
            try:
                df = pd.json_normalize(all_data)
                csv_file = os.path.join(target_dir, f"{base_name}.csv")
                df.to_csv(csv_file, index=False)
                logger.info(f"Converted API data to CSV: {csv_file}")
            except Exception as e:
                logger.warning(f"Could not convert API data to CSV: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error fetching API data for {source_id}" + 
                        (f" action {action_name}" if action_name else "") + 
                        f": {str(e)}", exc_info=True)
            return False

    def _fetch_openaire_api(self, source_id, source_config, target_dir, action_name=None):
        """
        Special handler for the OpenAIRE API.
        
        Args:
            source_id (str): The source identifier
            source_config (dict): The source configuration
            target_dir (str): The directory to save the API data
            action_name (str, optional): The name of the action
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use specific OpenAIRE API endpoint
            base_url = "http://api.openaire.eu/search/projects"
            logger.info(f"Fetching OpenAIRE projects data")
            
            # Initialize variables
            all_projects = []
            page = 1
            size = 100  # Max size allowed by API
            total_pages = None
            
            # Fetch projects with pagination
            while True:
                logger.info(f"Fetching OpenAIRE API page {page}" + 
                        (f" of {total_pages}" if total_pages else ""))
                
                params = {
                    "format": "json",
                    "page": page,
                    "size": size
                }
                
                try:
                    response = self.session.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Debug response structure
                    logger.debug(f"OpenAIRE API response structure: {json.dumps(list(data.keys()), indent=2)}")
                    
                    # Get project list
                    projects = data.get("response", {}).get("results", {}).get("result", [])
                    
                    # Ensure projects is a list
                    if not isinstance(projects, list):
                        if projects:  # If it's a single project
                            projects = [projects]
                        else:
                            projects = []
                    
                    # Get pagination info if not already set
                    if total_pages is None and "response" in data:
                        # Safely extract total results
                        header = data["response"].get("header", {})
                        total_str = header.get("total", {})
                        
                        # Handle various formats of 'total'
                        if isinstance(total_str, dict) and "$" in total_str:
                            total_str = total_str.get("$", "0")
                        elif isinstance(total_str, dict):
                            total_str = next(iter(total_str.values()), "0")
                        
                        try:
                            total_results = int(total_str)
                            total_pages = (total_results + size - 1) // size
                            logger.info(f"Total OpenAIRE projects: {total_results} (approx. {total_pages} pages)")
                        except (ValueError, TypeError):
                            logger.warning(f"Could not determine total projects, will continue until no more results")
                    
                    if not projects:
                        logger.info("No more projects. Stopping.")
                        break
                    
                    # Add projects to our collection
                    all_projects.extend(projects)
                    logger.info(f"Downloaded {len(projects)} projects from page {page}")
                    
                    # Move to next page
                    page += 1
                    time.sleep(1)  # Be nice to the API
                    
                    # Safety check - if we've processed more pages than expected
                    if total_pages and page > total_pages + 5:
                        logger.warning(f"Exceeded expected number of pages ({total_pages}). Stopping.")
                        break
                    
                    # Safety limit - don't go beyond 1000 pages
                    if page > 1000:
                        logger.warning("Reached maximum page limit (1000). Stopping.")
                        break
                    
                except Exception as e:
                    logger.error(f"Error on page {page}: {str(e)}")
                    # Save what we have so far
                    if all_projects:
                        break
                    else:
                        return False
            
            # Save all data
            base_name = f"{source_id}_{action_name}" if action_name else source_id
            output_file = os.path.join(target_dir, f"{base_name}.json")
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_projects, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Downloaded {len(all_projects)} OpenAIRE projects and saved to {output_file}")
            
            # Create metadata
            self._create_metadata(target_dir, source_id, source_config, {
                "api_url": base_url,
                "record_count": len(all_projects),
                "download_timestamp": datetime.now().isoformat(),
                "action": action_name,
                "pages_processed": page - 1
            })
            
            # Try to convert to CSV
            try:
                # Normalize the complex nested structure
                flat_projects = []
                for project in all_projects:
                    flat_project = {}
                    
                    try:
                        # Extract basic project info
                        metadata = project.get("metadata", {})
                        if "oaf:entity" in metadata:
                            metadata = metadata["oaf:entity"].get("oaf:project", {})
                        
                        # Extract common fields - safely handle different structures
                        if isinstance(metadata.get("code", {}), dict):
                            flat_project["code"] = metadata.get("code", {}).get("$", "")
                        else:
                            flat_project["code"] = str(metadata.get("code", ""))
                            
                        if isinstance(metadata.get("acronym", {}), dict):
                            flat_project["acronym"] = metadata.get("acronym", {}).get("$", "")
                        else:
                            flat_project["acronym"] = str(metadata.get("acronym", ""))
                            
                        if isinstance(metadata.get("title", {}), dict):
                            flat_project["title"] = metadata.get("title", {}).get("$", "")
                        else:
                            flat_project["title"] = str(metadata.get("title", ""))
                        
                        # Handle dates
                        if "startdate" in metadata:
                            if isinstance(metadata["startdate"], dict):
                                flat_project["start_date"] = metadata["startdate"].get("$", "")
                            else:
                                flat_project["start_date"] = str(metadata["startdate"])
                                
                        if "enddate" in metadata:
                            if isinstance(metadata["enddate"], dict):
                                flat_project["end_date"] = metadata["enddate"].get("$", "")
                            else:
                                flat_project["end_date"] = str(metadata["enddate"])
                        
                        # Handle funding
                        if "fundingtree" in metadata and "funder" in metadata["fundingtree"]:
                            funders = metadata["fundingtree"]["funder"]
                            if isinstance(funders, list):
                                funder_names = []
                                for f in funders:
                                    if isinstance(f.get("shortname", {}), dict):
                                        name = f.get("shortname", {}).get("$", "")
                                    else:
                                        name = str(f.get("shortname", ""))
                                    if name:
                                        funder_names.append(name)
                                flat_project["funder"] = "; ".join(funder_names)
                            else:
                                if isinstance(funders.get("shortname", {}), dict):
                                    flat_project["funder"] = funders.get("shortname", {}).get("$", "")
                                else:
                                    flat_project["funder"] = str(funders.get("shortname", ""))
                    except Exception as e:
                        logger.warning(f"Error flattening project: {str(e)}")
                        # Still add what we have
                        if not flat_project:
                            flat_project["raw_id"] = str(project.get("id", "unknown"))
                    
                    # Add to flat projects
                    flat_projects.append(flat_project)
                
                # Convert to DataFrame and save as CSV
                df = pd.DataFrame(flat_projects)
                csv_file = os.path.join(target_dir, f"{base_name}.csv")
                df.to_csv(csv_file, index=False)
                logger.info(f"Converted OpenAIRE data to CSV: {csv_file}")
                
            except Exception as e:
                logger.warning(f"Could not convert OpenAIRE data to CSV: {str(e)}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error processing OpenAIRE API: {str(e)}", exc_info=True)
            return False
    
    def _fetch_file(self, source_id, source_config, target_dir, action_name=None):
        """
        Download a file (CSV or Excel) from a URL.
        
        Args:
            source_id (str): The source identifier
            source_config (dict): The source configuration
            target_dir (str): The directory to save the downloaded file
            action_name (str, optional): The name of the action
            
        Returns:
            bool: True if successful, False otherwise
        """
        url = source_config.get("data_link")
        if not url:
            logger.error(f"No data link provided for source {source_id}" + 
                         (f" action {action_name}" if action_name else ""))
            return False
        
        # Determine filename from URL or use source_id
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            data_format = source_config.get("format", "csv").lower()
            extension = "xlsx" if data_format in ["excel", "xlsx"] else data_format
            # Use action name in filename if provided
            base_name = f"{source_id}_{action_name}" if action_name else source_id
            filename = f"{base_name}.{extension}"
        
        filepath = os.path.join(target_dir, filename)
        
        # Check if this source should skip SSL verification
        verify_ssl = str(source_config.get("skip_ssl_verify", "False")).lower() in ["true", "1", "yes", "y", "t"]
        
        if not verify_ssl:
            logger.warning(f"SSL verification disabled for {source_id}")
        
        # Download the file with retries
        for attempt in range(REQUEST_RETRIES):
            try:
                logger.info(f"Downloading {url} (attempt {attempt+1}/{REQUEST_RETRIES})")
                response = self.session.get(
                    url, 
                    timeout=REQUEST_TIMEOUT, 
                    stream=True,
                    verify=verify_ssl  # Set SSL verification based on source config
                )
                response.raise_for_status()
                
                # Save the file
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Successfully downloaded {url} to {filepath}")
                
                # Flag to track if file is valid
                file_valid = True
                file_size = os.path.getsize(filepath)
                
                # Verify the file by attempting to load it
                try:
                    self._verify_file(filepath, source_config.get("format"))
                except Exception as e:
                    # If the file exists and has content, consider it a successful download
                    # even if verification failed
                    if os.path.exists(filepath) and file_size > 0:
                        logger.warning(f"File verification failed but file was downloaded successfully. "
                                      f"Size: {file_size} bytes. Error: {str(e)}")
                        file_valid = False
                    else:
                        # Re-raise if file doesn't exist or is empty
                        raise
                
                # Create metadata file
                self._create_metadata(target_dir, source_id, source_config, {
                    "download_url": url,
                    "download_timestamp": datetime.now().isoformat(),
                    "file_size_bytes": file_size,
                    "ssl_verification": verify_ssl,
                    "action": action_name,
                    "verification_passed": file_valid
                })
                
                # Return true even if verification failed but file exists
                return True
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt+1} failed: {str(e)}")
                
                # If SSL verification is the issue and we haven't disabled it yet, try again with verification disabled
                if "CERTIFICATE_VERIFY_FAILED" in str(e) and verify_ssl and attempt == REQUEST_RETRIES - 2:
                    logger.warning(f"SSL certificate verification failed for {url}, attempting without verification")
                    verify_ssl = False
                elif attempt < REQUEST_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to download {url} after {REQUEST_RETRIES} attempts")
                    return False
            
            except Exception as e:
                logger.error(f"Error processing downloaded file: {str(e)}", exc_info=True)
                
                # Check if the file was at least downloaded
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    logger.warning(f"File was downloaded but processing failed. Marking as partially successful.")
                    
                    # Create metadata with error indication
                    self._create_metadata(target_dir, source_id, source_config, {
                        "download_url": url,
                        "download_timestamp": datetime.now().isoformat(),
                        "file_size_bytes": os.path.getsize(filepath),
                        "ssl_verification": verify_ssl,
                        "action": action_name,
                        "verification_passed": False,
                        "error": str(e)
                    })
                    
                    # Consider it a partial success if at least we have the file
                    return True
                else:
                    return False
    
    def _fetch_html(self, source_id, source_config, target_dir, action_name=None):
        """
        Fetch data by scraping HTML content and using LLM to extract structured data.
        
        Args:
            source_id (str): The source identifier
            source_config (dict): The source configuration
            target_dir (str): The directory to save the HTML data
            action_name (str, optional): The name of the action
            
        Returns:
            bool: True if successful, False otherwise
        """
        url = source_config.get("data_link") or source_config.get("data_link")
        if not url:
            logger.error(f"No URL provided for HTML scraping of source {source_id}" + 
                        (f" action {action_name}" if action_name else ""))
            return False
        
        try:
            # Simple request-based scraping
            logger.info(f"Fetching HTML from {url}")
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # Save the raw HTML
            # Use action name in filename if provided
            base_name = f"{source_id}_{action_name}" if action_name else source_id
            html_file = os.path.join(target_dir, f"{base_name}.html")
            with open(html_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            
            logger.info(f"Saved raw HTML to {html_file}")
            
            # Process HTML with LLM
            extracted_data = self._extract_data_with_llm(response.text, source_id, source_config, action_name)
            
            if extracted_data:
                # Save as JSON
                json_file = os.path.join(target_dir, f"{base_name}.json")
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(extracted_data, f, indent=2)
                logger.info(f"Saved extracted JSON data to {json_file}")
                
                # Also save as CSV if possible
                try:
                    if isinstance(extracted_data, list) and len(extracted_data) > 0:
                        df = pd.DataFrame(extracted_data)
                        csv_file = os.path.join(target_dir, f"{base_name}.csv")
                        df.to_csv(csv_file, index=False)
                        logger.info(f"Saved extracted CSV data to {csv_file}")
                except Exception as e:
                    logger.warning(f"Could not convert extracted data to CSV: {str(e)}")
            
            # Create metadata
            self._create_metadata(target_dir, source_id, source_config, {
                "scrape_url": url,
                "download_timestamp": datetime.now().isoformat(),
                "html_size_bytes": len(response.text),
                "action": action_name,
                "llm_processed": True
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Error scraping HTML for {source_id}" + 
                        (f" action {action_name}" if action_name else "") + 
                        f": {str(e)}", exc_info=True)
            return False

    def _extract_data_with_llm(self, html_content, source_id, source_config, action_name=None):
        """
        Extract structured data from HTML content using an LLM API.
        
        Args:
            html_content (str): The HTML content to process
            source_id (str): The source identifier
            source_config (dict): The source configuration
            action_name (str, optional): The name of the action
            
        Returns:
            dict/list: Extracted structured data or None if extraction failed
        """
        try:
            # Clean HTML content - extract text or reduce size if needed
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()
                
            # Get text content
            text_content = soup.get_text(separator='\n', strip=True)
            
            # Truncate content if too large (OpenAI has ~8k token limit for gpt-3.5-turbo)
            max_chars = 32000  # Approximate character limit (~8k tokens)
            if len(text_content) > max_chars:
                text_content = text_content[:max_chars]
                logger.warning(f"HTML content truncated for LLM processing (source: {source_id})")
            
            # Create prompt based on source and action
            source_desc = f"{source_config.get('funder', '')} - {action_name}" if action_name else source_config.get('funder', '')
            
            # Get API key from environment
            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                logger.error("OpenAI API key not found in environment variables")
                return None
            
            # Identify the type of data to extract based on the source
            prompt = f"""
            You are an expert at extracting structured data from HTML content. 
            The following text is from the website of {source_desc}, which contains information about funded projects or grants.
            
            Please extract all the available project information into a structured JSON array format. 
            Each project should include fields like:
            - project_title
            - principal_investigator
            - institution
            - funding_amount (with currency if available)
            - funding_year
            - duration (if available)
            - description
            - research_area
            
            Include any other relevant fields you find. Use null for missing values.
            Return ONLY the JSON array with no additional text or explanation.
            
            Here's the content:
            {text_content}
            """
            
            logger.info(f"Sending content to OpenAI API for extraction (source: {source_id})")
            
            # Make API request to OpenAI
            import requests
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            # Default to gpt-3.5-turbo if available, otherwise try gpt-4
            model = "gpt-3.5-turbo-16k"
            
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,  # Lower temperature for more deterministic output
                "max_tokens": 8000
            }
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers, 
                json=payload, 
                timeout=180  # Longer timeout for LLM
            )
            response.raise_for_status()
            
            # Parse the LLM response
            llm_response = response.json()
            print(llm_response)
            extracted_text = llm_response.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # Clean up the response to ensure it's valid JSON
            # Remove markdown code fences if present
            extracted_text = extracted_text.strip()
            if extracted_text.startswith("```json"):
                extracted_text = extracted_text[7:]
            elif extracted_text.startswith("```"):
                extracted_text = extracted_text[3:]
            if extracted_text.endswith("```"):
                extracted_text = extracted_text[:-3]
            extracted_text = extracted_text.strip()

            print(extracted_text)
            
            # Parse the JSON
            extracted_data = json.loads(extracted_text)
            logger.info(f"Successfully extracted structured data with LLM: {len(extracted_data) if isinstance(extracted_data, list) else 'N/A'} items")
            
            return extracted_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {str(e)}")
            # Try to save the raw response for debugging
            try:
                debug_file = os.path.join(target_dir, f"{source_id}_llm_response.txt")
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(extracted_text)
                logger.info(f"Saved raw LLM response for debugging to {debug_file}")
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"Error extracting data with LLM: {str(e)}", exc_info=True)
            return None
  
    
    def _verify_file(self, filepath, file_format):
        """
        Verify that the downloaded file can be read.
        
        Args:
            filepath (str): The path to the file to verify
            file_format (str): The format of the file
            
        Raises:
            Exception: If the file verification fails
        """
        try:
            if file_format.lower() in ["csv"]:
                # First attempt: standard reading
                try:
                    df = pd.read_csv(filepath)
                    logger.info(f"CSV verification: {len(df)} rows, {len(df.columns)} columns")
                    return
                except Exception as e:
                    logger.warning(f"Standard CSV reading failed: {str(e)}")
                    
                # Second attempt: try with different parsing options
                try:
                    # Try with error_bad_lines=False (skiprows for newer pandas versions)
                    if pd.__version__ >= '1.3.0':
                        df = pd.read_csv(filepath, on_bad_lines='skip')
                    else:
                        df = pd.read_csv(filepath, error_bad_lines=False)
                    logger.info(f"CSV verification (with bad lines skipped): {len(df)} rows, {len(df.columns)} columns")
                    return
                except Exception as e:
                    logger.warning(f"CSV reading with bad lines skipped failed: {str(e)}")
                
                # Third attempt: try with different quoting and delimiter options
                try:
                    import csv
                    df = pd.read_csv(filepath, quoting=csv.QUOTE_NONE, escapechar='\\')
                    logger.info(f"CSV verification (with QUOTE_NONE): {len(df)} rows, {len(df.columns)} columns")
                    return
                except Exception as e:
                    logger.warning(f"CSV reading with QUOTE_NONE failed: {str(e)}")
                
                # Fourth attempt: try with Python's csv module to analyze the file
                try:
                    with open(filepath, 'r', newline='', encoding='utf-8') as f:
                        sample = f.read(4096)  # Read a sample of the file
                        
                    # Try to detect the dialect
                    import csv
                    dialect = csv.Sniffer().sniff(sample)
                    logger.info(f"Detected CSV dialect: delimiter='{dialect.delimiter}', quotechar='{dialect.quotechar}'")
                    
                    # Try with detected dialect
                    df = pd.read_csv(filepath, sep=dialect.delimiter, quotechar=dialect.quotechar, 
                                    escapechar=dialect.escapechar)
                    logger.info(f"CSV verification (with detected dialect): {len(df)} rows, {len(df.columns)} columns")
                    return
                except Exception as e:
                    logger.warning(f"CSV reading with detected dialect failed: {str(e)}")
                
                # Last attempt: read the file as a raw text file to check if it exists and has content
                try:
                    with open(filepath, 'r', errors='replace') as f:
                        content = f.read(1024)  # Just read a bit to verify
                    
                    if content:
                        logger.info(f"File exists and contains content, but could not be parsed as CSV. Manual verification required.")
                        return
                    else:
                        logger.error(f"File exists but appears to be empty")
                        raise Exception("File is empty")
                except Exception as e:
                    logger.error(f"Failed to read file as text: {str(e)}")
                    raise
                
            elif file_format.lower() in ["excel", "xlsx"]:
                # First attempt: standard reading
                try:
                    df = pd.read_excel(filepath)
                    logger.info(f"Excel verification: {len(df)} rows, {len(df.columns)} columns")
                    return
                except Exception as e:
                    logger.warning(f"Standard Excel reading failed: {str(e)}")
                
                # Try with different engines
                try:
                    df = pd.read_excel(filepath, engine='openpyxl')
                    logger.info(f"Excel verification (with openpyxl): {len(df)} rows, {len(df.columns)} columns")
                    return
                except Exception as e:
                    logger.warning(f"Excel reading with openpyxl failed: {str(e)}")
                
                # Check if file exists and has content
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    logger.info(f"Excel file exists and contains data, but could not be parsed. Manual verification required.")
                    return
                else:
                    logger.error(f"Excel file is empty or does not exist")
                    raise Exception("File is empty or does not exist")
            else:
                # For other formats, just check if the file exists and has content
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    logger.info(f"File of format {file_format} exists and contains data. No verification method available.")
                    return
                else:
                    logger.error(f"File is empty or does not exist")
                    raise Exception("File is empty or does not exist")
        except Exception as e:
            logger.error(f"File verification failed: {str(e)}")
            raise
    
    def _create_metadata(self, directory, source_id, source_config, extra_info=None):
        """
        Create a metadata file with information about the fetch.
        
        Args:
            directory (str): The directory to create the metadata file in
            source_id (str): The source identifier
            source_config (dict): The source configuration
            extra_info (dict, optional): Additional information to include in the metadata
        """
        metadata = {
            "source_id": source_id,
            "funder": source_config.get("funder", ""),
            "source_name": source_config.get("source_name", ""),
            "country": source_config.get("country", ""),
            "type": source_config.get("type", ""),
            "status": "Downloaded",
            "format": source_config.get("format", ""),
            "timestamp": datetime.now().isoformat(),
        }
        
        if extra_info:
            metadata.update(extra_info)
        
        metadata_file = os.path.join(directory, "metadata.json")
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        
        # Also create/update the "last_download.json" file in the source directory
        source_dir = os.path.dirname(directory)
        last_download_file = os.path.join(source_dir, "last_download.json")
        
        last_download_info = {
            "timestamp": datetime.now().isoformat(),
            "directory": os.path.basename(directory),
            "source_id": source_id,
            "status": "success",
            "action": extra_info.get("action") if extra_info else None
        }
        
        with open(last_download_file, "w", encoding="utf-8") as f:
            json.dump(last_download_info, f, indent=2)
        
        logger.info(f"Created metadata file: {metadata_file}")
        logger.info(f"Updated last download info: {last_download_file}")
    
    def _is_recently_downloaded(self, source_path, max_age_days=7):
        """
        Check if a source has been downloaded recently.
        
        Args:
            source_path (str): The path to the source directory
            max_age_days (int): Maximum age in days to consider a download recent
            
        Returns:
            bool: True if the source was downloaded within the specified time frame
        """
        last_download_file = os.path.join(source_path, "last_download.json")
        
        # If the last_download.json file doesn't exist, the source hasn't been downloaded
        if not os.path.exists(last_download_file):
            return False
        
        try:
            # Read the last download information
            with open(last_download_file, "r", encoding="utf-8") as f:
                last_download = json.load(f)
            
            # Parse the timestamp
            download_time = datetime.fromisoformat(last_download.get("timestamp", ""))
            current_time = datetime.now()
            
            # Calculate the age in days
            age_days = (current_time - download_time).total_seconds() / (60 * 60 * 24)
            
            # Check if the download is recent enough
            if age_days <= max_age_days:
                action_info = f" action {last_download.get('action')}" if last_download.get('action') else ""
                logger.info(f"Source at {source_path}{action_info} was downloaded {age_days:.1f} days ago (max age: {max_age_days} days)")
                return True
            else:
                action_info = f" action {last_download.get('action')}" if last_download.get('action') else ""
                logger.info(f"Source at {source_path}{action_info} download is {age_days:.1f} days old, exceeding max age of {max_age_days} days")
                return False
                
        except Exception as e:
            logger.warning(f"Error checking last download for {source_path}: {str(e)}")
            return False


def initialize_from_csv(csv_path):
    """
    Initialize the data sources configuration from a CSV file.
    This function can be used to update sources.py.
    
    Args:
        csv_path (str): The path to the CSV file
    """
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return
    
    try:
        df = pd.read_csv(csv_path)
        sources = {}
        combined_sources = {}
        
        # First pass - identify sources that should be grouped by funder
        for _, row in df.iterrows():
            funder = row.get('Funder', '')
            action = row.get('Action', '')
            
            # If there's a funder and action, check if we should combine multiple actions
            # For sources like La Marató with multiple actions
            if funder and action:
                funder_key = funder.replace(' ', '_')
                if funder_key not in combined_sources:
                    combined_sources[funder_key] = []
                combined_sources[funder_key].append(row)
        
        # Now process all rows
        for _, row in df.iterrows():
            funder = row.get('Funder', '')
            source_type = row.get('Type', '')
            source_name = row.get('Source_name', '')
            action = row.get('Action', '')
            
            # Create source ID based on source type
            if source_type.lower() == 'aggregator':
                # For Aggregator type, use source_name as the ID
                source_id = source_name.replace(' ', '_')
                if not source_id:
                    continue
            else:
                # For non-aggregators with actions (like La Marató), use funder_action as ID
                if action:
                    source_id = f"{funder.replace(' ', '_')}-{action.replace(' ', '_')}"
                else:
                    # For regular sources, just use funder name
                    source_id = funder.replace(' ', '_')
                
                if not source_id:
                    continue
            
            # Determine parser type based on format
            format_type = row.get('Format', '').lower()
            if format_type == 'csv':
                parser = 'standard_csv'
            elif format_type in ['excel', 'xlsx']:
                parser = 'excel'
            elif format_type == 'api':
                parser = 'api'
            elif format_type == 'html':
                parser = 'html_scraper'
            else:
                parser = 'standard_csv'
            
            # Create new source configuration
            source_config = {
                'funder': funder,
                'action': action,
                'source_name': source_name,
                'country': row.get('Country', ''),
                'type': source_type,
                'status': row.get('Status', 'Not started'),
                'web_link': row.get('Link to web', ''),
                'data_link': row.get('Link to dump', ''),
                'format': format_type,
                'size': row.get('Size', ''),
                'notes': row.get('Notes', ''),
                'skip_ssl_verify': row.get('Skip SSL verify', 'False'),
                'parser': parser
            }
            
            sources[source_id] = source_config
        
        # Print the Python code for sources.py
        print("# Generated Data Sources Configuration")
        print("DATA_SOURCES = {")
        for source_id, config in sources.items():
            print(f"    \"{source_id}\": {{")
            for key, value in config.items():
                if key == 'actions' and isinstance(value, list):
                    print(f"        \"{key}\": [")
                    for action in value:
                        print(f"            {{")
                        for action_key, action_value in action.items():
                            print(f"                \"{action_key}\": \"{action_value}\",")
                        print(f"            }},")
                    print(f"        ],")
                else:
                    print(f"        \"{key}\": \"{value}\",")
            print("    },")
        print("}")
        
        logger.info(f"Successfully parsed {len(sources)} sources from CSV")
        
    except Exception as e:
        logger.error(f"Error initializing from CSV: {str(e)}", exc_info=True)


def main():
    """Main function to run the script."""
    import argparse

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="OpenJordi Data Fetcher")
    parser.add_argument("--init-from-csv", metavar="CSV_FILE", help="Initialize sources from CSV file")
    parser.add_argument("--force", action="store_true", help="Force refresh of all sources")
    parser.add_argument("--max-age", type=int, default=7, help="Maximum age in days to consider a source as valid (default: 7)")
    parser.add_argument("--sources", nargs="+", help="Specific source IDs to fetch (default: all sources)")
    
    args = parser.parse_args()
    
    # Check if running in initialization mode
    if args.init_from_csv:
        initialize_from_csv(args.init_from_csv)
        return
    
    # Normal operation - fetch data
    fetcher = DataFetcher()

    # Load environment variables from .env file in project root
    env_path = Path(__file__).resolve().parent.parent / '.env'
    env_loaded = load_dotenv(dotenv_path=env_path)
        
    # If specific sources are requested, filter DATA_SOURCES
    if args.sources:
        source_filter = {}
        for source_id in args.sources:
            if source_id in DATA_SOURCES:
                source_filter[source_id] = DATA_SOURCES[source_id]
            else:
                print(f"Warning: Source '{source_id}' not found in configuration")
        
        if not source_filter:
            print("Error: None of the specified sources were found")
            return
        
        # Make a temporary copy of filtered sources and use it
        temp_sources = source_filter.copy()
        results = fetcher.fetch_all_sources(temp_sources, force_refresh=args.force, max_age_days=args.max_age)
    else:
        # Fetch all sources
        results = fetcher.fetch_all_sources(DATA_SOURCES, force_refresh=args.force, max_age_days=args.max_age)
    
    # Report results
    successful = [source for source, result in results.items() if result is True]
    failed = [source for source, result in results.items() if result is False]
    skipped = [source for source, result in results.items() if result == "skipped"]
    
    print("\n===== FETCH SUMMARY =====")
    print(f"Successfully fetched: {len(successful)}/{len(results)} sources")
    print(f"Skipped (already downloaded): {len(skipped)}/{len(results)} sources")
    print(f"Failed: {len(failed)}/{len(results)} sources")
    
    if successful:
        print("\nSuccessful sources:")
        for source in successful:
            print(f"  ✅ {source}")
    
    if skipped:
        print("\nSkipped sources (already downloaded):")
        for source in skipped:
            print(f"  ⏭️ {source}")
    
    if failed:
        print("\nFailed sources:")
        for source in failed:
            print(f"  ❌ {source}")
    
    print("\nRaw data stored in:", RAW_DATA_DIR)


if __name__ == "__main__":
    main()