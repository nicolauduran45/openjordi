#!/usr/bin/env python3
"""
OpenJordi Column Mapper

This script maps columns from downloaded grant data files to the CrossRef grant ontology.
It follows a similar interface to fetch_data.py.

Usage:
    python map_columns.py --source SOURCE_ID     # Map columns for a specific source
    python map_columns.py --force                # Force remapping even if mapping exists
    python map_columns.py --all                  # Map all sources
"""

import os
import sys
import json
import logging
import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime
import openai
from dotenv import load_dotenv

# Add project root to path to ensure imports work correctly
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Import configuration
from config import (
    RAW_DATA_DIR, LOG_LEVEL, LOG_FILE,
    LLM_PROVIDER, LLM_API_KEY
)

LLM_MODEL = "gpt-4"

# Import schema definition
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ontology')))

from grant_ontology import crossref_metadata


# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("openjordi.map_columns")

# Ensure output directories exist
ONTOLOGY_DIR = os.path.join(project_root, "ontology")
MAPPINGS_DIR = os.path.join(ONTOLOGY_DIR, "mappings")
os.makedirs(MAPPINGS_DIR, exist_ok=True)


class ColumnMapper:
    """Class to handle mapping source columns to CrossRef schema."""
    
    def __init__(self):
        """Initialize the column mapper."""
        # Load environment variables
        load_dotenv()
        
        # Setup API key for LLM
        self.llm_provider = LLM_PROVIDER
        self.api_key = LLM_API_KEY or os.environ.get("OPENAI_API_KEY") or os.environ.get("SAMBANOVA_API_KEY")
        self.model = LLM_MODEL
        
        if not self.api_key:
            logger.error("No API key found for LLM. Set OPENAI_API_KEY or SAMBANOVA_API_KEY in environment.")
            raise ValueError("Missing API key for language model")
        
        # Initialize OpenAI client based on provider
        if self.llm_provider.lower() == "sambanova":
            self.client = openai.OpenAI(
                api_key=self.api_key,
                base_url="https://api.sambanova.ai/v1",
            )
        else:  # Default to OpenAI
            self.client = openai.OpenAI(api_key=self.api_key)
    
    def map_all_sources(self, force_remapping=False):
        """
        Map columns for all sources found in RAW_DATA_DIR.
        
        Args:
            force_remapping (bool): If True, remap columns even if mapping exists
            
        Returns:
            dict: Results of mapping operations by source
        """
        results = {}
        
        # Get all source directories in RAW_DATA_DIR
        for source_name in os.listdir(RAW_DATA_DIR):
            source_path = os.path.join(RAW_DATA_DIR, source_name)
            
            # Skip non-directory items
            if not os.path.isdir(source_path):
                continue
            
            # Process this source
            try:
                logger.info(f"Mapping columns for source: {source_name}")
                result = self.map_source(source_name, force_remapping)
                results[source_name] = result
            except Exception as e:
                logger.error(f"Error mapping columns for source {source_name}: {str(e)}", exc_info=True)
                results[source_name] = False
        
        return results
    
    def extract_columns_from_file(self, file_path):
        """
        Extract column names from a CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            list: List of column names
        """
        try:
            # Try multiple approaches to handle problematic CSVs
            try:
                df = pd.read_csv(file_path, skip_blank_lines=True)
            except Exception as e:
                logger.warning(f"Standard CSV reading failed: {str(e)}")
                # Try with error handling options
                try:
                    if pd.__version__ >= '1.3.0':
                        df = pd.read_csv(file_path, on_bad_lines='skip',skip_blank_lines=True)
                    else:
                        df = pd.read_csv(file_path, error_bad_lines=False,skip_blank_lines=True)
                except Exception:
                    # Last resort: try with different encoding
                    df = pd.read_csv(file_path, encoding='latin1', 
                                   on_bad_lines='skip' if pd.__version__ >= '1.3.0' else None)
            
            # Return column names
            return df
        
        except Exception as e:
            logger.error(f"Error extracting columns from {file_path}: {str(e)}", exc_info=True)
            return []
    
    def map_source(self, source_name, force_remapping=False):
        """
        Map columns for a specific source.
        
        Args:
            source_name (str): Name of the source to map
            force_remapping (bool): If True, remap columns even if mapping exists
            
        Returns:
            bool: True if successful, False otherwise
        """
        source_path = os.path.join(RAW_DATA_DIR, source_name)
        
        if not os.path.exists(source_path) or not os.path.isdir(source_path):
            logger.error(f"Source directory not found: {source_path}")
            return False
        
        # Look for the 'latest' directory which should contain the most recent download
        latest_dir = os.path.join(source_path, "latest")
        if not os.path.exists(latest_dir) or not os.path.isdir(latest_dir):
            # If 'latest' doesn't exist, find the most recent download folder
            download_dirs = [d for d in os.listdir(source_path) 
                           if os.path.isdir(os.path.join(source_path, d)) and 
                           d != "latest" and
                           not d.startswith(".")]
            
            if not download_dirs:
                logger.warning(f"No download directories found for source {source_name}, skipping")
                return False
            
            # Sort by name (which should be timestamp-based)
            download_dirs.sort(reverse=True)
            latest_dir = os.path.join(source_path, download_dirs[0])
        
        logger.info(f"Processing latest directory: {latest_dir}")
        
        # Look for CSV files in the latest directory
        csv_files = [f for f in os.listdir(latest_dir) if f.lower().endswith('.csv')]
        
        if not csv_files:
            logger.warning(f"No CSV files found in {latest_dir}, looking for other file types")
            
            # Look for Excel files
            excel_files = [f for f in os.listdir(latest_dir) if f.lower().endswith(('.xlsx', '.xls'))]
            if excel_files:
                logger.info(f"Found Excel files: {excel_files}")
                for excel_file in excel_files:
                    try:
                        # Convert Excel to CSV for processing
                        excel_path = os.path.join(latest_dir, excel_file)
                        csv_path = os.path.join(latest_dir, f"{os.path.splitext(excel_file)[0]}.csv")
                        
                        # Convert if CSV doesn't exist or force_remapping is True
                        if not os.path.exists(csv_path) or force_remapping:
                            logger.info(f"Converting Excel to CSV: {excel_file}")
                            df = pd.read_excel(excel_path)
                            df.to_csv(csv_path, index=False)
                            csv_files.append(os.path.basename(csv_path))
                    except Exception as e:
                        logger.error(f"Error converting Excel to CSV: {str(e)}", exc_info=True)
            
            # Look for JSON files
            json_files = [f for f in os.listdir(latest_dir) if f.lower().endswith('.json') and f != "metadata.json"]
            if json_files and not csv_files:
                logger.info(f"Found JSON files: {json_files}")
                for json_file in json_files:
                    try:
                        # Convert JSON to CSV for processing
                        json_path = os.path.join(latest_dir, json_file)
                        csv_path = os.path.join(latest_dir, f"{os.path.splitext(json_file)[0]}.csv")
                        
                        # Convert if CSV doesn't exist or force_remapping is True
                        if not os.path.exists(csv_path) or force_remapping:
                            logger.info(f"Converting JSON to CSV: {json_file}")
                            with open(json_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            if isinstance(data, list):
                                df = pd.DataFrame(data)
                            elif isinstance(data, dict) and any(isinstance(v, list) for v in data.values()):
                                # If it's a dict of lists (record-oriented)
                                for key, value in data.items():
                                    if isinstance(value, list) and value:
                                        df = pd.DataFrame(value)
                                        break
                                else:
                                    df = pd.DataFrame([data])
                            else:
                                # Fallback: convert to a single-row DataFrame
                                df = pd.DataFrame([data])
                            
                            df.to_csv(csv_path, index=False)
                            csv_files.append(os.path.basename(csv_path))
                    except Exception as e:
                        logger.error(f"Error converting JSON to CSV: {str(e)}", exc_info=True)
        
        if not csv_files:
            logger.warning(f"No CSV files or convertible files found for {source_name}")
            return False
        
        # Process each CSV file
        for csv_file in csv_files:
            try:
                csv_path = os.path.join(latest_dir, csv_file)
                logger.info(f"Processing CSV file: {csv_path}")
                
                # Extract columns from the CSV file
                df = self.extract_columns_from_file(csv_path)
                
                if not df.columns.any():
                    logger.warning(f"No columns found in {csv_file}")
                    continue
                
                logger.info(f"Found {len(df.columns)} columns in {csv_file}")
                
                # Check if mapping already exists
                mapping_exists = self.check_if_mapping_exists(source_name, df.columns)
                
                if mapping_exists and not force_remapping:
                    logger.info(f"Mapping already exists for {source_name}/{csv_file}, skipping")
                    continue
                
                # Get mapping for these columns
                mapping = self.get_column_mapping(df, source_name)
                
                if mapping:
                    # Save the mapping
                    self.save_mapping(source_name, df.columns, mapping)
                    logger.info(f"Successfully mapped {len(mapping)} columns for {source_name}/{csv_file}")
                else:
                    logger.warning(f"Failed to get mapping for {source_name}/{csv_file}")
            except Exception as e:
                logger.error(f"Error processing {csv_file}: {str(e)}", exc_info=True)
        
        return True
    
    def extract_file(self, file_path):
        """
        Extract column names from a CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            list: List of column names
        """
        try:
            # Try multiple approaches to handle problematic CSVs
            try:
                df = pd.read_csv(file_path, skip_blank_lines=True)
            except Exception as e:
                logger.warning(f"Standard CSV reading failed: {str(e)}")
                # Try with error handling options
                try:
                    if pd.__version__ >= '1.3.0':
                        df = pd.read_csv(file_path, on_bad_lines='skip',skip_blank_lines=True)
                    else:
                        df = pd.read_csv(file_path, error_bad_lines=False,skip_blank_lines=True)
                except Exception:
                    # Last resort: try with different encoding
                    df = pd.read_csv(file_path, encoding='latin1', 
                                   on_bad_lines='skip' if pd.__version__ >= '1.3.0' else None)
            
            # Return column names
            return df
        
        except Exception as e:
            logger.error(f"Error extracting columns from {file_path}: {str(e)}", exc_info=True)
            return []
    
    def check_if_mapping_exists(self, source_name, columns):
        """
        Check if a mapping already exists for this source and columns.
        
        Args:
            source_name (str): Name of the source
            columns (list): List of column names
            
        Returns:
            bool: True if mapping exists, False otherwise
        """
        # Create source-specific directory
        source_dir = os.path.join(MAPPINGS_DIR, source_name)
        
        if not os.path.exists(source_dir):
            return False
        
        # Create a hash of the columns to use as a key
        columns_hash = hashlib.md5(",".join(sorted(columns)).encode()).hexdigest()
        mapping_file = os.path.join(source_dir, f"{columns_hash}_mapping.json")
        
        return os.path.exists(mapping_file)
    
    def get_column_mapping(self, df, source_name):
        """
        Get mapping between source columns and CrossRef schema.
        
        Args:
            columns (list): List of column names
            source_name (str): Name of the source
            
        Returns:
            dict: Mapping from source columns to schema columns
        """
        # First check if we have a cached mapping
        mapping = self._get_cached_mapping(df.columns, source_name)
        
        if mapping is None:
            # Get mapping from LLM
            mapping = self._get_column_mapping_from_llm(df, source_name)
        
        return mapping
    
    def _get_cached_mapping(self, columns, source_name):
        """
        Check if we have a cached mapping for this source and columns.
        
        Args:
            columns (list): List of column names
            source_name (str): Name of the source
            
        Returns:
            dict or None: The cached mapping or None if not found
        """
        # Create source-specific directory
        source_dir = os.path.join(MAPPINGS_DIR, source_name)
        
        if not os.path.exists(source_dir):
            return None
        
        # Create a hash of the columns to use as a key
        columns_hash = hashlib.md5(",".join(sorted(columns)).encode()).hexdigest()
        mapping_file = os.path.join(source_dir, f"{columns_hash}_mapping.json")
        
        if os.path.exists(mapping_file):
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                logger.info(f"Using cached mapping for {source_name}")
                return cached_data["mapping"]
            except Exception as e:
                logger.warning(f"Error reading cached mapping: {str(e)}")
        
        return None
    
    def save_mapping(self, source_name, columns, mapping):
        """
        Save a column mapping to the ontology/mappings directory.
        
        Args:
            source_name (str): Name of the source
            columns (list): List of column names
            mapping (dict): The column mapping to save
        """
        # Create source-specific directory
        source_dir = os.path.join(MAPPINGS_DIR, source_name)
        os.makedirs(source_dir, exist_ok=True)
        
        # Create a hash of the columns to use as a key
        columns_hash = hashlib.md5(",".join(sorted(columns)).encode()).hexdigest()
        mapping_file = os.path.join(source_dir, f"{columns_hash}_mapping.json")
        
        # Prepare mapping data
        mapping_data = {
            "source": source_name,
            "columns": columns,
            "timestamp": datetime.now().isoformat(),
            "mapping": mapping
        }
        
        # Save mapping to file
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved mapping to {mapping_file}")
    
    def _get_column_mapping_from_llm(self, df, source_name):
        """
        Use an LLM to map source columns to CrossRef schema.
        
        Args:
            columns (list): List of column names to map
            source_name (str): The name of the source
            
        Returns:
            dict: Mapping from source columns to schema columns
        """

        columns = df.columns
        column_examples = {}
    
        for col in columns:
            if col in df.columns:
                if not df[col].iloc[0] is None and not pd.isna(df[col].iloc[0]):
                    column_examples[col] = str(df[col].iloc[0])
                else:
                    break

        print(df)
        # Format column information with examples
        column_info = []
        for col in columns:
            if col in column_examples:
                column_info.append(f"* {col}: {column_examples.get(col)}")
            else:
                column_info.append(f"* {col}")
        
        column_text = "\n".join(column_info)


        # Prepare schema information with descriptions
        schema_info = []
        for field, details in crossref_metadata.items():
            schema_info.append(f"- {field}: {details['Description']} ({details['Limits']})")
        
        schema_text = "\n".join(schema_info)
        
        # Create prompt for LLM
        prompt = f"""
        You are an expert in data schema mapping for academic grant data.
        
        I need to map columns from a dataset about research grants from '{source_name}' to the CrossRef grant metadata schema.
        
        SOURCE COLUMNS (with examples from first row if available):
        {column_text}
        
        TARGET SCHEMA (CrossRef grant metadata):
        {schema_text}
        
        For each source column, map it to the most appropriate CrossRef schema field, or 'null' if there is no appropriate match.
        Consider semantic meaning, not just exact name matches. Be thorough and consider all possible mappings.
        
        Return your response as a valid json with the following format:
        {{
            "source_column_name": "crossref_field_name",
            ...
        }}
        
        Only include the Python dictionary in your response, with no additional text.
        """

        


    
        
        try:
            logger.info(f"Requesting column mapping from LLM for {source_name}")
            
            # Make API request with retry
            for attempt in range(3):
                try:

                    print(prompt)
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": "You are a helpful academic data assistant that maps columns between schemas."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.1,
                        max_tokens=1500,
                    )
                    
                    # Get the response content
                    response_text = response.choices[0].message.content.strip()

                    print(response_text)
                    
                    # Clean up the response to ensure it's valid Python
                    response_text = self._clean_llm_response(response_text)
                    
                    # Convert to dictionary
                    mapping = eval(response_text)
                    
                    # Validate mapping
                    if not isinstance(mapping, dict):
                        raise ValueError("LLM response is not a valid dictionary")
                    
                    # Filter out any invalid mappings (where target isn't in schema)
                    valid_mapping = {}
                    for source_col, target_col in mapping.items():
                        if target_col in crossref_metadata or target_col == "null":
                            valid_mapping[source_col] = target_col
                        else:
                            logger.warning(f"Invalid target column '{target_col}' for source column '{source_col}'")
                    
                    logger.info(f"Successfully mapped {len(valid_mapping)} columns for {source_name}")
                    return valid_mapping
                
                except Exception as e:
                    logger.warning(f"LLM mapping attempt {attempt+1} failed: {str(e)}")
                    if attempt < 2:  # If not the last attempt
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        logger.error(f"All LLM mapping attempts failed for {source_name}")
                        raise
            
        except Exception as e:
            logger.error(f"Error getting column mapping from LLM: {str(e)}", exc_info=True)
            # Fallback: Return an empty mapping to avoid complete failure
            return {}
    
    def _clean_llm_response(self, response_text):
        """
        Clean the LLM response to ensure it's a valid Python dictionary.
        
        Args:
            response_text (str): The raw response from the LLM
            
        Returns:
            str: Cleaned response text that can be safely evaluated
        """
        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text[9:]
        elif response_text.startswith("```"):
            response_text = response_text[3:]
        
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        response_text = response_text.strip()
        
        # Ensure it starts and ends with curly braces
        if not response_text.startswith("{"):
            response_text = "{" + response_text
        if not response_text.endswith("}"):
            response_text = response_text + "}"
        
        return response_text


def main():
    """Main function to run the script."""
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="OpenJordi Column Mapper")
    parser.add_argument("--source", help="Specific source to map")
    parser.add_argument("--force", action="store_true", help="Force remapping even if mapping exists")
    parser.add_argument("--all", action="store_true", help="Map all sources")
    
    args = parser.parse_args()
    
    if not args.source and not args.all:
        parser.error("Either --source or --all must be provided")
    
    try:
        # Initialize mapper
        mapper = ColumnMapper()
        
        if args.all:
            # Map all sources
            logger.info("Mapping all sources")
            results = mapper.map_all_sources(force_remapping=args.force)
            
            # Report results
            successful = [source for source, result in results.items() if result is True]
            failed = [source for source, result in results.items() if result is False]
            
            print("\n===== MAPPING SUMMARY =====")
            print(f"Successfully mapped: {len(successful)}/{len(results)} sources")
            print(f"Failed: {len(failed)}/{len(results)} sources")
            
            if successful:
                print("\nSuccessful sources:")
                for source in successful:
                    print(f"  ✅ {source}")
            
            if failed:
                print("\nFailed sources:")
                for source in failed:
                    print(f"  ❌ {source}")
        else:
            # Map specific source
            logger.info(f"Mapping source: {args.source}")
            result = mapper.map_source(args.source, force_remapping=args.force)
            
            if result:
                print(f"Successfully mapped columns for {args.source}")
            else:
                print(f"Failed to map columns for {args.source}")
        
        print("\nMappings stored in:", MAPPINGS_DIR)
        
    except Exception as e:
        logger.error(f"Error in column mapping process: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()