#!/usr/bin/env python3
"""
OpenJordi Sources Importer

This script imports data source definitions from a CSV file and generates 
config/sources.py with the imported data.
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add project root to path to ensure imports work correctly
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))


def import_sources_from_csv(csv_path):
    """
    Import source definitions from a CSV file and generate sources.py
    
    Args:
        csv_path (str): Path to the CSV file with source definitions
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        required_columns = ['Funder', 'Source_name', 'Country', 'Type', 'Link to dump', 'Format']
        
        # Verify required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns in CSV: {', '.join(missing_columns)}")
            return False
        
        # Generate Python code for sources.py
        python_code = generate_sources_py(df)
        
        # Write to config/sources.py
        config_dir = os.path.join(project_root, "config")
        os.makedirs(config_dir, exist_ok=True)
        
        with open(os.path.join(config_dir, "sources.py"), "w", encoding="utf-8") as f:
            f.write(python_code)
        
        print(f"Successfully imported {len(df)} sources into config/sources.py")
        return True
        
    except Exception as e:
        print(f"Error importing sources: {str(e)}")
        return False


def generate_sources_py(df):
    """
    Generate the sources.py file content from DataFrame
    
    Args:
        df (pandas.DataFrame): DataFrame with source definitions
        
    Returns:
        str: Python code for sources.py
    """
    sources_dict = {}
    
    for _, row in df.iterrows():
        # Create a source ID from the funder name (cleaned)
        source_id = clean_source_id(row.get('Source_name', '')) + "-" + clean_source_id(row.get('Action', ''))
        
        if not source_id:
            continue
        
        # Determine parser type based on format
        format_type = str(row.get('Format', '')).lower()
        parser = determine_parser_type(format_type)
        
        # Create source configuration
        sources_dict[source_id] = {
            'funder': row.get('Funder', ''),
            'action': row.get('Action', ''),
            'source_name': row.get('Source_name', ''),
            'country': row.get('Country', ''),
            'type': row.get('Type', ''),
            'status': row.get('Status', 'Not started'),
            'web_link': row.get('Link to web', ''),
            'data_link': row.get('Link to dump', ''),
            'format': format_type,
            'size': row.get('Size', ''),
            'notes': row.get('Notes', ''),
            'skip_ssl_verify': row.get('skip_ssl_verify', False),
            'parser': parser
        }
    
    # Generate Python code
    code = [
        '"""',
        'Configuration file for data sources in OpenJordi.',
        'This file defines the structure and access information for all grant data sources.',
        '"""',
        '',
        '# Data sources configuration',
        'DATA_SOURCES = {'
    ]
    
    for source_id, config in sources_dict.items():
        code.append(f'    "{source_id}": {{')
        for key, value in config.items():
            # Handle None values
            if value is None or str(value) == 'nan':
                code.append(f'        "{key}": "",')
            else:
                code.append(f'        "{key}": "{value}",')
        code.append('    },')
    
    code.append('}')
    code.append('')
    code.append('# Add more data sources as needed')
    code.append('')
    
    # Add parser configurations
    code.extend([
        '# Parser definitions - specify how to handle different data formats',
        'PARSER_CONFIGS = {',
        '    "standard_csv": {',
        '        "encoding": "utf-8",',
        '        "delimiter": ",",',
        '        "quotechar": \'"\',',
        '        "date_format": "%Y-%m-%d"',
        '    },',
        '    "excel": {',
        '        "sheet_name": 0,  # Default to first sheet',
        '        "date_format": "%Y-%m-%d"',
        '    },',
        '    "api": {',
        '        "auth_required": False,',
        '        "pagination": False,',
        '        "rate_limit": 60  # requests per minute',
        '    },',
        '    "html": {',
        '        "use_selenium": False,',
        '        "wait_time": 2,',
        '        "selectors": {}',
        '    }',
        '}'
    ])
    
    return '\n'.join(code)


def clean_source_id(name):
    """
    Clean a string to be used as a source ID
    
    Args:
        name (str): The name to clean
        
    Returns:
        str: Cleaned source ID
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Replace spaces, remove special characters
    cleaned = name.replace(' ', '_')
    cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')
    
    return cleaned


def determine_parser_type(format_type):
    """
    Determine the parser type based on the format
    
    Args:
        format_type (str): The format type
        
    Returns:
        str: Parser type
    """
    format_type = str(format_type).lower()
    
    if format_type == 'csv':
        return 'standard_csv'
    elif format_type in ['excel', 'xlsx', 'xls']:
        return 'excel'
    elif format_type == 'api':
        return 'api'
    elif format_type == 'html':
        return 'html'
    else:
        return 'standard_csv'


def main():
    """Main function to run the script."""
    if len(sys.argv) < 2:
        print("Usage: python import_sources.py <path_to_sources.csv>")
        return
    
    csv_path = sys.argv[1]
    import_sources_from_csv(csv_path)


if __name__ == "__main__":
    main()