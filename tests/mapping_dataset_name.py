# mapping_dataset_name.py

import datetime
import pandas as pd

# Helper function to parse dates
def parse_date(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

# Function to extract investigator names (assuming the format is "Given Family")
def extract_investigator_names(name_str):
    parts = name_str.split(" ")
    if len(parts) > 1:
        return {"given_name": parts[0], "family_name": parts[1]}
    return {"given_name": None, "family_name": None}

# Mapping dictionary
mapping_dict = {
    "Research Project Title": "project_title",
    "Project Title (English)": "project_title",
    "Project/Area Number": "grant_id",
    "Project Period (FY)": {"start_date": "start_date", "end_date": "end_date"},
    "Principal Investigator": {"given_name": "investigator_given_name", "family_name": "investigator_family_name"},
    "Research Institution": "organization_name",
    "Total Cost (Overall)": "amount",
    "Keywords": "project_description"
}

# Function to apply transformations and rename columns in the dataframe
def apply_and_rename(df):
    # Track columns that have already been mapped to the same ontology field
    mapped_columns = set()

    # Split the Project Period (FY) into start_date and end_date
    if 'Project Period (FY)' in df.columns:
        df['start_date'], df['end_date'] = zip(*df['Project Period (FY)'].map(lambda x: [parse_date(date) for date in x.split(" – ")]))
        mapped_columns.add('start_date')
        mapped_columns.add('end_date')
    
    # Extract the investigator names only once
    if 'Principal Investigator' in df.columns:
        df['Principal Investigator'] = df['Principal Investigator'].map(lambda x: extract_investigator_names(x))

        # Extract given_name and family_name and map them only the first time they appear
        if 'investigator_given_name' not in mapped_columns:
            df['investigator_given_name'] = df['Principal Investigator'].apply(lambda x: x['given_name'])
            mapped_columns.add('investigator_given_name')
        
        if 'investigator_family_name' not in mapped_columns:
            df['investigator_family_name'] = df['Principal Investigator'].apply(lambda x: x['family_name'])
            mapped_columns.add('investigator_family_name')

    # Prepare the new DataFrame with renamed columns based on the mapping
    df_renamed = pd.DataFrame()

    for old_col, new_col in mapping_dict.items():
        if isinstance(new_col, dict):
            # For columns that need further mapping (like start_date, end_date)
            for sub_old, sub_new in new_col.items():
                if sub_old in df.columns and sub_new not in mapped_columns:
                    df_renamed[sub_new] = df[sub_old]
                    mapped_columns.add(sub_new)
        else:
            # For simple renaming
            if old_col in df.columns and new_col not in mapped_columns:
                df_renamed[new_col] = df[old_col]
                mapped_columns.add(new_col)

    return df_renamed