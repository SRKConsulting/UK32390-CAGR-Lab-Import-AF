""""
Helper functions
"""
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ClientAuthenticationError
from azure.keyvault.secrets import SecretClient

import pandas as pd
import io
import json
from datetime import datetime 
from collections import Counter
import os
import numpy as np


def fetch_file_contents(vault_id, container, filename, logger):
    identity = DefaultAzureCredential()
    secretClient = SecretClient(vault_url=f"https://{vault_id}.vault.azure.net/", credential=identity)
    try:
        blob_secret = secretClient.get_secret('blob-connection')
        blob_conn = blob_secret.value
        blob_service_client = BlobServiceClient.from_connection_string(blob_conn)
        if blob_service_client.get_blob_client(container, filename).exists():
            blob_client = blob_service_client.get_blob_client(container, filename)
            blob_download = blob_client.download_blob()
            stream = io.BytesIO()
            blob_download.download_to_stream(stream)
            df_workbook = pd.ExcelFile(stream)
            logger.info("Workbook loaded.")
            return df_workbook, ""
        else:
            return None, "File not found in storage"
    except ClientAuthenticationError as e:
        logger.error(e)
        return None, "Blob client authentication 'blob-connection' failed."

def get_sql_connection(vault_id, logger):
    identity = DefaultAzureCredential()
    secretClient = SecretClient(vault_url=f"https://{vault_id}.vault.azure.net/", credential=identity)
    try:
        sql_conn_string = secretClient.get_secret('sql-connection').value
        return sql_conn_string, ""
    except ClientAuthenticationError as e:
        logger.error(e)
        return None, "Secret client authentication 'sql-connection' failed."    

def file_header_info(df: pd.DataFrame):
        dfheader_info = pd.read_excel(df, header=None, sheet_name=0)
        work_order_status = str(dfheader_info.iloc[0, 0]).strip()
        client_ref = str(dfheader_info.iloc[1, 0].split(':')[1].strip()).strip()
        samples_submitted = str(dfheader_info.iloc[2, 0].split(':')[1]).strip()
        date_received = str(dfheader_info.iloc[3, 0].split('DATE')[2].split(' : ')[1]).strip()
        date_finalized = str(dfheader_info.iloc[3, 0].split('DATE')[1].split(' : ')[1]).strip()
        project = str(dfheader_info.iloc[4, 0].split(' : ')[1]).strip()
        comments = str(dfheader_info.iloc[5, 0].split(' : ')[1]).strip()
        po_number = str(dfheader_info.iloc[6, 0].split(' : ')[1]).strip()
        return{
            'work_order_status': work_order_status,
            'client_ref': client_ref,
            'samples_submitted': samples_submitted,
            'date_finalized': date_finalized,
            'date_receieved': date_received,
            'project': project,
            'comments': comments,
            'po_number': po_number
        }

def create_response(
        filename: str, 
        status: str, 
        log: str, 
        importance: str, 
        inserted_count: str, 
        sample_count: str, 
        work_order_status = '',
        client_ref = '',
        samples_submitted = '',
        date_received = '',
        date_finalized = '',
        project = '',
        comments = '',
        po_number = '',
        logger = ''
    ):
    """_summary_

    Args:
        filename (str): _description_
        status (str): _description_
        log (str): _description_
        importance (str): _description_
        inserted_count (str): _description_
        sample_count (str): _description_
        work_order_status (str): _description_
        client_ref (str): _description_
        samples_submitted (str): _description_
        date_received (str): _description_
        date_finalized (str): _description_
        project (str): _description_
        comments (str): _description_
        po_number (str): _description_
        
    Returns:
        _type_: _description_
    """
    output = json.dumps({
        "inputfile": filename,
        "status": status,
        "message": log,
        "importance": importance, 
        "status_code": 200, 
        "inserted_count": inserted_count,
        "sample_count" : sample_count,
        "work_order_status": work_order_status,
        "client_ref": client_ref,
        "samples_submitted": samples_submitted,
        "date_received": date_received,
        "date_finalized": date_finalized,
        "project": project,
        "comments": comments,
        "po_number": po_number
    })
    logger.info(f"INFO: JSON response {output}")
    return func.HttpResponse(output)

def parse_date(date_str):
    date_str = str(date_str).strip()
    formats = ['%Y', '%Y-%m', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def make_unique_columns(columns):
    """
    Make column names unique by appending a counter to duplicates.
    """
    counter = Counter()
    unique_columns = []
    for col in columns:
        if counter[col] > 0:
            unique_columns.append(f"{col}_{counter[col]}")
        else:
            unique_columns.append(col)
        counter[col] += 1
    return unique_columns

def parse_path(path: str) -> dict :
    """ Parses the given Sharepoint file path into 
    file name, 
    drilling status (WIP, Completed, Finalised), 
    drilling type (AC, DD)
    project name

    Args:
        path (str): path

    Returns:
        dict: dictionary containing parsed components
    """
    try:
        # Extract the file name
        file_name = os.path.basename(path)

        # Extract the parts of the path
        parts = path.split('/')

        # Ensure the path has enough parts to extract the required information
        if len(parts) < 5:
            raise ValueError("Path does not contain enough segments to extract all required information.")

        # Extract the status, drilling type, and project name
        status = parts[-2].split('_')[-1]  # Assuming status is always the last part before the file name
        drilling_type = parts[-3]  # Drilling type is the second to last part before the status
        project_name = parts[-4]  # Project name is the third to last part before the status

        # Create the unique path by removing the status part
        unique_path = project_name + "/" + drilling_type + "/" + file_name

        # Return the extracted components as a dictionary
        return {
            "filename": file_name,
            "status": status,
            "hole_type": drilling_type,
            "project_name": project_name,
            "unique_path": unique_path
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        return None 

def clean_lab_results(df: pd.DataFrame) -> pd.DataFrame:
    """Clean lab results from excel file in to results datafram

    Args:
        file_path (str): path to file

    Returns:
        pd.DataFrame: results dataframe
    """
    # import excel file dataframe
    df_results = pd.read_excel(df, header=None, sheet_name=0)
    #extract job title
    job_title = df_results.iloc[0, 0]
    # Remove the first 7 rows
    df_results = df_results.iloc[7:]
    # Transpose dataframe
    transposed_df_results = df_results.T
    # Merge first three columns
    transposed_df_results['Parameter'] = transposed_df_results.iloc[:, 0].astype(str) + '|' + \
                                transposed_df_results.iloc[:, 1].astype(str) + '|' + \
                                transposed_df_results.iloc[:, 2].astype(str)
    # Drop the original columns and keep the merged one
    transposed_df_results = transposed_df_results.drop(transposed_df_results.columns[:3], axis=1)
    # extract the columsn as a list
    columns = transposed_df_results.columns.to_list()
    #remove newly created column from dataframe
    if 'Parameter' in columns:
        columns.remove('Parameter')
    # rearrange the datafram so the new column is first
    df_results = transposed_df_results[['Parameter'] + columns]
    # re-transpose table
    df_results = df_results.T
    # Set the first row as column names
    new_headers = df_results.iloc[0].values
    df_results.columns = new_headers
    # Drop the first row since it's now the header
    df_results = df_results.iloc[1:].reset_index(drop=True)
    # melt (unpivot) the dataframe
    df_results = df_results.melt (id_vars = df_results.columns[0], var_name = 'attribute', value_name = 'text_value')
    # Split 'Column1' into two new columns based on the delimiter '|'
    df_results[['lab_method', 'analyte', 'unit']] = df_results['attribute'].str.split('|', expand=True)
    # drop split column
    df_results = df_results.drop(columns=['attribute'])
    #rename columns
    df_results = df_results.rename(columns={df_results.columns[0]: 'sample_id'})
    # remove null lab results from dataframe
    df_results = df_results[(~df_results['text_value'].isnull()) & (df_results['text_value'] != '')]
    # qualifier from value
    df_results['qualifier'] = np.where(df_results['text_value'].str.contains('<', na=False), '<',
                    np.where(df_results['text_value'].str.contains('>', na=False), '>', 
                    None))
    df_results['value'] = np.where(df_results['text_value'].str.contains('<|>', na=False), 
                        df_results['text_value'].str.extract(r'(\d+.\d+|\d+)')[0], 
                        df_results['text_value'])
    df_results['value'] = pd.to_numeric(df_results['value'], errors='coerce')
    df_results['job_title'] = job_title
    return df_results

def clean_lab_header(df: pd.DataFrame) -> pd.DataFrame:
    # import excel file dataframe
    df_header = pd.read_excel(df, header=None, sheet_name=0)
    #file_name_with_extension = file_path.split('/')[-1]
    #extract job title
    job_title = df_header.iloc[0, 0]
    # Remove the first 7 rows
    df_header = df_header.iloc[:7]

    # Transpose dataframe
    df_header = df_header.T
    df_header = df_header.replace('', pd.NA)
    df_header = df_header.dropna()

    # Get the name of the first column
    first_col = df_header.columns[0]
    # Split the first column and create the two new columns
    df_header['job_number'] = df_header[first_col].str.split('-').str[0]
    df_header['result_status'] = df_header[first_col].str.split('-').str[1]

    date_column = df_header[3]
    date_columns = date_column.str.split('DATE', expand=True)
    date_rec = date_columns[1].str.split(':', expand=True)
    date_received = date_rec[1] 
    date_final = date_columns[2].str.split(':', expand=True)
    date_finalised = date_final[1] 
    df_header['date_received'] = date_received
    df_header['date_finalized'] = date_finalised
    # Convert the `date_received` column to SQL-compatible date format
    df_header['date_received'] = pd.to_datetime(df_header['date_received'], errors='coerce')
    # Convert the `date_finalized` column to SQL-compatible date format
    df_header['date_finalized'] = pd.to_datetime(df_header['date_finalized'], errors='coerce')
    
    df_header = df_header.drop(columns=[3])
    df_header = df_header.rename(columns={0: 'job_title'})
    df_header = df_header.rename(columns={1: 'client_ref'})
    df_header = df_header.rename(columns={2: 'quantity'})
    df_header = df_header.rename(columns={4: 'project'})
    df_header = df_header.rename(columns={5: 'cert_comment'})
    df_header = df_header.rename(columns={6: 'po_number'})
    df_header['job_title'] = job_title
    return df_header

def filter_new_records(df, existing_records):
    """
    Filter out rows from lab `df` that already exist in the database.
    """
    # Use pandas merge with an outer join to find unmatched rows
    # Perform an anti-join to exclude existing records
    df_filtered = pd.merge(
        df,
        existing_records,
        how='left',  # Keep all rows from `df`, but flag matches from `existing_records`
        on=['sample_id', 'lab_method', 'analyte'],
        indicator=True  # Add a column to indicate whether a match exists
    )
    # Keep only rows where the indicator column is "left_only" (meaning not in `existing_records`)
    df_filtered = df_filtered[df_filtered['_merge'] == 'left_only']
    # Drop the indicator column
    df_filtered = df_filtered.drop('_merge', axis=1)
    return df_filtered
