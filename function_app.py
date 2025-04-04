import azure.functions as func
import logging
import utils
import sql
import pandas as pd
import variables as var
from datetime import datetime

br = '<br>'

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_lab")
def http_lab(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Lab trigger function processed a request.')

    # get variables from http request
    path = req.params.get('path')
    container = req.params.get('container')
    vault_id = req.params.get('keyvault')

    if not path:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            path = req_body.get('path')
            container = req_body.get('container')
            vault_id = req_body.get('keyvault')
    
    logging.info(
        f"""Request Parameters: 
        container | {container}; 
        path | {path}; 
        keyvault | https://{vault_id}.vault.azure.net/"""
    )
    if path:
        # status is failed by default
        status = 'failed'
        # parse the path to get filename, completion status, and project name
        parsed_path =  utils.parse_path(path)
        if not parsed_path :
            raise Exception('Error: Could not parse the file path. ')
        
        filename = parsed_path['filename']
        unique_path = parsed_path['unique_path']
        logging.info("This is unique path: "+str(unique_path))

        #? all are blank by default
        inserted_count = ''
        sample_count = ''
        work_order_status = ''
        client_ref = ''
        samples_submitted = ''
        date_finalized = ''
        date_received = ''
        project = ''
        comments = ''
        po_number = ''
        log = ''

        # get file contents
        df_workbook, log_fetch = utils.fetch_file_contents(vault_id, container, filename, logging)
        if df_workbook is None:
            log = 'Workbook not found. '
            log += log_fetch + br
            status = status
            return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            sample_count,
                            work_order_status,
                            client_ref,
                            samples_submitted,
                            date_received,
                            date_finalized,
                            project,
                            comments,
                            po_number,
                            logging
                        )
        logging.info('Fetch file contents successful')

        # Check for PO Number
        df_check = pd.read_excel(df_workbook, header=None, sheet_name=0)
        po_number_check_value = df_check.iloc[6,0]
        logging.info(f'PO Number check value: {po_number_check_value}')
        logging.info(f'Sample check value: {df_check.iloc[8,0]}')
        sample_check_value = df_check.iloc[8,0]
        if 'PO NUMBER' not in  str(po_number_check_value).upper():
            message = 'File Format Incorrect. PO NUMBER not found in the first column of the file.'
            logging.error(message)
            log += message + br
            status = 'failed'
            return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            sample_count,
                            work_order_status,
                            client_ref,
                            samples_submitted,
                            date_received,
                            date_finalized,
                            project,
                            comments,
                            po_number,
                            logging
                        )
        # Check for Sample
        if 'SAMPLE' not in str(sample_check_value).upper():
            message = 'File Format Incorrect. SAMPLE not found in the first column of the file.'
            logging.error(message)
            log += message + br
            status = 'failed'
            return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            sample_count,
                            work_order_status,
                            client_ref,
                            samples_submitted,
                            date_received,
                            date_finalized,
                            project,
                            comments,
                            po_number,
                            logging
                        )
        logging.info('File Format Check Successful')
        # clear df_check from memory
        del df_check

        ### Get access to sql connection ###
        sql_conn_string, log_sql_conn = utils.get_sql_connection(vault_id, logging)
        if sql_conn_string is None:
            log = 'SQL Connection string is not present'
            log += log_sql_conn + br
            return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            sample_count,
                            work_order_status,
                            client_ref,
                            samples_submitted,
                            date_received,
                            date_finalized,
                            project,
                            comments,
                            po_number,
                            logging
                        )
        logging.info('Get access to sql connection successful')

        ###! STARTING RESHAPE AND INSERT !###
        if df_workbook : 
            ## Connect to the database ##
            cnxn, log_sql_opendb = sql.open_database(sql_conn_string, logging)
            if cnxn is None:
                log = 'SQL Connection is not present.'
                log += log_sql_opendb + br
                return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            sample_count,
                            work_order_status,
                            client_ref,
                            samples_submitted,
                            date_received,
                            date_finalized,
                            project,
                            comments,
                            po_number,
                            logging
                        )
            logging.info('Open database successful')

        try:
            # Clean Results and Header infromation from Excel File
            df_headers = utils.clean_lab_header(df_workbook)
            logging.info('Cleaned headers')
            df_results = utils.clean_lab_results(df_workbook)
            logging.info('Cleaned results')

            # Join header on to results based on jobtitle 
            df = pd.merge(df_results, df_headers, on='job_title', how='left')
            logging.info('Merged headers and results')
            df['source_name'] = path.split('/')[-1]
            df['laboratory'] = 'ALS Arabia'
            # Add the current date and time to a column in the DataFrame
            df['srk_import_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')           

            # Store Header information in variables
            work_order_status = str(df_headers['job_title'].iloc[0])
            client_ref = str(df_headers['client_ref'].iloc[0])
            samples_submitted = str(df_headers['quantity'].iloc[0])
            date_received = str(df_headers['date_received'].iloc[0])
            date_finalized = str(df_headers['date_finalized'].iloc[0])
            project = str(df_headers['project'].iloc[0])
            comments = str(df_headers['cert_comment'].iloc[0])
            po_number = str(df_headers['po_number'].iloc[0])
            logging.info('Obtained header info')
            logging.info(f'Records to be inserted from file: {len(df)}')
        except:
            message = f'Error Cleaning Files before insertion.'
            logging.error(message)
            log += message + br

        try:
            # left is DF right is DB
            column_mappings = {
                'source_name': 'source_name',
                'sample_id': 'sample_id', 
                'lab_method': 'lab_method',
                'analyte': 'analyte', 
                'unit': 'unit', 
                'text_value': 'text_value',
                'qualifier': 'qualifier', 
                'value': 'value', 
                'job_title': 'job_title', 
                'client_ref': 'client_ref', 
                'quantity': 'quantity', 
                'project': 'project', 
                'cert_comment': 'cert_comment',
                'po_number':'po_number',
                'job_number':'job_number',
                'result_status':'result_status',
                'date_received':'date_received',
                'date_finalized':'date_finalised',
                'laboratory':'laboratory',
                'srk_import_timestamp':'srk_import_timestamp'
            }
            match_conditions = {
                'sample_id': 'sample_id', 
                'lab_method': 'lab_method',
                'analyte': 'analyte', 
            }
            table = 'assay_result'
            logging.info('Attempting to insert data into SQL')
            result = sql.db_insert_batch(cnxn, df, table, column_mappings, match_conditions, logging, 1000)
            logging.info(result)
            sample_count = result['distinct_count']
            inserted_count = result['inserted_count']
            message = f'No Errors inserting data. {inserted_count} records inserted.'
            logging.info(message)
            log = message
        except:
            message = f'Error inserting data. No data was inserted.'
            logging.error(message)
            log += message + br

        status = 'success'
        return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            sample_count,
                            work_order_status,
                            client_ref,
                            samples_submitted,
                            date_received,
                            date_finalized,
                            project,
                            comments,
                            po_number,
                            logging
                        )
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )