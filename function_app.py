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

        # # Checking for .XLS filetype.
        # if not filename.endswith('.XLS'):
        #     raise Exception('Error: Filetype not .XLS')

        #? all are blank by default
        inserted_count = ''
        sample_count = ''
        updated_count = '',
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
                            updated_count,
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
                            updated_count,
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
                            updated_count,
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
            logging.info('Added source name')
            df['laboratory'] = 'ALS Arabia'
            logging.info('Added laboratory')
            # Add the current date and time to a column in the DataFrame
            df['srk_import_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')           
            logging.info('Added timestamp')

            # Store Header information in variables
            #df_headers = utils.df_headers(df_workbook)
            work_order_status = str(df_headers['job_title'].iloc[0])
            client_ref = str(df_headers['client_ref'].iloc[0])
            samples_submitted = str(df_headers['quantity'].iloc[0])
            date_received = str(df_headers['date_received'].iloc[0])
            date_finalized = str(df_headers['date_finalized'].iloc[0])
            project = str(df_headers['project'].iloc[0])
            comments = str(df_headers['cert_comment'].iloc[0])
            po_number = str(df_headers['po_number'].iloc[0])
            logging.info('Obtained header info')

        except:
            message = f'Error Cleaning Files before insertion. \n '
            logging.error(message)
            log += message + br

        try:
            # existing_sql_records = sql.get_pk_records(cnxn)
            # df = utils.filter_new_records(df, existing_sql_records)
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
            table = 'assay_result_testing'
            # result = sql.db_insert(cnxn, df, table, column_mappings, logging)
            # logging.info(result)
            result = sql.db_merge_batch(cnxn, df, table, column_mappings, match_conditions, logging, 1000)
            # Extract counts inserted from SQL injection
            logging.info(result)
            sample_count = result['distinct_count']
            inserted_count = result['inserted_count']
            updated_count = result['updated_count']
            message = f'All lab results processed. \n'
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
                            updated_count,
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