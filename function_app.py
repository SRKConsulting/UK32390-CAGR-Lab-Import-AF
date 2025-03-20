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
        status = 'failed'
        # parse the path to get filename, completion status, and project name
        parsed_path =  utils.parse_path(path)
        if not parsed_path :
            raise Exception('Error: Could not parse the file path. ')
        
        filename = parsed_path['filename']
        unique_path = parsed_path['unique_path']
        logging.info("This is unique path: "+str(unique_path))
        # Initiate lists of successful and failed sheets
        success_sheets = []
        failed_sheets = ['_'] # all are failed by default
        log = ''
        # get file contents
        df_workbook, log_fetch = utils.fetch_file_contents(vault_id, container, filename, logging)
        if df_workbook is None:
            log += log_fetch + br
            status = status
            return utils.create_response(
                filename, 
                status, 
                log, 
                "high", 
                success_sheets, 
                failed_sheets,
                logging
            )
        logging.info('Fetch file contents successful')

        ### Get access to sql connection ###
        sql_conn_string, log_sql_conn = utils.get_sql_connection(vault_id, logging)
        if sql_conn_string is None:
            log += log_sql_conn + br
            return utils.create_response(
                filename, 
                status, 
                log, 
                "high", 
                success_sheets, 
                failed_sheets,
                logging
            )
        logging.info('Get access to sql connection successful')

        ### STARTING RESHAPE AND INSERT ###
        if df_workbook : 
        ## Connect to the database ##
            cnxn, log_sql_opendb = sql.open_database(sql_conn_string, logging)
            if cnxn is None:
                log += log_sql_opendb + br
                return utils.create_response(
                    filename, 
                    status, 
                    log, 
                    "high", 
                    success_sheets, 
                    failed_sheets,
                    logging
                )
            logging.info('Open database successful')

        try:
            df_headers = utils.clean_lab_header(df_workbook)
            logging.info('Cleaned headers')
            df_results = utils.clean_lab_results(df_workbook)
            logging.info('Cleaned results')

            # join header on to results based on jobtitle 
            df = pd.merge(df_results, df_headers, on='job_title', how='left')
            logging.info('Merged headers and results')
            df['source_name'] = path.split('/')[-1]
            logging.info('Added source name')

            # Fill NaN values with empty string and convert all columns to string
            df = df.fillna('')
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
                'date_finalized':'date_finalised'
            }
            table = 'assay_result'
            result = sql.db_replace(cnxn, df, table, column_mappings, logging)
            logging.info(result)

            #sheet_status = result['status']
            deleted_count = result['deleted_count']
            inserted_count = result['inserted_count']    
            logging.info(log)

        except:
            message = f'Error inserting data. No data was inserted. \n '
            logging.error(message)
            log += message + br
            
        status = 'success'
        return utils.create_response(
                            filename, 
                            status, 
                            log, 
                            "low", 
                            inserted_count,
                            deleted_count,
                            logging
                        )
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )