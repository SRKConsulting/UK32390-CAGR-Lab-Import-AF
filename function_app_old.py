import azure.functions as func
import logging
import utils
import sql
import pandas as pd
import variables as var
from datetime import datetime

br = '<br>'

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="ac")
def ac(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('AC trigger function processed a request.')

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
        log_status = parsed_path['status']
        hole_type = parsed_path['hole_type']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        # Initiate lists of successful and failed sheets
        success_sheets = []
        failed_sheets = ['Collar', 'Lithology', 'Alteration', 'Mineralisation', 'Samples'] # all are failed by default

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

            ## Collar Sheet ##
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Collar'
                table = 'collar'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                #df['Hole_Type'] = 'AC' # this is already in Collar sheet
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]
                
                column_mappings = {
                    'project': 'Project',
                    'hole_id': 'Hole_ID',
                    'prospect': 'Prospect',
                    'el_block': 'EL_Block',
                    'date_start': 'Date_Start',
                    'date_completed': 'Date_Completed',
                    'hole_type': 'Hole_Type',
                    'max_depth': 'Max_Depth',
                    'collar_dip': 'Collar_Dip',
                    'collar_azimuth': 'Collar_Azimuth',
                    'program_purpose': 'Program_Purpose',
                    'comments': 'Comments',
                    'completed_by': 'Completed_by',
                    'surveyed_by': 'surveyed_by',
                    'grid_id': 'Grid_ID',
                    'x': 'X',
                    'y': 'Y',
                    'z': 'Z',
                    'survey_method': 'Survey_Method',
                    'survey_type': 'Survey_Type',
                    'import_timestamp': 'srkImport_Timestamp',
                    'logging_status': 'srkLogging_Status',
                    'import_file': 'srkImport_File'
                }

                result = sql.db_replace(cnxn, df, table, column_mappings, logging)

                logging.info(result)

                sheet_status = result['status']
                deleted_count = result['deleted_count']
                inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n \n'
                logging.error(message)
                log += message + br
            '''
            ## Lithology Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Lithology'
                table = 'lithology'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'AC' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]
                
                column_mappings = {
                    'hole_id': 'Hole_ID',
                    'depth_from': 'Depth_From', 
                    'depth_to': 'Depth_To',
                    'lith1_code': 'Lith1_Code', 
                    'regolith_code': 'Regolith_Code', 
                    'lith1_colour': 'Lith1_Colour',
                    'weathering': 'Weathering', 
                    'logged_by': 'Logged_by', 
                    'comments': 'Comments', 
                    'import_timestamp': 'srkImport_Timestamp', 
                    'hole_type': 'srkHole_Type', 
                    'logging_status': 'srkLogging_Status', 
                    'import_file': 'srkImport_File'
                }

                result = sql.db_replace(cnxn, df, table, column_mappings, logging)

                logging.info(result)

                sheet_status = result['status']
                deleted_count = result['deleted_count']
                inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br
            
            ## Alteration Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Alteration'
                table = 'alteration'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'AC' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)
                
                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'hole_id': 'Hole_ID',
                    'depth_from': 'Depth_From',
                    'depth_to': 'Depth_To',
                    'logged_by': 'Logged_By',
                    'comments': 'Comments',
                    'alt1_type': 'Alt1_Type',
                    'alt1_style': 'Alt1_Style',
                    'alt1_intensity': 'Alt1_intensity',
                    'alt2_type': 'Alt2_Type',
                    'alt2_style': 'Alt2_Style',
                    'alt2_intensity': 'Alt2_Intensity',
                    'alt3_type': 'Alt3_Type',
                    'alt3_style': 'Alt3_Style',
                    'alt3_intensity': 'Alt3_Intensity',
                    'import_timestamp': 'srkImport_Timestamp', 
                    'logging_status': 'srkLogging_Status', 
                    'hole_type': 'srkHole_Type', 
                    'import_file': 'srkImport_File'
                }

                result = sql.db_replace(cnxn, df, table, column_mappings, logging)

                logging.info(result)

                sheet_status = result['status']
                deleted_count = result['deleted_count']
                inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Mineralisation Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Mineralisation'
                table = 'mineralisation'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'AC' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]
                
                column_mappings = {
                    'hole_id': 'Hole_ID', 
                    'depth_from': 'Depth_From',
                    'depth_to': 'Depth_To',
                    'logged_by': 'Logged_By',
                    'min1_type': 'Min1_Type',
                    'min1_style': 'Min1_Style',
                    'min1_pct': 'Min1_Pct',
                    'min2_type': 'Min2_Type',
                    'min2_style': 'Min2_Style',
                    'min2_pct': 'Min2_Pct',
                    'min3_type': 'Min3_Type',
                    'min3_style': 'Min3_Style',
                    'min3_pct': 'Min3_Pct',
                    'min4_type': 'Min4_Type',
                    'min4_style': 'Min4_Style',
                    'min3_pct4': 'Min3_Pct4',
                    'comments': 'Comments',
                    'import_timestamp': 'srkImport_Timestamp', 
                    'logging_status': 'srkLogging_Status', 
                    'hole_type': 'srkHole_Type', 
                    'import_file': 'srkImport_File'
                }

                result = sql.db_replace(cnxn, df, table, column_mappings, logging)

                logging.info(result)

                sheet_status = result['status']
                deleted_count = result['deleted_count']
                inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Samples Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Samples'
                table = 'samples'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'AC' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'hole_id': 'Hole_ID',
                    'depth_from': 'Depth_From',
                    'depth_to': 'Depth_To',
                    'sampled_by': 'Sampled_By',
                    'sampleid': 'SampleID',
                    'sample_type': 'Sample_Type',
                    'standard_code': 'Standard_Code',
                    'sample_moisture': 'Sample_Moisture',
                    'sample_method': 'Sample_Method',
                    'total sample\nweight (kg)': 'Total_Sample_Weight_kg',
                    'analysis sample weight (kg)': 'Analysis_Sample_Weight_kg',
                    'parentid': 'ParentID',
                    'comments': 'Comments',
                    'reference sample\nweight (kg)': 'Reference_Sample_Weight_kg',
                    'import_timestamp': 'srkImport_Timestamp', 
                    'logging_status': 'srkLogging_Status', 
                    'hole_type': 'srkHole_Type', 
                    'import_file': 'srkImport_File'
                }

                result = sql.db_replace(cnxn, df, table, column_mappings, logging)

                logging.info(result)

                sheet_status = result['status']
                deleted_count = result['deleted_count']
                inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            if len(failed_sheets) > 0:
                status = 'failed'
            else: 
                status = 'success'
            
            return utils.create_response(
                    filename, 
                    status, 
                    log, 
                    "low", 
                    success_sheets, 
                    failed_sheets,
                    logging
                )
        else : # no workbook found
            log += 'No workbook found'
            return utils.create_response(
                    filename, 
                    status, 
                    log, 
                    "high", 
                    success_sheets, 
                    failed_sheets,
                    logging
            )
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    

@app.route(route="dd")
def dd(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('DD trigger function processed a request.')

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
        log_status = parsed_path['status']
        hole_type = parsed_path['hole_type']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        # Initiate lists of successful and failed sheets
        success_sheets = []
        failed_sheets = [
            'Collar', 'Collar_Survey', 'Lithology', 
            'Alteration', 'Mineralisation', 'Oxidation', 
            'Weathering', 'Geotech', 'Structures', 
            'Samples'
        ] # all are failed by default

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

            ## Collar Sheet ##
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Collar'
                table = 'collar'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                #df['Hole_Type'] = 'DD' # this is already in Collar sheet
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Project': 'Project',
                    'Hole_ID': 'Hole_ID',
                    'Prospect': 'Prospect',
                    'EL_Block': 'EL_Block',
                    'Date_Start': 'Date_Start',
                    'Date_Completed': 'Date_Completed',
                    'Hole_Type': 'Hole_Type',
                    'Max_Depth': 'Max_Depth',
                    'Collar_Dip': 'Collar_Dip',
                    'Collar_Azimuth': 'Collar_Azimuth',
                    'Program_Purpose': 'Program_Purpose',
                    'Comments': 'Comments',
                    'Completed_by': 'Completed_by',
                    'Import_Timestamp': 'srkImport_Timestamp',
                    'Logging_Status': 'srkLogging_Status',
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}
                
                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)

                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br
            
            ## Collar_Survey Sheet ##
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Collar_Survey'
                table = 'collar_survey'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]
                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD'
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID',
                    'Survey_Type': 'Survey_Type',
                    'Grid_ID': 'Grid_ID',
                    'X': 'X',
                    'Y': 'Y',
                    'Z': 'Z',
                    'Survey_Method': 'Survey_Method',
                    'Surveyed_Date': 'Surveyed_Date',
                    'Surveyed_By': 'Survey_By',
                    'Comments': 'Comments',
                    'Hole_Type': 'srkHole_Type',
                    'Import_Timestamp': 'srkImport_Timestamp',
                    'Logging_Status': 'srkLogging_Status',
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}


                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Lithology Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Lithology'
                table = 'lithology'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID',
                    'Depth_From': 'Depth_From', 
                    'Depth_To': 'Depth_To',
                    'Logged_by': 'Logged_By',
                    'Lith1_Code': 'Lith1_Code',
                    'Lith1_Colour': 'Lith1_Colour',
                    'Lith1_GrainSize': 'Lith1_GrainSize',
                    'Lith1_Texture': 'Lith1_Texture',
                    'Lith1_Pct': 'Lith1_Pct',
                    'Lith1_Contact': 'Lith1_Contact',
                    'Lith1_ContactAngle': 'Lith1_ContactAngle',
                    'Lith2_Texture': 'Lith2_Texture',
                    'Lith2_Code': 'Lith2_Code',
                    'Lith2_Pct': 'Lith2_Pct',
                    'REMARKS': 'REMARKS',
                    'WEATHERING': 'WEATHERING',
                    'HARDNESS': 'HARDNESS',
                    'QTZ_PERC': 'QTZ_PERC',
                    'Oxidation_type': 'Oxidation_type',
                    'MOISTURE': 'MOISTURE',
                    'Comments': 'Comments',
                    'Hole_Type': 'srkHole_Type',
                    'Logging_Status': 'srkLogging_Status',
                    'Import_Timestamp': 'srkImport_Timestamp',
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br
            
            ## Alteration Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Alteration'
                table = 'alteration'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID',
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'Logged_By': 'Logged_By',
                    'Comments': 'Comments',
                    'Alt1_Type': 'Alt1_Type',
                    'Alt1_Style': 'Alt1_Style',
                    'Alt1_intensity': 'Alt1_intensity',
                    'Alt2_Type': 'Alt2_Type',
                    'Alt2_Style': 'Alt2_Style',
                    'Alt2_Intensity': 'Alt2_Intensity',
                    'Alt3_Type': 'Alt3_Type',
                    'Alt3_Style': 'Alt3_Style',
                    'Alt3_Intensity': 'Alt3_Intensity',
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Mineralisation Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Mineralisation'
                table = 'mineralisation'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID', 
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'Logged_by': 'Logged_By',
                    'Min1_Type': 'Min1_Type',
                    'Min1_Style': 'Min1_Style',
                    'Min1_Pct': 'Min1_Pct',
                    'Min2_Type': 'Min2_Type',
                    'Min2_Style': 'Min2_Style',
                    'Min2_Pct': 'Min2_Pct',
                    'Min3_Type': 'Min3_Type',
                    'Min3_Style': 'Min3_Style',
                    'Min3_Pct': 'Min3_Pct',
                    'Min4_Type': 'Min4_Type',
                    'Min4_Style': 'Min4_Style',
                    'Min3_Pct4': 'Min3_Pct4',
                    'Comments': 'Comments',
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Oxidation Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Oxidation'
                table = 'oxidation'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID', 
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'Logged_by': 'Logged_By',
                    'Oxidation': 'Oxidation',
                    'Weathering_Style': 'Weathering_Style',
                    'Oxide_Pct': 'Oxide_Pct', 
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Weathering Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Weathering'
                table = 'weathering'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID', 
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'Weathering': 'WEATHERING', 
                    'Weathering_Pct': 'Weathering_pct',
                    'Comments': 'Comments',
                    'Logged_by': 'Logged_By',
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Geotech Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Geotech'
                table = 'geotech'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD'
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID', 
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'Lost_Core': 'Lost_core',
                    'Interval_Length': 'Interval_Length',
                    'Recovery_Length': 'Recovery_Length', 
                    'Recovery_Percent': 'Recovery_Percent', 
                    'RQD_Length': 'RQD_Length',
                    'RQD_Percent': 'RQD_Percent',
                    'Test-Code': 'Test_Code',
                    'Logged_by': 'Logged_By',
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Structures Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Structures'
                table = 'structures'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD'
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID', 
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'STR_Type': 'STR_Type',
                    'CORE_angle': 'CORE_angle',
                    'alpha_angle': 'alpha_angle',
                    'beta_angle': 'beta_angle',
                    'Comments': 'Comments',
                    'Logged_by': 'Logged_By',
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br

            ## Samples Sheet ##
            
            try :
                # Read sheet into dataframe & insert into SQL
                sheet = 'Samples'
                table = 'samples'

                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Hole_ID'].notna()]

                df['Import_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = unique_path
                df['Hole_Type'] = 'DD' 
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                # Convert all column names to lower case
                df.columns = [col.lower() for col in df.columns]

                column_mappings = {
                    'Hole_ID': 'Hole_ID',
                    'Depth_From': 'Depth_From',
                    'Depth_To': 'Depth_To',
                    'Sampled_by': 'Sampled_By',
                    'SampleID': 'SampleID',
                    'Sample_Type': 'Sample_Type',
                    'Sample_Code': 'Sample_Code',
                    'ParentID': 'ParentID',
                    'Comments': 'Comments',
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Hole_Type': 'srkHole_Type', 
                    'Import_File': 'srkImport_File'
                }
                column_mappings = {k.lower(): v for k, v in column_mappings.items()}

                # call sql replace function only if the df is not empty 
                if len(df) == 0 :
                    sheet_status = 'success'
                    deleted_count = 0
                    inserted_count = 0
                else :
                    result = sql.db_replace(cnxn, df, table, column_mappings, logging)
                    logging.info(result)
                    sheet_status = result['status']
                    deleted_count = result['deleted_count']
                    inserted_count = result['inserted_count']
                
                if sheet_status == 'success' :
                    success_sheets.append(sheet)
                    log += f'{table}\t\t\{inserted_count}\t{deleted_count}\n'
                    if sheet in failed_sheets:
                        failed_sheets.remove(sheet)
                else :
                    log += f'{sheet_status}'
                    raise Exception(sheet_status)
                
                logging.info(log)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br
            '''
            if len(failed_sheets) > 0:
                status = 'failed'
            else: 
                status = 'success'
            
            return utils.create_response(
                    filename, 
                    status, 
                    log, 
                    "low", 
                    success_sheets, 
                    failed_sheets,
                    logging
                )
        else : # no workbook found
            log += 'No workbook found'
            return utils.create_response(
                    filename, 
                    status, 
                    log, 
                    "high", 
                    success_sheets, 
                    failed_sheets,
                    logging
            )
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


@app.route(route="soil")
def soil(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger SOIL function processed a request.')

    # get variables from http request
    filename = req.params.get('filename')
    container = req.params.get('container')
    vault_id = req.params.get('keyvault')
    log_status = req.params.get('logstatus')
    success_sheets = []
    failed_sheets = ['Soil']

    br = '<br>'

    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            filename = req_body.get('filename')
            container = req_body.get('container')
            vault_id = req_body.get('keyvault')
            log_status = req_body.get('logstatus')
    
    logging.info(
        f"""Request Parameters: 
        container | {container}; 
        filename | {filename}; 
        keyvault | https://{vault_id}.vault.azure.net/"""
    )

    if filename:
        log = ''
        # get file contents
        df_workbook, log_fetch = utils.fetch_file_contents(vault_id, container, filename, logging)
        if df_workbook is None:
            log += log_fetch + br
            status = 'failed'
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
            status = 'failed'
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
                status = 'failed'
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

            ## Soil Sheet ##
            sheet = 'Soil'
            
            # Read sheet into dataframe & insert into SQL
            try:
                df = pd.read_excel(df_workbook, sheet, header=0)
                df = df[df['Sample Type'].notna()]
                logging.info(f'df size:{len(df)}')
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                df['Import_Timestamp'] = now
                df['Update_Timestamp'] = now
                df['Logging_Status'] = log_status
                df['Import_File'] = filename

                df = df.rename(columns={
                    'SampleID': 'Sample_ID',
                    'UTM Zone': 'UTM_Zone',
                    'UTM X': 'UTM_X',
                    'UTM Y': 'UTM_Y',
                    'Sample Date Time': 'Sample_Date_Time',
                    'Sample Method': 'Sample_Method',
                    'Sample Type': 'Sample_Type', 
                    'Parent SampleID': 'Parent_Sample_ID',
                    'Sieve Size (mm)': 'Sieve_Size_mm',
                    'Sample Depth (cm)': 'Sample_Depth_cm',
                    'Sample Weight (kg)': 'Sample_Weight_kg', 
                    'Regolith Type': 'Regolith_Type', 
                    'Clast Lithology1': 'Clast_Lithology1',
                    'Clast Lithology2': 'Clast_Lithology2', 
                    'Grain Size Dominant': 'Grain_Size_Dominant',
                    'Grain Roundness': 'Grain_Roundness', 
                    'Grain Sorting': 'Grain_Sorting',
                    'Sample Photo': 'Sample_Photo', 
                    'Sample Weight (kg)': 'Sample_Weight_kg', 
                    'Sample Comments': 'Sample_Comments'
                })
                
                # Fill NaN values with empty string and convert all columns to string
                df = df.fillna('')
                for column in df.columns:
                    df[column] = df[column].astype(str)

                column_mappings = {
                    'Sample_ID': 'Sample_ID',
                    'UTM_Zone': 'UTM_Zone', 
                    'UTM_X': 'UTM_X', 
                    'UTM_Y': 'UTM_Y', 
                    'Z': 'Z', 
                    'Sample_Date_Time': 'Sample_Date_Time', 
                    'Sampler': 'Sampler', 
                    'Sample_Method': 'Sample_Method', 
                    'Sample_Type': 'Sample_Type', 
                    'Parent_Sample_ID': 'Parent_Sample_ID', 
                    'Sieve_Size_mm': 'Sieve_Size_mm', 
                    'Sample_Depth_cm': 'Sample_Depth_cm', 
                    'Moisture': 'Moisture', 
                    'Regolith_Type': 'Regolith_Type',  
                    'Clast_Lithology1': 'Clast_Lithology1', 
                    'Clast_Lithology2': 'Clast_Lithology2', 
                    'Mineralisation': 'Mineralisation', 
                    'Alteration': 'Alteration', 
                    'Colour': 'Colour', 
                    'Grain_Size_Dominant': 'Grain_Size_Dominant', 
                    'Grain_Roundness': 'Grain_Roundness', 
                    'Grain_Sorting': 'Grain_Sorting', 
                    'Slope': 'Slope', 
                    'Contamination': 'Contamination', 
                    'Sample_Photo': 'Sample_Photo', 
                    'Sample_Weight_kg': 'Sample_Weight_kg', 
                    'Sample_Comments': 'Sample_Comments', 
                    'Import_Timestamp': 'srkImport_Timestamp', 
                    'Logging_Status': 'srkLogging_Status', 
                    'Import_File': 'srkImport_File', 
                    'Update_Timestamp': 'srkUpdate_Timestamp'
                }
                match_conditions = {
                    'Sample_ID': 'Sample_ID'
                }
                table = 'soil'
                result = sql.db_merge_batch(cnxn, df, table, column_mappings, match_conditions, logging)
                logging.info(result)
                status = result['status']
                updated_count = result['updated_count']
                inserted_count = result['inserted_count']
                log += f'''Status: {status}.\nTable\t\t Inserted\t Updated \nSOIL\t\t\ {inserted_count}\t {updated_count}\n'''
                logging.info(log)
                if status != 'success' :
                    raise Exception(status)
                
                success_sheets.append(sheet)
                if sheet in failed_sheets:
                    failed_sheets.remove(sheet)

            except Exception as e:
                message = f'Error reading {sheet} sheet or inserting data: {e}. No data was inserted. \n '
                logging.error(message)
                log += message + br
                status = 'failed'

        return utils.create_response(
                    filename, 
                    status, 
                    log, 
                    "high", 
                    success_sheets, 
                    failed_sheets,
                    logging
        )
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
