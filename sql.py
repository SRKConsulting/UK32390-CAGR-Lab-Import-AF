"""
All functions related to SQL Server commands  
"""
import pyodbc
from azure.identity import DefaultAzureCredential
import pandas as pd

def open_database(conn_string, logger, autocommit=False):
    """ Connect to the database """
    # connect without auto commit
    cnxn = None
    try:
        cnxn = pyodbc.connect(conn_string)
        return cnxn, ''
    except Exception as e:
        error = 'Could not establish connection: ' + str(e)
        logger.error(error)
        return None, error

def db_merge_batch(cnxn: pyodbc.Connection, df: pd.DataFrame, table: str, column_mappings: dict, match_conditions: dict, logger, batch_size=5000):
    """
    Merges records into a table using batch processing.

    Parameters:
    - cnxn: Database connection object.
    - df: DataFrame containing the data to be merged.
    - table: db table
    - column_mappings: Dictionary mapping DataFrame columns to table columns.
    - match_conditions: Dictionary mapping target columns to source columns for the ON clause.
    - batch_size: Number of rows to process in each batch.

    Returns:
    - A dictionary with counts of updated and inserted records, and a success/failure status.
    """
    try:
        cursor = cnxn.cursor()
        updated_count = 0
        inserted_count = 0

        # Retrieve the column definitions from the main table
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """)
        column_definitions = cursor.fetchall()

        # Construct the CREATE TABLE statement for the temp table
        temp_table_columns = ', '.join([
            f"{col[0]} {col[1]}" + 
            (f"({col[2]})" if col[2] and col[2] != -1 else "(max)" if col[1] in ['nvarchar', 'varchar', 'varbinary'] else "") + 
            (" NULL" if col[3] == 'YES' else " NOT NULL")
            for col in column_definitions
        ])

        # Process data in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]

            # Create a temp table for this batch
            cursor.execute(f"CREATE TABLE #TempBatch ({temp_table_columns})")
            logger.info(f"Executed CREATE TABLE #TempBatch ({temp_table_columns})")

            # Insert batch data into temp table
            params = []
            for _, row in batch.iterrows():
                params.append(tuple(row[col] for col in column_mappings.keys()))

            insert_placeholders = ', '.join(['?' for _ in column_mappings])
            logger.info(f"Executing INSERT INTO #TempBatch ({', '.join(column_mappings.values())}) VALUES ({insert_placeholders})...")
            cursor.executemany(
                f"INSERT INTO #TempBatch ({', '.join(column_mappings.values())}) VALUES ({insert_placeholders})",
                params
            )
            
            # Prepare the SQL query with dynamic columns
            update_columns = ', '.join([
                f"target.{col} = source.{col}" for col in column_mappings.values() if col != 'srkImport_Timestamp'
            ])
            insert_columns = ', '.join(column_mappings.values())
            insert_values = ', '.join([f"source.{col}" for col in column_mappings.values()])
            match_clause = ' AND '.join([f"target.{target_col} = source.{source_col}" for target_col, source_col in match_conditions.items()])

            # Perform MERGE operation with OUTPUT clause
            sql_query = f"""
                MERGE INTO {table} AS target
                USING #TempBatch AS source
                ON {match_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
                OUTPUT $action;
            """
            cursor.execute(sql_query)

            # Get the result of the OUTPUT clause
            for action in cursor.fetchall():
                if action[0] == 'UPDATE':
                    updated_count += 1
                elif action[0] == 'INSERT':
                    inserted_count += 1

            # Drop the temporary table
            cursor.execute("DROP TABLE #TempBatch")

            cnxn.commit()
            print(f"Processed batch {i//batch_size + 1}, rows {i+1} to {min(i+batch_size, len(df))}")

        cursor.close()
        return {
            'updated_count': updated_count,
            'inserted_count': inserted_count,
            'status': 'success'
        }
    except Exception as e:
        return {
            'updated_count': updated_count,
            'inserted_count': inserted_count,
            'status': f'failure: {str(e)}'
        }
    
def db_merge(cnxn: pyodbc.Connection, df: pd.DataFrame, table: str, column_mappings: dict, match_conditions: dict):
    """
    ! This function has been replaced by the much faster db_merge_batch() !

    Merges records into table.

    Parameters:
    - cnxn: Database connection object.
    - df: DataFrame containing the data to be merged.
    - table: db table
    - column_mappings: Dictionary mapping DataFrame columns to table columns.
    - match_conditions: Dictionary mapping target columns to source columns for the ON clause.

    Returns:
    - A dictionary with counts of updated and inserted records, 
      and a success/failure status.
    """
    try:
        cursor = cnxn.cursor()
        updated_count = 0
        inserted_count = 0

        for index, row in df.iterrows():
            # Prepare the SQL query with dynamic columns
            source_columns = ', '.join([f"? AS {col}" for col in column_mappings.values()])
            
            # Exclude Import_Timestamp from update_columns
            update_columns = ', '.join([
                f"{col} = source.{col}" for col in column_mappings.values() if col != 'srkImport_Timestamp'
            ])
            
            insert_columns = ', '.join(column_mappings.values())
            insert_values = ', '.join([f"source.{col}" for col in column_mappings.values()])
            match_clause = ' AND '.join([f"target.{target_col} = source.{source_col}" for target_col, source_col in match_conditions.items()])

            sql_query = f"""
                MERGE INTO {table} AS target
                USING (SELECT {source_columns}) AS source
                ON {match_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
                OUTPUT $action;
            """

            # Execute the query
            cursor.execute(sql_query, tuple(row[col] for col in column_mappings.keys()))

            # Get the result of the OUTPUT clause
            action = cursor.fetchone()[0]
            if action == 'UPDATE':
                updated_count += 1
            elif action == 'INSERT':
                inserted_count += 1

        cnxn.commit()
        cursor.close()
        return {
            'updated_count': updated_count,
            'inserted_count': inserted_count,
            'status': 'success'
        }
    except Exception as e:
        return {
            'updated_count': updated_count,
            'inserted_count': inserted_count,
            'status': f'failure: {str(e)}'
        }
    
def db_replace(cnxn: pyodbc.Connection, df: pd.DataFrame, table: str, column_mappings: dict, logger):
    """
    Replaces records in the table based on the Import_File column.

    Parameters:
    - cnxn: Database connection object.
    - df: DataFrame containing the data to be inserted.
    - table: Database table.
    - column_mappings: Dictionary mapping DataFrame columns to table columns.

    Returns:
    - A dictionary with counts of deleted and inserted records,
      and a success/failure status.
    """
    try:
        cursor = cnxn.cursor()
        
        # Assume there is only one unique Import_File value in the DataFrame
        import_file_value = df['source_name'].iloc[0]

        # Step 1: Delete matching records
        delete_query = f"DELETE FROM {table} WHERE {column_mappings['source_name']} = ?"
        cursor.execute(delete_query, import_file_value)
        deleted_count = cursor.rowcount
        logger.info(f"Executed: DELETE FROM {table} WHERE {column_mappings['source_name']} = '{import_file_value}'")

        # Step 2: Insert all records from the DataFrame
        insert_columns = ', '.join(column_mappings.values())
        insert_placeholders = ', '.join(['?'] * len(column_mappings))
        insert_query = f"INSERT INTO {table} ({insert_columns}) VALUES ({insert_placeholders})"
        inserted_count = 0
        for index, row in df.iterrows():
            cursor.execute(insert_query, tuple(row[col] for col in column_mappings.keys()))
            inserted_count += 1

        cnxn.commit()
        cursor.close()
        return {
            'deleted_count': deleted_count,
            'inserted_count': inserted_count,
            'status': 'success'
        }
    except Exception as e:
        return {
            'deleted_count': 0,
            'inserted_count': 0,
            'status': f'failure: {str(e)}'
        }