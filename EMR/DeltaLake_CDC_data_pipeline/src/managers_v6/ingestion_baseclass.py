import traceback
from datetime import datetime
from functools import reduce

from pyspark.sql.functions import col, when, collect_list
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from managers_v6.arguments_manager import ArgumentsManager
from managers_v6.columns_manager import ColumnsManager
from managers_v6.console_manager import write_log
from managers_v6.s3_manager import S3Manager, Mode
from managers_v6.rc_custom_query_manager import RCQueryManager
from managers_v6.cdc_manager import CdcManager

import boto3

    
# ----------------------------------------
# Base class for Ingestion.
# ----------------------------------------
class Ingestion_BaseClass:
    # ----------------------------------------
    # Constructor – Initializes the spark object and sets “load_time” variables
    #   mode - InitialLoad or CDC
    # ----------------------------------------
    def __init__(self, mode = Mode.InitialLoad,tableOptions = None):
        write_log(__file__, "__init__()", 'info', f'Ingestion_BaseClass::__init__()')
        self.mode = mode
        
        # Initial Managers
        self.s3_manager = S3Manager(mode, tableOptions)
        self.columns_manager = ColumnsManager()
        
        # Initialize variables
        self.__initialize_variables()
        
    #*********************************************************************************************************
    # *** PRIVATE METHODS ***
    #*********************************************************************************************************
    # ----------------------------------------
    # Initialize variables
    # ----------------------------------------
    def __initialize_variables(self):
        self.start_time = datetime.now()
        self.load_time = datetime.now().strftime('%Y%m%d%H%M%S')
        write_log(__file__, "__initialize_variables()", 'info', f'start_time: {self.start_time.strftime("%H:%M:%S")}')

    # ----------------------------------------
    # Create raw and raw current for daily full load by using hash column
    #   ret_df – Landing Dataframe
    #   prev_df - The previous days rc dataframe
    # ----------------------------------------
    def __get_raw_raw_current(self, ret_df, prev_df, tableOptions):
        write_log(__file__, "__get_raw_raw_current()", 'info', f'Create raw and raw current for daily full load')
        cdc_manager = CdcManager(self.mode, tableOptions)
        prev_df = prev_df.drop(*['ROWID', 'pk'])
        columns = ret_df.columns
        pk_ls = tableOptions["raw_current_pk_columns"]
        try:
            # to get updates and inserts
            cdc_landing_only = ret_df.join(prev_df, ['sha_hash'], 'leftanti')
            cdc_landing_only = cdc_landing_only.drop("operation")
            cdc_df_ui = cdc_landing_only.join(prev_df, pk_ls, "left").select(cdc_landing_only['*'],
                            when(prev_df.sha_hash.isNull(),'INSERT').otherwise('UPDATE').alias("operation"))
            # to get deletes(old record of update)
            cdc_raw_snapshot_only = prev_df.join(ret_df, ['sha_hash'], 'leftanti')
            cdc_raw_snapshot_only = cdc_raw_snapshot_only.drop("operation")
            cdc_df_ud = cdc_raw_snapshot_only.join(ret_df, pk_ls, "left").select(cdc_raw_snapshot_only['*'],
                            when(ret_df.sha_hash.isNull(),'DELETE').otherwise('INSERT').alias("operation"))

            # filter data frame to get only deleted records
            cdc_df_d = cdc_df_ud.where(cdc_df_ud.operation == 'DELETE')
            cdc_df_ui = cdc_df_ui.select(columns)
            cdc_df_d = cdc_df_d.select(columns)

            # union inserts, updates and deleted records to get cdc data
            cdc_df_final = cdc_df_ui.union(cdc_df_d)

            # get list of 'sha_hash' values of deletes records
            del_hash_ls = cdc_df_final.where(cdc_df_final.operation == 'DELETE').select(collect_list('sha_hash')).first()[0]
            # filter out deleted records and union with last processed raw_current and remove the deleted records from it
            cdc_df_wt_del = cdc_df_final.where(cdc_df_final.operation != 'DELETE')
            prev_df = prev_df.select(columns)
            cdc_df_wt_del = cdc_df_wt_del.select(columns)
            raw_snapshot_result = prev_df.union(cdc_df_wt_del)
            raw_snapshot_result = raw_snapshot_result.filter(~raw_snapshot_result.sha_hash.isin(del_hash_ls))

            raw_snapshot_result = cdc_manager.filter_raw_current(raw_snapshot_result, tableOptions)
        except AnalysisException as e:
            write_log(__file__, "__get_raw_raw_current()", 'info', f'AnalysisException: Error creating raw, rc')
            raise (e)
        return cdc_df_final, raw_snapshot_result
               
    # ----------------------------------------
    # Combine the previous days data with the current
    #   df – Dataframe
    #   prev_df - The previous days dataframe
    # ---------------------------------------- 
    def __combine_dataframes(self, df, prev_df, tableOptions):
        print('__combine_dataframes entered')
        prev_df = prev_df.drop(col("partition_0"))
        write_log(__file__, "__combine_dataframes()", 'info', f'Combining current and previous dataframes')

        df.createOrReplaceTempView("new")
        prev_df.createOrReplaceTempView("old")
        
        sql = f"""
                SELECT o.* FROM old o LEFT ANTI JOIN new n on o.pk= n.pk
            """
            
        df_o = self.s3_manager.read_with_sql(sql)
        
        ArgumentsManager.is_not_list_error(tableOptions.get("raw_current_pk_sort_columns"), "raw_current_pk_sort_columns is not a list")
        ArgumentsManager.is_not_list_error(tableOptions.get("raw_current_pk_columns"), "raw_current_pk_columns is not a list")
            
        # if list is empty then default to timestamp column
        pk_sort_columns = ArgumentsManager.is_list_empty_or_not_defined(tableOptions["raw_current_pk_sort_columns"], ["timestamp_col desc"])
        
        sort_column = ",".join(pk_sort_columns)
        
        if '/appflow/' in tableOptions['s3_landing_folder'].lower():
            if ('RECORDTYPE' in tableOptions['table'].upper()) or ('USER' in tableOptions['table'].upper()):
                delete_operation = ""
            else:
                delete_operation = "and IsDeleted != 'true'"
        else:
            delete_operation = "and operation NOT IN ('DELETE')"

        if tableOptions.get("sql_in_code",False) == True:
            rc_qm = RCQueryManager()
            sql = rc_qm.retrieve_current_snapshot_sql_by_tbl_name(tableOptions['table'])
        else:
            # need to add in 3379 and 746 config to have <sort_column> <DESC>
            sql = f"""
                    SELECT *
                    FROM (
                        SELECT 
                            n.*,
                            row_number() over (partition by {",".join(tableOptions["raw_current_pk_columns"])} order by {sort_column} ) as ROWID
                        FROM new n 
                    ) t
                    WHERE ROWID = 1 {delete_operation}
                """
        write_log(__file__, "__combine_dataframes()", 'info', f'raw_current sql: {sql}')
        df_n = self.s3_manager.read_with_sql(sql)
        
        df = df_o.unionByName(df_n, allowMissingColumns=True)
        
        return df
                     
    # ----------------------------------------
    # Process and combine the dataframes for CDC (previous days and current)
    #   tableOptions - table options
    #   prev_date - the prev load date
    #   schema - current schema
    #   ret_df - the dataframe to be returned
    # ---------------------------------------- 
    def __cdc_raw_current(self, tableOptions, the_date, prev_date, ret_df):
        if (self.mode == Mode.CDC):
            # ----------------------------------------
            # Get previous days information
            # ----------------------------------------
            prev_date = datetime.strptime(prev_date, '%Y%m%d').strftime("%Y-%m-%d")
            
            try:
                path = self.s3_manager.get_raw_prev_date_path(tableOptions, prev_date)
                format = "parquet"
                prev_df = self.s3_manager.read_s3_with_pk(the_date, self.load_time, format, path, tableOptions, self.columns_manager, raw_current_schema=True)
            except AnalysisException as e:
                write_log(__file__, "__cdc_raw_current()", 'info', f'AnalysisException: [prev_df] No data found (prev_day) for path: {path}')
                raise(e)

            if tableOptions.get('daily_full_load', False):
                ret_df, ret_df_combined = self.__get_raw_raw_current(ret_df, prev_df, tableOptions)
            else:
                try:
                    # Append current and previous dataframes
                    ret_df_combined = self.__combine_dataframes(ret_df, prev_df, tableOptions)
                except AnalysisException as e:
                    write_log(__file__, "__cdc_raw_current()", 'info', f'AnalysisException: Error combining data')
                    raise(e)

        else:
            ret_df_combined = ret_df

        if tableOptions.get('daily_full_load', False):
            return ret_df_combined, ret_df
        else:
            return ret_df_combined, None
    
    #*********************************************************************************************************
    # *** PUBLIC METHODS ***
    #*********************************************************************************************************
    # -----------------------------------
    # Process initial loads
    # -----------------------------------
    def process_initial_load(self, tableOptions, the_date: str):
        write_log(__file__, "process_initial_load()", 'info', f"{tableOptions['table']}::process_initial_loads()")
        
        # Read and combine data using the tableOptions provided        
        df, df_combined = self.read_and_combine_data(tableOptions, the_date)
        
        if (df == None):
            return
        
        # Write dataframe to S3
        self.write_data(df, df_combined, "overwrite", tableOptions)            

    # -----------------------------------
    # Process CDC
    # -----------------------------------
    def process_cdc(self, tableOptions: dict, the_date: str, the_prev_date: str):
        write_log(__file__, "process_cdc()", 'info', f"{tableOptions['table']}::process_cdc()")
        
        # Read and combine data using the tableOptions provided        
        df, df_combined = self.read_and_combine_data(tableOptions, the_date, the_prev_date)
        
        if (df == None):
            return
        
        # Write dataframe to S3
        self.write_data(df, df_combined, "overwrite", tableOptions) 
        
    # ----------------------------------------
    # Read landing data and add standard columns
    # tableOptions: The options/variables used
    # the_date: The date to load
    # prev_date: Used for CDC
    # ----------------------------------------
    def read_and_combine_data(self, tableOptions, the_date, prev_date=None):
        write_log(__file__, "read_and_combine_data()", 'info', f"started Read and combining data")
        # Create list of dataframes
        dfs = []
        dfs_landing = []
        
        # Loop through all schemas
        for schema in tableOptions["schemas"]:
            if (self.mode == Mode.InitialLoad or "dms" in (tableOptions["s3_landing_folder"]).lower()):
                table = tableOptions["table"]
            elif '/appflow/' in tableOptions['s3_landing_folder'].lower():
                table = tableOptions["table"]
            else:
                table = tableOptions["table"] + "__ct"
                
            # Construct the query
            df = None
            
            if (self.mode == Mode.InitialLoad):
                try:
                    path_l = self.s3_manager.get_landing_path(tableOptions, schema, table, self.mode)

                    df = self.s3_manager.read_s3(tableOptions["s3_landing_format"], path_l, tableOptions, self.columns_manager)
                    
                    if (df == None or df.count() == 0):
                        write_log(__file__, "read_and_combine_data()", 'info', f"no data found for schema: {schema}")
                        continue     
                    dfs_landing.append(df)
                    # add hash column to handle daily full load
                    if tableOptions.get('daily_full_load', False):
                        df = self.columns_manager.add_sha_hash(df)
                    # add additional columns
                    df = self.columns_manager.add_additional_columns(df, the_date, self.load_time, schema)
                    # df = self.columns_manager.add_standard_columns(df,tableOptions)
                    dfs.append(df)
                    
                except AnalysisException:
                     write_log(__file__, "read_and_combine_data()", 'info', f"AnalysisException: no data found for schema: {schema}")
                     traceback.format_exc()
                     continue              
                except Exception:
                    write_log(__file__, "read_and_combine_data()", 'info', f"Exception: Error reading path: {path_l} and format: {format}\n{traceback.format_exc()}")
                    raise
            else:
                # CDC processing
                try:
                    if tableOptions.get('daily_full_load', False):
                        path_l = self.s3_manager.get_landing_path(tableOptions, schema, table, Mode.InitialLoad)
                        df = self.s3_manager.read_s3(tableOptions["s3_landing_format"], path_l, tableOptions, self.columns_manager)
                    else:
                        # For today
                        path_l = self.s3_manager.get_landing_path_w_date(tableOptions, schema, table, the_date, self.mode)
                        df = self.s3_manager.read_s3_with_pk(the_date, self.load_time, tableOptions["s3_landing_format"], path_l, tableOptions, self.columns_manager, schema=schema)
                    if (df == None or df.count() == 0):
                        write_log(__file__, "read_and_combine_data()", 'info', f"df none or empty - No data found for schema: {schema}")
                        continue

                    if tableOptions.get('daily_full_load', False):
                        # add hash column to handle daily full load
                        df = self.columns_manager.add_sha_hash(df)
                        # add additional columns
                        df = self.columns_manager.add_additional_columns(df, the_date, self.load_time, schema)
                    dfs.append(df)
                    dfs_landing.append(df)
                except AnalysisException as e:
                    write_log(__file__, "read_and_combine_data()", 'info', f"AnalysisException: No data found for schema: {schema}")
                    continue              

        if (len(dfs) == 0):
            write_log(__file__, "read_and_combine_data()", 'info', f"*** ENDING *** No data found for any schema")
            if tableOptions.get('data_sanity_check'):
                return None, None, None
            else:
                return None, None 

        write_log(__file__, "read_and_combine_data()", 'info', f"Combining dataframes")
        
        # Combine all dataframes        
        ret_df = reduce(DataFrame.unionAll, dfs)
        df_landing = reduce(DataFrame.unionAll, dfs_landing)
        ret_df_combined = None

        # Process fixed datatypes
        ret_df = self.columns_manager.update_datatypes(ret_df, tableOptions)

        # Process cleansing rules
        ret_df = self.columns_manager.clean_dataframe(self.mode, ret_df, tableOptions)

        # Add process_id column only if defined in table options
        ret_df = self.columns_manager.add_exec_ids(ret_df, tableOptions)
        
        if (not tableOptions.get("is_large_table", False)) and (tableOptions.get("create_raw_current", True)):
            # Process and combine the dataframes for CDC (previous days and current)
            ret_df_combined, ret_df_pd = self.__cdc_raw_current(tableOptions, the_date, prev_date, ret_df)
        elif not tableOptions.get("create_raw_current"):
            write_log(__file__, "read_and_combine_data()", 'info', f"create_raw_current is False.  Skipping raw_current")
        else:
            write_log(__file__, "read_and_combine_data()", 'info', f"Large table processing.  Skipping raw_current")

        if (self.mode == Mode.CDC) and tableOptions.get('daily_full_load', False):
            ret_df = ret_df_pd
            if ret_df.count() == 0:
                df_landing, ret_df, ret_df_combined = None, None, None
            # Drop pk column
        else:
            ret_df = ret_df.drop(col("pk"))

        write_log(__file__, "read_and_combine_data()", 'info', f"Completed creating and raw, raw_current dataframes")
        # Return the dataframe
        if tableOptions.get('data_sanity_check'):
            return df_landing, ret_df, ret_df_combined
        else:
            return ret_df, ret_df_combined
    
    # ----------------------------------------
    # Write data to S3
    # ----------------------------------------
    def write_data(self, df, df_combined, writeMode, tableOptions):
        write_log(__file__, "read_and_combine_data()", 'info', f"Writing RAW data")
        folder = self.s3_manager.get_raw_folder(tableOptions)
        if df != None:
            self.s3_manager.write_s3(df, folder, writeMode, "load_date", "schema_name")
            write_log(__file__, "read_and_combine_data()", 'info', f"Written RAW data to {folder}")
        else:
            write_log(__file__, "read_and_combine_data()", 'info', f"Skipped RAW data write because of no data")
            
        if (not tableOptions.get("is_large_table", False)) and (tableOptions.get("create_raw_current", True)):
            
            write_log(__file__, "read_and_combine_data()", 'info', f"Writing RAW_CURRENT data")
            #Write dataframe to S3 (RAW_CURRENT)
            folder = self.s3_manager.get_raw_current_folder(tableOptions)
            if df_combined != None:
                if tableOptions.get("s3_raw_current_latest_path",False):
                    write_log(__file__, "read_and_combine_data()", 'info', f'Writing to RAW_CURRENT (latest) data to {tableOptions.get("s3_raw_bucket")}{tableOptions.get("s3_raw_current_latest_path")}{(tableOptions["table"]).lower()}')
                    self.s3_manager.write_s3(df_combined, f'{tableOptions.get("s3_raw_bucket")}{tableOptions.get("s3_raw_current_latest_path")}{(tableOptions["table"]).lower()}', writeMode, "")
                    write_log(__file__, "read_and_combine_data()", 'info', f'Written RAW_CURRENT (latest) data to {tableOptions.get("s3_raw_bucket")}{tableOptions.get("s3_raw_current_latest_path")}{(tableOptions["table"]).lower()}')
                self.s3_manager.write_s3(df_combined, folder, writeMode, "load_date")
                write_log(__file__, "read_and_combine_data()", 'info', f"Written RAW_CURRENT data to {folder}")
            else:
                write_log(__file__, "read_and_combine_data()", 'info', f"Skipped RAW_CURRENT data write because of no data")

    def initial_load_cleanup(self, tableOptions):
        write_log(__file__, "initial_load_cleanup()", 'info', f"Cleaning RAW and RAW Current data")
        s3_client = boto3.resource('s3')

        # Raw Data Cleaning
        raw_bucket = (self.s3_manager.s3_raw_bucket).split("/")[2]
        write_log(__file__, "initial_load_cleanup()", 'info', f"raw bucket : {raw_bucket}")
        raw_bucket = s3_client.Bucket(raw_bucket)
        raw_subfolder_address = self.s3_manager.s3_raw_folder[1:] + tableOptions["table"].lower() + "/"
        write_log(__file__, "initial_load_cleanup()", 'info', f"raw address : {raw_subfolder_address}")
        raw_bucket.objects.filter(Prefix=raw_subfolder_address).delete()

        # Raw Current Cleaning
        rawcurrent_subfolder_address = self.s3_manager.s3_raw_current_folder[1:] + tableOptions["table"].lower() + "/"
        write_log(__file__, "initial_load_cleanup()", 'info', f"raw current address : {rawcurrent_subfolder_address}")
        raw_bucket.objects.filter(Prefix=rawcurrent_subfolder_address).delete()
