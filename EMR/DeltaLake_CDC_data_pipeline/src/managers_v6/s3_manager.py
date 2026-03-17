from email import message
from pyspark.sql.functions import concat_ws, length
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from functools import reduce

from managers_v6.redis_manager import get_redis_hash_parameter
from managers_v6.console_manager import write_log
from managers_v6.schema_manager import SchemaManager
from datetime import datetime, timedelta

import boto3
import traceback

import enum
class Mode(enum.Enum):
    InitialLoad = 1
    CDC = 2

class S3Manager:
    # -----------------------------------
    # Initialize S3Manager
    # -----------------------------------
    def __init__(self, mode, tableOptions=None):
        # Store spark object
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        # Set the mode (CDC or Initial Load)
        self.mode = mode
        self.schema_manager = SchemaManager()

        self.s3_raw_bucket = tableOptions['s3_raw_bucket'] if (tableOptions and tableOptions.get('s3_raw_bucket')) else get_redis_hash_parameter("BUCKETS", "S3_RAW_BUCKET") + "/"
        self.s3_landing_bucket = tableOptions['s3_landing_bucket'] if (tableOptions and tableOptions.get('s3_landing_bucket')) else get_redis_hash_parameter("BUCKETS", "S3_LANDING_BUCKET") + "/"
        self.s3_landing_bucket_secure = tableOptions['s3_landing_secure_bucket'] if (tableOptions and tableOptions.get('s3_landing_secure_bucket')) else get_redis_hash_parameter("BUCKETS", "S3_LANDING_BUCKET_SECURE") + "/"
        self.s3_raw_folder = tableOptions['s3_raw_path'] if (tableOptions and tableOptions.get('s3_raw_path')) else "<s3_prefix>/"
        self.s3_raw_current_folder = tableOptions['s3_raw_current_path'] if (tableOptions and tableOptions.get('s3_raw_current_path')) else "<s3_prefix>/"
        
        # Set spark settings
        self.__set_spark_settings()
        
        
    # ----------------------------------------
    # Set Spark Settings used for the ingestion process
    # ----------------------------------------
    def __set_spark_settings(self):
        write_log(__file__, "__set_spark_settings()", 'info', f'Setting Spark Settings')
        
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED")
        self.spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED")
        self.spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED")
        self.spark.sql("set spark.sql.parquet.enableVectorizedReader=false")
        self.spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")
        
        if (self.mode == Mode.CDC):
            write_log(__file__, "__set_spark_settings()", 'info', f'CDC mode - Dynamic')
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        else:
            write_log(__file__, "__set_spark_settings()", 'info', f'Initial Load mode - Overwrite')

    # ----------------------------------------
    # Get RAW folder
    # ----------------------------------------
    def get_raw_folder(self, tableOptions):
        return self.s3_raw_bucket + self.s3_raw_folder + tableOptions["table"].lower()
    
    # ----------------------------------------
    # Get RAW CURRENT folder
    # ----------------------------------------
    def get_raw_current_folder(self, tableOptions):
        return self.s3_raw_bucket + self.s3_raw_current_folder + (tableOptions["table"]).lower()
    
    # ----------------------------------------
    # Get RAW prev date folder
    # ----------------------------------------
    def get_raw_prev_date_path(self, tableOptions, prev_date):
        return self.s3_raw_bucket + self.s3_raw_current_folder + (tableOptions["table"]).lower() + "/load_date=" + prev_date + "/"
    
    # ----------------------------------------
    # Get landing folder path
    # ----------------------------------------
    def get_landing_path(self, tableOptions, schema, table, mode):
        landing_bucket = ""

        run_date = tableOptions['run_date']

        if 'dms' in (tableOptions["s3_landing_folder"]).lower():
            service = 'dms'
        elif 'attunity' in (tableOptions["s3_landing_folder"]).lower():
            service = 'attunity'
        elif 'appflow' in (tableOptions["s3_landing_folder"]).lower():
            service = 'appflow'
        else:
            service = 'na'
        
        # Read secure from secure
        if schema.lower() in ["schema_name_1", "schema_name_2"] or tableOptions.get("use_secure_folder", False):
            landing_bucket = self.s3_landing_bucket_secure
        else:
            landing_bucket = self.s3_landing_bucket
           
        if service == 'appflow':
            ret_path = tableOptions["s3_landing_folder"].lstrip('/') + table
        else:
            ret_path = landing_bucket + tableOptions["s3_landing_folder"] + schema + "." + table

        if service == 'dms':
            dms_folder = '/' + [folder for folder in ret_path.split('/') if folder.startswith('dms')][0] + '/'
            ret_path_wt_bucket = tableOptions["s3_landing_folder"] + schema + "." + table
             ## Adding path_flag to take the DMS landing folder path as is from config file, not include 'cdc/' and 'fullload/' folder
            ## Reason: Applicable for DMS since it still runs with dms version prefix (no CDC Fullload partition in Tasks)
            if tableOptions.get("path_flag",False):
                ret_path_cdc = ret_path_wt_bucket
                if mode == Mode.InitialLoad: ## When running FullLoad
                    ret_path_l = [ret_path_wt_bucket + "/LOAD*", ret_path_wt_bucket + "/SEGMENT*"]                    
                    ret_path_l.extend(self.get_list_of_initial_load_files(ret_path_cdc.lstrip('/'), run_date, service))
                    print("ret_path_l (mode = FL):", ret_path_l)
                else: ## For CDC or Both
                    ret_path_l = ret_path_l = [landing_bucket + ret_path_cdc]
                    print("ret_path_l (mode = CDC):", ret_path_l)
            elif 'dms_version_number' in tableOptions["s3_landing_folder"]:
                ret_path_wt_bucket = tableOptions["s3_landing_folder"] + schema + "/" + table 
                index_cdc = ret_path_wt_bucket.find(dms_folder) + len(dms_folder)
                ret_path_cdc = ret_path_wt_bucket[:index_cdc] + ret_path_wt_bucket[index_cdc:] + '__CT'
                if mode == Mode.InitialLoad:
                    ret_path = landing_bucket + tableOptions["s3_landing_folder"] + schema + "/" + table
                    index_fl = ret_path.find(dms_folder) + len(dms_folder)
                    ret_path_fl = ret_path[:index_fl] + ret_path[index_fl:] + '__FL'
                    ret_path_l = [ret_path_fl + "/LOAD*", ret_path_fl + "/SEGMENT*"]
                    # get cdc files till run_date for initial load
                    ret_path_l.extend(self.get_list_of_initial_load_files(ret_path_cdc.lstrip('/'), run_date, service))
                # for cdc path --> get_landing_path_w_date
                else:
                    ret_path_l = [landing_bucket + ret_path_cdc]

            else:
                index_cdc = ret_path_wt_bucket.find(dms_folder) + len(dms_folder)
                ret_path_cdc = ret_path_wt_bucket[:index_cdc] + 'cdc/' + ret_path_wt_bucket[index_cdc:] + '/'
                if mode == Mode.InitialLoad:
                    index_fl = ret_path.find(dms_folder) + len(dms_folder)
                    if tableOptions.get('dms_full_load_flag', False):
                        ret_path_fl = ret_path[:index_fl] + 'full-load/' + ret_path[index_fl:]
                    else:
                        ret_path_fl = ret_path[:index_fl] + 'fullload/' + ret_path[index_fl:]
                    ret_path_l = [ret_path_fl + "/LOAD*", ret_path_fl + "/SEGMENT*"]
                    # get cdc files till run_date for initial load
                    ret_path_l.extend(self.get_list_of_initial_load_files(ret_path_cdc.lstrip('/'), run_date, service))
                # for cdc path --> get_landing_path_w_date
                else:
                    ret_path_l = [landing_bucket + ret_path_cdc]
        elif service == 'appflow':
            delta_path = ret_path + '_delta/'
            if mode == Mode.InitialLoad:
                ret_path_l = self.get_list_of_initial_load_files(ret_path+'_FullLoad/', run_date, service)
                ret_path_l.extend(self.get_list_of_initial_load_files(delta_path, run_date, service))
            # for cdc path --> get_landing_path_w_date
            else:
                ret_path_l = delta_path
        elif mode == Mode.InitialLoad and service == 'attunity':
            ret_path_l = [ret_path + "/LOAD*", ret_path + "/SEGMENT*"]
            ret_path_cdc = tableOptions["s3_landing_folder"].lstrip('/') + schema + "." + table + "__ct"
            ret_path_l.extend(self.get_list_of_initial_load_files(ret_path_cdc, run_date, service))
        else:
            ret_path_l = [ret_path]
            
        return ret_path_l

    # ----------------------------------------
    # Get list of landing files as per the path
    # ----------------------------------------

    def get_s3_files(self, path):
        s3_client = boto3.resource('s3')
        file_list = []
        landing_bucket = self.s3_landing_bucket.replace('s3://', '').split('/').pop(0)
        src_bucket = s3_client.Bucket(landing_bucket)

        for bucket_object_summary in src_bucket.objects.filter(Prefix=path):
            file_name = bucket_object_summary.key
            if not file_name.endswith('/'):
                file_list.append(self.s3_landing_bucket + '/' + file_name)

        return file_list

    # --------------------------------------------------------------------------
    # Get list of initial load files based on run date (all files till run_date)
    # --------------------------------------------------------------------------
    def get_list_of_initial_load_files(self, path, run_date, service):
        if not run_date:
            return []

        initial_load_files = self.get_s3_files(path)

        len_files_list = len(initial_load_files)

        if len_files_list > 0:
            if service == 'appflow':
                initial_load_files = [file_name for file_name in initial_load_files if datetime.strptime(''.join(file_name.split('/')[-5:-2]), "%Y%m%d") <= datetime.strptime(run_date, "%Y%m%d")]
            elif service == 'attunity':
                initial_load_files = [file_name for file_name in initial_load_files if datetime.strptime(file_name.split('/')[-1][:8], "%Y%m%d") <= datetime.strptime(run_date, "%Y%m%d")]
                initial_load_files = ['/'.join(file_name.split('/')[:-1]) for file_name in initial_load_files]
                initial_load_files = sorted(set(initial_load_files))
                initial_load_files = [file_name+'/*' for file_name in initial_load_files]
            elif service == 'dms':
                initial_load_files = [file_name for file_name in initial_load_files if ('LOAD' or '_SUCCESS') not in file_name]
                initial_load_files = [file_name for file_name in initial_load_files if datetime.strptime(file_name.split('/')[-1][:8], "%Y%m%d") <= datetime.strptime(run_date, "%Y%m%d")]
                initial_load_files = [file_name.rsplit('-', 1)[0] for file_name in initial_load_files]
                initial_load_files = sorted(set(initial_load_files))
                initial_load_files = [file_name+'*' for file_name in initial_load_files]
        return initial_load_files


    # ----------------------------------------
    # Get landing folder path with date
    # ----------------------------------------
    def get_landing_path_w_date(self, tableOptions, schema, table, the_date, mode):
        # calling function to get landing path
        path_l = self.get_landing_path(tableOptions, schema, table, mode)
        if '/appflow/' in tableOptions['s3_landing_folder'].lower():
            delta_path = f"{path_l}{datetime.strptime(the_date, '%Y%m%d').strftime('%Y/%m/%d')}/"
            path_l = self.get_s3_files(delta_path)
        elif "attunity" in (tableOptions["s3_landing_folder"]).lower():
            next_date = datetime.strptime(the_date, "%Y%m%d") + timedelta(1)
            next_date_str = datetime.strftime(next_date, "%Y%m%d")
            date_path_str = f"{the_date}T000000_{next_date_str}T000000/"
            if (datetime.strptime(the_date, "%Y%m%d").date() == datetime.now().date() - timedelta(days=1)) and tableOptions.get('include_today', True):
                today_date = next_date_str
                tomorrow_date = datetime.strftime(next_date + timedelta(1), "%Y%m%d")
                today_date_path_str = f"{today_date}T000000_{tomorrow_date}T000000/"
                path_l = [f"{path}/{date_path_str}*" for path in path_l] + [f"{path}/{today_date_path_str}*" for path in path_l]
            else:
                path_l = [f"{path}/{date_path_str}*" for path in path_l]
        # introduced this condition to avoid taking today's data together with yesterday, this is specifically for RTS-746, RTS-3379
        elif not tableOptions.get('include_today', True):
            path_l = [f"{path}/{the_date}*" for path in path_l]
        # if we are processing yesterday then also include today
        elif datetime.strptime(the_date, "%Y%m%d").date() == datetime.now().date() - timedelta(days=1):
            path_l = [f"{path}/{the_date}*" for path in path_l] + [f"{path}/{datetime.now().date().strftime('%Y%m%d')}*" for path in path_l]
        else:
            path_l = [f"{path}/{the_date}*" for path in path_l]
        return path_l
    
    # ----------------------------------------
    # Reading using sql_query
    #   sql_query - the sql query to be used
    # ----------------------------------------    
    def read_with_sql(self, sql_query, message_flag=False):
        if message_flag:
            write_log(__file__, "read_with_sql()", 'info', f'Reading S3 with sql \n {sql_query}')
        df = self.spark.sql(sql_query)
        return df
    
    
    # ----------------------------------------
    # Create data from path using the provided format
    #   format - the format to be used (parquet, json, csv, etc.)
    #   path - the path to be used; location of the data
    #   tableOptions - it will check whether src_table_schema is present and use it while reading
    #                - ex: client_id=char|name=varchar|taxcode=varchar|street1=varchar|street2=varchar
    # ---------------------------------------- 
    def read_s3(self, format,  paths, tableOptions, columns_manager = None,is_helper=False):
        if 'dms' in (tableOptions["s3_landing_folder"]).lower():
            service = 'dms'
        elif 'attunity' in (tableOptions["s3_landing_folder"]).lower():
            service = 'attunity'
        elif 'appflow' in (tableOptions["s3_landing_folder"]).lower():
            service = 'appflow'
        else:
            service = 'na'

        if service == 'appflow':
            paths_l = sorted(set(['/'.join(path.split('/')[:-2])+'/*' for path in paths]))
            write_log(__file__, "read_s3()", 'info', f'Reading S3 - {paths_l}')
        else:
            write_log(__file__, "read_s3()", 'info', f'Reading S3 - {paths}')

        df = None

        if service == 'appflow':
            df = self.spark.read.format(format).load(paths)
            # Process Data type casting to string only if defined in table options
            df = columns_manager.convert_columns_string_data_type(df, tableOptions)
        else:
            dfs = []
            for path in paths:
                init_load_flag = any(item in path for item in ['LOAD', 'SEGMENT'])
                
                try:
                    if tableOptions.get('src_table_schema'):
                        schema = tableOptions['src_table_schema'] if not is_helper else tableOptions['helper_table_schema']
                        spark_schema = self.schema_manager.get_spark_schema(schema, service = service, init_load_flag= init_load_flag)
                        ds = self.spark.read.schema(spark_schema).format(format).load(path)
                        
                    else:
                        ds = self.spark.read.format(format).load(path)
                    # to add drop and add standard columns - only for initial load files
                    if init_load_flag and columns_manager:
                        ds = columns_manager.add_standard_columns(ds, tableOptions)

                except Exception as e:
                    write_log(__file__, "read_s3()", 'info', f'Exception: while reading {path} \n {traceback.format_exc()}')
                    continue

                dfs.append(ds)
            try:
                df = reduce(DataFrame.unionByName, dfs)
            except Exception as e:
                write_log(__file__, "read_s3()", 'error', f'Error combining landing data:\n{traceback.format_exc()}')

        return df
    
    # ----------------------------------------
    # Create data from path for the given date using the provided format
    #   the_date - the date to be used
    #   format - the format to be used (parquet, json, csv, etc.)
    #   schema - current schema
    #   path - the path to be used; location of the data
    #   tableOptions - the table options
    #                - it will check whether src_table_schema is present and use it while reading
    #                - ex: id=char|name=varchar|code=varchar|street1=varchar|street2=varchar
    # ---------------------------------------- 
    def read_s3_with_pk(self, the_date, load_time, format, path_l, tableOptions, columns_manager, raw_current_schema = None, schema = None):
        write_log(__file__, "read_s3_with_pk()", 'info', f'Reading S3 w/ PK - {path_l}')

        if 'dms' in (tableOptions["s3_landing_folder"]).lower():
            service = 'dms'
        elif 'attunity' in (tableOptions["s3_landing_folder"]).lower():
            service = 'attunity'
        elif 'appflow' in (tableOptions["s3_landing_folder"]).lower():
            service = 'appflow'
        else:
            service = 'na'

        if tableOptions.get('daily_full_load'):
            daily_load = True
        else:
            daily_load = False
        
        if not isinstance(path_l, list):
            path_l = [path_l]

        empty_df_flag = False

        if service == 'appflow':
            try:
                df = self.spark.read.format(format).load(path_l)
            except Exception as e:
                write_log(__file__, "read_s3_with_pk()", 'info', f'Error reading data:{traceback.format_exc()}')
                empty_df_flag = True
            
        else: 
            dfs = []
            for path in path_l:
                try:
                    if (tableOptions.get('src_table_schema')) and ('-raw-' not in path):
                        spark_schema = self.schema_manager.get_spark_schema(tableOptions['src_table_schema'], raw_current_schema, service = service, daily_load=daily_load)
                        print(f'----------- reading with schema \n {spark_schema}')
                        ds = self.spark.read.schema(spark_schema).format(format).load(path)
                    else:
                        ds = self.spark.read.format(format).load(path)
                except Exception as e:
                    write_log(__file__, "read_s3_with_pk()", 'info', f'Error reading data:{traceback.format_exc()}')
                    continue

                dfs.append(ds)
            
            if not dfs:
                empty_df_flag = True
            else:
                try:
                    df = reduce(DataFrame.union, dfs)
                except Exception as e:
                    write_log(__file__, "read_s3_with_pk()", 'info', f'Error combining landing data:{traceback.format_exc()}')

        if not empty_df_flag:
            try:
                # Process Data type casting to string only if defined in table options
                df = columns_manager.convert_columns_string_data_type(df, tableOptions)
                # add additional columns
                df = columns_manager.add_additional_columns(df, the_date, load_time, schema)
                # Add pk (Primary Key) column
                df = df.withColumn("pk", concat_ws("|", *tableOptions["raw_current_pk_columns"]))
            except Exception as e:
                    write_log(__file__, "read_s3_with_pk()", 'error', f'Error processing landing data:{traceback.format_exc()}')
                    exit(1)
        else:
            write_log(__file__, "read_s3_with_pk()", 'info', f'returning empty DF')
            df = None
        return df

    # ----------------------------------------
    # Write data to path using the provided format
    #    df - The dataframe to be written
    #    tableOptions - the table options
    #    writeMode - the write mode (append, overwrite, etc.)
    # ----------------------------------------
    def write_s3(self, df, folder, writeMode, *partitionColumns):
        # Write dataframe to S3 (RAW)
        if len(partitionColumns[0]) == 0:
            df.write.mode(writeMode).parquet(folder)
            write_log(__file__, "write_s3()", 'info', f'DF written to {folder}')
        # Write dataframe to S3 (RAW_CURRENT)
        else:
            df.write.partitionBy(partitionColumns).mode(writeMode).parquet(folder)
            write_log(__file__, "write_s3()", 'info', f'DF written to {folder} with partitions {partitionColumns}')
