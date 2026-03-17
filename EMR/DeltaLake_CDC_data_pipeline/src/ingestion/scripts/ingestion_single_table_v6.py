from datetime import datetime, date, timedelta
from managers_v6.console_manager import write_log,write_message
from managers_v6.ingestion_baseclass import Ingestion_BaseClass, Mode
from managers_v6.notification_manager import SNSPublish
from managers_v6 import ods_manager
from managers_v6.cdc_manager import CdcManager
from managers_v6 import audit_manager

import pytz
import argparse
import json
import traceback
import boto3



class Table2Class(Ingestion_BaseClass):
    
    def initialize_variables(self, table_defination_file, common_config, env):

        self.tableOptions = {
            "is_large_table": False if table_defination_file["is_large_table"].lower() == 'false' else True,
            "schemas": table_defination_file["schemas"],
            "table": table_defination_file["table"],     
            "fixed_columns": table_defination_file["fixed_columns"],
            "fixedColumnsCleansingRules": table_defination_file["fixedColumnsCleansingRules"],
            "raw_current_pk_columns": table_defination_file["raw_current_pk_columns"],    
            "raw_current_pk_sort_columns": table_defination_file["raw_current_pk_sort_columns"],
            "s3_landing_format": table_defination_file["s3_landing_format"],
            "s3_landing_folder": table_defination_file["s3_landing_folder"],
            "src_table_schema": table_defination_file["src_table_schema"],
            "process_id": table_defination_file["process_id"],
            "s3_landing_secure_bucket": table_defination_file["s3_landing_secure_bucket"],
            "s3_landing_bucket": table_defination_file["s3_landing_bucket"],
            "s3_raw_bucket": table_defination_file["s3_raw_bucket"],
            "s3_landing_path": table_defination_file["s3_landing_path"],
            "s3_raw_path": table_defination_file["s3_raw_path"],
            "s3_raw_current_path": table_defination_file["s3_raw_current_path"],
            "prcs_job_id": table_defination_file["prcs_job_id"],
            "user": table_defination_file["user"],
            "env": env,
            "run_date": table_defination_file["run_date"]
        }
        if "src_table_schema" in table_defination_file:
            if self.tableOptions["src_table_schema"] == "schema_in_code":
                self.tableOptions.update({"src_table_schema": self.get_schema_in_code()})
            else:
                self.tableOptions.update({"src_table_schema": table_defination_file["src_table_schema"]})
        if "global_date_cleansing_rule" in table_defination_file:
            self.tableOptions.update({"global_date_cleansing_rule": table_defination_file["global_date_cleansing_rule"]})
        if "dataTypeCastingToString" in table_defination_file:
            self.tableOptions.update({"dataTypeCastingToString": True if table_defination_file["dataTypeCastingToString"].lower() == 'true' else False})
        if "data_sanity_check" in table_defination_file:
            self.tableOptions.update({"data_sanity_check": True if table_defination_file["data_sanity_check"].lower() == 'true' else False})
        if "create_raw_current" in table_defination_file:
            self.tableOptions.update({"create_raw_current": True if table_defination_file["create_raw_current"].lower() == 'true' else False})
        if "sql_in_code" in table_defination_file:
            self.tableOptions.update({"sql_in_code": True if table_defination_file["sql_in_code"].lower() == 'true' else False})
        if "include_today" in table_defination_file:
            self.tableOptions.update({"include_today": True if table_defination_file["include_today"].lower() == 'true' else False})
        if "helper_schema" in table_defination_file:
            self.tableOptions.update({"helper_schema": table_defination_file["helper_schema"]})
        if "helper_table_name" in table_defination_file:
            self.tableOptions.update({"helper_table_name": table_defination_file["helper_table_name"]})
        if "helper_table_schema" in table_defination_file:
            self.tableOptions.update({"helper_table_schema": table_defination_file["helper_table_schema"]})
        if "use_secure_folder" in table_defination_file:
            self.tableOptions.update({"use_secure_folder": True if table_defination_file["use_secure_folder"].lower() == 'true' else False})
        if "s3_raw_current_latest_path" in table_defination_file:
            self.tableOptions.update({"s3_raw_current_latest_path": False if table_defination_file["s3_raw_current_latest_path"].lower() == 'false' else table_defination_file["s3_raw_current_latest_path"]})
        if "daily_full_load" in table_defination_file:
            self.tableOptions.update({"daily_full_load": True if table_defination_file["daily_full_load"].lower() == 'true' else False})
        if "dms_full_load_flag" in table_defination_file:
            self.tableOptions.update({"dms_full_load_flag": True if table_defination_file["dms_full_load_flag"].lower() == 'true' else False})
        if "path_flag" in table_defination_file:
            self.tableOptions.update({"path_flag": True if table_defination_file["path_flag"].lower() == 'true' else False})

        self.cst = pytz.timezone('US/Central')
        self.email = SNSPublish()
        self.ods_manger = ods_manager.ODSManager(env)
        self.common_config = common_config
        self.env = env
        
        write_log(__file__, "initialize_variables()", 'info', f'variable initialized tableOptions is :\n{self.tableOptions}')

    def get_schema_in_code(self):
        if str(self.tableOptions["table"]) == 'tbl_name':
            schema_in_code = "col1=CHARACTER|col2=DECIMAL(17,0)|col3=SMALLINT|col4=DATE|col5=TIMESTAMP|col6=BIGINT|col7=FLOAT|col8=DOUBLE|col9=STRING"
        elif str(self.tableOptions["table"]) == 'tbl_name':
            schema_in_code = "col1=CHARACTER|col2=DECIMAL(17,0)|col3=SMALLINT|col4=DATE|col5=TIMESTAMP|col6=BIGINT|col7=FLOAT|col8=DOUBLE|col9=STRING"
        return schema_in_code

    def completed_job_status_udpate(self):
        prcs_job_update_return = self.ods_manger.update_processjobid(
                    prcs_job_exec_end_dt = (datetime.now(self.cst)).strftime('%Y-%m-%d %H:%M:%S'),   
                    prcs_job_exec_sts = 'COMPLETED', 
                    prcs_job_exec_failed_rsn = '', 
                    updtd_usr = self.tableOptions['user'], 
                    prcs_job_exec_id = prcs_job_id
                    )
        prcs_job_update_sts = json.loads(prcs_job_update_return)['statusCode']
        prcs_job_update_body = json.loads(prcs_job_update_return)['body']
        if int(prcs_job_update_sts) != 200:
            write_log(__file__, "completed_job_status_udpate()", 'error', f'can not update job id with C status. response is {prcs_job_update_body}')
            send_email = self.email.email_notification(
                aws_region = self.common_config["AWS_REGION"],
                arn = self.common_config["ARN"],
                custom_message = f'Process job Id update failed for the table {str(self.tableOptions["table"])} with process id {str(self.tableOptions["process_id"])} and job id {prcs_job_id}.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_update_body)}\n\n Thank You.',
                env = self.env,
                subject = f"ERROR::Data_Ingestion_Process::"
            )
            exit(1)
        else:
            write_log(__file__, "completed_job_status_udpate()", 'info', f'updated process job id to Completed. response is {prcs_job_update_body}')

    # This function will be called to udpate the jobs status from INITIATED state to COMPLETED, PC
    def ds_based_job_status_udpate(self, l_r_check, r_rc_check, df_r, df_rc):
        # Sanity check is successful and data is matching
        if (l_r_check == True and r_rc_check == True): 
            write_log(__file__, "ds_based_job_status_udpate()", 'info', f'l->r and r->rc are matching')
            self.completed_job_status_udpate()
        else:
            # Sanity check is successful but data is NOT MATCHING 
            if (l_r_check == False and r_rc_check == False):
                failed_rsn = 'SANITY CHECK BOTH FAILED'
                check = 'both raw and raw_current'
            # Sanity check is successful
            elif (l_r_check != None and r_rc_check != None):
                # Data NOT matching for one of the layer
                failed_rsn = 'SANITY CHECK RAW FAILED' if l_r_check == False else 'SANITY CHECK RAW_CURRENT FAILED'
                check = 'raw' if l_r_check == False else 'raw_current'
            # Sanity check has ERROR (failed)
            else:
                # Data is present in raw AND raw_current
                if (df_r != None and df_rc != None):
                    if (l_r_check == None and r_rc_check == None):
                        failed_rsn = 'SANITY CHECK BOTH ERROR'
                        check = 'no-email'
                    elif (l_r_check == None):
                        failed_rsn = 'SANITY CHECK RAW ERROR' if r_rc_check == True else 'SANITY CHECK RAW ERROR AND RAW_CURRENT FAILED'
                        check = 'no-email'
                    elif (r_rc_check == None):
                        failed_rsn = 'SANITY CHECK RAW_CURRENT ERROR' if l_r_check == True else 'SANITY CHECK RAW_CURRENT ERROR AND RAW FAILED'
                        check = 'no-email'
                # Data is NOT present for raw AND/OR raw_current
                else: 
                    failed_rsn = 'SKIPPED'
                    check = 'no-email'
            if check != 'no-email':
                send_email = self.email.email_notification(
                                    aws_region = self.common_config["AWS_REGION"],
                                    arn = self.common_config["ARN"],
                                    custom_message = f'ds_based_job_status_update():: {check} not matching for the table {str(self.tableOptions["table"])} with process id {str(self.tableOptions["process_id"])} and job id {prcs_job_id}.',
                                    env = self.env,
                                    subject = f"ERROR::Data_Ingestion_Process::")
            
            prcs_job_update_return = self.ods_manger.update_processjobid(
                prcs_job_exec_end_dt = (datetime.now(self.cst)).strftime('%Y-%m-%d %H:%M:%S'),   
                prcs_job_exec_sts = 'PARTIALLY COMPLETED', 
                prcs_job_exec_failed_rsn = failed_rsn, 
                updtd_usr = self.tableOptions['user'], 
                prcs_job_exec_id = prcs_job_id
                )
            prcs_job_update_sts = json.loads(prcs_job_update_return)['statusCode']
            prcs_job_update_body = json.loads(prcs_job_update_return)['body']
            if int(prcs_job_update_sts) != 200:
                write_log(__file__, "ds_based_job_status_udpate()", 'error', f'can not update job id with PC status. response is {prcs_job_update_body}')
                send_email = self.email.email_notification(
                    aws_region = self.common_config["AWS_REGION"],
                    arn = self.common_config["ARN"],
                    custom_message = f'Process job Id update failed for the table {str(self.tableOptions["table"])} with process id {str(self.tableOptions["process_id"])} and job id {prcs_job_id}.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_update_body)}\n\n Thank You.',
                    env = env,
                    subject = f"ERROR::Data_Ingestion_Process::"
                )
                exit(1)
            else:
                write_log(__file__, "ds_based_job_status_udpate()", 'info', f'updated process job id to Partically Completed. response is {prcs_job_update_body}')
    
    def process_initial_load(self, the_date, prev_date):
        
        write_log(__file__, "process_initial_load()", 'info', f"inital load started for table: {self.tableOptions['table']}")
        cdc_manager = CdcManager(mode=Mode.InitialLoad, tableOptions = self.tableOptions)

        ## R, RC dataframe >> data sanity check (insert audit logs) >> write dataframes to s3 >> update the job id status
        if self.tableOptions.get('data_sanity_check') == True:
            # Create raw and raw_current dataframes
            df_landing, df, df_combined = self.read_and_combine_data(self.tableOptions, the_date, prev_date)
            if self.tableOptions.get("create_raw_current", True) and df_combined:
                df_combined = cdc_manager.filter_raw_current(df_combined, self.tableOptions)

            # Check l->r and r->rc counts ==> insert 2 records into the audit log
            audit_manger = audit_manager.AuditManager(self.tableOptions, Mode.InitialLoad, self.common_config, self.env)
            l_r_check = audit_manger.landing_raw_check(df_landing, df, prev_date)
            if self.tableOptions.get("create_raw_current", True):
                r_rc_check = audit_manger.raw_rawcurrent_check(df, df_combined, prev_date)
            else:
                r_rc_check = True
            write_log(__file__, "process_initial_load()", 'info', f'l->r and r->rc checks are done, {l_r_check} and {r_rc_check} are respective ouputs')

            # Write the raw and raw_current datafrmaes to s3
            self.write_data(df, df_combined, "overwrite", self.tableOptions)

            # udpate the job id status
            self.ds_based_job_status_udpate(l_r_check, r_rc_check, df, df_combined)

        ## R, RC dataframe >> write dataframes to s3 >> update the job id status
        else:
            df, df_combined = self.read_and_combine_data(self.tableOptions, the_date, prev_date)
            if self.tableOptions.get("create_raw_current", True):
                df_combined = cdc_manager.filter_raw_current(df_combined, self.tableOptions)
            self.write_data(df, df_combined, "overwrite", self.tableOptions)
            self.completed_job_status_udpate()
            
    def process_cdc(self, the_date, the_prev_date):

        write_log(__file__, "process_cdc()", 'info', f"cdc load started for table: {self.tableOptions['table']}")
        
        ## R, RC dataframe >> data snaity check (insert audit logs) >> write daframe to s3 >> update the job id status
        if self.tableOptions.get('data_sanity_check') == True:
            # Create raw and raw_current dataframes
            df_landing, df, df_combined = self.read_and_combine_data(self.tableOptions, the_date, the_prev_date)

            # Check l->r and r->rc counts ==> insert 2 records into the audit log
            audit_manger = audit_manager.AuditManager(self.tableOptions, Mode.CDC, self.common_config, self.env)
            l_r_check = audit_manger.landing_raw_check(df_landing, df, the_prev_date)
            if self.tableOptions.get("create_raw_current", True):
                r_rc_check = audit_manger.raw_rawcurrent_check(df, df_combined, the_prev_date)
            else:
                r_rc_check = True
            write_log(__file__, "process_cdc()", 'info', f'l->r and r->rc checks are done for the date {the_prev_date}. {l_r_check} and {r_rc_check} are respective ouputs')

            # Write the raw and raw_current datafrmaes to s3
            self.write_data(df, df_combined, "overwrite", self.tableOptions)

            # udpate the job id status
            self.ds_based_job_status_udpate(l_r_check, r_rc_check, df, df_combined)

        ## R, RC dataframe >> write dataframes to s3 >> update the job id status
        else:
            df, df_combined = self.read_and_combine_data(self.tableOptions,the_date,the_prev_date)
            self.write_data(df, df_combined, "overwrite", self.tableOptions) 
            self.completed_job_status_udpate()  

def main(run_mode, env, table_defination_file, common_config, prcs_id, force_initial_load, run_date, rerun_run_date=None):
        cst = pytz.timezone('US/Central')
        email = SNSPublish()
        ods_manger = ods_manager.ODSManager(env)

        if 'user' not in table_defination_file:
            table_defination_file.update({"user":"ingestion-framework"})

        table_defination_file.update({"process_id":prcs_id})
        bucket_options ={"s3_landing_secure_bucket": "s3://" + str(common_config["s3_buckets"][0]["s3_landing_secure_bucket"]),
                        "s3_landing_bucket": "s3://" + str(common_config["s3_buckets"][0]["s3_landing_bucket"]),
                        "s3_raw_bucket": "s3a://" + str(common_config["s3_buckets"][0]["s3_raw_bucket"]),
                        "s3_landing_path": str(common_config["s3_buckets"][0]["s3_landing_path"]),
                        "s3_raw_path": str(common_config["s3_buckets"][0]["s3_raw_path"]),
                        "s3_raw_current_path": str(common_config["s3_buckets"][0]["s3_raw_current_path"]),
                        "s3_raw_current_latest_path": str(common_config["s3_buckets"][0].get("s3_raw_current_latest_path", False))
                        }
        table_defination_file.update(bucket_options)

        if table_defination_file.get("create_raw_current") == 'false':
            exec_tbl_tp = 'RAW_ONLY'
        else:
            exec_tbl_tp = 'RAW_RC'

        table_defination_file.update({"run_date": run_date})

        if table_defination_file.get("daily_full_load", 'false').lower() == 'true':
            prcs_job_exec_tp = 'DAILY_INGESTION_FULL_LOAD'
        else:
            prcs_job_exec_tp = 'INGESTION_LOAD'
    
        try:
            ####################################################
            ############# Initial load #########################
            ####################################################    
            if run_mode in ["InitialLoad", "Both"]:
                
                ############# Process JOB ID Creation     
                prcs_job_id_return = ods_manger.create_processjobid(
                    prcs_exec_id = prcs_id, 
                    prcs_job_exec_nme = table_defination_file["table"], 
                    prcs_job_lob = common_config["lob"],
                    prcs_job_data_load_dt = run_date,
                    prcs_job_exec_acvy_tp = 'FULL_LOAD', 
                    prcs_job_exec_tp = prcs_job_exec_tp,
                    prcs_job_exec_strt_dt = (datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S'), 
                    created_usr = table_defination_file['user'],
                    exec_tbl_tp = exec_tbl_tp
                    )
                
                prcs_job_id_sts = json.loads(prcs_job_id_return)['statusCode']
                prcs_job_id_body = json.loads(prcs_job_id_return)['body']

                if int(prcs_job_id_sts) != 200:
                    write_log(__file__, "main()", 'error', f'can not create a job id.Process job Id creation failed for the table {str(table_defination_file["table"])} with process id {prcs_id}.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_id_return)}')
                    send_email = email.email_notification(
                        aws_region = common_config["AWS_REGION"],
                        arn = common_config["ARN"],
                        custom_message = f'Process job Id creation failed for the table {str(table_defination_file["table"])} with process id {prcs_id}.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_id_return)}\n\n Thank You.',
                        env = env,
                        subject = f"ERROR::Data_Ingestion_Process::"
                    )
                    exit(1)
                else:
                    global prcs_job_id
                    prcs_job_id = prcs_job_id_body["PRCS_JOB_EXEC_ID"]
                    table_defination_file.update({"prcs_job_id":prcs_job_id})
                    write_log(__file__, "main()", 'info', f'lambda returned processed job id as : {prcs_job_id}')

                mode = Mode.InitialLoad
                ingestion = Table2Class(mode, table_defination_file)
                ingestion.initialize_variables(table_defination_file, common_config, env)

                ############# S3 raw, raw current cleanup
                if  force_initial_load == "True":
                    write_log(__file__, "main()", 'info', f'force_initial_load given is : {force_initial_load}')
                    try:
                        ingestion.initial_load_cleanup(table_defination_file)
                    except Exception as e:
                        send_email = email.email_notification(
                            aws_region = common_config["AWS_REGION"],
                            arn = common_config["ARN"],
                            custom_message = f'While Cleaning S3 for Initial Load an error occured in table {str(table_defination_file["table"])} with process id {prcs_id} and job id {prcs_job_id}..Please find the details below:\n{traceback.format_exc()}\n\n Thank You.',
                            env = env,
                            subject = f"ERROR::Data_Ingestion_Process::"
                        )
                        write_log(__file__, "main()", 'error', f'While Cleaning S3 for Initial Load an error occured in table {str(table_defination_file["table"])} with process id {prcs_id} and job id {prcs_job_id}..Please find the details below:\n{traceback.format_exc()}')
                        exit(1)
                
                ############# Initial load Processing
                prev_date_cdc = None  ##Hardecoded for initial load only
                
                try:
                    ingestion.process_initial_load(run_date, prev_date_cdc)
                    write_log(__file__, "main()", 'info', f'initial load completed for {table_defination_file["table"]} table')
                except Exception as e:
                    write_log(__file__, "main()", 'error', f'Initial Load failed with an error in table {str(table_defination_file["table"])} with process id {prcs_id} and job id {prcs_job_id}..Please find the details below:\n{traceback.format_exc()}')
                    err_desc = traceback.format_exc()
                    err_desc = err_desc if len(str(err_desc)) > 0 else ''
                    prcs_job_update_return = ods_manger.update_processjobid(
                        prcs_job_exec_end_dt = (datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S'), 
                        prcs_job_exec_sts = 'FAILED',    
                        prcs_job_exec_failed_rsn = err_desc[:199] if len(err_desc) > 200 else err_desc, 
                        updtd_usr = table_defination_file['user'], 
                        prcs_job_exec_id = prcs_job_id
                        )
                    prcs_job_update_sts = json.loads(prcs_job_update_return)['statusCode']
                    prcs_job_update_body = json.loads(prcs_job_update_return)['body']
                    if int(prcs_job_update_sts) != 200:   
                        write_log(__file__, "main()", 'error', f'can not update jod id. Process job Id update failed for the table {str(table_defination_file["table"])} with process id {prcs_id} and job id {prcs_job_id} and Ingestion data is also failed.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_update_body)}.\nPlease find the details of error below:\n{err_desc}')
                        send_email = email.email_notification(
                            aws_region = common_config["AWS_REGION"],
                            arn = common_config["ARN"],
                            custom_message = f'Process job Id update failed for the table {str(table_defination_file["table"])} with process id {prcs_id} and job id {prcs_job_id} and Ingestion data is also failed.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_update_body)}.\nPlease find the details of error below:\n{err_desc}\n\n Thank You.',
                            env = env,
                            subject = f"ERROR::Data_Ingestion_Process::"
                        )
                        exit(1)
                    else:
                        write_log(__file__, "main()", 'info', f'job id udpated as failed status. Initial load failed with errors.\n{traceback.format_exc()}')
                        send_email = email.email_notification(
                            aws_region = common_config["AWS_REGION"],
                            arn = common_config["ARN"],
                            custom_message = f'initial load processing for table {str(table_defination_file["table"])} failed with process id {prcs_id} and job id {prcs_job_id}.\nPlease find the details below:\n{err_desc}\n\n Thank You.',
                            env = env,
                            subject = f"ERROR::Data_Ingestion_Process::"
                        )
                        exit(1)
                    
            ####################################################
            ############# CDC load #############################
            ####################################################
            if run_mode in ["CDC", "Both"]:

                mode = Mode.CDC
                prev_details = ods_manger.get_lastcompleted_job_details(table_defination_file["table"],common_config["lob"])
                prev_details = json.loads(prev_details.decode())
                prev_details_sts = prev_details['statusCode']

                if int(prev_details_sts) != 200:
                    write_log(__file__, "main()", 'error',
                              f'get_lastcompleted_job_details failed for the table {str(table_defination_file["table"])}.\nPlease find the details below that was returned from lambda:\n{str(prev_details)}')
                    send_email = email.email_notification(
                        aws_region=common_config["AWS_REGION"],
                        arn=common_config["ARN"],
                        custom_message=f'get_lastcompleted_job_details failed for the table {str(table_defination_file["table"])}.\nPlease find the details below that was returned from lambda:\n{str(prev_details)}\n\n Thank You.',
                        env=env,
                        subject=f"ERROR::Data_Ingestion_Process::"
                    )
                    exit(1)
                
                print(f'prev_details is {prev_details}')

                prev_date_cdc = prev_details["body"][0][0]
                prev_exec_tp = prev_details["body"][0][1]

                write_log(__file__, "main()", 'info', f'In cdc mode and prev_date_cdc is {prev_date_cdc}, prev_exec_tp is {prev_exec_tp}')

                dates = CdcManager(mode, table_defination_file)

                if table_defination_file.get('daily_full_load', 'false').lower() == 'true':
                    date_cdc = [run_date]
                else:
                    date_cdc = dates.get_cdc_calender_dates(prev_date_cdc, run_date, rerun_run_date, prev_exec_tp)

                write_log(__file__, "main()", 'info', f'latest cdc dates are {date_cdc}')

                if len(date_cdc) > 0:
                    for current_date in date_cdc:
                        # add wait 1 sec 
                        write_message(f'started processing {table_defination_file["table"]} for date: {current_date}')
                        ######## Job ID Creation 
                        prcs_job_id_return = ods_manger.create_processjobid(
                            prcs_exec_id = prcs_id, 
                            prcs_job_exec_nme = table_defination_file["table"], 
                            prcs_job_lob = common_config["lob"],
                            prcs_job_data_load_dt = current_date,
                            prcs_job_exec_acvy_tp = 'PARTITION_APPEND', 
                            prcs_job_exec_tp = prcs_job_exec_tp,
                            prcs_job_exec_strt_dt = (datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S'), 
                            created_usr = table_defination_file['user'],
                            exec_tbl_tp=exec_tbl_tp
                            )
                        prcs_job_id_sts = json.loads(prcs_job_id_return)['statusCode']
                        prcs_job_id_body = json.loads(prcs_job_id_return)['body']

                        if int(prcs_job_id_sts) != 200:
                            write_log(__file__, "main()", 'error', f'Process job Id creation failed for the table {str(table_defination_file["table"])}.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_id_return)}')
                            send_email = email.email_notification(
                                aws_region = common_config["AWS_REGION"],
                                arn = common_config["ARN"],
                                custom_message = f'Process job Id creation failed for the table {str(table_defination_file["table"])}.\nPlease find the details below that was returned from lambda:\n{str(prcs_job_id_return)}\n\n Thank You.',
                                env = env,
                                subject = f"ERROR::Data_Ingestion_Process::"
                            )
                            exit(1)
                        else:
                            prcs_job_id = prcs_job_id_body["PRCS_JOB_EXEC_ID"]
                            table_defination_file.update({"prcs_job_id":prcs_job_id})
                            write_log(__file__, "main()", 'info', f'created job id and lambda returned processed job id as : {prcs_job_id}')

                        ######## Process CDC for one day   
                        try:
                            ingestion = Table2Class(mode, table_defination_file)
                            ingestion.initialize_variables(table_defination_file, common_config, env)
                            ingestion.process_cdc(current_date, prev_date_cdc)
                            
                            # udpate the prev_date_cdc for next iteration
                            prev_date_cdc = ods_manger.get_lastcompleted_job_details(table_defination_file["table"],common_config["lob"])
                            prev_date_cdc = json.loads(prev_date_cdc.decode())
                            prev_date_cdc = prev_date_cdc["body"][0][0]
                            write_log(__file__, "main()", 'info', f'next iteration prev_date_cdc is {prev_date_cdc}')
                            write_log(__file__, "main()", 'info', f'CDC load completed')
                            
                        except Exception as e:
                            write_log(__file__, "main()", 'error', f'CDC Load failed with an error in table {str(table_defination_file["table"])} with process id {prcs_id} and job id {prcs_job_id}..Please find the details below:\n{traceback.format_exc()}')
                            err_desc =traceback.format_exc()
                            prcs_job_update_return = ods_manger.update_processjobid(
                                prcs_job_exec_end_dt = (datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S'), 
                                prcs_job_exec_sts = 'FAILED',    
                                prcs_job_exec_failed_rsn = err_desc[:199] if len(err_desc) > 200 else err_desc, 
                                updtd_usr = table_defination_file['user'], 
                                prcs_job_exec_id = prcs_job_id
                                )
                            prcs_job_update_sts = json.loads(prcs_job_update_return)['statusCode']
                            prcs_job_update_body = json.loads(prcs_job_update_return)['body']

                            if int(prcs_job_update_sts) != 200:
                                write_log(__file__, "main()", 'error', f'Process job Id update failed for the table {str(table_defination_file["table"])}.\nPlease find the details below: {str(prcs_job_update_return)}')
                                
                                send_email = email.email_notification(
                                    aws_region = common_config["AWS_REGION"],
                                    arn = common_config["ARN"],
                                    custom_message = "Process job Id update failed for the table" + str(table_defination_file["table"]) + ".Please find the details below:\n" + str(prcs_job_update_return) + "\n\n Thank You.",
                                    env = env,
                                    subject  = "ERROR:Data_Ingestion_Process_Status_Notification- Environment- "+ str(env) + "-" + str(date.today())
                                )
                                exit(1)
                            else:
                                write_log(__file__, "main()", 'info', f'updated the process id. CDC load failed with following errors.\n{traceback.format_exc()}')
                                
                                send_email = email.email_notification(
                                    aws_region = common_config["AWS_REGION"],
                                    arn = common_config["ARN"],
                                    custom_message = "Data Ingestion has errored in cdc table " + str(table_defination_file["table"]) + ".Please find the details below:\n" + err_desc + "\n\n Thank You.",
                                    env = env,
                                    subject  = "ERROR:Data_Ingestion_Process_Status_Notification- Environment- "+ str(env) + "-" + str(date.today())
                                )
                                exit(1)
                else:
                    write_log(__file__, "main()", 'info', f'There are no CDC files for the table {str(table_defination_file["table"])}. Skipping this table for ingestion process')
                    
        except Exception as e:
            send_email = email.email_notification(
                            aws_region = common_config["AWS_REGION"],
                            arn = common_config["ARN"],
                            custom_message = f'Data Ingestion has errored in table {str(table_defination_file["table"])} with process id {prcs_id}.\nPlease find the details:\n{traceback.format_exc()}\n\n Thank You.',
                            env = env,
                            subject = f"ERROR::Data_Ingestion_Process::"
                        )
            write_log(__file__, "main()", 'error', f'Data Ingestion has errored in table {str(table_defination_file["table"])} with process id {prcs_id}.\nPlease find the details:\n{traceback.format_exc()}\n{traceback.format_exc()}')
            traceback.format_exc()
            exit(1)


if __name__ == "__main__":
    print('started')
    ############# Aruguments Check #########################    
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_mode", help="Run Mode (InitialLoad, CDC, Both)", required=True)
    parser.add_argument('--environment', help="Should be dev/test/model/prod. Required field", required=True)
    parser.add_argument('--prcs_id',help="Provide common Arguments",required=True)
    parser.add_argument('--table_defination_file', help="Provides the data lake configurations. Required field", required=False)
    parser.add_argument('--common_config',help="Provide common Arguments",required=False)
    parser.add_argument('--force_initial_load',help="Provide True or False",required=False,default="False")
    parser.add_argument('--run_date', help="run date - provide in date in '%Y%m%d' string format, optional parameter if not given takes yesterday's date", required=False)
    parser.add_argument('--rerun_run_date', help="optional parameter to re-run run_date, to handle multiple runs in same day - provide True or False in string", required=False)
    parser.add_argument('--config_location',help="Provide common Arguments",required=False)
    parser.add_argument('--table_index',help="Provide common Arguments",required=False)
    args = parser.parse_args()

    if all(v is None for v in [args.run_mode, args.environment, args.prcs_id])\
    or args.environment not in ["dev", "tst", "mdl","prd"] \
    or args.run_mode not in ["InitialLoad", "CDC","Both"]:
        write_log(__file__, "__main__", 'error', f'Missing --run_mode or --table_defination_file -- enivronment or incorrect --common_config, --force_iniital_load')
        exit(-1)
    
    run_mode = args.run_mode
    env = args.environment
    try:
        if args.table_defination_file:
            table_defination_file = json.loads(args.table_defination_file.replace("'","\"").replace("@","'"))
            common_config = json.loads(args.common_config.replace("'","\""))
        else:
            config_location = args.config_location
            table_index = int(args.table_index)
            s3 = boto3.resource('s3')
            s3_bucket = s3.Bucket(config_location.split('@')[0])
            content_object = s3_bucket.Object(config_location.split('@')[1])
            file_content = content_object.get()['Body'].read().decode('utf-8')
            config_file = json.loads(file_content)

            common_config = config_file['common_parameters']
            table_defination_file = json.loads(str(config_file['tables_definitions'][table_index]).replace("'",'"').replace("@","'"))
    except Exception as e :
        print(f'exception occured at {e}')
        # write code to udpate ODS as Step Failed
        exit(1)

    prcs_id = args.prcs_id
    force_initial_load = args.force_initial_load

    if args.run_date:
        run_date = args.run_date
        write_log(__file__, "__main__", 'info', f'run_date parameter is passed : {run_date}')
    else:
        run_date = datetime.now().date() - timedelta(days=1)
        run_date = run_date.strftime("%Y%m%d")
        write_log(__file__, "__main__", 'info',f"run_date parameter is not passed taking yesterday's date : {run_date}")

    if args.rerun_run_date:
        rerun_run_date = args.rerun_run_date
    else:
        rerun_run_date = None

    ############# Invoke Main #########################    
    main(run_mode, env, table_defination_file, common_config, prcs_id, force_initial_load, run_date, rerun_run_date)
