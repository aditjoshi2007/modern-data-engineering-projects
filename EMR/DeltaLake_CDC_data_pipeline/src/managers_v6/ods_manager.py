################################################## OBJECTIVE #######################################################
# This class should allow you to invoke Operation Data Service Lambda - which will
#   1. Create a Process ID  in Process Table (prcs_exec_info) - Need to do it before you start you batch load
#   2. Create a JOB ID in Job Table (prcs_job_exec_info) - Need to have a job id for each table run in your batch
#   3. Udpate the JOB ID status, once the table load is done, following are the allowed status.
#           'INITIATED'             --> when you create a job id for a table.
#           'COMPLETED'             --> when your job's table load is successful, without any errors.
#           'PARTIALLY COMPLETED'   --> when your job's table load is sucessful but the sanity check failed. OR no data to process for the given day
#           'FAILED'                --> when your job's table while process encounters an error. or 'SKIPPED' keyword when no data to process for the given day
#   4. Update the Process ID status, once all the jobs (table) processing is done.
#           'INITIATED'             --> when you create a process id for a batch run
#           'COMPLETED'             --> when your job's are done, check all you "data-processing" jobs and if all jobs are in COMPLETED state
#           'PARTIALLY COMPLETED'   --> when your job's are done, check all you "data-processing" jobs and if any one of the JOB is in PARTIALLY COMPLETED or FAILED state
#           'FAILED'                --> when your job's are done, check all you "data-processing" jobs and if all jobs are in FAILED state
#           'FAILED-EMR STARTUP'    --> when your EMR while creating i,e before adding steps - runs into an issue.
#   5. Retrive the latest sucessfully processed date, for a give table.
#   6. Retrive all the Jobs Status of a given process id.
#   7. Insert record into audit logs table (data sanity check).
####################################################################################################################
# The return value of all methods in this class are of bytes.
# The lambda "<lambda_name>" is designed in such a way that it will have {status:XXX;body:<string/dict>}.
# Hence, use the return of these methods as json.loads(<return value>) to convert to dictionary.
# status code 200 is sucess and 4XX are client errors and 5xx are server errors.
####################################################################################################################
import sys
import time as t
from datetime import datetime

import boto3
import pytz
from botocore.config import Config as boto3_config

sys.path.insert(1,
                u'/Scripts/deltalake_cdc_data_pipeline/managers_v6.zip')
from managers_v6.console_manager import write_log


class ODSManager:

    ### Constructor ###
    ## change the lambda name and regionm in case if the TF deployment results in different resource and region.
    def __init__(self, env):
        self.lambda_fun='<lambda_function_name>'
        self.client=boto3.client('lambda', region_name='us-east-1')
        self.invocation_type='RequestResponse'
        self.log_type='Tail'
        self.env=env

        acc_env={
            "dev": "***********",
            "tst": "***********",
            "mdl": "***********",
            "prd": "***********"
        }

        acc=acc_env[env]
        self.bucket=f's3-{acc}-codedeployment-bucket-{env}'
        self.ods_rds_key=f'deltalake_cdc_data_pipeline/config/{env}/ods_rds.json'

    # returns the current CST time zone based date and time.
    def get_est_current_dt(self):
        cst=pytz.timezone('US/Central')  # CST
        cd=(datetime.now(cst)).strftime('%Y-%m-%d')
        cdt=(datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S')
        return cd, cdt

    # Lambda call function
    def call_lambda_function(self, payload):
        success_response=False
        call_counter=0
        while success_response == False:
            try:
                write_log(__file__, "lambda_call_function()", 'info',
                          f'****************Lambda call try: {str(call_counter)}')
                call_counter+=1
                write_log(__file__, "lambda_call_function()", 'info',
                          f'****************Incrementing call counter to: {str(call_counter)}')
                response=self.client.invoke(
                    FunctionName=self.lambda_fun,
                    InvocationType=self.invocation_type,
                    LogType=self.log_type,
                    Payload=payload.encode()
                )['Payload'].read()
                success_response=True
            except Exception as e:
                if call_counter == 5:
                    write_log(__file__, "lambda_call_function()", 'info',
                              f'****************Lambda call max tries reached: {str(call_counter)}')
                    raise e
                else:
                    t.sleep(60)
                    continue
        return response

    def create_processid(self, prcs_exec_nme, prcs_exec_tp, prcs_exec_acvy_tp, creatd_usr):
        current_date, current_datetime=self.get_est_current_dt()
        payload="{" + f'"invoke_function": "create_processid", "PRCS_EXEC_NME": "{prcs_exec_nme}", "PRCS_EXEC_TP": "{prcs_exec_tp}", "PRCS_EXEC_ACVY_TP": "{prcs_exec_acvy_tp}", "PRCS_EXEC_ACVY_DT": "{current_date}",   "PRCS_EXEC_STRT_DT": "{current_datetime}", "CREATD_USR": "{creatd_usr}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "create_processid()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "create_processid()", 'info', f'response from lambda is: {response}')
        return response

    def create_processjobid(self, prcs_exec_id, prcs_job_exec_nme, prcs_job_lob, prcs_job_data_load_dt,
                            prcs_job_exec_acvy_tp, prcs_job_exec_tp, prcs_job_exec_strt_dt, created_usr, exec_tbl_tp):
        payload="{" + f'"invoke_function": "create_processjobid", "PRCS_EXEC_ID": "{prcs_exec_id}", "PRCS_JOB_EXEC_NME": "{prcs_job_exec_nme}", "PRCS_JOB_LOB":"{prcs_job_lob}", "PRCS_JOB_DATA_LOAD_DT": "{prcs_job_data_load_dt}", "PRCS_JOB_EXEC_TP": "{prcs_job_exec_tp}", "PRCS_JOB_EXEC_ACVY_TP": "{prcs_job_exec_acvy_tp}", "PRCS_JOB_EXEC_STRT_DT": "{prcs_job_exec_strt_dt}", "CREATD_USR": "{created_usr}", "PRCS_JOB_EXEC_TBL_TP": "{exec_tbl_tp}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "create_processjobid()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "create_processjobid()", 'info', f'response from lambda is: {response}')
        return response

    def update_processjobid(self, prcs_job_exec_end_dt, prcs_job_exec_sts, prcs_job_exec_failed_rsn, updtd_usr,
                            prcs_job_exec_id):
        prcs_job_exec_failed_rsn=prcs_job_exec_failed_rsn.replace('"', ' ').replace("'", " ")
        payload="{" + f'"invoke_function": "update_processjobid", "PRCS_JOB_EXEC_END_DT": "{prcs_job_exec_end_dt}", "PRCS_JOB_EXEC_STS": "{prcs_job_exec_sts}", "PRCS_JOB_EXEC_FAILED_RSN": "{prcs_job_exec_failed_rsn}", "UPDTD_USR": "{updtd_usr}", "PRCS_JOB_EXEC_ID": "{prcs_job_exec_id}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "update_processjobid()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "update_processjobid()", 'info', f'response from lambda is: {response}')
        return response

    def update_processid(self, prcs_exec_end_dt, prcs_exec_sts, prcs_exec_failed_rsn, prcs_exec_id, updtd_usr):
        prcs_exec_failed_rsn=prcs_exec_failed_rsn.replace('"', ' ').replace("'", " ")
        payload="{" + f'"invoke_function": "update_process_exec_id", "PRCS_EXEC_END_DT": "{prcs_exec_end_dt}", "PRCS_EXEC_STS": "{prcs_exec_sts}", "PRCS_EXEC_FAILED_RSN": "{prcs_exec_failed_rsn}", "PRCS_EXEC_ID": "{prcs_exec_id}", "UPDTD_USR": "{updtd_usr}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "update_processid()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "update_processid()", 'info', f'response from lambda is: {response}')
        return response

    def get_lastcompleted_job_details(self, prcs_job_exec_nme, prcs_job_lob):
        payload="{" + f'"invoke_function": "get_lastcompleted_job_details", "PRCS_JOB_EXEC_NME": "{prcs_job_exec_nme}", "PRCS_JOB_LOB": "{prcs_job_lob}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "get_lastcompleted_job_details()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "get_lastcompleted_job_details()", 'info', f'response from lambda is: {response}')
        return response

    def insert_auditlog(self, prcs_job_exec_id, tbl_nme, exec_tbl_tp, src_cnt, tgt_cnt, src_loc, tgt_loc,
                        src_i_cnt=None, src_u_cnt=None, src_d_cnt=None, tgt_i_cnt=None, tgt_u_cnt=None, tgt_d_cnt=None):
        payload="{" + f'"invoke_function": "insert_auditlog", "PRCS_JOB_EXEC_ID": "{prcs_job_exec_id}", "PRCS_JOB_TBL_NME": "{tbl_nme}", "PRCS_JOB_EXEC_TBL_TP": "{exec_tbl_tp}", "PRCS_JOB_EXEC_TBL_SRC_CNT": {src_cnt}, "PRCS_JOB_EXEC_TBL_TGT_CNT": {tgt_cnt}, "PRCS_JOB_EXEC_TBL_SRC_LOC": "{src_loc}", "PRCS_JOB_EXEC_TBL_TGT_LOC": "{tgt_loc}", "SRC_INSERT_CNT": "{src_i_cnt}", "SRC_UPDATE_CNT": "{src_u_cnt}", "SRC_DELETE_CNT": "{src_d_cnt}", "TGT_INSERT_CNT": "{tgt_i_cnt}", "TGT_UPDATE_CNT": "{tgt_u_cnt}", "TGT_DELETE_CNT": "{tgt_d_cnt}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "insert_auditlog()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "insert_auditlog()", 'info', f'response from lambda is: {response}')
        return response

    def get_alljobstatus_by_prcs_id(self, prcs_exec_id):
        payload="{" + f'"invoke_function": "get_alljobstatus_by_prcs_exec_id", "PRCS_EXEC_ID": "{prcs_exec_id}","env":"{self.env}", "bucket":"{self.bucket}", "ods_rds_key":"{self.ods_rds_key}"' + "}"
        write_log(__file__, "get_alljobstatus_by_prcs_id()", 'info', f'payload sending to lambda is :{payload}')

        response=self.call_lambda_function(payload)

        write_log(__file__, "get_alljobstatus_by_prcs_id()", 'info', f'response from lambda is: {response}')
        return response
