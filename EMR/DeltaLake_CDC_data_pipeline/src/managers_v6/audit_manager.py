################################################## OBJECTIVE #######################################################


import boto3
import pytz
import json
import traceback

from datetime import datetime
from pyspark.sql.functions import count, col

from managers_v6.ods_manager import ODSManager
from managers_v6.arguments_manager import ArgumentsManager
from managers_v6.columns_manager import ColumnsManager
from managers_v6.s3_manager import S3Manager, Mode
from managers_v6.console_manager import write_log
from managers_v6.rc_custom_query_manager import RCQueryManager
from managers_v6.notification_manager import SNSPublish

class AuditManager:

    ### Constructor ###
    ## change the lambda name and region in case if the TF deployment results in different resource and region.
    def __init__(self, tableOptions, mode, common_config, env):
        write_log(__file__, "__init__", 'info', f'initializing AuditManager')
        self.ods_manager = ODSManager(tableOptions['env'])
        self.s3_manager = S3Manager(mode, tableOptions)
        self.columns_manager = ColumnsManager()
        self.tableOptions = tableOptions
        self.mode = mode
        self.email = SNSPublish()
        self.common_config = common_config
        self.env = env
        write_log(__file__, "__init__", 'info', f'initialized AuditManager')
    
    # Take the 
    def landing_raw_check(self, df_landing, df, prev_date):
        write_log(__file__, "landing_raw_check", 'info', f'Starting landing to raw check')
        if 'dms' in (self.tableOptions["s3_landing_folder"]).lower():
            service = 'dms'
        elif 'attunity' in (self.tableOptions["s3_landing_folder"]).lower():
            service = 'attunity'
        elif 'appflow' in (self.tableOptions["s3_landing_folder"]).lower():
            service = 'appflow'
        else:
            service = None

        if not service:
            write_log(__file__, "landing_raw_check()", 'error', f'currently, only supports services, dms, attunity, appflow')
            send_email = self.email.email_notification(
                aws_region=self.common_config["AWS_REGION"],
                arn=self.common_config["ARN"],
                custom_message=f'landing_raw_check():: currently, only supports services, dms, attunity, appflow',
                env=self.env,
                subject=f"ERROR::Data_Ingestion_Process::")
            exit(1)

        try:
            if df != None and df_landing != None:

                l_cnt = df_landing.count()
                r_cnt = df.count()

                if service in ['dms', 'attunity']:
                    src_d_cnt = df_landing.filter(df_landing.header__operation == "DELETE").count()
                    src_i_cnt = df_landing.filter(df_landing.header__operation == "INSERT").count()
                    src_u_cnt = df_landing.filter(df_landing.header__operation == "UPDATE").count()

                    tgt_d_cnt = df.filter(df.header__operation == "DELETE").count()
                    tgt_i_cnt = df.filter(df.header__operation == "INSERT").count()
                    tgt_u_cnt = df.filter(df.header__operation == "UPDATE").count()

                elif service == 'appflow':
                    # in case there is no raw_current or mode is initial load, won't be able to find inserts and updates, hence all will be considered as inserts
                    if (not self.tableOptions.get("create_raw_current",True)) or (self.mode == Mode.InitialLoad):
                        src_d_cnt = 0
                        src_u_cnt = 0
                        src_i_cnt = l_cnt

                        tgt_d_cnt = 0
                        tgt_u_cnt = 0
                        tgt_i_cnt = r_cnt
                    else:
                        path = self.s3_manager.get_raw_prev_date_path(self.tableOptions, prev_date[0:4] + '-' + prev_date[4:6] + '-' + prev_date[6:8])
                        df_rc_prev = self.s3_manager.read_s3('parquet', [path], self.tableOptions, self.columns_manager)
                        pk = self.tableOptions["raw_current_pk_columns"]

                        src_d_cnt = 0 if ('RECORDTYPE' in self.tableOptions['table'].upper()) or ('USER' in self.tableOptions['table'].upper()) else df_landing.filter((df_landing.IsDeleted == "TRUE") | (df_landing.IsDeleted == "true")).count()
                        src_u_cnt = df_landing.join(df_rc_prev, [*pk],"inner").count()
                        src_i_cnt = df_landing.join(df_rc_prev, [*pk],"left_anti").count()

                        tgt_d_cnt = 0 if ('RECORDTYPE' in self.tableOptions['table'].upper()) or ('USER' in self.tableOptions['table'].upper()) else df.filter((df.IsDeleted == "TRUE") | (df.IsDeleted == "true")).count()
                        tgt_u_cnt = df.join(df_rc_prev, [*pk],"inner").count()
                        tgt_i_cnt = df.join(df_rc_prev, [*pk],"left_anti").count()

                audit_ins = self.ods_manager.insert_auditlog(
                    prcs_job_exec_id = self.tableOptions['prcs_job_id'], 
                    tbl_nme = self.tableOptions['table'], 
                    exec_tbl_tp = 'LAN_RAW_TBL', 
                    src_cnt = l_cnt, 
                    tgt_cnt = r_cnt, 
                    src_loc = self.tableOptions['s3_landing_bucket']+ self.tableOptions['s3_landing_path'], 
                    tgt_loc = self.tableOptions['s3_raw_bucket']+ self.tableOptions['s3_raw_path'],
                    src_i_cnt = src_i_cnt,
                    src_u_cnt = src_u_cnt,
                    src_d_cnt = src_d_cnt,
                    tgt_i_cnt = tgt_i_cnt,
                    tgt_u_cnt = tgt_u_cnt,
                    tgt_d_cnt=tgt_d_cnt
                    )
                audit_ins_sts = json.loads(audit_ins)['statusCode']
                audit_ins_body = json.loads(audit_ins)['body']

                if int(audit_ins_sts) != 200:
                    write_log(__file__, "landing_raw_check()", 'error', f'can not insert l->r audit info for process_id. response is{audit_ins_body}')
                    send_email = self.email.email_notification(
                                    aws_region = self.common_config["AWS_REGION"],
                                    arn = self.common_config["ARN"],
                                    custom_message = f'insert into audit table failed, l-> r check',
                                    env = self.env,
                                    subject = f"ERROR::Data_Ingestion_Process::"
                                )
                    #exit(1) check with Team if failing here is better or making the table as complete and continuing is better
                    return None
                else:
                    prcs_job_id = audit_ins_body["PRCS_JOB_EXEC_ID"]
                    write_log(__file__, "landing_raw_check()", 'info', f'inserted audit log for l->r for job Id: {prcs_job_id}')

                    if self.tableOptions.get('daily_full_load', False):
                        return True
                    else:
                        return True if l_cnt == r_cnt else False
            else:
                write_log(__file__, "landing_raw_check()", 'error', f'either landing or raw dataframe is empty')
                #exit(1) check with Team if failing here is better or making the table as complete and continuing is better
                return None
        except Exception as e :
            send_email = self.email.email_notification(
                                aws_region = self.common_config["AWS_REGION"],
                                arn = self.common_config["ARN"],
                                custom_message = f'landing_raw_check():: following exception occured in l->r check\n{traceback.format_exc()}',
                                env = self.env,
                                subject = f"ERROR::Data_Ingestion_Process::"
                            )
            print(f'following exception occured in l->r check \n{traceback.format_exc()}')
            exit(1)

    def raw_rawcurrent_check(self, df, df_combined, prev_date):
        write_log(__file__, "raw_rawcurrent_check", 'info', f'Starting raw to raw_current check')
        if 'dms' in (self.tableOptions["s3_landing_folder"]).lower():
            service = 'dms'
        elif 'attunity' in (self.tableOptions["s3_landing_folder"]).lower():
            service = 'attunity'
        elif 'appflow' in (self.tableOptions["s3_landing_folder"]).lower():
            service = 'appflow'
        else:
            service = None

        if not service:
            write_log(__file__, "raw_rawcurrent_check()", 'error', f'currently, only supports services, dms, attunity, appflow')
            send_email = self.email.email_notification(
                aws_region=self.common_config["AWS_REGION"],
                arn=self.common_config["ARN"],
                custom_message=f'raw_rawcurrent_check():: currently, only supports services, dms, attunity, appflow',
                env=self.env,
                subject=f"ERROR::Data_Ingestion_Process::")
            exit(1)
        try:
            if df != None and df_combined != None:

                r_cnt = df.count()
                rc_cnt = df_combined.filter(df_combined.job_id == self.tableOptions['prcs_job_id'] ).count()
                pk_sort_columns = ArgumentsManager.is_list_empty_or_not_defined(self.tableOptions["raw_current_pk_sort_columns"], ["header__timestamp desc"])
                sort_column = ",".join(pk_sort_columns)
                df.createOrReplaceTempView("new")

                if self.tableOptions.get("sql_in_code", False) == True:
                    rc_qm = RCQueryManager()
                    sql = rc_qm.retrieve_current_snapshot_sql_by_tbl_name(self.tableOptions['table'])   
                else:
                    sql = f"""
                            SELECT *
                            FROM (
                                SELECT
                                    n.*,
                                    row_number() over (partition by {",".join(self.tableOptions["raw_current_pk_columns"])} order by {sort_column} ) as ROWID
                                FROM new n
                            ) t
                            WHERE ROWID = 1
                        """

                df_r_latest = self.s3_manager.read_with_sql(sql,message_flag=False)

                df_rc_latest = df_combined.filter(df_combined.job_id == self.tableOptions['prcs_job_id'] )
                
                if service in ['dms', 'attunity']:
                    src_d_cnt = df.filter(df.header__operation == "DELETE").count()
                    src_i_cnt = df.filter(df.header__operation == "INSERT").count()
                    src_u_cnt = df.filter(df.header__operation == "UPDATE").count()

                    tgt_d_cnt = df_rc_latest.filter(df_rc_latest.header__operation == "DELETE").count()
                    tgt_i_cnt = df_rc_latest.filter(df_rc_latest.header__operation == "INSERT").count()
                    tgt_u_cnt = df_rc_latest.filter(df_rc_latest.header__operation == "UPDATE").count()

                    # get absolute insert count, won't include insert that got updated afterwards
                    src_ab_i_cnt = df_r_latest.filter(df_r_latest.header__operation == "INSERT").count()
                
                elif service == 'appflow':
                    pk = self.tableOptions["raw_current_pk_columns"]
                    if self.mode == Mode.InitialLoad:
                        src_d_cnt = 0
                        src_u_cnt = 0
                        src_i_cnt = r_cnt

                        tgt_d_cnt = 0
                        tgt_u_cnt = 0
                        tgt_i_cnt = rc_cnt

                        df_r_latest_no_del = df_r_latest if ('RECORDTYPE' in self.tableOptions['table'].upper()) or ('USER' in self.tableOptions['table'].upper()) else df_r_latest.filter((df_r_latest.IsDeleted != "TRUE") & (df_r_latest.IsDeleted != "true"))
                        src_ab_i_cnt = df_r_latest_no_del.count()
                    else:
                        path = self.s3_manager.get_raw_prev_date_path(self.tableOptions, prev_date[0:4]+'-'+prev_date[4:6]+'-'+prev_date[6:8])
                        df_rc_prev = self.s3_manager.read_s3('parquet', [path], self.tableOptions, self.columns_manager)

                        src_d_cnt = 0 if ('RECORDTYPE' in self.tableOptions['table'].upper()) or ('USER' in self.tableOptions['table'].upper()) else df.filter((df.IsDeleted == "TRUE") | (df.IsDeleted == "true")).count()
                        src_u_cnt = df.join(df_rc_prev, [*pk],"inner").count()
                        src_i_cnt = df.join(df_rc_prev, [*pk],"left_anti").count()

                        tgt_d_cnt = 0 if ('RECORDTYPE' in self.tableOptions['table'].upper()) or ('USER' in self.tableOptions['table'].upper()) else df_rc_latest.filter((df_rc_latest.IsDeleted == "TRUE") | (df_rc_latest.IsDeleted == "true")).count()
                        tgt_u_cnt = df_rc_latest.join(df_rc_prev, [*pk],"inner").count()
                        tgt_i_cnt = df_rc_latest.join(df_rc_prev, [*pk],"left_anti").count()
                        
                        df_r_latest_no_del = df_r_latest if ('RECORDTYPE' in self.tableOptions['table'].upper()) or ('USER' in self.tableOptions['table'].upper()) else df_r_latest.filter((df_r_latest.IsDeleted != "TRUE") & (df_r_latest.IsDeleted != "true"))
                        src_ab_i_cnt = df_r_latest_no_del.join(df_rc_prev, [*pk],"left_anti").count()

                audit_ins = self.ods_manager.insert_auditlog(
                    prcs_job_exec_id = self.tableOptions['prcs_job_id'],
                    tbl_nme = self.tableOptions['table'],
                    exec_tbl_tp = 'RAW_RC_TBL',
                    src_cnt = r_cnt,
                    tgt_cnt = rc_cnt,
                    src_loc = self.tableOptions['s3_raw_bucket']+ self.tableOptions['s3_raw_path'],
                    tgt_loc = self.tableOptions['s3_raw_bucket']+ self.tableOptions['s3_raw_current_path'],
                    src_i_cnt=src_i_cnt,
                    src_u_cnt=src_u_cnt,
                    src_d_cnt=src_d_cnt,
                    tgt_i_cnt=tgt_i_cnt,
                    tgt_u_cnt=tgt_u_cnt,
                    tgt_d_cnt=tgt_d_cnt
                    )
                audit_ins_sts = json.loads(audit_ins)['statusCode']
                audit_ins_body = json.loads(audit_ins)['body']

                if int(audit_ins_sts) != 200:
                    write_log(__file__, "raw_rawcurrent_check()", 'error', f'can not update r->rc audit info for process_id. response is{audit_ins_body}')
                    send_email = self.email.email_notification(
                                aws_region = self.common_config["AWS_REGION"],
                                arn = self.common_config["ARN"],
                                custom_message = f'raw_rawcurrent_check():: can not update r->rc audit info for process_id. response is{audit_ins_body}',
                                env = self.env,
                                subject = f"ERROR::Data_Ingestion_Process::"
                            )
                    #exit(1) check with Team if failing here is better or making the table as complete and continuing is better
                    return None
                else:
                    prcs_id = audit_ins_body["PRCS_JOB_EXEC_ID"]
                    write_log(__file__, "raw_rawcurrent_check()", 'info', f'inserted r->rc check into audit log for job Id: {prcs_id}')

                    check_flag = True if src_ab_i_cnt == tgt_i_cnt else False
                    return check_flag
            else:
                write_log(__file__, "raw_rawcurrent_check()", 'info', f'either raw or raw current dataframe is empty')
                #exit(1) check with Team if failing here is better or making the table as complete and continuing is better
                return None
        except Exception as e :
            send_email = self.email.email_notification(
                                aws_region = self.common_config["AWS_REGION"],
                                arn = self.common_config["ARN"],
                                custom_message = f'raw_rawcurrent_check():: following exception occured in raw_rc_check\n{traceback.format_exc()}',
                                env = self.env,
                                subject = f"ERROR::Data_Ingestion_Process::"
                            )
            print(f'following exception occured in raw_rc_check\n{traceback.format_exc()}')
            exit(1)