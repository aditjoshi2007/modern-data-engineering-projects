from managers_v6.s3_manager import S3Manager, Mode
from managers_v6.arguments_manager import ArgumentsManager
from managers_v6.rc_custom_query_manager import RCQueryManager
import traceback
from datetime import datetime, timedelta
from managers_v6.console_manager import write_log


class CdcManager:

    def __init__(self, mode=Mode.CDC, tableOptions = None):
        self.mode = mode
        self.s3_manager = S3Manager(self.mode, tableOptions)

    def filter_raw_current(self, df, tableOptions):
        write_log(__file__, "filter_raw_current()", 'info', f'Filtering raw_current to remove duplicate records')
        try:
            df.createOrReplaceTempView("new")
            pk_sort_columns = ArgumentsManager.is_list_empty_or_not_defined(tableOptions["raw_current_pk_sort_columns"], ["timestamp_col desc"])
    
            sort_column = ",".join(pk_sort_columns)

            if '/appflow/' in tableOptions.get('s3_landing_folder').lower():
                if ('RECORDTYPE' in tableOptions['table'].upper()) or ('USER' in tableOptions['table'].upper()):
                    delete_operation = ""
                else:
                    delete_operation = "and IsDeleted != 'true'"
            else:
                delete_operation = "and operation_col NOT IN('DELETE')"
            
            if tableOptions.get("sql_in_code",False) == True:
                rc_qm = RCQueryManager()
                sql = rc_qm.retrieve_current_snapshot_sql_by_tbl_name(tableOptions['table'])
            else:
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
            df_n = self.s3_manager.read_with_sql(sql)

            return df_n
        except Exception as e:
            write_log(__file__, "filter_raw_current()", 'error', f'following exception occurred in filter_raw_current\n{traceback.format_exc(e)}')
            exit(1)

    # get list of calendar dates between the last processed date and run_date
    # if re_run_flag is True it will also include the last_processed_date as well
    @staticmethod
    def get_cdc_calender_dates(last_processed_date, run_date, re_run_flag, prev_exec_tp, start_offset=1):

        write_log(__file__, "get_cdc_calender_dates()", 'info',
                  f'last_processed_date={last_processed_date}, run_date={run_date}, re_run_flag={re_run_flag}')

        if re_run_flag:
            re_run_flag = True if re_run_flag.lower() == 'true' else False

        # this is to avoid overwriting full load run_date in raw folder
        if prev_exec_tp.lower() == 'full_load':
            re_run_flag = False

        start = datetime.strptime(last_processed_date, "%Y%m%d")
        end = datetime.strptime(run_date, "%Y%m%d")
        step = timedelta(days=1)

        start = start if re_run_flag else start + (start_offset * step)

        days = []
        while start <= end:
            days.append(start.strftime('%Y%m%d'))
            start = start + step

        # only process last 7 days of data if data has been missing for more than 7 days
        return days[-7:]

