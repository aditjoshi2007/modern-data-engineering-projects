import traceback

class RCQueryManager: 
    
    def retrieve_current_snapshot_sql_by_tbl_name(self, tbl_name):
        if tbl_name.lower() == "tbl_1":
            sql = self.retrieve_current_snapshot_sql_tbl_1()
        elif tbl_name.lower() == "tbl_2":
            sql = self.retrieve_current_snapshot_sql_tbl_2()
        else:
            sql = ""

        return sql

    def retrieve_current_snapshot_sql_tbl_1(self):
        sql = f"""
            SELECT  A.*
            FROM     (
            SELECT   A.*,
                    ROW_NUMBER() OVER (PARTITION BY A.pk_col, A.pk_col ORDER BY CHANGE_SEQ_COL DESC, TIMESTAMP_COL DESC  mod_ts_col) AS ROWID
            FROM     new AS A
                                ) AS A
            WHERE    ROWID = 1
            AND      OPERATION_COL != 'DELETE'

            """

        return sql

    def retrieve_current_snapshot_sql_tbl_2(self):
        mod_ts = ", MOD_TS_COL DESC"
        sql = f"""
        SELECT   A.*
            FROM     (
            SELECT   A.*,
                    ROW_NUMBER() OVER (PARTITION BY A.pk_col, A.pk_col, A.pk_col ORDER BY CHANGE_SEQ_COL DESC, TIMESTAMP_COL DESC {mod_ts}) AS ROWID
            FROM     new AS A
                    ) AS A
            WHERE    ROWID = 1
            AND      OPERATION_COL != 'DELETE'
        """

        return sql