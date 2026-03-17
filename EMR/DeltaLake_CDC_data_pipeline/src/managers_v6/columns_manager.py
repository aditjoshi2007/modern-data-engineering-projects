import traceback
from datetime import date, datetime
from pyspark.sql import functions as f
from pyspark.sql.functions import to_timestamp, lit, col, coalesce, substring, date_format
from pyspark.sql.types import StringType, TimestampType, DateType

from managers_v6.console_manager import write_log
from managers_v6.s3_manager import S3Manager, Mode
from pyspark.sql.functions import col, sha2, concat_ws, when, collect_list


class ColumnsManager:
    # ----------------------------------------------------------------------------------------------------------------------
    # Remove non-ascii characters from columns (cols)
    # ----------------------------------------------------------------------------------------------------------------------
    def data_cleansing_for_non_ascii_characters(self, df, cols):
        # Apply rule on multiple columns if needed.
        for col in cols:
            df=df.withColumn(col["src_col"],
                             f.when(
                                 f.col(col["src_col"]).isNotNull(),
                                 f.regexp_replace(f.col["src_col"](col["src_col"]), '[^\x20-\x7E]', '')
                             ).otherwise(None)
                             )

        return df

    def data_cleansing_for_timestamp(self, df, cols):
        # Apply rule on multiple columns if needed.
        for col in cols:
            df=df.withColumn(col, df[col].cast(StringType()))
            df=df.withColumn(col, substring(df[col], 1, 26))
            # df=df.withColumn(col, to_timestamp(df[col], 'yyyy-MM-dd HH:mm:ss'))
            # df=df.withColumn(col, date_format(df[col], 'yyyy-MM-dd HH:mm:ss'))
        return df

    def data_cleansing_for_timestamp_date(self, df, cols):
        # Apply rule on multiple columns if needed.
        for col in cols:
            df=df.withColumn(col, df[col].cast(DateType()))
            # df=df.withColumn(col, substring(df[col], 1, 19))
            # df=df.withColumn(col, to_timestamp(df[col], 'yyyy-MM-dd HH:mm:ss'))
            # df=df.withColumn(col, date_format(df[col], 'yyyy-MM-dd HH:mm:ss'))
        return df

    def clean_date(self, df, src_col):
        cleansed_col=f.when(f.to_date(df[src_col], "yyyyMMdd").isNotNull(),
                            f.date_format(f.to_date(df[src_col], "yyyyMMdd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy-MM-dd").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy-MM-dd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy MM dd").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy MM dd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "MM/dd/yyyy").isNotNull(),
            f.date_format(f.to_date(df[src_col], "MM/dd/yyyy"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy MMMM dd").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy MMMM dd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy MMMM dd E").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy MMMM dd E"), "yyyy-MM-dd")).otherwise("1900-01-01")
        return cleansed_col

    def clean_first_name(self, df, src_col):
        cleansed_col=f.when(
            df[src_col].isNotNull() & (f.length(f.split(df[src_col], " ")[1]) == 1),
            f.upper(f.regexp_replace(f.split(df[src_col], " ")[0], "[^a-zA-Z\s\-]+", ""))
        ).when(
            df[src_col].isNotNull() & (f.length(f.split(df[src_col], " ")[1]) > 1),
            f.upper(f.regexp_replace(df[src_col], "[^a-zA-Z\s\-]+", ""))
        ).when(
            df[src_col].isNotNull() & (f.size(f.split(df[src_col], " ")) == 1),
            f.upper(f.regexp_replace(df[src_col], "[^a-zA-Z\s\-]+", ""))
        ).otherwise(None)
        return cleansed_col

    def clean_middle_name(self, df, src_col):
        cleansed_col=f.when(
            df[src_col].isNotNull(),
            f.upper(f.regexp_replace(df[src_col], "[^a-zA-Z]+", ""))
        ).otherwise(None)
        return cleansed_col

    def clean_last_name(self, df, src_col):
        cleansed_col=f.when(
            (df[src_col].isNotNull()) | (f.length(df[src_col]) > 1),
            f.upper(f.trim(df[src_col]))
        ).otherwise(None)
        return cleansed_col

    def clean_suffix(self, df, src_col):
        cleansed_col=f.when(
            df[src_col].isNotNull(),
            f.upper(f.regexp_replace(df[src_col], "[^a-zA-Z\.]+", ""))
        ).otherwise(None)
        return cleansed_col

    def data_cleansing_for_date(self, df, cols):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]

            df=df.withColumn(tgt_col, self.clean_date(df, src_col))
        return df

    def data_cleansing_for_gender(self, df, cols, gender_df):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            df=df.join(gender_df,
                       [df[src_col] == gender_df.value, df.id == gender_df.id],
                       "leftouter")
            df=df.withColumn(
                tgt_col,
                f.when(f.col(src_col).isNotNull(), df.text).otherwise(None)
            ).drop("value", "text", "id")
        return df

    def data_cleansing_for_marital_status(self, df, cols, marital_status_df):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            df=df.join(marital_status_df,
                       [df[src_col] == marital_status_df.value, df.id == marital_status_df.id],
                       "leftouter")
            df=df.withColumn(
                tgt_col,
                f.when(f.col(src_col).isNotNull(),
                       f.when(df.text.isNotNull(), df.text).otherwise("N/A")).otherwise("N/A")
            ).drop("value", "text", "id")
        return df

    def data_cleansing_for_telephone(self, df, cols):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            df=df.withColumn(
                tgt_col,
                f.when(
                    f.col(src_col).isNotNull(),
                    f.regexp_replace(src_col, "[^0-9]", "")
                    # regex to retain "-" as well
                    # sql_func.regexp_replace(src_col, "[^0-9\-]", "")
                ).otherwise(None)
            )
        return df

    def data_cleansing_for_taxid(self, df, cols):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            df=df.withColumn(
                tgt_col + "_hash",
                f.when(
                    f.col(src_col).isNotNull(),
                    f.md5(f.regexp_replace(src_col, "[^0-9\-]", ""))
                ).otherwise(None)
            ).withColumn(
                tgt_col,
                f.when(
                    f.col(src_col).isNotNull(),
                    f.regexp_replace(src_col, "[^0-9\-]", "")
                ).otherwise(None)
            )
        return df

    def data_cleansing_for_address(self, df, cols):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            action=col["action"]

            if action == "address_line_1" or action == "address_line_2":
                df=df.withColumn(
                    tgt_col,
                    f.when(
                        f.col(src_col).isNotNull(),
                        f.upper(f.trim(f.col(src_col)))
                    ).otherwise(None)
                )
            elif action == "city":
                df=df.withColumn(
                    tgt_col,
                    f.when(
                        f.col(src_col).isNotNull(),
                        f.upper(
                            f.regexp_replace(src_col, "[^a-zA-Z\.\-\s]+", "")
                        )
                    ).otherwise(None)
                )
            elif action == "zipcode5" or action == "zipcode4":
                df=df.withColumn(
                    tgt_col,
                    f.when(
                        f.col(src_col).isNotNull(),
                        f.upper(f.regexp_replace(src_col, "[^a-zA-Z0-9]+", ""))
                    ).otherwise(None)
                )
        return df

    def data_cleansing_for_name(self, df, cols):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            action=col["action"]
            has_suffix=True if len(src_col) > 3 else False

            if action == "full_name":
                if has_suffix:
                    df=df.withColumn(tgt_col,
                                     f.concat_ws(' ',
                                                 self.clean_first_name(df, src_col[0]),
                                                 self.clean_middle_name(df, src_col[1]),
                                                 self.clean_last_name(df, src_col[2]),
                                                 self.clean_suffix(df, src_col[3])
                                                 )
                                     )
                else:
                    df=df.withColumn(tgt_col,
                                     f.concat_ws(' ',
                                                 self.clean_first_name(df, src_col[0]),
                                                 self.clean_middle_name(df, src_col[1]),
                                                 self.clean_last_name(df, src_col[2]),
                                                 )
                                     )
                df=df.withColumn(
                    "flag_valid_fullname",
                    f.when((self.clean_first_name(df, src_col[0]).isNotNull() & self.clean_last_name(df, src_col[
                        2]).isNotNull()), 1)
                    .otherwise(0)
                )
            elif action == "first_name":
                df=df.withColumn(
                    tgt_col,
                    self.clean_first_name(df, src_col)
                ).withColumn(
                    f'flag_valid_{src_col}',
                    f.when(f.col(src_col).isNotNull(), 1).otherwise(0)
                )
            elif action == "middle_name":
                df=df.withColumn(
                    tgt_col,
                    self.clean_middle_name(df, src_col)
                ).withColumn(
                    f'flag_valid_{src_col}',
                    f.when(f.col(src_col).isNotNull(), 1).otherwise(0)
                )
            elif action == "last_name":
                df=df.withColumn(
                    tgt_col,
                    self.clean_last_name(df, src_col)
                ).withColumn(
                    f'flag_valid_{src_col}',
                    f.when(f.col(src_col).isNotNull(), 1).otherwise(0)
                )
            elif action == "suffix":
                df=df.withColumn(
                    tgt_col,
                    self.clean_suffix(df, src_col)
                )
        return df

    def data_cleansing_for_state(self, df, cols, states_df):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            df=df.join(states_df, [df[src_col] == states_df.value, df.id == states_df.id],
                       "leftouter")
            df=df.withColumn(
                tgt_col,
                f.when(
                    f.col(src_col).isNotNull(),
                    df.value
                ).otherwise(None)
            ).drop("df.value", "df.text", "states_df.id")
        return df

    def data_cleansing_for_country(self, df, cols, country_df):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]
            df=df.join(country_df, [df[src_col] == country_df.value, df.id == country_df.id],
                       "leftouter")
            df=df.withColumn(
                tgt_col,
                f.when(
                    f.col(src_col).isNotNull(),
                    df.text
                ).otherwise(None)
            ).drop("df.value", "df.text", "country_df.id")
        return df

    def data_cleansing_for_leading_and_trailing_spaces(self, df, cols):
        for col in cols:
            src_col=col["src_col"]
            tgt_col=col["tgt_col"]

            df=df.withColumn(
                tgt_col + "_new", f.trim(f.col(src_col)))
            df=df.withColumnRenamed(src_col, tgt_col).withColumnRenamed(tgt_col + "_new", src_col)
        return df

    def data_cleansing_for_rtrim(self, s3_manager, df, cols):
        rtrim_col_list=[]
        for col in cols:
            rtrim_col_list.append(col["src_col"])
        sql=''
        db_temp_view="db_temp_view"
        for col in df.columns:
            if col in rtrim_col_list:
                sql+=f'rtrim({col}) as {col}, '
            else:
                sql+=f'{col}, '

        sql=sql[:-2]  # remove the last 2 chars (", ")
        sql=f'select {sql} from {db_temp_view}'
        df.createOrReplaceTempView(db_temp_view)
        df=s3_manager.read_with_sql(sql)
        return df

    def clean_date(self, df, src_col):
        cleansed_col=f.when(f.to_date(df[src_col], "yyyyMMdd").isNotNull(),
                            f.date_format(f.to_date(df[src_col], "yyyyMMdd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy-MM-dd").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy-MM-dd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy MM dd").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy MM dd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "MM/dd/yyyy").isNotNull(),
            f.date_format(f.to_date(df[src_col], "MM/dd/yyyy"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy MMMM dd").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy MMMM dd"), "yyyy-MM-dd")).when(
            f.to_date(df[src_col], "yyyy MMMM dd E").isNotNull(),
            f.date_format(f.to_date(df[src_col], "yyyy MMMM dd E"), "yyyy-MM-dd")).otherwise("1900-01-01")
        return cleansed_col

    # ----------------------------------------
    # Add the core columns
    #   df – Dataframe
    #   the_date - the load date
    #   load_time - the load time
    #   schema - current schema
    # ----------------------------------------
    def add_additional_columns(self, df, the_date, load_time, schema=None):
        # add additional columns
        dateFormat=datetime.strptime(the_date, "%Y%m%d").strftime("%Y-%m-%d")
        if schema != None:
            df=df.withColumn("schema_name", f.lower(f.lit(schema)))
        df=df.withColumn("load_date", f.lower(f.lit(dateFormat)))
        df=df.withColumn("load_time", f.lower(f.lit(load_time)))

        return df

    # ----------------------------------------
    # Add standard columns
    #   df – Dataframe
    #   timestamp - the timestamp
    # ----------------------------------------
    def add_standard_columns(self, df, tableOptions):
        # Drop columns if already exists  (DMS adds them)
        df=df.drop(col("change_seq")).drop(col("operation")).drop(col("transaction_id")).drop(
            col("timestamp"))

        # In DMS, the standard columns are created in the end of the table columns
        if 'dms' in (tableOptions["s3_landing_folder"]).lower():
            df=df.select(
                "*",
                lit('00000000000000000000000000000000000').alias("change_seq"),
                lit("INSERT").alias("operation"),
                lit("").cast(StringType()).alias("transaction_id"),
                lit(date.today().strftime('%Y-%m-%d 00:00:00.000')).alias("timestamp"))
        else:
            # In Attunity, the standard columns are created in the begining of the table columns
            df=df.drop(col("change_mask")).drop(col("change_oper")).drop(
                col("stream_position")).drop(
                col("partition_name"))
            df=df.select(
                lit("").cast(StringType()).alias("change_mask"),
                lit("I").alias("change_oper"),
                lit('00000000000000000000000000000000000').alias("change_seq"),
                lit("INSERT").alias("operation"),
                lit("").cast(StringType()).alias("stream_position"),
                lit("").cast(StringType()).alias("transaction_id"),
                lit(date.today().strftime('%Y-%m-%d 00:00:00.000')).alias("timestamp"),
                lit("").cast(StringType()).alias("partition_name"),
                "*")
        return df

    # ----------------------------------------
    # Add process_id column
    #   process_id - add process_id column if present in table options
    # ----------------------------------------
    @staticmethod
    def add_exec_ids(df, tableOptions):

        if tableOptions.get("process_id") and tableOptions.get("prcs_job_id"):
            write_log(__file__, "add_exec_ids()", 'info', f'Adding process_id and job id columns')
            df=df.withColumn("process_id", f.lit(tableOptions["process_id"])).withColumn("job_id", f.lit(
                tableOptions["prcs_job_id"]))
            return df
        else:
            return df

    # ----------------------------------------
    # Data type casting functionality to add for columns. converting to string
    # ----------------------------------------
    @staticmethod
    def convert_columns_string_data_type(df, tableOptions):

        if tableOptions.get("dataTypeCastingToString"):
            write_log(__file__, "convert_columns_string_data_type()", 'info', f'Data Type casting to string')
            dfs=df.select(*map(lambda col: df[col].cast('string'), df.columns))
            return dfs
        else:
            return df

    def get_helper_df(self, s3_manager, tableOptions, query):
        paths_l=[]
        if s3_manager.mode == Mode.InitialLoad:
            for schema in tableOptions['helper_schema']:
                paths=s3_manager.get_landing_path(tableOptions, schema, tableOptions["helper_table_name"],
                                                  s3_manager.mode)
                paths_l=paths_l + paths
            df_temp=s3_manager.read_s3('json', paths_l, tableOptions, columns_manager=self, is_helper=True)
            df_temp.createOrReplaceTempView("helper")
        elif s3_manager.mode == Mode.CDC:
            df_temp=s3_manager.read_with_sql(
                f'select * from raw_current_layer.{tableOptions["helper_table_name"]}')
            df_temp.createOrReplaceTempView("helper")

        df=s3_manager.read_with_sql(query)
        df_temp.unpersist(blocking=False)

        return df

    # ----------------------------------------
    # Processing cleansing rules
    #   df – Dataframe
    #   tableOptions - table options
    # ----------------------------------------
    def clean_dataframe(self, mode, df, tableOptions):
        write_log(__file__, "clean_dataframe()", 'info', f'started cleaning dataframe')
        s3_manager=S3Manager(mode, tableOptions)

        for fc_cleansingrule in tableOptions["fixedColumnsCleansingRules"]:
            if fc_cleansingrule["rule"] == "ascii_only":
                df=self.data_cleansing_for_non_ascii_characters(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "timestamp":
                df=self.data_cleansing_for_timestamp(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "timestamp_date":
                df=self.data_cleansing_for_timestamp_date(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "dob" or fc_cleansingrule["rule"] == "date":
                df=self.data_cleansing_for_date(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "gender":
                query=fc_cleansingrule["helper_table_query"]
                gender_df=self.get_helper_df(s3_manager, tableOptions, query)
                df=self.data_cleansing_for_gender(df, fc_cleansingrule["columns"], gender_df)
            elif fc_cleansingrule["rule"] == "marital_status":
                query=fc_cleansingrule["helper_table_query"]
                marital_status_df=self.get_helper_df(s3_manager, tableOptions, query)
                df=self.data_cleansing_for_marital_status(df, fc_cleansingrule["columns"], marital_status_df)
            elif fc_cleansingrule["rule"] == "telephone":
                df=self.data_cleansing_for_telephone(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "taxid":
                df=self.data_cleansing_for_taxid(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "address":
                df=self.data_cleansing_for_address(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "name":
                df=self.data_cleansing_for_name(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "state":
                query=fc_cleansingrule["helper_table_query"]
                states_df=self.get_helper_df(s3_manager, tableOptions, query)
                df=self.data_cleansing_for_state(df, fc_cleansingrule["columns"], states_df)
            elif fc_cleansingrule["rule"] == "country":
                query=fc_cleansingrule["helper_table_query"]
                country_df=self.get_helper_df(s3_manager, tableOptions, query)
                df=self.data_cleansing_for_country(df, fc_cleansingrule["columns"], country_df)
            elif fc_cleansingrule["rule"] == "trim_whitespace":
                df=self.data_cleansing_for_leading_and_trailing_spaces(df, fc_cleansingrule["columns"])
            elif fc_cleansingrule["rule"] == "rtrim":
                df=self.data_cleansing_for_rtrim(s3_manager, df, fc_cleansingrule["columns"])
            else:
                print('error in cleaning rule')
                exit(1)
        # Return the dataframe
        return df

    # ----------------------------------------
    # Process data types
    #   df – Dataframe
    #   fixed_columns – List of columns to be processed
    # ----------------------------------------
    def update_datatypes(self, df, tableOptions):
        write_log(__file__, "update_datatypes()", 'info', f'started setting fixed datatypes')

        for col_detail in tableOptions["fixed_columns"]:
            try:
                df=df.withColumn(col_detail["column"], df[col_detail["column"]].cast(col_detail["data_type"]))
            except Exception as e:
                write_log(__file__, "update_datatypes()", 'error',
                          f'updating data type for column: {col_detail} \n {traceback.format_exc()}')
                raise Exception(f"Error updating data type for column: {col_detail}. Error: {str(e)}")

        if tableOptions.get("global_date_cleansing_rule") != None:
            suffixes=tableOptions["global_date_cleansing_rule"]["suffix"]
            for column in df.columns:
                for suffix in suffixes:
                    if column.endswith(suffix):
                        try:
                            print(f"Global date cleanse on >> {column}")
                            df=df.withColumn(f"c_{column}", self.clean_date(df, column))
                        except:
                            print(
                                f"failed at global_date_cleansing_rule. column name={column}. Ignore the error and continue.")
        return df

    # ----------------------------------------
    # Add hash columns based on business columns - for daily full load
    #   df – Dataframe
    # ----------------------------------------
    @staticmethod
    def add_sha_hash(df):
        ops_cols=['OP', 'operation', 'transaction_id', 'timestamp',
                  'process_id', 'job_id', 'schema_name', 'load_date', 'load_time']
        tot_cols=df.columns
        business_cols=[column for column in tot_cols if column not in ops_cols]
        df=df.withColumn("sha_hash", sha2(concat_ws("||", *business_cols), 256))
        return df
