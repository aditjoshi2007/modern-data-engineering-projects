import argparse
import json
import sys
from datetime import datetime, timedelta

import boto3
import psycopg2
import pytz
from botocore.config import Config as boto3_config
from managers_v5.console_manager import write_log, write_message
from managers_v5.get_secrets import get_secret
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace

spark=SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sql_context=SQLContext(spark.sparkContext)


def get_config_doc(bucket_in, key_in):
    """
    Get config file content from s3 bucket

    Parameters
    ----------
    bucket_in (string): String representing s3 bucket name
    key_in (string): String repersenting s3 prefix of config json filename

    Returns
    -------
    config (dict): Dict representing json file content
    """
    s3_client=boto3.client('s3')
    s3_object=s3_client.get_object(Bucket=bucket_in, Key=key_in)
    s3_file_content=s3_object['Body'].read().decode('utf-8')
    config=json.loads(s3_file_content)
    return config


def get_env_from_aws_acct():
    """
    Get current environment and aws account id

    Parameters
    ----------
    None

    Returns
    -------
    env (string): String representing current environment
    account_id (string): String representing current aws account id
    """
    return env, account_id


def get_db_postgress_cur():
    """
    Get reports Postgres DB psycopg2 connection

    Parameters
    ----------
    config_doc (dict): Dict containing Postgres DB details

    Returns
    -------
    conn (Connection Object): Object representing the connection to Postgres database and allows you to interact with it
    """
    try:
        conn=psycopg2.connect(database=<db_name>,
                              host=<host>,
                              port=<port>,
                              user=<user>,
                              password=<pwd>)
        return conn
    except Exception as get_db_conn_cur_exp:
        write_log(__file__, "get_db_postgress_cur", 'error',
                  f'Connection to reports postgres DB failed: {str(get_db_conn_cur_exp)}')
        sys.exit(1)


def delete_data_postgresql(data):
    """
    Delete data from reports Postgres DB using psycopg2 connection for inserts and updates

    Parameters
    ----------
    data (rdd): rdd containing pk values of table

    Returns
    -------
    None
    """
    delete_command="""delete from {} where id='{}' and number='{}' and task_no='{}'"""
    conn=get_db_postgress_cur()
    cur=conn.cursor()
    for x in data:
        cur.execute(delete_command.format(pg_gold_tbl, x.id, x.number, x.task_no))
    conn.commit()
    conn.close()


def run_pg_sp():
    """
    Triggers stored procedure from staging schema to commit inserts and updates from staging table to main table

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    sp_call_stmt=f"call {pg_sp}('{pg_gold_tbl.split('.')[1]}')"
    conn=get_db_postgress_cur()
    cur=conn.cursor()
    cur.execute(sp_call_stmt)
    conn.commit()
    conn.close()


def s3_clean_up():
    """
    Cleans up S3 bucket to maintain only last two date partitions of table
    data and empty the S3 tempDir location used for redshift connection

    Parameters
    ----------
    env (str): strings representing current environment
    acct (str): strings representing current aws account

    Returns
    -------
    None
    """

    date_list=[]
    date_format="%Y-%m-%d"

    # Get max two date present in s3 curated bucket of table data
    for obj in list(bucket_obj.objects.filter(Prefix=f'{s3_prefix}')):
        date_list.append(obj.key.split('/')[2])

    distinct_dates=list(set(date_list))
    previous_dates=[datetime.strptime(date_str, date_format) for date_str in distinct_dates]
    previous_dates.sort(reverse=True)
    try:
        cleanup_dates=[previous_dates[0].strftime(date_format), previous_dates[1].strftime(date_format)]
        write_log(__file__, "s3_clean_up()", 'info', f'Max dates present in s3 are {str(cleanup_dates)}')

        if len(cleanup_dates) < 2:
            write_log(__file__, "s3_clean_up()", 'info', f'S3 cleanup not needed due to insufficient data')
        else:
            # Delete all the other date partition and only leave latest max two days of data in s3
            # curated bucket of table data
            for obj in list(bucket_obj.objects.filter(Prefix=f'{s3_prefix}')):
                if obj.key.split('/')[2] not in cleanup_dates:
                    s3.Object(s3_bucket, obj.key).delete()
            write_log(__file__, "s3_clean_up()", 'info', f'S3 cleanup finished successfully')
    except:
        write_log(__file__, "s3_clean_up()", 'info', f'S3 cleanup not needed due to insufficient data')
        write_log(__file__, "s3_clean_up()", 'info', f'Previous dates : {str(distinct_dates)}')

    # Delete all files under tempDir s3 location used for Redshift connection
    for obj in list(bucket_obj.objects.filter(Prefix=f'{cleanup_tmp_s3_prefix}')):
        s3.Object(s3_bucket, obj.key).delete()
    write_log(__file__, "s3_clean_up()", 'info', f'Temp S3 cleanup finished successfully')


def main_init():
    """
    Loads initial load in reports PostgresSQL DB for report table
    """
    write_log(__file__, "main_init()", 'info', f'Initiating full load for reports table')

    # Read view from schema of redshift
    df=sql_context.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", <url>) \
        .option("dbtable", <dbtable>) \
        .option("user", <user>) \
        .option("password", <pwd>) \
        .option("aws_iam_role", <arn>) \
        .option("tempdir", <tempdir>) \
        .load()

    if df.count() == 0:
        write_log(__file__, "main_init()", 'info', f'No data found in Redshift view {redshift_tbl}')
        exit(0)

    # Filter initial load dataframe to remove UTF-8 null characters
    new_changed_df=df.select(
        *(regexp_replace(f.col(c), null, '').alias(c) if c in string_columns else c for c in df.columns))

    write_log(__file__, "main_init()", 'info', f'Writing full load for reports in postgres DB')

    # Write initial load dataframe to postgres db
    new_changed_df.repartition(30).write.jdbc(url=postgres_url, table=pg_gold_tbl, mode="append",
                                              properties=postgres_properties)

    # Write redshift data to s3 for next days cdc processing
    new_changed_df.write.mode("overwrite").parquet(
        f"s3://{s3_bucket}/{s3_prefix}{yesterday_date}/")

    write_log(__file__, "main_init()", 'info',
              f'Writing reports data in postgres DB completed successfully')
    write_log(__file__, "main_init()", 'info', f'Initiating s3 cleanup process')

    # Clean up s3 tempDir used for getting data to S3 and only maintain max 2 days of data in S3
    s3_clean_up()


def main_snapshot():
    """
    Loads reports data in PostgresSQL DB for report table
    """
    write_log(__file__, "main_snapshot()", 'info', f'Initiating snapshot data load for reports table')
    date_list=[]

    # Read view from schema of redshift
    df=sql_context.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", <url>) \
        .option("dbtable", <dbtable>) \
        .option("user", <user>) \
        .option("password", <pwd>) \
        .option("aws_iam_role", <arn>) \
        .option("tempdir", <tempdir>) \
        .load()

    if df.count() == 0:
        write_log(__file__, "main_snapshot()", 'info', f'No data found in Redshift view {redshift_tbl}')
        exit(0)

    # Filter snapshot dataframe to remove UTF-8 null characters
    latest_df=df.select(
        *(regexp_replace(f.col(c), null, '').alias(c) if c in string_columns else c for c in df.columns))

    latest_df.persist()
    # Write redshift data to s3 for next days cdc processing
    latest_df.write.mode("overwrite").parquet(f"s3://{s3_bucket}/{s3_prefix}{yesterday_date}/")

    # Retrieve previous date existing in s3 curated
    for obj in list(bucket_obj.objects.filter(Prefix=f'{s3_prefix}')):
        date_list.append(obj.key.split('/')[2])
    distinct_dates=list(set(date_list))
    previous_dates=[datetime.strptime(date, "%Y-%m-%d") for date in distinct_dates]
    previous_dates.sort(reverse=True)
    str_previous_dates=[date.strftime("%Y-%m-%d") for date in previous_dates]
    previous_date=str_previous_dates[1]

    write_log(__file__, "main_snapshot()", 'info', f'Previous date for snapshot data load is {previous_date}')

    # Fetch data from previous days (i.e. current_day-2) data from s3
    previous_df=spark.read.format('parquet').schema(latest_df.schema).load(
        f's3://{s3_bucket}/{s3_prefix}{previous_date}/',
        recursiveFileLookup=True)

    write_log(__file__, "main_snapshot()", 'info',
              f'Writing latest data for reports table in bronze layer of postgres DB')

    # Update the properties to truncate the table
    postgres_properties["truncate"] = "true"

    # Filter out constant columns : col1 and col2 from current_day-1 and current_day-2 dfs
    latest_df_select=latest_df.select(select_columns)
    previous_df_select=previous_df.select(select_columns)

    # Subtract current_day-1 and current_day-2 dfs to get the delta records
    changed_df=latest_df_select.subtract(previous_df_select)

    # Once delta records are identified add constant columns back to delta records
    changed_df=changed_df.withColumn('col1', f.lit(datetime.now())).withColumn('col2',
                                                                                         f.lit(datetime.now().date()))
    # Write data to bronze layer table for reports in Postgres DB
    changed_df.repartition(30).write.jdbc(url=postgres_url, table=pg_staging_tbl, mode="overwrite",
                                          properties=postgres_properties)

    write_log(__file__, "main_snapshot()", 'info',
              f'Writing latest data for reports table in bronze layer of postgres DB completed successfully')

    # Create temp dfs of current_day-1 and current_day-2 data with only PK columns
    temp_main_df=latest_df.select(*table_primary_keys)
    temp_prev_df=previous_df.select(*table_primary_keys)

    # Delete keys, do repartition the dataframe since we need to restrict number of connections
    delete_data=temp_prev_df.subtract(temp_main_df).repartition(20)
    delete_data=delete_data.withColumn('col1', regexp_replace('col1', '\'', '\'\''))

    write_log(__file__, "main_snapshot()", 'info', f'deleting previous data from gold layer of postgres DB')

    # Deleting the data using each partition
    delete_data.rdd.foreachPartition(delete_data_postgresql)

    write_log(__file__, "main_snapshot()", 'info', f'Trigger SP to commit update and inserts in reports table')
    # Run stored procedure to upsert data into reports table in gold layer
    run_pg_sp()

    write_log(__file__, "main_snapshot()", 'info', f'Initiating s3 cleanup process')
    # Clean up s3 tempDir used for getting data to S3 and only maintain max 2 days of data in S3
    s3_clean_up()


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--config_bucket', help="use dev,tst,mdl or prd config bucket. Required Field", required=True)
    parser.add_argument('--config_key', help="use dev,tst,mdl or prd s3 json file path. Required Field", required=True)
    parser.add_argument('--load_type', help="Choose load type init/snapshot. Required Field",
                        required=True)

    args=parser.parse_args()
    if args.config_bucket is None:
        error_msg="config_bucket is required. use env config bucket"
        print(error_msg)
        raise ValueError(error_msg)
        # raise MissingArgError
    else:
        config_bucket=args.config_bucket
    if args.config_key is None:
        error_msg="config_key is required. use env s3 json file path"
        print(error_msg)
        raise ValueError(error_msg)
        # raise MissingArgError
    else:
        config_key=args.config_key

    if args.load_type is None:
        error_msg="Choose either initi/snapshot for ETL load type"
        print(error_msg)
        raise ValueError(error_msg)
        # raise MissingArgError
    else:
        load_type=args.load_type

    # Initialize s3, redshift and postgres variables
    config_doc=get_config_doc(config_bucket, config_key)
    env, acct=get_env_from_aws_acct()
    s3=boto3.resource('s3')
    null=u'\u0000'
    cst_timezone=pytz.timezone('your_tz')
    yesterday_date=(datetime.now(cst_timezone).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    s3_bucket=<get_from_conf_file>
    bucket_obj=s3.Bucket(s3_bucket)
    s3_prefix=<get_from_conf_file>
    cleanup_tmp_s3_prefix=<get_from_conf_file>
    redshift_username=<get_from_conf_file>
    redshift_password=<get_from_conf_file>
    redshift_url=<get_from_conf_file>
    redshift_tbl=<get_from_conf_file>
    redshift_iam_role_arn=<get_from_conf_file>
    redshift_s3_temp_dir=<get_from_conf_file>
    postgres_config=<get_from_conf_file>
    postgres_properties=<get_from_conf_file>
    postgres_url=<get_from_conf_file>
    table_primary_keys=<get_from_conf_file>
    string_columns=<get_from_conf_file>
    select_columns=<get_from_conf_file>
    pg_bronze_tbl=<get_from_conf_file>
    pg_gold_tbl=<get_from_conf_file>
    pg_sp=<get_from_conf_file>

    write_message(
        f'Started processing reports for date: {(datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")}')
    # Call main depending on load_type
    if load_type == "init":
        main_init()
    else:
        main_snapshot()
    write_message(
        f'reports table for date: {(datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")} has been ingested in reports Postgres DB')
