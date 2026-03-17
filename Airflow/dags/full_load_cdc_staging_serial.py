"""
Full Load & CDC Staging Loader for Amazon Redshift

This module performs:
- S3 file discovery (DMS / AppFlow / Attunity)
- Manifest generation for Redshift COPY
- Execution via Redshift Data API
- Raw-current table materialization

Design principles:
✅ Manifest-driven bulk loads
✅ Restart-safe execution
✅ Service-agnostic ingestion
✅ Scales to billions of rows
"""

import argparse
# import datetime
import json
import time
from datetime import date
from datetime import timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from special_sql import retrieve_current_snapshot_sql_by_tbl_name


# -------------------------------------------------------------------
# Utility: S3 File Discovery
# -------------------------------------------------------------------


def get_file_fullLoad(s3_bucket, subfolders_address, cdc_subfolders_address):
    """
    Discover full-load and CDC files written by AWS DMS.

    Returns:
        fullload_files_list, fullload_content,
        cdc_files_list, cdc_content
    """
    s3_client=boto3.resource('s3')
    src_bucket=s3_client.Bucket(s3_bucket)
    fullload_files_list=[]
    fullload_content=[]
    cdc_files_list=[]
    cdc_content=[]
    for bucket_object_summary in src_bucket.objects.filter(Prefix=subfolders_address[1:]):
        file_name=bucket_object_summary.key
        #        print(file_name)
        if "LOAD" in file_name or "SEGMENT" in file_name:
            fullload_files_list.append('s3://' + s3_bucket + '/' + file_name)
            fullload_content.append(bucket_object_summary.size)
    for cdc_bucket_object_summary in src_bucket.objects.filter(Prefix=cdc_subfolders_address[1:]):
        cdc_file_name=cdc_bucket_object_summary.key
        #        print(file_name)
        cdc_files_list.append('s3://' + s3_bucket + '/' + cdc_file_name)
        cdc_content.append(cdc_bucket_object_summary.size)
    #    if len(cdc_files_list) > 1:
    #        cdc_files_list.pop(0)
    #    if len(cdc_content) > 1:
    #        cdc_content.pop(0)
    return fullload_files_list, fullload_content, cdc_files_list, cdc_content


def get_manifest_json(s3_bucket, subfolders_address):
    s3_client=boto3.resource('s3')
    src_bucket=s3_client.Bucket(s3_bucket)
    json_gz_file=[]
    json_file=[]
    for bucket_object_summary in src_bucket.objects.filter(Prefix=subfolders_address[1:]):
        file_name=bucket_object_summary.key
        if "json.gz" in file_name:
            json_gz_file.append('s3://' + s3_bucket + '/' + file_name)
        elif "json" in file_name:
            json_file.append('s3://' + s3_bucket + '/' + file_name)

    return json_gz_file, json_file


# -------------------------------------------------------------------
# Utility: Redshift Data API Execution
# -------------------------------------------------------------------

def execute_sql_data_api(
        redshift_data_api_client,
        redshift_database_name,
        query,
        redshift_user,
        redshift_cluster_id,
        isSynchronous,
):
    """
    Executes SQL using Redshift Data API with optional synchronous polling.

    Supports:
    - Single SQL statements
    - Batch execution (list of SQL statements)

    Returns:
        (query_status, query_id)
    """
    MAX_WAIT_CYCLES=300
    attempts=0
    # Calling Redshift Data API with executeStatement()
    try:
        res=redshift_data_api_client.execute_statement(
            Database=redshift_database_name, DbUser=redshift_user, Sql=query, ClusterIdentifier=redshift_cluster_id)
    except:
        res=redshift_data_api_client.batch_execute_statement(
            Database=redshift_database_name, DbUser=redshift_user, Sqls=query, ClusterIdentifier=redshift_cluster_id)
    print("***************")
    #    print(res)
    query_id=res["Id"]
    desc=redshift_data_api_client.describe_statement(Id=query_id)
    query_status=desc["Status"]
    print(f"Query status: {query_status} .... for query-->{query}")
    done=False

    # Wait until query is finished or max cycles limit has been reached.
    while not done and isSynchronous and attempts < MAX_WAIT_CYCLES:
        attempts+=1
        time.sleep(60)
        print("wait time count is :", attempts)
        desc=redshift_data_api_client.describe_statement(Id=query_id)
        #        print("*********************")
        #        print(desc)
        #        print("*********************")
        query_status=desc["Status"]

        if query_status == "FAILED":
            raise Exception('SQL query failed:' +
                            query_id + ": " + desc["Error"])

        elif query_status == "ABORTED":
            raise Exception('SQL query ABORTED:' +
                            query_id + ": " + desc["Error"])

        elif query_status == "FINISHED":
            print(f"query status is: {query_status} for query id: {query_id}")

            done=True
            # print result if there is a result (typically from Select statement)
            try:
                if desc['HasResultSet']:
                    response=redshift_data_api_client.get_statement_result(
                        Id=query_id)

                    print(f"Printing response of  query --> {response['Records']}")
            except:
                print(f"Current working... query status is: {query_status} ")

        else:
            print(f"Current working... query status is: {query_status} ")

    # Timeout Precaution
    if done == False and attempts >= MAX_WAIT_CYCLES and isSynchronous:
        print(f"Redshift will run continue the run the query till it finishes execution for query_id  {query_id}. \n")
        query_status="RUNNING"

    return query_status, query_id


# -------------------------------------------------------------------
# SQL Generators (Pure, Testable Functions)
# -------------------------------------------------------------------

def dms_load_fullload(schema_name, table_name, src_schema,
                      manifest_address_file, role, file_type):
    """
    Generates Redshift COPY SQL for DMS full-load data.
    """
    dms_fullload_query=f"""copy {schema_name}.{table_name} ({src_schema}) from '{manifest_address_file}' iam_role '{role}' FORMAT AS {file_type} manifest;"""
    return dms_fullload_query


def dms_load_fullload_encode(schema_name, table_name, src_schema, manifest_address_file, role, file_type):
    """
    Generates Redshift COPY SQL for DMS full-load data.
    """
    dms_fullload_query=f"""copy {schema_name}.{table_name} ({src_schema}) from '{manifest_address_file}' iam_role '{role}' ACCEPTINVCHARS FORMAT AS {file_type} manifest;"""
    return dms_fullload_query


def dms_load_cdc(schema_name, table_name, manifest_address_file, role, file_type, raw_current_schema):
    """
    Generates Redshift COPY SQL for CDC data.
    """
    dms_cdc_query=f"""copy {schema_name}.{table_name} ({raw_current_schema}) from '{manifest_address_file}' iam_role '{role}' FORMAT AS {file_type} manifest;"""
    return dms_cdc_query


def dms_load_cdc_encode(schema_name, table_name, manifest_address_file, role, file_type, raw_current_schema):
    """
    Generates Redshift COPY SQL for CDC data.
    """
    dms_cdc_query=f"""copy {schema_name}.{table_name} ({raw_current_schema}) from '{manifest_address_file}' iam_role '{role}' ACCEPTINVCHARS FORMAT AS {file_type} manifest;"""
    return dms_cdc_query


def attunity_fullload(full_load_file, delta_file, schema_name, table_name, src_schema, role, file_type,
                      raw_current_schema):
    #    full_load_query = f"""copy {schema_name}.{table_name} ({src_schema}) from '{full_load_file}' iam_role '{role}' FORMAT AS {file_type};"""
    #    delta_load_query = f"""copy {schema_name}.{table_name}  ({raw_current_schema}) from '{delta_file}' iam_role '{role}' FORMAT AS {file_type};"""
    full_load_query=f"""copy {schema_name}.{table_name}  from '{full_load_file}' iam_role '{role}' FORMAT AS {file_type} ROUNDEC;"""
    delta_load_query=f"""copy {schema_name}.{table_name}   from '{delta_file}' iam_role '{role}' FORMAT AS {file_type} ROUNDEC;"""
    return full_load_query, delta_load_query


def manifest_fulload_json_gz(manifest_bucket, manifest_key, table_name, fullload_files_list, schema_name, role,
                             file_type):
    manifest_full_key=str(manifest_key) + str(table_name) + "_fullload.manifest"

    ###now preparing for full load
    if len(fullload_files_list) > 0:
        manifest_full_table={}
        manifest_full_table["entries"]=[]
        for i in range(len(fullload_files_list)):
            new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True)
            manifest_full_table["entries"].append(new_item_full)
        new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_full_key,
            Body=new_data_full
        )
        print(f"file {table_name}_fullload.manifest has been created")
        manifest_fullload_file='s3://' + manifest_bucket + "/" + manifest_full_key

        manifest_fullload_query=f"""copy {schema_name}.{table_name}  from '{manifest_fullload_file}' iam_role '{role}' FORMAT AS {file_type} GZIP manifest ROUNDEC;"""

        return manifest_fullload_query


def manifest_fulload_json(manifest_bucket, manifest_key, table_name, fullload_files_list, schema_name, role, file_type):
    manifest_full_key=str(manifest_key) + str(table_name) + "_fullload_json.manifest"

    ###now preparing for full load
    if len(fullload_files_list) > 0:
        manifest_full_table={}
        manifest_full_table["entries"]=[]
        for i in range(len(fullload_files_list)):
            new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True)
            manifest_full_table["entries"].append(new_item_full)
        new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_full_key,
            Body=new_data_full
        )
        print(f"file {table_name}_fullload_json.manifest has been created")
        manifest_fullload_file='s3://' + manifest_bucket + "/" + manifest_full_key

        manifest_fullload_query=f"""copy {schema_name}.{table_name}  from '{manifest_fullload_file}' iam_role '{role}' FORMAT AS {file_type}  manifest ROUNDEC;"""

        return manifest_fullload_query


def manifest_cdc_json_gz(manifest_bucket, manifest_key, table_name, cdc_files_list, schema_name, role, file_type):
    manifest_cdc_key=str(manifest_key) + str(table_name) + "_cdc.manifest"

    ###now preparing for full load
    if len(cdc_files_list) > 0:
        manifest_cdc_table={}
        manifest_cdc_table["entries"]=[]
        for i in range(len(cdc_files_list)):
            new_item_cdc=dict(url=str(cdc_files_list[i]), mandatory=True)
            manifest_cdc_table["entries"].append(new_item_cdc)
        new_data_cdc=json.dumps(manifest_cdc_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_cdc_key,
            Body=new_data_cdc
        )
        print(f"file {table_name}_cdc.manifest has been created")
        manifest_cdc_file='s3://' + manifest_bucket + "/" + manifest_cdc_key

        manifest_cdc_query=f"""copy {schema_name}.{table_name}  from '{manifest_cdc_file}' iam_role '{role}' FORMAT AS {file_type} GZIP manifest ROUNDEC;"""

        return manifest_cdc_query


def manifest_cdc_json(manifest_bucket, manifest_key, table_name, cdc_files_list, schema_name, role, file_type):
    manifest_cdc_key=str(manifest_key) + str(table_name) + "_cdc_json.manifest"

    ###now preparing for full load
    if len(cdc_files_list) > 0:
        manifest_cdc_table={}
        manifest_cdc_table["entries"]=[]
        for i in range(len(cdc_files_list)):
            new_item_cdc=dict(url=str(cdc_files_list[i]), mandatory=True)
            manifest_cdc_table["entries"].append(new_item_cdc)
        new_data_cdc=json.dumps(manifest_cdc_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_cdc_key,
            Body=new_data_cdc
        )
        print(f"file {table_name}_cdc_json.manifest has been created")
        manifest_cdc_file='s3://' + manifest_bucket + "/" + manifest_cdc_key

        manifest_cdc_query=f"""copy {schema_name}.{table_name}  from '{manifest_cdc_file}' iam_role '{role}' FORMAT AS {file_type}  manifest ROUNDEC;"""

        return manifest_cdc_query


def get_file_appflow(s3_bucket, subfolders_address):
    s3_client=boto3.resource('s3')
    src_bucket=s3_client.Bucket(s3_bucket)
    #    subfolders_address = "/" + subfolders_address
    files_list=[]
    files_content=[]
    for bucket_object_summary in src_bucket.objects.filter(Prefix=subfolders_address[1:]):
        file_name=bucket_object_summary.key
        #        print(file_name)
        files_list.append('s3://' + s3_bucket + '/' + file_name)
        files_content.append(bucket_object_summary.size)
    return files_list, files_content


def appflow_load(schema_name, table_name, src_schema, manifest_address_file, role, file_type):
    appflow_fullload_query=f"""copy {schema_name}.{table_name} ({src_schema}) from '{manifest_address_file}' iam_role '{role}' FORMAT AS {file_type} manifest;"""
    return appflow_fullload_query


def appflow_fullload_manifest(manifest_key, manifest_bucket, table_name, fullload_files_list, fullload_content,
                              schema_name, src_schema, role, file_type):
    manifest_full_key=str(manifest_key) + str(table_name) + "_fullload.manifest"

    ###now preparing for full load
    if len(fullload_files_list) > 0:
        manifest_full_table={}
        manifest_full_table["entries"]=[]
        for i in range(len(fullload_files_list)):
            new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True,
                               meta=dict(content_length=fullload_content[i]))
            manifest_full_table["entries"].append(new_item_full)
        new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_full_key,
            Body=new_data_full
        )
        print(f"file {table_name}_fullload.manifest has been created")
        manifest_fullload_file='s3://' + manifest_bucket + "/" + manifest_full_key

        appflow_fullload_query=appflow_load(schema_name, table_name, src_schema, manifest_fullload_file, role,
                                            file_type)
        return appflow_fullload_query


def appflow_delta_manifest(manifest_key, manifest_bucket, table_name, delta_files_list, delta_content, schema_name,
                           src_schema, role, file_type):
    manifest_delta_key=str(manifest_key) + str(table_name) + "_delta.manifest"

    ###now preparing for cdcd load
    if len(delta_files_list) > 0:
        manifest_full_table={}
        manifest_full_table["entries"]=[]
        for i in range(len(delta_files_list)):
            new_item_full=dict(url=str(delta_files_list[i]), mandatory=True, meta=dict(content_length=delta_content[i]))
            manifest_full_table["entries"].append(new_item_full)
        new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
        # Upload JSON String to an S3 Object
        client=boto3.client('s3')
        client.put_object(
            Bucket=manifest_bucket,
            Key=manifest_delta_key,
            Body=new_data_full
        )
        print(f"file {table_name}_delta.manifest has been created")
        manifest_fullload_file='s3://' + manifest_bucket + "/" + manifest_delta_key

        appflow_delta_query=appflow_load(schema_name, table_name, src_schema, manifest_fullload_file, role, file_type)
        return appflow_delta_query


def get_load_date(red_client, redshift_database_name, redshift_user, redshift_cluster_id, audit_schema,
                  batch_date_table):
    date_sql=f"select  max(batch_date)::date  from {audit_schema}.{batch_date_table};"
    try:
        res_audit=red_client.execute_statement(
            Database=redshift_database_name, DbUser=redshift_user, Sql=date_sql, ClusterIdentifier=redshift_cluster_id)

        query_id=res_audit["Id"]
        has_result=False
        while not has_result:
            time.sleep(1)
            desc=red_client.describe_statement(Id=query_id)
            has_result=desc['HasResultSet']

        result=red_client.get_statement_result(Id=query_id)
        load_date1=result['Records'][0][0]['stringValue']
        load_date1=load_date1.split("-")
        load_date=date(int(load_date1[0]), int(load_date1[1]), int(load_date1[2]))

    except:
        load_date=date.today() - timedelta(days=1)
    print(load_date)
    return load_date


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------

def main(config_file, env, manifest_file):
    """
    Main execution entry point.

    Responsibilities:
    - Read configuration from S3
    - Loop through table definitions
    - Load staging tables
    - Materialize raw-current tables

    Designed to be callable from:
    - Airflow
    - CLI
    - Backfill jobs
    """

    # Read configuration JSON from S3
    try:
        bucket=str(config_file).split('@')[0]
        key=str(config_file).split('@')[1]
        s3=boto3.resource('s3')
        s3_bucket=s3.Bucket(bucket)
        content_object=s3_bucket.Object(key)
        file_content=content_object.get()['Body'].read().decode('utf-8')
        config_data=json.loads(file_content)
    except Exception as e:
        print('Following exception occured while reading config json files.\n' + str(e))
        exit(1)

    if manifest_file == None:
        pass
    else:
        try:
            manifest_bucket=str(manifest_file).split('@')[0]
            manifest_key=str(manifest_file).split('@')[1]
        except Exception as e:
            print('Following exception occured while reading manifest json files.\n' + str(e))
            exit(1)

    red_client=boto3.client('redshift-data')
    isSynchronous=True
    object_properties=config_data['object_properties']

    Database=config_data["Database"]
    ClusterIdentifier=config_data["ClusterIdentifier"]
    user=config_data["user"]
    role=config_data["iam_role"]
    audit_schema="audit_table"
    batch_date_table="batch_date_table"
    query_status_full=''

    # rc_schema_name=object_properties[0]["rc_schema_name"]
    # raw_current_table=object_properties[0]["raw_current_table"]

    # truncate_raw_current_query=f'truncate table {rc_schema_name}.{raw_current_table};'
    # query_status_trunc_rc, query_id_trunc_rc=execute_sql_data_api(red_client, Database, truncate_raw_current_query, user, ClusterIdentifier, isSynchronous)
    # print(f'Raw Current table successfully truncated for {rc_schema_name}.{raw_current_table}')

    load_date=get_load_date(red_client, Database, user, ClusterIdentifier, audit_schema, batch_date_table)

    for i in range(len(object_properties)):
        """
        Each iteration processes one table independently.
        This enables horizontal scaling at the table level.
        """
        file_type=object_properties[i]["file_type"]
        schema_name=object_properties[i]["schema_name"]
        rc_schema_name=object_properties[i]["rc_schema_name"]
        table_name=object_properties[i]["table_name"]
        primary_key=object_properties[i]["primary_key"]
        raw_current_table=object_properties[i]["raw_current_table"]
        sort_column=object_properties[i]["sort_column"]
        table_schema=object_properties[i]["table_schema"]
        raw_current_schema=object_properties[i]["raw_current_schema"]
        src_schema=object_properties[i]["src_schema"]
        bucket=object_properties[i]["bucket"]
        key=object_properties[i]["key"]
        op_column=object_properties[i]["op_column"]
        special_table=object_properties[i]["special_table"]

        truncate_staging_query=f'truncate table {schema_name}.{table_name};'

        query_status_trunc_staging, query_id_trunc_staging=execute_sql_data_api(red_client, Database,
                                                                                truncate_staging_query, user,
                                                                                ClusterIdentifier, isSynchronous)
        print(f'Staging table successfully truncated for {schema_name}.{table_name}')

        start_time=time.time()
        print(start_time)
        if '/dms' in key.lower():
            service="dms"
            full_load_key=key
            delta_key=key.replace("fullload", "cdc")
        elif '/attunity' in key.lower():
            service="attunity"
            full_load_key=key
            delta_key=str(key)[:-1] + "__ct/"
        elif '/appflow' in key.lower():
            service="appflow"
            full_load_key=str(key)[:-1] + "_FullLoad/"
            delta_key=str(key)[:-1] + "_delta/"
        else:
            print(f"invalid path provided. Please provide the proper path for the table {table_name}")
            exit(1)

        if service == "appflow":
            print("-------------------------")
            ##processing full load data to staging
            appflow_fullload_files_list, appflow_fullload_files_content=get_file_appflow(bucket, full_load_key)
            print(appflow_fullload_files_list)

            ##processing fullload .
            if len(appflow_fullload_files_list) > 0:
                appflow_fullload_query=appflow_fullload_manifest(manifest_key, manifest_bucket, table_name,
                                                                 appflow_fullload_files_list,
                                                                 appflow_fullload_files_content, schema_name,
                                                                 src_schema, role, file_type)
                query_status_full, query_id_full_appflow=execute_sql_data_api(red_client, Database,
                                                                              appflow_fullload_query, user,
                                                                              ClusterIdentifier, isSynchronous)
                print(f"Full Load Data for compressed files for {table_name} has been loaded in redshift")
            else:
                query_status_full="FINISHED"
            ############################
            ##processing delta load data to staging
            appflow_delta_files_list, appflow_delta_files_content=get_file_appflow(bucket, delta_key)
            print(appflow_delta_files_list)
            ##processing delta .
            if len(appflow_delta_files_list) > 0:
                appflow_delta_query=appflow_delta_manifest(manifest_key, manifest_bucket, table_name,
                                                           appflow_delta_files_list, appflow_delta_files_content,
                                                           schema_name, src_schema, role, file_type)
                query_status_cdc, query_id_delta_appflow=execute_sql_data_api(red_client, Database, appflow_delta_query,
                                                                              user, ClusterIdentifier, isSynchronous)
                print(f"delta Load Data for compressed files for {table_name} has been loaded in redshift")
            else:
                query_status_cdc="FINISHED"

        elif service == "attunity":
            print("-------------------------")
            ##processing full load data to stating
            fullload_json_gz, fullload_json=get_manifest_json(bucket, full_load_key)
            ##processing fullload json.gz files.
            if len(fullload_json_gz) > 0:
                json_fullload_gz_query=manifest_fulload_json_gz(manifest_bucket, manifest_key, table_name,
                                                                fullload_json_gz, schema_name, role, file_type)
                #                json_fullload_gz_query = json_fullload_gz_query.replace("'",'"')
                query_status_full_gz, query_id_full_gz=execute_sql_data_api(red_client, Database,
                                                                            json_fullload_gz_query, user,
                                                                            ClusterIdentifier, isSynchronous)
                print(f"Full Load Data for compressed files for {table_name} has been loaded in redshift")
            else:
                query_status_full_gz="FINISHED"
            ##processing fullload json files.
            if len(fullload_json) > 0:
                json_fullload_query=manifest_fulload_json(manifest_bucket, manifest_key, table_name, fullload_json,
                                                          schema_name, role, file_type)
                #                json_fullload_query = json_fullload_query.replace("'",'"')
                query_status_full_js, query_id_full_js=execute_sql_data_api(red_client, Database, json_fullload_query,
                                                                            user, ClusterIdentifier, isSynchronous)
                print(f"Full Load Data for uncompressed files for {table_name} has been loaded in redshift")
            else:
                query_status_full_js="FINISHED"

            ### this is used as flag for raw_current processing
            if ((query_status_full_gz == "FINISHED") and (query_status_full_js == "FINISHED")):
                query_status_full="FINISHED"
            elif ((query_status_full_gz == "RUNNING") or (query_status_full_js == "RUNNING")):
                query_status_full="RUNNING"
            else:
                query_status_full="FAILED"

            ##processing delta load to staging
            cdc_json_gz, cdc_json=get_manifest_json(bucket, delta_key)
            ##proccesing cdc json.gz files
            if len(cdc_json_gz) > 0:
                json_cdc_gz_query=manifest_cdc_json_gz(manifest_bucket, manifest_key, table_name, cdc_json_gz,
                                                       schema_name, role, file_type)
                #                json_cdc_gz_query = json_cdc_gz_query.replace("'",'"')
                query_status_cdc_gz, query_id_cdc_gz=execute_sql_data_api(red_client, Database, json_cdc_gz_query, user,
                                                                          ClusterIdentifier, isSynchronous)
                print(f"Delta Load Data for compressed files for {table_name} has been loaded in redshift")
            else:
                query_status_cdc_gz="FINISHED"
            ##processing  cdc json files
            if len(cdc_json) > 0:
                json_cdc_query=manifest_cdc_json(manifest_bucket, manifest_key, table_name, cdc_json, schema_name, role,
                                                 file_type)
                #                json_cdc_query = json_cdc_query.replace("'",'"')
                query_status_cdc_js, query_id_cdc_js=execute_sql_data_api(red_client, Database, json_cdc_query, user,
                                                                          ClusterIdentifier, isSynchronous)
                print(f"Delta Load Data for uncompressed files for {table_name} has been loaded in redshift")
            else:
                query_status_cdc_js="FINISHED"

            ### this is used as flag for raw_current processing
            if ((query_status_cdc_gz == "FINISHED") and (query_status_cdc_js == "FINISHED")):
                query_status_cdc="FINISHED"
            elif ((query_status_cdc_gz == "RUNNING") or (query_status_cdc_js == "RUNNING")):
                query_status_cdc="RUNNING"
            else:
                query_status_cdc="FAILED"
            print(f"Data for table {table_name} has been loaded in redshift")
        elif service == "dms":
            fullload_files_list, fullload_content, cdc_files_list, cdc_content=get_file_fullLoad(bucket, key, delta_key)
            manifest_cdc_key=str(manifest_key) + str(table_name) + "_cdc.manifest"
            manifest_full_key=str(manifest_key) + str(table_name) + "_fullload.manifest"

            ###now preparing for full load
            if len(fullload_files_list) > 0:
                manifest_full_table={}
                manifest_full_table["entries"]=[]
                for i in range(len(fullload_files_list)):
                    new_item_full=dict(url=str(fullload_files_list[i]), mandatory=True,
                                       meta=dict(content_length=fullload_content[i]))
                    manifest_full_table["entries"].append(new_item_full)
                new_data_full=json.dumps(manifest_full_table, indent=2, default=str)
                # Upload JSON String to an S3 Object
                client=boto3.client('s3')
                client.put_object(
                    Bucket=manifest_bucket,
                    Key=manifest_full_key,
                    Body=new_data_full
                )
                print(f"file {table_name}_fullload.manifest has been created")
                manifest_fullload_file='s3://' + manifest_bucket + "/" + manifest_full_key

                try:
                    dms_fullload_query=dms_load_fullload(schema_name, table_name, src_schema, manifest_fullload_file,
                                                         role, file_type)
                    query_status_full, query_id_full=execute_sql_data_api(red_client, Database, dms_fullload_query,
                                                                          user, ClusterIdentifier, isSynchronous)
                    print(f"Full Load Data for table {table_name} has been loaded in redshift")
                except:
                    dms_fullload_query=dms_load_fullload_encode(schema_name, table_name, src_schema,
                                                                manifest_fullload_file, role, file_type)
                    query_status_full, query_id_full=execute_sql_data_api(red_client, Database, dms_fullload_query,
                                                                          user, ClusterIdentifier, isSynchronous)
                    print(f"Full Load Data for table {table_name} has been loaded in redshift")
            ##preparing manifest for cdc
            if len(cdc_files_list) > 0:
                manifest_cdc_table={}
                manifest_cdc_table["entries"]=[]
                for i in range(len(cdc_files_list)):
                    new_item=dict(url=str(cdc_files_list[i]), mandatory=True, meta=dict(content_length=cdc_content[i]))
                    manifest_cdc_table["entries"].append(new_item)
                new_data=json.dumps(manifest_cdc_table, indent=2, default=str)
                #            print(new_data)
                # Upload JSON String to an S3 Object
                client=boto3.client('s3')
                client.put_object(
                    Bucket=manifest_bucket,
                    Key=manifest_cdc_key,
                    Body=new_data
                )
                print(f"file {table_name}_cdc.manifest has been created")
                manifest_cdc_file='s3://' + manifest_bucket + "/" + manifest_cdc_key
                try:
                    dms_cdc_query=dms_load_cdc(schema_name, table_name, manifest_cdc_file, role, file_type,
                                               raw_current_schema)
                    query_status_cdc, query_id_cdc=execute_sql_data_api(red_client, Database, dms_cdc_query, user,
                                                                        ClusterIdentifier, isSynchronous)
                    print(f"Delta Load Data for table {table_name} has been loaded in redshift")
                except:
                    dms_cdc_query=dms_load_cdc_encode(schema_name, table_name, manifest_cdc_file, role, file_type,
                                                      raw_current_schema)
                    query_status_cdc, query_id_cdc=execute_sql_data_api(red_client, Database, dms_cdc_query, user,
                                                                        ClusterIdentifier, isSynchronous)
                    print(f"Delta Load Data for table {table_name} has been loaded in redshift")
            else:
                query_status_cdc="FINISHED"
            print(f"Data for table {table_name} has been loaded in redshift")

        ##processing raw-current tables now
        if ((query_status_cdc == "FINISHED") and (query_status_full == "FINISHED")):
            print(f"processing raw current for table {table_name}")
            sql_query=[]
            ##creating temp table from staging table####
            if service == "appflow":
                if "recordtype" in table_name:
                    sql_create_temp=f"""Create Temp Table {table_name}_temp1 as (SELECT * FROM ( SELECT n.*, '{table_schema}' as schema_name, case when n.{op_column} = 'true'  then 'INSERT' else 'DELETE' end as  delete_flag, getdate() as process_timestamp, '{load_date}' as load_date, row_number() over (partition by {primary_key} order by {sort_column} DESC NULLS LAST) as ROWID FROM {schema_name}.{table_name} n) t WHERE ROWID = 1);"""
                    print(sql_create_temp)
                else:
                    sql_create_temp=f"""Create Temp Table {table_name}_temp1 as (SELECT * FROM ( SELECT n.*, '{table_schema}' as schema_name, case when n.{op_column} = 'true'  then 'DELETE' else 'INSERT' end as  delete_flag, getdate() as process_timestamp, '{load_date}' as load_date, row_number() over (partition by {primary_key} order by {sort_column} DESC NULLS LAST) as ROWID FROM {schema_name}.{table_name} n) t WHERE ROWID = 1);"""
                    print(sql_create_temp)
                run_sql_script=f"call {rc_schema_name}.wrapper_get_column_data_salesforce('{schema_name}','{table_name}','{rc_schema_name}','{raw_current_table}','{table_name}_temp1');"
            else:
                if special_table == "yes":
                    sql_create_temp=retrieve_current_snapshot_sql_by_tbl_name(table_schema, op_column, table_name,
                                                                              schema_name, load_date)
                elif special_table == "no":
                    sql_create_temp=f"""Create Temp Table {table_name}_temp1 as (SELECT * FROM ( SELECT *, '{table_schema}' as schema_name, case when {op_column} = null  then 'INSERT' else {op_column} end as  delete_flag, getdate() as process_timestamp, '{load_date}' as load_date, row_number() over (partition by {primary_key} order by {sort_column} DESC NULLS LAST) as ROWID FROM {schema_name}.{table_name} where coalesce(upper(trim({op_column})), 'XYZ') not in ('B', 'BEFOREIMAGE')) WHERE coalesce(upper(trim({op_column})), 'XYZ') not in ('DELETE', 'D') AND ROWID = 1 );"""
                #                        print(sql_create_temp)
                ###-- creating delete query to upsert data effectively###
                run_sql_script=f"call {rc_schema_name}.wrapper_get_column_data('{schema_name}','{table_name}','{rc_schema_name}','{raw_current_table}','{table_name}_temp1');"

            # Removing this delete logic as the data from wp_rawcurrent table is getting wiped off w.r.t schema_name before insert from temp table
            '''
            try:
                delete_key=primary_key.split(",")
            except:
                delete_key=list(primary_key)

            sql1=f"""delete from {rc_schema_name}.{raw_current_table} using {table_name}_temp where """
            delete_sql=sql1

            for i in range(len(delete_key)):
                if i == len(delete_key) - 1:
                    b=f"""{rc_schema_name}.{raw_current_table}.{delete_key[i]} = {table_name}_temp.{delete_key[i]};"""
                    delete_sql=delete_sql + str(b)
                else:
                    a=f"""{rc_schema_name}.{raw_current_table}.{delete_key[i]} = {table_name}_temp.{delete_key[i]} and """
                    delete_sql=delete_sql + str(a)
            print(delete_sql)
            '''

            delete_sql=f"""delete from {rc_schema_name}.{raw_current_table} where schema_name = '{table_schema}'"""

            ###insert records from tmp table to raw_current table:###
            raw_current_schema_new=raw_current_schema
            raw_current_schema_new=raw_current_schema_new.replace("timestamp", "'timestamp'")
            raw_current_schema_new=raw_current_schema_new.replace("header__'timestamp'", "header__timestamp")
            raw_current_schema_new=raw_current_schema_new.replace("insert_'timestamp'", "insert_timestamp")
            insert_query=f"""insert into  {rc_schema_name}.{raw_current_table} ({raw_current_schema}, schema_name, delete_flag, process_timestamp,load_date) (select {raw_current_schema_new}, schema_name, delete_flag, process_timestamp, cast(load_date as date) from {table_name}_temp);"""
            insert_query=insert_query.replace("404c_compl_cd", '"404c_compl_cd"')
            insert_query=insert_query.replace("410b_fail_safe", '"410b_fail_safe"')
            ######adjustments to be given for rollup column for pgalliance#####
            print(insert_query)

            ###delete temp tables#####
            delete_temp=f"""drop table  {table_name}_temp;"""
            print(delete_temp)
            delete_temp1=f"""drop table  {table_name}_temp1;"""

            trun_table=f"""truncate table {schema_name}.{table_name};"""
            print(trun_table)

            count_query=f"select count(*) from {rc_schema_name}.{raw_current_table};"
            print(count_query)

            if "plan_prov_grp" in table_name:
                sql_query=sql_create_temp.split("|")
                sql_query.append(run_sql_script)
                sql_query.append(delete_sql)
                sql_query.append(insert_query)
                sql_query.append(delete_temp1)
                sql_query.append(delete_temp)
                # sql_query.append(trun_table)
            else:
                sql_query.append(sql_create_temp)
                sql_query.append(run_sql_script)
                sql_query.append(delete_sql)
                sql_query.append(insert_query)
                sql_query.append(delete_temp1)
                sql_query.append(delete_temp)
                # sql_query.append(trun_table)

            try:
                copy_response, query_id=execute_sql_data_api(red_client, Database, sql_query, user, ClusterIdentifier,
                                                             isSynchronous)
                print(f"Copied raw_current successfully for {table_name}")
            except Exception as e:
                print(str(e))
                raise e
        else:
            print(f"Query status for table {table_name} is not success. Skipping this table for now.")

        end_time=time.time()
        print(start_time)
        print("****************************************************************************************************")
        print(
            f'start time for table {table_name} is {start_time} and end time epoch {end_time} ==> total time {end_time - start_time}')
        print("****************************************************************************************************")
    red_client.close()
    print("End of boto3 session 'redshift-data'")
    print("End of program")


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--config_file', help="Provides the datalake configurations. Required field", required=True)
    parser.add_argument('--manifest_file', help="Provides the datalake configurations. Required field", required=False)
    parser.add_argument('--environment', help="Should be dev/test/model/prod. Required field", required=True)
    args=parser.parse_args()

    if args.environment is None:
        print("--env is required. use dev,tst,mdl or prd")
        exit(1)
    elif args.environment.lower() in ['dev', 'tst', 'mdl', 'prd']:
        env=args.environment.lower()
    else:
        print('invalid argument value is passed for environment. it should be one of (dev/tst/mdl/prd)')
        exit(1)

    if args.manifest_file is None:
        manifest_file=None
    else:
        manifest_file=args.manifest_file

    if args.config_file is None:
        print('--config_file mandatory file is not passed')
        exit(1)
    else:
        main(args.config_file, env, manifest_file)
