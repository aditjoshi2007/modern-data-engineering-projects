############ OBJECTIVE ##############################################
## This script is specific to a given project.
## Builds an EMR, runs steps and terminates.
############ Example Run Command ( from ControlM EC2 ) ##############
## 
#####################################################################

import boto3
import json
import time
import argparse
import sys
import create_emr
import emr_waiter
import emr_step_waiter
import pytz
import traceback

from botocore.config import Config as boto3_config
from datetime import datetime, timedelta, date

sys.path.insert(1, u'/Scripts/deltalake_cdc_data_pipeline/src/managers_v6.zip')
from managers_v6 import ods_manager
from managers_v6.console_manager import write_log

def check_job_status(prcs_id, env):
    
    ods_manger = ods_manager.ODSManager(env)
    prcs_job_sts_rt = ods_manger.get_alljobstatus_by_prcs_id(prcs_id)
    
    prcs_job_sts_rt_sts = json.loads(prcs_job_sts_rt)['statusCode']
    prcs_job_sts_rt_body = json.loads(prcs_job_sts_rt)['body']

    write_log(__file__, "check_job_status()", 'info', f'status received prcs_job_sts_rt_sts:{prcs_job_sts_rt_sts}')
    write_log(__file__, "check_job_status()", 'info', f'body received prcs_job_sts_rt_body:{prcs_job_sts_rt_body}')
    
    if prcs_job_sts_rt_sts == 200:
        prcs_failed_rsn = ''
        if [None] in prcs_job_sts_rt_body:
            print(f"1:{__file__.split('/')[-1]} :: check_job_status() :: error' :: report to dev team - one of the job has not updated its status, Repository_details: ingestion-framework , Execution_start_time: {main_start}")
            # prcs_job_sts_rt_body.remove([None])
            exit(1)

        if len(prcs_job_sts_rt_body) == 0:
            prcs_sts = 'FAILED' # intention is we can say here failed - no step created
            prcs_failed_rsn = 'No data processing step (job_id) has been created'
        elif ['COMPLETED'] in prcs_job_sts_rt_body and len(prcs_job_sts_rt_body) == 1:
            prcs_sts = 'COMPLETED'
        elif ['COMPLETED'] in prcs_job_sts_rt_body and len(prcs_job_sts_rt_body) > 1:
            prcs_sts = 'PARTIALLY COMPLETED'
        elif ['COMPLETED'] not in prcs_job_sts_rt_body and len(prcs_job_sts_rt_body) >= 1:
            prcs_sts = 'FAILED'
            prcs_failed_rsn = 'None of the Data Processing steps (job id ) is completed.'
        else:
            write_log(__file__, "check_job_status()", 'error', f'report to dev team - SHOULD NOT enter this clause')

    else:
        prcs_sts = 'FAILED'
        prcs_failed_rsn = prcs_job_sts_rt_body[0:199]
    
    return prcs_sts, prcs_failed_rsn


# terminates the emr based on the ID 
def execute_terminate_emr(cluster_id, prcs_sts, prcs_failed_rsn, prcs_id, env):
    if prcs_sts == None:
        prcs_sts = 'FAILED'
        prcs_failed_rsn = 'Check EMR logs'
    try:
        ods_manger = ods_manager.ODSManager(env)
        ods_manger.update_processid(
        prcs_exec_end_dt = (datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S'),
        prcs_exec_sts = prcs_sts, 
        prcs_exec_failed_rsn = prcs_failed_rsn, 
        prcs_exec_id = prcs_id, 
        updtd_usr = f'{emr_name}-{env}'[0:49]
        )
        write_log(__file__, "execute_terminate_emr()", 'info', f'updated the process id: {prcs_id}')
    except Exception as e:
        write_log(__file__, "execute_terminate_emr()", 'error', f'failed to updated the process id: {prcs_id}. details are {e}')
    try:
        config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
        )
        client = boto3.client('emr', region_name='<aws_region>', config=config)
        client.terminate_job_flows(JobFlowIds=[cluster_id])

    except Exception as e:
        write_log(__file__, "execute_terminate_emr()", 'error', f'while terminating the emr with process id: {prcs_id}. followinf are the detials {e}')
        print(f"1:{__file__.split('/')[-1]} :: execute_terminate_emr() :: error' :: while terminating the emr {e} , Repository_details: repo_name, Execution_start_time: {main_start}")
        exit(1)


# read table_config >>> find #tables
# create steps iteratively 
# add to EMR >>> wait for ALL steps completion or other failure status
def parallel_table_steps_executer(cluster_id, emr_name, env, acct, table_config, run_mode, prcs_id, force_initial_load):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    emr_client = boto3.client("emr", "<aws_region>", config=config)
    emr_steps = []
    failed_cancelled_steps = []

    # run_date is today except for initial load which is yesterday --> to avoid overwriting in raw folder
    if run_mode == 'InitialLoad':
        run_date = datetime.now().date() - timedelta(days=1)
        run_date = run_date.strftime("%Y%m%d")
    else:
        run_date = datetime.now().date().strftime("%Y%m%d")
    # to handle multiple runs in a day
    rerun_run_date = 'true'

    for table_index in range(len(table_config['tables_definitions'])):
        job_args = [
            'spark-submit',
            '--deploy-mode', 'cluster',
            '--master', 'yarn', 
            '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
            '--conf', 'spark.sql.hive.convertMetastoreParquet=false',
            '--conf', 'spark.driver.maxResultSize=0',
            '--py-files',f's3://<s3-{acct}-codedeployment-bucket-{env}>/deltalake_cdc_data_pipeline/src/managers_v6.zip',
            f's3://<s3-{acct}-codedeployment-bucket-{env}>/deltalake_cdc_data_pipeline/src/ingestion/scripts/ingestion_single_table_v6.py',
            '--run_mode', run_mode,
            '--common_config', str(table_config['common_parameters']), 
            '--table_defination_file',str(table_config['tables_definitions'][table_index]),
            '--environment', env,
            '--prcs_id', prcs_id,
            '--force_initial_load', force_initial_load,
            '--run_date', run_date,
            '--rerun_run_date', rerun_run_date
            ]
        step = {
            "Name": "step-1."+ str(table_index+1) + "-ingestion-" + table_config['tables_definitions'][table_index]['table'],
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": job_args
                }
            }
        emr_steps.append(step)
    response = emr_client.add_job_flow_steps(JobFlowId = cluster_id, Steps = emr_steps)
    for step_index in range(len(response["StepIds"])):
            step_status = emr_step_waiter.emrStepWaiter(cluster_id, response["StepIds"][step_index])
            if step_status['Step']['Status']['State'] == 'FAILED':
                failed_steps_details = {}
                failed_steps_details['stepId']= response["StepIds"][step_index]
                failed_steps_details['failedScriptName']='ingestion_single_table_v6.py'
                failed_steps_details['logFilePath'] = step_status['Step']['Status']['FailureDetails']['LogFile']
                failed_cancelled_steps.append(failed_steps_details)
                print(step_status['Step']['Status']['FailureDetails']['LogFile'])

            elif (step_status['Step']['Status']['State'] == 'CANCELLED') or (step_status['Step']['Status']['State'] == 'CANCEL_PENDING'):
                cancelled_steps_details = {}
                cancelled_steps_details['stepId'] = response["StepIds"][step_index]
                cancelled_steps_details['cancelledScriptName'] = 'ingestion_single_table_v6.py'
                cancelled_steps_details['stepStatus'] = step_status['Step']['Status']['State']
                failed_cancelled_steps.append(cancelled_steps_details)
    return failed_cancelled_steps

# adds one steop to EMR >> wait for completion or other failure status
def single_step_executer(cluster_id, job_args, step_name, script_name):
    failed_cancelled_steps = []
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    emr_client = boto3.client("emr", "<aws_region>", config=config)
    step = {
            "Name": step_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": job_args
                }
            }
    response = emr_client.add_job_flow_steps(JobFlowId = cluster_id, Steps = [step])
    step_status = emr_step_waiter.emrStepWaiter(cluster_id, response["StepIds"][0])
    if step_status['Step']['Status']['State'] == 'FAILED':
        failed_steps_details = {}
        failed_steps_details['stepId'] = response["StepIds"][0]
        failed_steps_details['failedScriptName'] = job_args[1].split('/')[-1]
        failed_steps_details['logFilePath'] = step_status['Step']['Status']['FailureDetails']['LogFile']
        failed_cancelled_steps.append(failed_steps_details)
        print(step_status['Step']['Status']['FailureDetails']['LogFile'])

    elif (step_status['Step']['Status']['State'] == 'CANCELLED') or (step_status['Step']['Status']['State'] == 'CANCEL_PENDING'):
        cancelled_steps_details = {}
        cancelled_steps_details['stepId'] = response["StepIds"][0]
        cancelled_steps_details['cancelledScriptName'] = script_name
        cancelled_steps_details['stepStatus'] = step_status['Step']['Status']['State']
        failed_cancelled_steps.append(cancelled_steps_details)
    return failed_cancelled_steps

# creates an EMR and returns the ID
def execute_create_emr(env, emr_name, cluster_size, local_dir, environment, prcs_id, step_conncurrency):
    try:
        global global_cluster_id
        cluster_id = create_emr.execute_create_emr(
                create_emr.get_instance_fleets(cluster_size, local_dir, environment), 
                create_emr.get_initial_inputs(env, emr_name, local_dir, environment),
                step_conncurrency
                )

        global_cluster_id = cluster_id
        emr_waiter.emrCreationWaiter(cluster_id)
        return cluster_id

    except Exception as e:
        prcs_sts = 'FAILED-EMR STARTUP'
        prcs_failed_rsn = str(e)[0:199]
        
        ods_manger = ods_manager.ODSManager(env)
        prcs_id_u_return = ods_manger.update_processid(
        prcs_exec_end_dt = (datetime.now(cst)).strftime('%Y-%m-%d %H:%M:%S'),
        prcs_exec_sts = prcs_sts, 
        prcs_exec_failed_rsn = prcs_failed_rsn, 
        prcs_exec_id = prcs_id, 
        updtd_usr = f'{emr_name}-{env}'[0:49]
        )
        
        prcs_id_u_return_sts = json.loads(prcs_id_u_return)['statusCode']
        prcs_id_u_return_body = json.loads(prcs_id_u_return)['body']

        if int(prcs_id_u_return_sts) != 200:
            write_log(__file__, "execute_create_emr()", 'error', f'can not update process_id. response is{prcs_id_u_return_body}')
        else:
            prcs_id = prcs_id_u_return_body["PRCS_EXEC_ID"]
            write_log(__file__, "execute_create_emr()", 'info', f'udpated process_id: {prcs_id}')

        print(f"1:{__file__.split('/')[-1]} :: execute_create_emr() :: error :: emr creation failed with the process id: {prcs_id} and error: {e}, Repository_details: ingestion-framework , Execution_start_time: {main_start}")
        exit(1)

# execute steps ( seq steps + parallel steps + seq steps) >>> terminate EMR
def execute_script(env, acct, emr_name, cluster_id, table_config, run_mode, prcs_id, config_file, force_initial_load):
    try:
        f_c_steps = []
        
        ### STEP 1 - INGESTION ####
        f_c_steps.append(parallel_table_steps_executer(cluster_id, emr_name, env, acct, table_config, run_mode, prcs_id, force_initial_load))
        
        ### STEP 2 - GLUE Crawler ( landing, raw, raw_current) #######
        job_args =["spark-submit", "--deploy-mode", "client","--master", "yarn", "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer", "--conf", "spark.sql.hive.convertMetastoreParquet=false", '--py-files', f"s3://<s3-{acct}-codedeployment-bucket-{env}>/deltalake_cdc_data_pipeline/src.zip", f"s3://<s3-{acct}-codedeployment-bucket-{env}>/deltalake_cdc_data_pipeline/src/ingestion/scripts/glue_crawler.py", "--config_file", config_file, "--env", env]
        step_name = "step-2-GlueCrawler"
        script_name = "glue_crawler.py"
        f_c_steps.append(single_step_executer(cluster_id, job_args, step_name, script_name))

        prcs_sts, prcs_failed_rsn = check_job_status(prcs_id, env)
        return f_c_steps

    except Exception as e:
        prcs_sts, prcs_failed_rsn = check_job_status(prcs_id, env)
        prcs_failed_rsn = str(e)[0:199]
        write_log(__file__, "execute_script()", 'error', f'step execution has an error: {e}')
    finally:
        execute_terminate_emr(cluster_id, prcs_sts, prcs_failed_rsn, prcs_id, env)

# create a process ID
# create an EMR 
# add steps
def main(env, run_mode, acct, emr_name, cluster_size, table_config, local_dir, config_file, force_initial_load, step_conncurrency):
    global cst  
    cst = pytz.timezone('US/Central') 

    global main_start
    main_start = datetime.now(cst)

    if run_mode == 'CDC':
        load_type = 'DAILY_CDC_LOAD'
    elif run_mode == 'InitialLoad' or run_mode == 'Both':
        load_type = 'FULL_LOAD'

    # process ID creation 
    ods_manger = ods_manager.ODSManager(env)

    prcs_id_return = ods_manger.create_processid(
        'Ingestion',
        load_type,
        'INGESTION_LOAD',
        f'{emr_name}-{env}'[0:49])

    try:
        prcs_id_return_sts = json.loads(prcs_id_return)['statusCode']
        prcs_id_return_body = json.loads(prcs_id_return)['body']
    except Exception as e:
        print(f"1: {__file__.split('/')[-1]} :: main() :: error :: can not create process id and the error is : {e}, Repository_details: repo_name , Execution_start_time: {main_start}")
        exit(1)

    if int(prcs_id_return_sts) != 200:
        print(f"1: {__file__.split('/')[-1]} :: main() :: error :: can not create process id and the response is: {prcs_id_return_body}, Repository_details: repo_name , Execution_start_time: {main_start}")
        exit(1)
    else:
        prcs_id = prcs_id_return_body["PRCS_EXEC_ID"]
        write_log(__file__, "main()", 'info', f'process id created: {prcs_id}')
        print(f'lambda return {prcs_id}')
    
    # EMR Creation
    emr_cluster_id = execute_create_emr(env, emr_name, cluster_size, local_dir, env, prcs_id, step_conncurrency)

    # Step Execution
    f_c_steps = execute_script(env, acct, emr_name, emr_cluster_id, table_config, run_mode, prcs_id, config_file, force_initial_load)

    main_end = datetime.now(cst)
    ## For New Wrapper to Send SNS in case of any failed steps
    if not any(f_c_steps):
        print(f'0:EMR has no failed steps')
        exit(0)
    else:
        print(f'1:EMR_ID {emr_cluster_id} has Failed steps. Repository_details: repo_name , Execution_start_time: {main_start} , Execution_end_time: {main_end}.Following are the step and log_file details{f_c_steps}')
        exit(1)
    

# Check input paramters and read config file
# invoke main()
if __name__ == '__main__':

    local_dir = '/Scripts/deltalake_cdc_data_pipeline/src/orchestration/control-m/emr_execute_script'

    parser = argparse.ArgumentParser()
    parser.add_argument('--env', help="use dev,tst,mdl or prd. Required Field", required=True)
    parser.add_argument("--run_mode", help="Run Mode (InitialLoad, CDC, Both)", required=True)
    parser.add_argument('--acct', help="it is for S3 acct. Example: 123456789. Required Field", required=True)
    parser.add_argument('--emr_name', help="Provides the ingestion type. Required Field", required=True)
    parser.add_argument('--cluster_size', help="Provides the EMR cluster size. Required Field", required=True)
    parser.add_argument('--config_file', help="Provides the EMR cluster size. Required Field", required=True)
    parser.add_argument('--step_conncurrency', help="Provides the EMR cluster size. default=large", required=False)

    args = parser.parse_args()
    if args.env.lower() not in ['dev','tst','mdl','prd']:
        print(f"1:{__file__.split('/')[-1]} :: __main__ :: error' :: env is required. use dev, tst, mdl or prd, Repository_details: repo_name , Execution_start_time: 000000")
        exit(1)
    else:
        env = args.env.lower()

    if args.run_mode not in ['InitialLoad','CDC','Both']:
        print(f"1:{__file__.split('/')[-1]} :: __main__ :: error' :: mode is required. use initialload, cdc, both, Repository_details: repo_name , Execution_start_time: 000000")
        exit(1)
    else:
        if args.run_mode  in ['InitialLoad','Both']:
            force_initial_load = 'True'
        else :
            force_initial_load = 'False'
        run_mode = args.run_mode

    if args.acct not in ['123456789']:
        print(f"1:{__file__.split('/')[-1]} :: __main__ :: error' :: acct is required. it is for S3 acct. Example: 123456789. Required Field, Repository_details: repo_name , Execution_start_time: 000000")
        exit(1)
    else:
        acct = args.acct.lower()

    if args.cluster_size not in ['xsmall', 'small', 'medium', 'large', 'xlarge']:
        print(f"1:{__file__.split('/')[-1]} :: __main__ :: error' :: cluster_size is required. Cluster sizes are. Example: xsmall, small, medium, large, xlarge  etc., Repository_details: repo_name , Execution_start_time: 000000")
        exit(1)
    else:
        cluster_size = args.cluster_size.lower()

    emr_name = args.emr_name.lower() + '-' + run_mode.lower()

    if args.config_file is not None:
        try: 
            config_file = str(args.config_file)
            print(config_file)
            bucket = str(args.config_file).split('@')[0]
            key = str(args.config_file).split('@')[1]

            s3 = boto3.resource('s3')
            s3_bucket = s3.Bucket(bucket)
            content_object = s3_bucket.Object(key)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            table_config = json.loads(file_content)
        except Exception as e:
            print(f"1:{__file__.split('/')[-1]} :: __main__ :: error' :: following exception occured while reading config json files : {str(e)}, Repository_details: repo_name , Execution_start_time: 000000")
            exit(1)
    else:
        print(f"1:{__file__.split('/')[-1]} :: __main__ :: error' :: config_file is required. Example 'bucket@key', Repository_details: repo_name , Execution_start_time: 000000")
        exit(1)

    if args.step_conncurrency is None:
        step_conncurrency = 1
    else:
        step_conncurrency=args.step_conncurrency

    main(env, run_mode, acct, emr_name, cluster_size, table_config, local_dir, config_file, force_initial_load, step_conncurrency)
    
