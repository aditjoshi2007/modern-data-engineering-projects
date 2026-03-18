############################## OBJECTIVE ###########################################
# To trigger the appflow 
################################ INVOKE EXAMPLE #######################################
# python3 trigger_appflow.py --s3_bucket <s3_code_deployment_bucket> --config_key <s3_prefix_to_json_config_file>
#######################################################################################
import boto3
from datetime import date
import time
import traceback
from datetime import datetime,timedelta
from dateutil.parser import parse
import json
import argparse
from pytz import timezone
from pyspark.sql import SparkSession

#### function to send email notification####
def email_notification(aws_region,arn,custom_message,env):
    client = boto3.client('sns', region_name=aws_region)
    response = client.publish(TopicArn = arn,                              
                               Message = custom_message,
                               Subject = "AWS_Appflow_Status_Notification- Environment- "+env+"-"+ str(date.today())
                               )
    return response

def write_log(filename, function, level, message):
    cst = timezone('US/Central')
    print(datetime.now(cst).strftime("%d-%m-%Y %I:%M:%S %p") + "::" + filename.split('/')[-1] + "::" + function + "::" + level + "::" + message)


## function to create boto3 coonection and return given flow's details 
def create_client_connect(flow, region_name):

    client = boto3.client('appflow', region_name = region_name)   
    response = client.describe_flow_execution_records(flowName=flow)

    detail_response=dict(response)
    check_flow= client.describe_flow(flowName=flow)
    flow_status=check_flow['flowStatus']
 
    latest_run_status=detail_response['flowExecutions'][0]['executionStatus']
    latest_run_status_date=detail_response['flowExecutions'][0]['lastUpdatedAt'].date()
    write_log(__file__, 'create_client_connect()', 'info',f'flow_status, latest_run_status, latest_run_status_date are - {flow_status},{latest_run_status},{latest_run_status_date}')
    return flow_status, latest_run_status, latest_run_status_date 

###activate the non-activate flows
def start_flow(flow,region_name):
    client = boto3.client('appflow', region_name=region_name)
    response = client.start_flow(flowName=flow)
    execution_id=response['executionId']
    write_log(__file__, 'start_flow()', 'info',f'flow has been started - {response}')
    write_log(__file__, 'start_flow()', 'info',f'flow has been started and executionid is- {execution_id}')
    return execution_id

    
    
# check the flow status of active/draft - will start it and wait for completion or failure.
# if the flow status is deprecated/deleted.errored - will return "failure"
# this doesn't have any email or exit conditions - only return status. calling program/function needs to exit/email based on the return status

def check_status_s3_sf(flow,region_name):
    
    flow_status,latest_run_status,latest_run_status_date=create_client_connect(flow,region_name) 
    wait_count=0  
    execution_id = None    

    ## If the flow is in Active or draft - start the flow and wait for completion or failure 
    if flow_status in ['Draft','Active']:
        ### S3_sf is trigger and wait for 60 secs
        execution_id = start_flow(flow,region_name)
        time.sleep(60)

        flow_status,latest_run_status,latest_run_status_date=create_client_connect(flow,region_name) 

        ### cases where the s3_sf completed with in 60 seconds
        if (latest_run_status=='Successful') and (latest_run_status_date==date.today()): 
            status="success" 
            write_log(__file__, 'check_status_s3_sf()', 'info',f's3_sf completed in 60 secs. return status assigned to "{status}"')

        ### cases where the s3_sf is errored with in 60 seconds
        elif (latest_run_status=='Error') and (latest_run_status_date==date.today()): # added to handle error last runs/current triggers
            status = "failure"
            write_log(__file__, 'check_status_s3_sf()', 'error', f"{flow} has flow status as '{flow_status}' and latest run status as '{latest_run_status}' and date as '{latest_run_status_date}'. return status assigned to'{status}'")
        
        ### cases where the s3_sf is processing for more than 60 sec
        elif (latest_run_status=='InProgress'):
            while (latest_run_status  != 'Successful'):
                time.sleep(60)
                wait_count=wait_count+1
                if wait_count > 10:
                    write_log(__file__, 'check_status_s3_sf()', 'info',f's3_sf flow "{flow}" is in "{latest_run_status}" status for more than {wait_count*60} seconds.')
                    #### Use this block to terminate the program if appflow is running for longer time.
                    #### As of now we are not terminating the program, flow will keep running, program will wait till it is done.
                else:
                    write_log(__file__, 'check_status_s3_sf()', 'info',f's3_sf flow "{flow}" is in "{latest_run_status}" status - while loop check')

                flow_status, latest_run_status, latest_run_status_date = create_client_connect(flow,region_name)
                ### cases where the s3_sf is successful after some wait time passed.
                if (latest_run_status=='Successful'):
                    status="success"
                    write_log(__file__, 'check_status_s3_sf()', 'info',f's3_sf flow "{flow}" is in "{latest_run_status}" status. return status assigned to "{status}"')
                    break
                ### cases where the s3_sf is errored after some wait time passed.
                elif (latest_run_status=='Error')or (flow_status=='Errored'):
                    status="failure"   
                    write_log(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}" is in "{latest_run_status}" status. return status assigned to "{status}"')
                    break
        
        ### cases where the s3_sf is neither InProgress nor Error nor Successful - unknow status
        else:
            status = "failure"
            write_log(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}" is in "{latest_run_status}" status. return status assigned to "{status}"')
            write_log(__file__, 'check_status_s3_sf()', 'error',f'REPORT TO DEVELOPERS - UNKNOW STATUS')

    ## If the flow is in 'Deprecated','Deleted','Errored'  return 'failure' status value
    elif flow_status in ['Deprecated','Deleted','Errored']:
        status="failure"   
        write_log(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}"  has flow status as "{flow_status}". return status assigned to "{status}"')

    ## To catch any unknow status
    else: 
        status="failure"   
        write_log(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}"  has flow status as "{flow_status}". return status assigned to "{status}"')
        write_log(__file__, 'check_status_s3_sf()', 'error',f'REPORT TO DEVELOPERS - UNKNOW FLOW STATUS')
    
    return status, wait_count, execution_id
        

def rejected_file_check(s3_bucket, reject_data_prefix, execution_id):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(s3_bucket)
    rejected_files_list = []

    for object_summary in my_bucket.objects.filter(Prefix = f'{reject_data_prefix}/{execution_id}/'):      
        files=str(object_summary.key).split("/")[-1:]
        rejected_files_list.append(files)
    write_log(__file__, 'rejected_file_check()', 'info',f'rejected_file_check details are "{s3_bucket}" , "{reject_data_prefix}", "{execution_id} and {rejected_files_list}"')
       
    if len(rejected_files_list) >= 1:
        return True  
    else: 
        return False


if __name__=='__main__': 
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--flow_name', help="name of the appflow you want to trigger ( s3 to SF ) ", required=True)
        parser.add_argument('--environment', help="Should be dev/test/model/prod. Required field", required=True)
        parser.add_argument('--config_file', help="Provides the datalake configurations. Required field", required=True)
        args = parser.parse_args()

        if args.environment not in ["dev", "tst", "mdl","prd"]:
            write_log(__file__, "__main__", 'error', f'--enivronment is value incorrect {args.environment}')
            exit(1)
        else:
            env = args.environment
    
        if args.config_file is None:
            write_log(__file__, '__main__', 'error', '--config_file mandatory file is not passed')
            exit(1)
        else:
            try:
                config_file = str(args.config_file)
                bucket = str(args.config_file).split('@')[0]
                key = str(args.config_file).split('@')[1]
                s3 = boto3.resource('s3')
                s3_bucket = s3.Bucket(bucket)
                content_object = s3_bucket.Object(key)
                file_content = content_object.get()['Body'].read().decode('utf-8')
                table_config = json.loads(file_content)
                reject_data_prefix = table_config['tables_definitions'][0]['Bucket_Prefix_error']
                aws_region = table_config['AWS_REGION']
                arn= table_config['ARN']
                bucket_name=table_config['Bucket_Name']

            except Exception as e:
                write_log(__file__, '__main__', 'error', f'{traceback.format_exc()}')
                write_log(__file__, '__main__', 'error',
                        f'following exception occured while reading config json files.\n{str(e)}')
                exit(1)
          
        if args.flow_name is None:
            write_log(__file__, '__main__', 'error', '--flow_name mandatory is not passed')
            exit(1)
        else:
            status, wait_count, execution_id = check_status_s3_sf(args.flow_name,aws_region)
            time.sleep(60)
            if status == "success":
                # reject check 
                is_rejected = rejected_file_check(bucket_name, reject_data_prefix, execution_id)
                write_log(__file__, '__main__', 'info', f'rejected_file_check is {is_rejected}')
                
                if is_rejected == True :
                    # email that partial completion 
                    custom_message=f"Hi,\nAppflow has successfully been triggered and there is rejected data. Please check rejected data in s3 \ns3://{bucket_name}/{reject_data_prefix}/{execution_id}/.\n\n\nThanks,"
                    email_notification(aws_region,arn,custom_message,env)
                elif is_rejected == False : 
                    # email - successfull appflow sent the data 
                    custom_message="Hi,\nAppflow has successfully been triggered and there is no rejected data.\n\n\nThanks,"
                    email_notification(aws_region,arn,custom_message,env)

            elif status == "failure": 
                custom_message=f"Hi,\nAppflow hasn't been triggered successfully. Please trigger the appflow again.\n\n\nThanks,"
                email_notification(aws_region,arn,custom_message,env)
 
            else : 
                exit(1)
    except Exception as e:
        write_log(__file__, '__main__', 'error', traceback.format_exc())
        print("Following error has occured:\n" + str(e))