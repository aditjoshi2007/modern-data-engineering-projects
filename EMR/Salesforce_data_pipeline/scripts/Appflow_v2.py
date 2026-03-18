import boto3
from datetime import date
import time
from datetime import datetime,timedelta
from dateutil.parser import parse
import json
import argparse
from pytz import timezone
from pyspark.sql import SparkSession
from src.ingestion.utilities import log_message
from botocore.config import Config as boto3_config


#### function to send email notification####
def email_notification(aws_region,arn,custom_message,env):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    client = boto3.client('sns', region_name=aws_region,config=config)
    response = client.publish(TopicArn = arn,                              
                               Message = custom_message,
                               Subject = "AWS_Appflow_Status_Notification- Environment- "+env+"-"+ str(date.today())
                               )
    return response

## function to create boto3 coonection and return given flow's details 
def create_client_connect(flow, region_name):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    client = boto3.client('appflow', region_name = region_name,config=config)
    response = client.describe_flow_execution_records(flowName=flow)

    detail_response=dict(response)
    check_flow= client.describe_flow(flowName=flow)
    flow_status=check_flow['flowStatus']

    ### If delta flow has been created but not triggered as schedule time is in future then 
    ### this will take care of this scenario by updating trigger time to current time +1 minute and run the flow
    if len(detail_response['flowExecutions'])==0:
        log_message(__file__, 'create_client_connect()', 'info','Delta flow has not yet triggered for the first time;hence returning empty string')
        return flow_status,"",datetime.date(datetime.today()-timedelta(days=2))
    else:  
        latest_run_status=detail_response['flowExecutions'][0]['executionStatus']
        latest_run_status_date=detail_response['flowExecutions'][0]['lastUpdatedAt'].date()
        log_message(__file__, 'create_client_connect()', 'info',f'flow_status, latest_run_status, latest_run_status_date are - {flow_status},{latest_run_status},{latest_run_status_date}')
        return flow_status, latest_run_status, latest_run_status_date

 
###activate the non-activate flows
def start_flow(flow,region_name):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    client = boto3.client('appflow', region_name=region_name,config=config)
    response = client.start_flow(flowName=flow)
    return response


# udpates the flow number of fields and also the trigger time to 60 sec from now
def update_flow(flow,region_name,obj,buk_name,buk_pre,out_typ,end_time,time_zone,connector_profile,req_mapping):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    client = boto3.client('appflow', region_name=region_name,config=config)
    response = client.update_flow(
    flowName=flow,
    description='flow_updated',
    triggerConfig={
        'triggerType': 'Scheduled',
        'triggerProperties': {
            'Scheduled': {
                'scheduleExpression': 'rate(1days)',
                'dataPullMode': 'Incremental',
                'scheduleStartTime': datetime.now()+timedelta(seconds=60),
                'scheduleEndTime': eval(end_time),
                'timezone': time_zone,
            }
        }
    },
    sourceFlowConfig={
        'connectorType': 'Salesforce',
        'connectorProfileName': connector_profile,
        'sourceConnectorProperties': {           
            'Salesforce': {
                'object': obj, 
                'enableDynamicFieldUpdate': False,
                'includeDeletedRecords': True
            }            
        },
        'incrementalPullConfig': {
        'datetimeTypeFieldName': 'SystemModstamp'}
    },
    destinationFlowConfigList=[
        {
            'connectorType': 'S3',
            'connectorProfileName': connector_profile,  
            'destinationConnectorProperties': {
                'S3': {
                    'bucketName': buk_name,  
                    'bucketPrefix': buk_pre,    
                    's3OutputFormatConfig': {
                        'fileType': out_typ,
                        'prefixConfig': {
                            'prefixType': 'PATH_AND_FILENAME',       
                            'prefixFormat': 'DAY'              
                        },
                        'aggregationConfig': {
                            'aggregationType': 'None'
                        }
                    }
                },               
            }
        },
    ],
    tasks=req_mapping
    )
    return response 
 

# reverts the trigger to the time defined in src/config appflow jsons and also udpates the number of fields            
def revert_to_original_flow(flow,region_name,obj,buk_name,buk_pre,out_typ,start_time,end_time,time_zone,connector_profile,req_mapping):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    client = boto3.client('appflow', region_name=region_name,config=config)
    response = client.update_flow(
    flowName=flow,
    description='flow_updated',
    triggerConfig={
        'triggerType': 'Scheduled',
        'triggerProperties': {
            'Scheduled': {
                'scheduleExpression': 'rate(1days)',
                'dataPullMode': 'Incremental',
                'scheduleStartTime': eval(start_time),
                'scheduleEndTime': eval(end_time),
                'timezone': time_zone,
            }
        }
    },
    sourceFlowConfig={
        'connectorType': 'Salesforce',
        'connectorProfileName': connector_profile,
        'sourceConnectorProperties': {         
            'Salesforce': {
                'object': obj,  
                'enableDynamicFieldUpdate': False,
                'includeDeletedRecords': True
            }          
        },
        'incrementalPullConfig': {
        'datetimeTypeFieldName': 'SystemModstamp'}
    },
    destinationFlowConfigList=[
        {
            'connectorType': 'S3',
            'connectorProfileName': connector_profile,  
            'destinationConnectorProperties': {
                'S3': {
                    'bucketName': buk_name,  
                    'bucketPrefix': buk_pre,    
                    's3OutputFormatConfig': {
                        'fileType': out_typ,
                        'prefixConfig': {
                            'prefixType': 'PATH_AND_FILENAME',       
                            'prefixFormat': 'DAY'              
                        },
                        'aggregationConfig': {
                            'aggregationType': 'None'
                        }
                    }
                },              
            }
        },
    ],
    tasks=req_mapping
    )
    return response


def mapping_csv_data(buck_name,buck_pre,flow):

    spark = SparkSession.builder.enableHiveSupport()\
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")\
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")\
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")\
            .config("spark.driver.maxResultSize", "4g")\
            .getOrCreate()
            
    df = spark.read.option('header','true').csv('s3://'+buck_name+'//'+buck_pre+'//'+flow+'.csv')  
    source_fields=df.select('source_fields').rdd.flatMap(lambda x: x).collect()
    destination_fields=df.select('destination_fields').rdd.flatMap(lambda x: x).collect()
    source_data_type=df.select('source_data_type').rdd.flatMap(lambda x: x).collect()   
    return source_fields,destination_fields,source_data_type

# check the flow status of active/draft - will start it and wait for completion or failure.
# if the flow status is deprecated/deleted.errored - will return "failure"
# this doesn't have any email or exit conditions - only return status. calling program/function needs to exit/email based on the return status
def check_status_s3_sf(flow,region_name):
    
    flow_status,latest_run_status,latest_run_status_date=create_client_connect(flow,region_name) 
    wait_count=0      

    ## If the flow is in Active or draft - start the flow and wait for completion or failure 
    if flow_status in ['Draft','Active']:
        ### S3_sf is trigger and wait for 60 secs
        start_flow(flow,region_name)
        time.sleep(60)

        flow_status,latest_run_status,latest_run_status_date=create_client_connect(flow,region_name) 

        ### cases where the s3_sf completed with in 60 seconds
        if (latest_run_status=='Successful') and (latest_run_status_date==date.today()): 
            status="success" 
            log_message(__file__, 'check_status_s3_sf()', 'info',f's3_sf completed in 60 secs. return status assigned to "{status}"')

        ### cases where the s3_sf is errored with in 60 seconds
        elif (latest_run_status=='Error') and (latest_run_status_date==date.today()): # added by ravi to handle error last runs/current triggers
            status = "failure"
            log_message(__file__, 'check_status_s3_sf()', 'error', f"{flow} has flow status as '{flow_status}' and latest run status as '{latest_run_status}' and date as '{latest_run_status_date}'. return status assigned to'{status}'")
        
        ### cases where the s3_sf is processing for more than 60 sec
        elif (latest_run_status=='InProgress'):
            while (latest_run_status  != 'Successful'):
                time.sleep(60)
                wait_count=wait_count+1
                if wait_count > 10:
                    log_message(__file__, 'check_status_s3_sf()', 'info',f's3_sf flow "{flow}" is in "{latest_run_status}" status for more than {wait_count*60} seconds.')
                    #### Use this block to terminate the program if appflow is running for longer time.
                    #### As of now we are not terminating the program, flow will keep running, program will wait till it is done.
                else:
                    log_message(__file__, 'check_status_s3_sf()', 'info',f's3_sf flow "{flow}" is in "{latest_run_status}" status - while loop check')

                flow_status, latest_run_status, latest_run_status_date = create_client_connect(flow,region_name)
                ### cases where the s3_sf is successful after some wait time passed.
                if (latest_run_status=='Successful'):
                    status="success"
                    log_message(__file__, 'check_status_s3_sf()', 'info',f's3_sf flow "{flow}" is in "{latest_run_status}" status. return status assigned to "{status}"')
                    break
                ### cases where the s3_sf is errored after some wait time passed.
                elif (latest_run_status=='Error')or (flow_status=='Errored'):
                    status="failure"   
                    log_message(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}" is in "{latest_run_status}" status. return status assigned to "{status}"')
                    break
        
        ### cases where the s3_sf is neither InProgress nor Error nor Successful - unknow status
        else:
            status = "failure"
            log_message(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}" is in "{latest_run_status}" status. return status assigned to "{status}"')
            log_message(__file__, 'check_status_s3_sf()', 'error',f'REPORT TO DEVELOPERS - UNKNOW STATUS')

    ## If the flow is in 'Deprecated','Deleted','Errored'  return 'failure' status value
    elif flow_status in ['Deprecated','Deleted','Errored']:
        status="failure"   
        log_message(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}"  has flow status as "{flow_status}". return status assigned to "{status}"')

    ## To catch any unknow status
    else: 
        status="failure"   
        log_message(__file__, 'check_status_s3_sf()', 'error',f's3_sf flow "{flow}"  has flow status as "{flow_status}". return status assigned to "{status}"')
        log_message(__file__, 'check_status_s3_sf()', 'error',f'REPORT TO DEVELOPERS - UNKNOW FLOW STATUS')
    
    return status, wait_count
        
def rejected_file_check(s3_bucket, subfolders_address):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(s3_bucket)
    fullload_files_list =[]
    
    ### subfolder address is path where rejected file is stored for the particular flow
    for object_summary in my_bucket.objects.filter(Prefix = subfolders_address):      
        files=str(object_summary.key).split("/")[-1:]
        #files=files.split("/")
        #files=files[-1:]
        fullload_files_list.append(files)

    if len(fullload_files_list) >= 1:
        # check if first file [1:]
        # fullload_files_list=fullload_files_list[1:]    
        #dates=[]
        dates_list=[]
        file_date=[]
        for file in fullload_files_list:
            # this for loop is because the datatype of fullload_files_list is list of lists [[]]
            for file_name in file:
                #dates=file_name.split(".")[0]
                #dates_l1=dates[0]
                dates = file_name
            dates_list.append(dates)
            #file_date.append(datetime.fromisoformat(dates).date())
            file_date.append(parse(dates).date())
        ## this will look for the max file - doesn't guarantee that the max file is the latest file 
        ## example flow1 and flow2 uses the same appflow and flow1 generates the rejected data and flow2 doesn't generate.
        ## in that case flow2 will also send notification that there is some rejected data.
        if (max(file_date)) == datetime.today().date():
            email_flag=True
            log_message(__file__, 'rejected_file_check()', 'info', f"{subfolders_address} : has status as rejected data with date - {max(file_date)}")
        else:
            email_flag=False
            log_message(__file__, 'rejected_file_check()', 'info', f"{subfolders_address} : has no rejected data for {datetime.today().date()}")
    else:
        email_flag=False
        log_message(__file__, 'rejected_file_check()', 'info', f"{subfolders_address} : has no rejected data")
   
    return email_flag

# This main is only for SF to S3 NOT FOR S3 to SF
def main(config_file,env,req_flows,req_obj,req_name):

    try:
        bucket = str(config_file).split('@')[0]
        key = str(config_file).split('@')[1]
        s3 = boto3.resource('s3')
        s3_bucket = s3.Bucket(bucket)
        content_object = s3_bucket.Object(key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        l2r_config_data = json.loads(file_content)
    except Exception as e:
        log_message(__file__, 'main()', 'error','Following exception occured while reading config json files.\n'+str(e))
        #email not being sent here as the topic name and region are coming from config file, and we have an error reading it
        exit(1)
    common_parameters = l2r_config_data['common_parameters']

    aws_region = common_parameters['AWS_REGION']
    connector_profile= common_parameters['connector_profile']
    bucket_name= common_parameters['Bucket_Name']
    bucket_prefix= common_parameters['Bucket_Prefix']
    output_file= common_parameters['Output_file']
    arn= common_parameters['ARN_SNS']
    start_time= common_parameters['START_TIME']
    end_time= common_parameters['END_TIME']
    time_zone= common_parameters['TIME_ZONE']
    csv_bucket= common_parameters['csv_bucket']
    csv_path= common_parameters['csv_path']
    object_properties= l2r_config_data['tables_definitions']
    flow=[] # flow name with _delta - used for triggering the flows
    flow_simple=[] # only the common name - used for reading csv mapping files
    object_name=[]
    wait_time=[]

    start_time = start_time + f".astimezone(timezone('{time_zone}'))"

    for index in range(len(object_properties)):
        flow.append(object_properties[index]['table']+"_delta")
        flow_simple.append(object_properties[index]['table'])
        object_name.append(object_properties[index]['object'])
        wait_time.append(object_properties[index]['Wait_Time'])    
    
    if len(req_flows) > 0:
        flow = req_flows # overwriting the flows to be run by the argument req_flows instead of taking it from the json file
    if len(req_obj) > 0:  
        object_name=req_obj
    if len(req_name) > 0:  
        flow_simple=req_name
    log_message(__file__, 'main()', 'info',f'flow length is {str(len(flow))} ')
    
    try:
        sucess=0
        failure=0 
        sucessful_flows=[]
        failed_flows=[]
        failed_flow_status=[]
        failed_latest_run_status=[]
        
        for i in range(len(flow)):
            log_message(__file__, 'main()', 'info',f'processing flow - {flow[i]} ')
            source_fields, destination_fields, source_data_type= mapping_csv_data(csv_bucket, csv_path,flow_simple[i])
            req_mapping=[{'sourceFields':source_fields,'connectorOperator':{'Salesforce':'PROJECTION'},'taskType':'Filter','taskProperties':{}}]            
            for j in range(len(source_fields)):
                if (source_data_type[j]) == None:
                    element={'sourceFields':[source_fields[j]],'connectorOperator':{'Salesforce':'NO_OP'},
                                'destinationField':destination_fields[j],'taskType':'Map','taskProperties':{}}
                    req_mapping.append((element))
                else:
                    element={'sourceFields':[source_fields[j]],'connectorOperator':{'Salesforce':'NO_OP'},
                                'destinationField':destination_fields[j],'taskType':'Map','taskProperties':{'SOURCE_DATA_TYPE': source_data_type[j]}}
                    req_mapping.append((element))
                    
            flow_status, latest_run_status, latest_run_status_date = create_client_connect(flow[i],aws_region)
            
            wait_count=0        
            if flow_status in ['Draft','Active','Suspended']:
                try:
                    # if it is in suspended/draft we need to start - so that it becomes active and then update
                    start_flow(flow[i],aws_region)
                    update_flow(flow[i],aws_region,object_name[i],bucket_name,bucket_prefix,output_file,end_time,time_zone,connector_profile,req_mapping)
                except:
                    # if the satus is in active - we get error during start_flow and reach this block 
                    update_flow(flow[i],aws_region,object_name[i],bucket_name,bucket_prefix,output_file,end_time,time_zone,connector_profile,req_mapping)

                time.sleep(120)
                flow_status, latest_run_status, latest_run_status_date = create_client_connect(flow[i], aws_region)

                ## This is if the delta flow is modified to schedule in 60 sec & in the next 60 secs if the flow completes successfully 
                if (latest_run_status=='Successful') and (latest_run_status_date==date.today()):
                    status="success"
                    sucess=sucess+1
                    sucessful_flows.append(flow[i])
                    revert_to_original_flow(flow[i],aws_region,object_name[i],bucket_name,bucket_prefix,output_file,start_time,end_time,time_zone,connector_profile,req_mapping)
                    log_message(__file__, 'main()', 'info',f'sf_s3 {flow[i]}  flow has udpated the schedule to trigger now. completed in 60 secs after updating schedule. Also reverted back to original schedule. return status assigned to "{status}"')

                ## This is if the delta flow is modified to schedule in 60 sec & in the next 60 secs if the flow fails  
                elif (latest_run_status=='Error') and (latest_run_status_date==date.today()):
                    status="failure"                 
                    failure=failure+1
                    failed_flows.append(flow[i])
                    failed_flow_status.append(flow_status)
                    failed_latest_run_status.append(latest_run_status)
                    log_message(__file__, 'main()', 'error',f"sf_s3 {flow[i]} flow has udpated schedule to trigger now. the flow's current run status is {latest_run_status} with date as {latest_run_status_date}.The flow is not reverted to original schedule. return status assigned to {status}")
                    break

                elif (latest_run_status=='InProgress'):
                    while (latest_run_status  != 'Successful'):
                        time.sleep(60)
                        wait_count=wait_count+1
                        if wait_count > 30:
                            log_message(__file__, 'main()', 'info',f'sf_s3 flow "{flow[i]}" is in "{latest_run_status}" status for more than {wait_count*60} seconds.')
                            #### Use this block to terminate the program if appflow is running for longer time.
                            #### As of now we are not terminating the program, flow will keep running, program will wait till it is done.
                        else:
                            log_message(__file__, 'main()', 'info',f'sf_s3 flow "{flow[i]}" is in "{latest_run_status}" status - while loop check')

                        flow_status, latest_run_status, latest_run_status_date = create_client_connect(flow[i], aws_region)
                        if (latest_run_status=='Successful') and (latest_run_status_date==date.today()):
                            status="success"
                            revert_to_original_flow(flow[i],aws_region,object_name[i],bucket_name,bucket_prefix,output_file,start_time,end_time,time_zone,connector_profile,req_mapping)
                            sucess=sucess+1
                            sucessful_flows.append(flow[i])
                            log_message(__file__, 'main()', 'info',f'sf_s3 {flow[i]} flow has udpated the schedule to trigger now. completed in after 60 secs. Also reverted back to original schedule. return status assigned to "{status}"')
                            break                        
                        elif (latest_run_status=='Error') or (flow_status=='Errored'):
                            status="failure"   
                            failure=failure+1
                            failed_flows.append(flow[i])
                            failed_flow_status.append(flow_status)
                            failed_latest_run_status.append(latest_run_status)
                            log_message(__file__, 'main()', 'error',f"sf_s3 {flow[i]} flow has udpated schedule to trigger now. After 60 sec from triggering, the flow's current ran into {latest_run_status} status with date as {latest_run_status_date}.The flow is not reverted to original schedule. return status assigned to {status}")
                            break
                
                else:
                    # this block is written to handle unknow status or the status not covered above
                    status="failure"   
                    failure=failure+1
                    failed_flows.append(flow[i])
                    failed_flow_status.append(flow_status)
                    failed_latest_run_status.append(latest_run_status)
                    log_message(__file__, 'main()', 'error',f"sf_s3 {flow[i]} has unknow latest run status as {latest_run_status} with date as {latest_run_status_date}.The flow is not reverted to original schedule. status assigned to {status}")
                    log_message(__file__, 'main()', 'error',f'REPORT TO DEVELOPERS - UNKNOW STATUS')
        
            elif flow_status in ['Deprecated','Deleted','Errored']:
                status="failure"   
                failure=failure+1
                failed_flows.append(flow[i])
                failed_flow_status.append(flow_status)
                failed_latest_run_status.append(latest_run_status)
                log_message(__file__, 'main()', 'error',f"sf_s3 {flow[i]} has status {flow_status}. return status assigned to {status}")

            ## To catch any unknow status
            else: 
                status="failure"   
                log_message(__file__, 'main()', 'error',f'sf_s3 flow "{flow[i]}"  has flow status as "{flow_status}". return status assigned to "{status}"')
                log_message(__file__, 'main()', 'error',f'REPORT TO DEVELOPERS - UNKNOW FLOW STATUS')                          

        log_message(__file__, 'main()', 'info',f"number of flow ran successfully are - {str(sucess)}. successfully flow names - {sucessful_flows}")
        log_message(__file__, 'main()', 'info',f"number of flow failed are - {str(failure)}. failed flow names - {failed_flows}")
        
        if len(failed_flows)>0:
            custom_message="Hi,\nSome of the Appflows have errored.Following are the details:\n"
            for i in range(len(failed_flows)):
                custom_message=custom_message+"\n"+str(i+1)+". flow_name: '{flow}'   , flow_status: '{flow_status}'   , latest_run_status: {latest_run_status}\n".format(flow=flow[i],flow_status=failed_flow_status[i],latest_run_status=failed_latest_run_status[i])
            
            custom_message=custom_message+"\n\nThanks," 
            email_notification(aws_region,arn,custom_message,env)
        else:
            pass

    #Need to send email notification to user:To be  modified accordingly
    except Exception as e:     
        custom_message="Hi,\nErrored has occcured while running appflow check.Following are the details:\n\n"+str(e)+"\n\n\nThanks,"
        email_notification(aws_region,arn,custom_message,env)
        status = "error"
        wait_count = -1
        log_message(__file__, 'main()', 'error',"Following are the details:\n"+str(e))
    
    return status,wait_count

if __name__=='__main__':   
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', help="Provides the datalake configurations. Required field", required=True)
    # parser.add_argument('--force_initial_load', help="Deletes respective table records from the cdc_raw_metadata table and considers as initial load. Optional field", required=False)
    parser.add_argument('--environment', help="Should be dev/test/model/prod. Required field", required=True)
    args = parser.parse_args()
    
    if args.environment is None:
        
        log_message(__file__, '__main__', 'error', '--env is required. use dev,tst,mdl or prd')
        exit(1)
    elif args.environment.lower() in ['dev', 'tst', 'mdl', 'prd']: 
        env = args.environment.lower()
    else:
        log_message(__file__, '__main__', 'error', 'invalid argument value is passed for environment. it should be one of (dev/tst/mdl/prd)')
        exit(1)
    
    if args.config_file is None:
        log_message(__file__, '__main__', 'error', '--config_file mandatory file is not passed')
        exit(1)
    else:
        req_flows = []
        req_obj=[]
        req_name=[]
        main(args.config_file, env, req_flows,req_obj,req_name)