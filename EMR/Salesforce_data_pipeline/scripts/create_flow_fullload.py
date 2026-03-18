import boto3
from datetime import date
import time
from datetime import datetime,timedelta
import json
import argparse
from pyspark.sql import SparkSession
from pytz import timezone
from botocore.config import Config as boto3_config

def log_message(filename, function, level, message):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "::" + filename.split('/')[
        -1] + "::" + function + "::" + level + "::" + message)

def get_kms_arn(aws_region, env):
    try:
        global kms_cmk_arn
        config = boto3_config(
            retries={
                'max_attempts': 30,
                'mode': 'standard'
            }
        )
        client = boto3.client('kms', region_name = aws_region,config=config)
        if env in ['dev', 'tst']:
            kms_cmk_key = f'alias/<kms_key_name>'
        elif env in ['mdl', 'prd']:
            kms_cmk_key = f'alias/<kms_key_name>'
        response = client.describe_key(KeyId=kms_cmk_key)
        kms_cmk_arn = response['KeyMetadata']['Arn']
    except Exception as e:
        log_message(__file__, 'get_kms_arn', 'error', f'Failed to get kms_arn. Below is the error:\n' + str(e))

def create_flow(region_name,flow,object_name,buck_name,buck_pre,out_typ,
                start_time,end_time,time_zone,connector_profile,req_mapping, kms_cmk_arn):
    try:
        config = boto3_config(
            retries={
                'max_attempts': 30,
                'mode': 'standard'
            }
        )
        client = boto3.client('appflow', region_name=region_name,config=config)
        response = client.create_flow(
        flowName = flow,
        description ='creating_FullLoad_flows_using_csv',
        kmsArn = kms_cmk_arn,
            triggerConfig={
                'triggerType': 'OnDemand',
            },
            sourceFlowConfig={
                'connectorType': 'Salesforce',
                'connectorProfileName': connector_profile,
                'sourceConnectorProperties': {
                    'Salesforce': {
                        'object': object_name,
                        'enableDynamicFieldUpdate': False,
                        'includeDeletedRecords': True
                    }
                },

            },
            destinationFlowConfigList=[
                {
                    'connectorType': 'S3',
                    'connectorProfileName': connector_profile,
                    'destinationConnectorProperties': {
                        'S3': {
                            'bucketName': buck_name,
                            'bucketPrefix': buck_pre,
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
        print(f'created {response}')
        return response
    except Exception as e:
        log_message(__file__, 'create_flow', 'error', f'Failed to create appflow. Below is the error:\n' + str(e))


def update_flow(region_name,flow,object_name,buck_name,buck_pre,out_typ,
                start_time,end_time,time_zone,connector_profile,req_mapping):
    try:

        config = boto3_config(
            retries={
                'max_attempts': 30,
                'mode': 'standard'
            }
        )
        client = boto3.client('appflow', region_name=region_name,config=config)
        response = client.update_flow(
        flowName=flow,
        description='modify fileds using csv json files',
        triggerConfig={
            'triggerType': 'OnDemand',
        },
        sourceFlowConfig={
            'connectorType': 'Salesforce',
            'connectorProfileName': connector_profile,
            'sourceConnectorProperties': {
                'Salesforce': {
                    'object': object_name,
                    'enableDynamicFieldUpdate': False,
                    'includeDeletedRecords': True
                }
            },
            
        },
        destinationFlowConfigList=[
            {
                'connectorType': 'S3',
                'connectorProfileName': connector_profile,
                'destinationConnectorProperties': {
                    'S3': {
                        'bucketName': buck_name,
                        'bucketPrefix': buck_pre,
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
    except Exception as e:
        log_message(__file__, 'update_flow', 'error', f'Failed to update appflow. Below is the error:\n' + str(e))


def start_flow(region_name,flow):
    try:
        config = boto3_config(
            retries={
                'max_attempts': 30,
                'mode': 'standard'
            }
        )
        client = boto3.client('appflow', region_name=region_name,config=config)
        response = client.start_flow(flowName=flow)
        return response
    except Exception as e:
        log_message(__file__, 'start_flow', 'error', f'Failed to start appflow. Below is the error:\n' + str(e))


def mapping_csv_data(buck_name,buck_pre,flow):

    try:

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
    except Exception as e:
        log_message(__file__, 'mapping_csv_data', 'error', f'Failed to map_csv_file. Below is the error:\n' + str(e))

if __name__=='__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--config_file', help="Provides the datalake configurations. Required field", required=True)
        parser.add_argument('--environment', help="Should be dev/test/model/prod. Required field", required=True)
        args = parser.parse_args()
        
        if args.environment is None:
            print('m(): --environment mandatory argument is not passsed')
            exit(1)
        elif args.environment.lower() in ['dev', 'tst', 'mdl', 'prd']: 
            env = args.environment.lower()
        else:
            print('m(): invalid arugument value is passed for environment. it should be one of (dev/tst/mdl/prd)')
            exit(1)
        
        if args.config_file is None:
            print('m(): --config_file manadatory file is not passed')
            exit(1)
            
        else:
            bucket = str(args.config_file).split('@')[0]
            key = str(args.config_file).split('@')[1]
            s3 = boto3.resource('s3')
            s3_bucket = s3.Bucket(bucket)
            content_object = s3_bucket.Object(key)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            config_data = json.loads(file_content)
            
        aws_region = config_data['AWS_REGION']
        connector_profile= config_data['connector_profile']
        bucket_name= config_data['Bucket_Name']
        bucket_prefix= config_data['Bucket_Prefix']
        output_file= config_data['Output_file']
        start_time= config_data['START_TIME']
        end_time= config_data['END_TIME']
        time_zone= config_data['TIME_ZONE']
        csv_bucket= config_data['csv_bucket']     
        csv_path= config_data['csv_path']        
        object_properties= config_data['tables_definitions']
        count_created=0
        count_updated=0    
        flow=[]
        flow_simple=[]
        Object_Name=[]  
        
        get_kms_arn(aws_region, env)

        
        for index in range(len(object_properties)):
            flow.append(object_properties[index]['flow_common_name']+"_FullLoad")
            flow_simple.append(object_properties[index]['flow_common_name'])
            Object_Name.append(object_properties[index]['name'])    
        
        for i in range(len(flow)):
            print("*********")
            print(flow[i])
            source_fields,destination_fields,source_data_type= mapping_csv_data(csv_bucket,csv_path,flow_simple[i])
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
            
            try:
                create_flow(aws_region,flow[i],Object_Name[i],bucket_name,bucket_prefix,output_file,
                            start_time,end_time,time_zone,connector_profile,req_mapping, kms_cmk_arn)
                count_created=count_created+1
                time.sleep(10) 
                start_flow(aws_region,flow[i])
            except:
                update_flow(aws_region,flow[i],Object_Name[i],bucket_name,bucket_prefix,output_file,
                            start_time,end_time,time_zone,connector_profile,req_mapping)
                count_updated=count_updated+1
                time.sleep(10) 
                start_flow(aws_region,flow[i])
                
        print("No of Flows created :",count_created)
        print("----------------------")
        print("No of Flows updated :",count_updated)
        
    except Exception as e:
        print("Following error has occured:\n"+ str(e))
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    