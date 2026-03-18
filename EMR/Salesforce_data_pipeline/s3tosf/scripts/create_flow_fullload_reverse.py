import boto3
from datetime import date
import time
from datetime import datetime, timedelta
import json
import argparse
from pyspark.sql import SparkSession
from pytz import timezone

def get_kms_arn(aws_region, env):
    global kms_cmk_arn
    client = boto3.client('kms', region_name = aws_region)
    if env in ['dev', 'tst']:
        kms_cmk_key = f'alias/<kms_key_alias>'
    elif env in ['mdl', 'prd']:
        kms_cmk_key = f'alias/<kms_key_alias>'
    response = client.describe_key(KeyId=kms_cmk_key)
    kms_cmk_arn = response['KeyMetadata']['Arn']


def create_flow(region_name, flow, object_name, buck_name, buck_pre, buck_pre_error, file_type, connector_profile,
                req_mapping, ext_id, writeOperationType, kms_cmk_arn):
    client = boto3.client('appflow', region_name=region_name)
    try:
        print('entered create_flow block')
        response = client.create_flow(
        flowName=flow,
        description='creating_FullLoad_flows_using_csv',
        kmsArn = kms_cmk_arn,
        triggerConfig={'triggerType': 'OnDemand'},
        sourceFlowConfig={
            'connectorType': 'S3',
            'connectorProfileName': connector_profile,
            'sourceConnectorProperties': {
                'S3': {
                    'bucketName': buck_name,
                    'bucketPrefix': buck_pre,
                    's3InputFormatConfig': {'s3InputFileType': file_type}
                }
            }
        },

        destinationFlowConfigList=[
            {
                'connectorType': 'Salesforce',
                'connectorProfileName': connector_profile,
                'destinationConnectorProperties': {
                    'Salesforce': {
                        'object': object_name,
                        'idFieldNames': [ext_id],
                        'errorHandlingConfig': {
                            'failOnFirstDestinationError': False,
                            'bucketPrefix': buck_pre_error,
                            'bucketName': buck_name
                        },
                        'writeOperationType': writeOperationType
                    }
                }
            }
        ],

        tasks=req_mapping
        )
        return response
    except Exception as e :
        try:
            print(f'create_flow ran into exception and it is \n {e}')
            response = client.create_flow(
            flowName=flow,
            description='creating_FullLoad_flows_using_csv',
            kmsArn = kms_cmk_arn,
            triggerConfig={'triggerType': 'OnDemand'},
            sourceFlowConfig={
                'connectorType': 'S3',
                'connectorProfileName': connector_profile,
                'sourceConnectorProperties': {
                    'S3': {
                        'bucketName': buck_name,
                        'bucketPrefix': buck_pre,
                        's3InputFormatConfig': {'s3InputFileType': file_type}
                    }
                }
            },

            destinationFlowConfigList=[
                {
                    'connectorType': 'Salesforce',
                    'connectorProfileName': connector_profile,
                    'destinationConnectorProperties': {
                        'Salesforce': {
                            'object': object_name,
                            'errorHandlingConfig': {
                                'failOnFirstDestinationError': False,
                                'bucketPrefix': buck_pre_error,
                                'bucketName': buck_name
                            },
                            'writeOperationType': writeOperationType
                        }
                    }
                }
            ],

            tasks=req_mapping
            )
            print(f'create_flow exception block is done \n{response}')
            return response
        except Exception as e:
            print(f'create_flow exception block also gave an exception \n{e}')


def update_flow(region_name, flow, object_name, buck_name, buck_pre, buck_pre_error, file_type, connector_profile,
                req_mapping, ext_id, writeOperationType):
    client = boto3.client('appflow', region_name=region_name)
    try:
        print(f'update_flow block')
        response = client.update_flow(
        flowName=flow,
        description='creating_FullLoad_flows_using_csv',
        triggerConfig={'triggerType': 'OnDemand'},
        sourceFlowConfig={
            'connectorType': 'S3',
            'sourceConnectorProperties': {
                'S3': {
                    'bucketName': buck_name,
                    'bucketPrefix': buck_pre,
                    's3InputFormatConfig': {'s3InputFileType': file_type}
                }
            }
        },

        destinationFlowConfigList=[
            {
                'connectorType': 'Salesforce',
                'connectorProfileName': connector_profile,
                'destinationConnectorProperties': {
                    'Salesforce': {
                        'object': object_name,
                        'idFieldNames': [ext_id],
                        'errorHandlingConfig': {
                            'failOnFirstDestinationError': False,
                            'bucketPrefix': buck_pre_error,
                            'bucketName': buck_name
                        },
                        'writeOperationType': writeOperationType
                    }
                }
            }
        ],

        tasks=req_mapping
        )
        return response
    except Exception as e:
        print(f'update_flow block ran into exception and it is \n {e}')
        response = client.update_flow(
        flowName=flow,
        description='creating_FullLoad_flows_using_csv',
        triggerConfig={'triggerType': 'OnDemand'},
        sourceFlowConfig={
            'connectorType': 'S3',
            'sourceConnectorProperties': {
                'S3': {
                    'bucketName': buck_name,
                    'bucketPrefix': buck_pre,
                    's3InputFormatConfig': {'s3InputFileType': file_type}
                }
            }
        },

        destinationFlowConfigList=[
            {
                'connectorType': 'Salesforce',
                'connectorProfileName': connector_profile,
                'destinationConnectorProperties': {
                    'Salesforce': {
                        'object': object_name,
                        'errorHandlingConfig': {
                            'failOnFirstDestinationError': False,
                            'bucketPrefix': buck_pre_error,
                            'bucketName': buck_name
                        },
                        'writeOperationType': writeOperationType
                    }
                }
            }
        ],

        tasks=req_mapping
        )
        print(f'update_flow exception is done and the response is \n {response}')
        return response


def start_flow(region_name, flow):
    client = boto3.client('appflow', region_name=region_name)
    response = client.start_flow(flowName=flow)
    return response


def mapping_csv_data(buck_name, buck_pre, flow):
    spark = SparkSession.builder.enableHiveSupport() \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()

    df = spark.read.option('header', 'true').csv('s3://' + buck_name + '//' + buck_pre + '//' + flow + '.csv')
    source_fields = df.select('source_fields').rdd.flatMap(lambda x: x).collect()
    destination_fields = df.select('destination_fields').rdd.flatMap(lambda x: x).collect()
    source_data_type = df.select('source_data_type').rdd.flatMap(lambda x: x).collect()
    return source_fields, destination_fields, source_data_type


def main(config_file, env):
    bucket = str(config_file).split('@')[0]
    key = str(config_file).split('@')[1]
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    content_object = s3_bucket.Object(key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    config_data = json.loads(file_content)

    aws_region = config_data['AWS_REGION']
    connector_profile = config_data['connector_profile']
    bucket_name = config_data['Bucket_Name']
    file_type = config_data['File_type']
    csv_bucket = config_data['csv_bucket']
    csv_path = config_data['csv_path']
    object_properties = config_data['tables_definitions']
    count_created = 0
    count_updated = 0
    flow = []
    flow_simple = []
    Object_Name = []
    external_id = []
    bucket_prefix = []
    bucket_prefix_error = []
    writeOperationType = []

    get_kms_arn(aws_region, env)

    for index in range(len(object_properties)):
        flow.append(object_properties[index]['flow_common_name'] + "_FullLoad")
        flow_simple.append(object_properties[index]['flow_common_name'])
        Object_Name.append(object_properties[index]['name'])
        external_id.append(object_properties[index]['ext_id'])
        bucket_prefix.append(object_properties[index]['Bucket_Prefix'])
        bucket_prefix_error.append(object_properties[index]['Bucket_Prefix_error'])
        writeOperationType.append(object_properties[index]['writeOperationType'])

    for i in range(len(flow)):
        print(flow[i])
        print("*********")
        source_fields, destination_fields, source_data_type = mapping_csv_data(csv_bucket, csv_path, flow_simple[i])
        req_mapping = [{'sourceFields': source_fields, 'connectorOperator': {'S3': 'PROJECTION'}, 'taskType': 'Filter',
                        'taskProperties': {}}]

        for j in range(len(source_fields)):
            if (source_data_type[j]) == None:
                element = {'sourceFields': [source_fields[j]], 'connectorOperator': {'S3': 'NO_OP'},
                           'destinationField': destination_fields[j], 'taskType': 'Map', 'taskProperties': {}}
                req_mapping.append((element))
            else:
                element = {'sourceFields': [source_fields[j]], 'connectorOperator': {'S3': 'NO_OP'},
                           'destinationField': destination_fields[j], 'taskType': 'Map',
                           'taskProperties': {'DESTINATION_DATA_TYPE': source_data_type[j]}}
                req_mapping.append((element))

        try:
            create_flow(aws_region, flow[i], Object_Name[i], bucket_name, bucket_prefix[i], bucket_prefix_error[i],
                        file_type,
                        connector_profile, req_mapping, external_id[i], writeOperationType[i], kms_cmk_arn)
            count_created = count_created + 1
            start_flow(aws_region,flow[i])
        except:
            update_flow(aws_region, flow[i], Object_Name[i], bucket_name, bucket_prefix[i], bucket_prefix_error[i],
                        file_type,
                        connector_profile, req_mapping, external_id[i], writeOperationType[i])
            count_updated = count_updated + 1

    print("No of Flows created :", count_created)
    print("----------------------")
    print("No of Flows updated :", count_updated)


if __name__ == '__main__':
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
            main(args.config_file, env)

    except Exception as e:
        print("Following error has occured:\n" + str(e))
