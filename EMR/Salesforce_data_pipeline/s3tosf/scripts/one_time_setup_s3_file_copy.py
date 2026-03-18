################################# OBJECTIVE ###########################################
# To copy objects from one location to other in s3. If the destination prefix doesn't have any objects
################################ INVOKE EXAMPLE #######################################
# python3 /EC2/Scripts/Salesforce_data_pipeline/src/s3tosf/scripts/one_time_setup_s3_file_copy.py --s3_bucket <s3_code_deployment_bucket> --config_key <s3_prefix_to_json_config_file>
#######################################################################################

import boto3
import argparse
import traceback
import json
from botocore.client import ClientError


def check_objects(bucket, prefix):
    s3_client = boto3.resource('s3')
    s3_bucket = s3_client.Bucket(bucket) 
    file_check_flag = False
    for bucket_object_summary in s3_bucket.objects.filter(Prefix = prefix):
            file_check_flag = True
    return file_check_flag


if __name__ == "__main__":

    s3_client = boto3.resource('s3')

    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_bucket', help="Provide s3 bucket name", required=True)
    parser.add_argument('--config_key', help="Provide config_key", required=True)
    args = parser.parse_args()

    if args.s3_bucket is None:
        print('__main__: --s3_bucket mandatory argument is not passsed')
        exit(1)
    
    if args.config_key is None:
        print('__main__: --config_key manadatory argument is not passed')
        exit(1)
    else:
        try:
            bucket = s3_client.Bucket(args.s3_bucket)
            content_object = bucket.Object(args.config_key)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            cp_config_data = json.loads(file_content)
            for object_details in cp_config_data['copy_object']:

                key = object_details['s3_dest_key']
                object_index = len(key.split('/')) - 1
                prefix = key[0:key.find(key.split('/')[object_index])] # comes with sufix "/" 
                s3_bucket = object_details['s3_dest_bucket']

                object_check_flag = check_objects(s3_bucket,prefix)
                if object_check_flag:
                    print(f'folder already has some files in {s3_bucket} bucket {prefix} prefix')
                else:
                    copy_source = {
                        'Bucket': object_details['s3_src_bucket'],
                        'Key': object_details['s3_src_key']
                    }
                    s3_client.meta.client.copy(copy_source, object_details['s3_dest_bucket'], object_details['s3_dest_key'])
                    print(f'copy done to {s3_bucket} bucket {prefix} prefix')
            print('-----EOP---')

        except :
            print('Following error occured:')
            print(traceback.format_exc())
            exit(1)
