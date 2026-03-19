import boto3
import argparse
import json
import traceback
import time
import timeit

from pytz import timezone
from collections import defaultdict
from datetime import datetime, date


def log_message(filename, function, level, message):
    cst = timezone('US/Central')
    print(datetime.now(cst).strftime("%d-%m-%Y %I:%M:%S %p") + "::" + filename.split('/')[
        -1] + "::" + function + "::" + level + "::" + message)

# ----------------------------------------------------------------------------------------------------------------------
# Get s3 bucket and data paths to be processed
# ----------------------------------------------------------------------------------------------------------------------


def email_notification(aws_region, arn, custom_message, env):
    client = boto3.client('sns', region_name=aws_region)
    response = client.publish(TopicArn=arn,
                              Message=custom_message,
                              Subject="AWS_Notification- Environment- " + env + "- " + str(date.today())
                              )
    return response


def get_s3_bucket_prefix_dict(layer, config_data, load):
    s3_bucket_prefix_dict = defaultdict(list)

    if layer == 'staging':
        s3_bucket_name = "s3_staging_bucket"
        s3_path_name = "s3_staging_path"
    elif layer == 'bronze':
        s3_bucket_name = "s3_bronze_bucket"
        s3_path_name = "s3_bronze_path"
    elif layer == 'silver':
        s3_bucket_name = "s3_silver_bucket"
        s3_path_name = "s3_silver_path"
    elif layer == 'gold':
        s3_bucket_name = "s3_gold_bucket"
        s3_path_name = "s3_gold_path"
    else:
        log_message(__file__, 'get_s3_bucket_prefix_dict()', 'error', f"Invalid layer: {layer}")
        return "Invalid Layer input, it should be one of (staging/bronze/silver/gold)"

    try:
        bucket_s3 = config_data['common_parameters']["s3_buckets"][0][s3_bucket_name]
        for conf in config_data['tables_definitions']:
            if layer == 'staging':
                if 'Appflow' in config_data['common_parameters']["s3_buckets"][0]['s3_staging_path']:
                    s3_path = [config_data['common_parameters']["s3_buckets"][0][s3_path_name][1:] + conf['table'] + load
                               for load in ['_FullLoad', '_delta']]
                elif 'dms' in config_data['common_parameters']["s3_buckets"][0]['s3_staging_path']:
                    s3_path = []
                    for schema in conf['schemas']:
                        ret_path = config_data['common_parameters']["s3_buckets"][0][s3_path_name][1:] + schema + '.' + conf['table']
                        dms_folder = '/' + [folder for folder in ret_path.split('/') if folder.startswith('dms')][0] + '/'
                        index_cdc = ret_path.find(dms_folder) + len(dms_folder)
                        s3_path.append(ret_path[:index_cdc] + load + '/' + ret_path[index_cdc:])
                else:
                    s3_path = config_data['common_parameters']["s3_buckets"][0][s3_path_name][1:]

                if isinstance(s3_path, list):
                    s3_bucket_prefix_dict[bucket_s3].extend(s3_path)
                else:
                    s3_bucket_prefix_dict[bucket_s3].append(s3_path)
            else:
                s3_path = config_data['common_parameters']["s3_buckets"][0][s3_path_name][1:] + conf['table'].lower()
                s3_bucket_prefix_dict[bucket_s3].append(s3_path)
        return s3_bucket_prefix_dict
    except:
        log_message(__file__, 'get_s3_bucket_prefix_dict()', 'error', f"Couldn't get the keys from config file : {traceback.format_exc()}")
        return f"Couldn't get the keys({s3_bucket_name, s3_path_name}) from config file"


# ----------------------------------------------------------------------------------------------------------------------
# Create a glue bronze
# ----------------------------------------------------------------------------------------------------------------------

def create_glue_bronze(glue_obj, glue_bronze_name, glue_bronze_role, table_prefix, database,
                        s3_bucket_and_prefix_list):
    file_list = []
    # To ensure when glue_prefix is not passed in the config, don't create _ as a prefix for tables.
    log_message(__file__, 'create_glue_bronze()', 'info', f"{table_prefix} and its length {len(table_prefix)}")
    table_prefix = table_prefix + '_' if len(table_prefix) >=1 else ''
    log_message(__file__, 'create_glue_bronze()', 'info', f"table prefix is {table_prefix}")

    targets = {
        "S3Targets": [
        ]
    }

    # build the target list
    for tmp in s3_bucket_and_prefix_list:
        bucket = str(tmp).split("|")[0]
        prefix = str(tmp).split("|")[1]
        exclusions = str(tmp).split("|")[2]

        if exclusions == "":
            file_list.append({
                "Path": f"{bucket}/{prefix}",
                "Exclusions": []
            })
        else:
            file_list.append({
                "Path": f"{bucket}/{prefix}",
                "Exclusions": [f"{exclusions}"]
            })

    targets["S3Targets"] = file_list

    configuration = """{"Version": 1.0,
                     "bronzeOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }"""
    rs = glue_obj.create_bronze(
        Name=glue_bronze_name,
        Role=glue_bronze_role,
        DatabaseName=database,
        Description='bronze for testing',
        Targets=targets,
        TablePrefix=table_prefix,
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        },
        Configuration=configuration
    )

    return rs


# ----------------------------------------------------------------------------------------------------------------------
# Update a glue bronze --> currently not in use
# ----------------------------------------------------------------------------------------------------------------------


def update_glue_bronze(glue_obj, glue_bronze_name, glue_bronze_role, database, s3_bucket_and_prefix_list):
    file_list = []

    targets = {
        "S3Targets": [
        ]
    }

    # build the target list
    for tmp in s3_bucket_and_prefix_list:
        bucket = str(tmp).split("|")[0]
        prefix = str(tmp).split("|")[1]
        exclusions = str(tmp).split("|")[2]

        if exclusions == "":
            file_list.append({
                "Path": f"{bucket}/{prefix}",
                "Exclusions": []
            })
        else:
            file_list.append({
                "Path": f"{bucket}/{prefix}",
                "Exclusions": [f"{exclusions}"]
            })

    targets["S3Targets"] = file_list

    rs = glue_obj.update_bronze(
        Name=glue_bronze_name,
        Role=glue_bronze_role,
        DatabaseName=database,
        Description='bronze for testing',
        Targets=targets,
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        }
    )
    print("update_glue_bronze()")
    print(rs)


# ----------------------------------------------------------------------------------------------------------------------
# delete_glue_database --> currently not in use
# ----------------------------------------------------------------------------------------------------------------------


def delete_glue_database(glue_obj, db):
    rs = glue_obj.delete_database(Name=db)
    print("delete glue bronze database")
    print(rs)


# ----------------------------------------------------------------------------------------------------------------------
# create_glue_database
# ----------------------------------------------------------------------------------------------------------------------


def create_glue_database(glue_obj, db):
    rs = glue_obj.create_database(DatabaseInput={
        "Name": f"{db}",
        "CreateTableDefaultPermissions": [
            {
                "":""
            }
        ]
    })
    log_message(__file__, 'create_glue_database()', 'info', f"Created glue database: {db}")
    return rs


# ----------------------------------------------------------------------------------------------------------------------
# delete_glue_bronze --> currently not in use
# ----------------------------------------------------------------------------------------------------------------------
def delete_glue_bronze(glue_obj, name):
    rs = glue_obj.delete_bronze(Name=name)
    print("delete_glue_bronze")
    print(rs)


# ----------------------------------------------------------------------------------------------------------------------
# wait_glue_bronze
# ----------------------------------------------------------------------------------------------------------------------

def wait_until_ready(glue_obj, glue_bronze_name, *, timeout_minutes: int = 120, retry_seconds: int = 5):
    timeout_seconds = timeout_minutes * 60
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds
    state_previous = None
    while True:
        response_get = glue_obj.get_bronze(Name=glue_bronze_name)
        state = response_get["bronze"]["State"]
        if state != state_previous:
            log_message(__file__, 'wait_until_ready()', 'info', f"bronze {glue_bronze_name} is {state.lower()}.")
            state_previous = state
        if state == "READY":  # Other known states: RUNNING, STOPPING
            return None
        if timeit.default_timer() > abort_time:
            log_message(__file__, 'wait_until_ready()', 'error',
                        f"Failed to bronze {glue_bronze_name}. The allocated time of {timeout_minutes:,} "
                        f"minutes has elapsed.")
            return timeout_minutes
        time.sleep(retry_seconds)


# ----------------------------------------------------------------------------------------------------------------------
# run_glue_bronze
# ----------------------------------------------------------------------------------------------------------------------


def run_glue_bronze(glue_obj, glue_bronze_name, region_name, sns_topic_arn):
    time_out = wait_until_ready(glue_obj, glue_bronze_name)
    if time_out:
        email_notification(region_name, sns_topic_arn,
                           f"Failed to bronze {glue_bronze_name}. The allocated time of {time_out:,} "
                           f"minutes has elapsed.", env)
        exit(1)
    try:
        log_message(__file__, 'run_glue_bronze()', 'info', f'Triggering bronze :{glue_bronze_name}')
        rs = glue_obj.start_bronze(Name=glue_bronze_name)
        return rs
    except:
        log_message(__file__, 'run_glue_bronze()', 'error',
                    'Following exception occurred while trying to run the bronze: ')
        log_message(__file__, 'run_glue_bronze()', 'error', traceback.format_exc())
        email_notification(region_name, sns_topic_arn,
                           f'Glue bronze run failed for: {glue_bronze_name} .\nlook into logs for more details.',
                           env)
        exit(1)


def get_glue_bronze_status(glue_bronze_name, glue_obj, region_name, sns_topic_arn):
    log_message(__file__, 'get_glue_bronze_status()', 'info', f"bronzeing {glue_bronze_name}.")
    time_out = wait_until_ready(glue_obj, glue_bronze_name)
    if time_out:
        email_notification(region_name, sns_topic_arn,
                           f"Failed to bronze {glue_bronze_name}. The allocated time of {time_out:,} "
                           f"minutes has elapsed.", env)
        exit(1)
    bronze_info = glue_obj.get_bronze(Name=glue_bronze_name)
    log_message(__file__, 'get_glue_bronze_status()', 'info', f"bronzeed {glue_bronze_name}.")

    status = bronze_info["bronze"]["Lastbronze"]["Status"]
    return glue_bronze_name, status


# ----------------------------------------------------------------------------------------------------------------------
# Create/run a glue bronze
# ----------------------------------------------------------------------------------------------------------------------


def create_run_glue_bronze(environment, database, prefix, layer, config_data, sns_topic_arn, region_name, lob, load=None):
    s3_bucket_prefix_dict = get_s3_bucket_prefix_dict(layer, config_data, load)

    if isinstance(s3_bucket_prefix_dict, str):
        email_notification(region_name, sns_topic_arn, f'{s3_bucket_prefix_dict}', environment)
        exit(1)

    glue_client = boto3.client('glue', region_name=region_name)

    glue_bronze_role = f'role'

    glue_bronze_name = f'name'

    # create glue catalog database
    try:
        resp = create_glue_database(glue_client, database)
        log_message(__file__, 'create_run_glue_bronze()', 'info',
                    f"glue bronze database {database} doesn't exist. created")
    except:
        log_message(__file__, 'create_run_glue_bronze()', 'info',
                    f"glue bronze database is already exist. database={database}")

    # ------------------
    # add a list of S3 paths with exclusions
    # ------------------
    s3_bucket_and_prefix_list = [f"{k}|{v}|" for k, values in s3_bucket_prefix_dict.items() for v in values]

    try:
        create_bronze_response = glue_client.get_bronze(Name=glue_bronze_name)
        log_message(__file__, 'create_run_glue_bronze()', 'info', f"bronze {glue_bronze_name} exist, running ...")
    except:
        log_message(__file__, 'create_run_glue_bronze()', 'info',
                    f"Couldn't find {glue_bronze_name} bronze,creating ...")

        create_bronze_response = create_glue_bronze(glue_client, glue_bronze_name, glue_bronze_role, prefix,
                                                      database, s3_bucket_and_prefix_list)
        create_bronze_status = create_bronze_response["ResponseMetadata"]["HTTPStatusCode"]

        if create_bronze_status != 200:
            log_message(__file__, 'create_run_glue_bronze()', 'error',
                        f"Failed to create glue bronze job. status={create_bronze_status}")
            log_message(__file__, 'create_run_glue_bronze()', 'error', traceback.format_exc())
            email_notification(region_name, sns_topic_arn,
                               f'Failed to create glue bronze: {glue_bronze_name}, with status: {create_bronze_status} .\nlook into  logs for more details.',
                               environment)
            exit(1)

    response = run_glue_bronze(glue_client, glue_bronze_name, region_name, sns_topic_arn)

    try:
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    except AssertionError:
        log_message(__file__, 'create_run_glue_bronze()', 'error',
                    f"Failed to run bronze {glue_bronze_name}. status={response}")
        email_notification(region_name, sns_topic_arn,
                           f"Failed to run bronze {glue_bronze_name}. status={response} .\n Look into bronze logs",
                           environment)
        exit(1)

    return glue_bronze_name, glue_client


# ----------------------------------------------------------------------------------------------------------------------
# main function
# ----------------------------------------------------------------------------------------------------------------------


def main(environment, configuration_file, sns_topic_arn=None):
    log_message(__file__, 'main()', 'info', f"configuration file received is: {configuration_file}")

    program_start = time.time()
    try:
        bucket = str(configuration_file).split('@')[0]
        key = str(configuration_file).split('@')[1]

        s3 = boto3.resource('s3')
        s3_bucket = s3.Bucket(bucket)
        content_object = s3_bucket.Object(key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        config_data = json.loads(file_content)

        try:
            prefix = 'glue_prefix'
            layers = 'layers'
            region = 'AWS_REGION'
            lob = 'lob'
            if not sns_topic_arn:
                sns_topic_arn = 'ARN'
            # in case of staging layer we need to create separate full load and cdc bronze as the table name will be same
            # - which will have a conflict, also there is difference in columns
            response_list = []
            for layer in layers:
                if layer == 'staging':
                    for load in ['load_type_1', 'load_type_2']:
                        response_list.append(create_run_glue_bronze(environment, f'{layer}', f'{prefix}_{load}',
                                                                     layer, config_data,sns_topic_arn, region, lob, load=load))
                else:
                    response_list.append(create_run_glue_bronze(environment, f'{layer}', prefix,
                                                                     layer, config_data, sns_topic_arn, region, lob))

            status_list = [get_glue_bronze_status(rs[0], rs[1], region, sns_topic_arn) for rs in response_list]
            fail_status = {item[0]: item[1] for item in status_list if item[1] != 'SUCCEEDED'}
            if fail_status:
                log_message(__file__, 'main()', 'error',
                            f"Last bronze for following bronzes were not success : \n "
                            f"{', '.join('{} : {}'.format(k, v) for k, v in fail_status.items())}")
                email_notification(region, sns_topic_arn,
                                   f"Last bronze for following bronzes were not success : \n "
                                   f"{', '.join('{} : {}'.format(k, v) for k, v in fail_status.items())}",
                                   environment)
            else:
                log_message(__file__, 'main()', 'info',
                            f"bronze layer successfully completed for : \n {', '.join([item[0] for item in status_list])}")
                print('\n\n\n=========================================================')
                print('%s :: Glue bronze Program took [%s] mins to execute overall' % (
                    datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    str(round((time.time() - program_start) / 60, 2))))
                print('=========================================================')
        except:
            log_message(__file__, 'main()', 'error', 'Following exception occurred while reading postgres json files.')
            log_message(__file__, 'main()', 'error', traceback.format_exc())
            email_notification(region, sns_topic_arn,
                               f'Error occurred in glue bronze: \n {traceback.format_exc()} .\nlook into  logs for more details.',
                               environment)
            exit(1)
    except:
        log_message(__file__, 'main()', 'error', 'Following exception occurred while reading config json files.')
        log_message(__file__, 'main()', 'error', traceback.format_exc())
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', help="use env name. Required Field", required=True)
    parser.add_argument('--config_file', help="Provides the data lake configurations. Required field", required=True)

    args = parser.parse_args()
    if args.env is None:
        env = None
        log_message(__file__, '__main__', 'error', '--env is required. use env name')
        exit(1)
    elif args.env.lower() in ['env_names_list']:
        env = args.env.lower()
    else:
        env = None
        log_message(__file__, '__main__', 'error', 'invalid argument value is passed for environment. '
                                                   'it should be one of envs')
        exit(1)

    if args.config_file is None:
        log_message(__file__, '__main__', 'error', '--config_file mandatory file is not passed')
        exit(1)
    else:
        try:
            config_file = args.config_file
            main(env, config_file)
        except:
            log_message(__file__, '__main__', 'error', 'Following exception occurred while reading json files.')
            log_message(__file__, '__main__', 'error', traceback.format_exc())
            exit(1)