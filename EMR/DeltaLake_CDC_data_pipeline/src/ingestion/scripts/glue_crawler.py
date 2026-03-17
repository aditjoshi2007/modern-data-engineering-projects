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

    if layer == 'landing':
        s3_bucket_name = "s3_landing_bucket"
        s3_path_name = "s3_landing_path"
    elif layer == 'raw':
        s3_bucket_name = "s3_raw_bucket"
        s3_path_name = "s3_raw_path"
    elif layer == 'raw_current':
        s3_bucket_name = "s3_raw_bucket"
        s3_path_name = "s3_raw_current_latest_path"
    elif layer == 'curated':
        s3_bucket_name = "s3_curated_bucket"
        s3_path_name = "s3_curated_path"
    else:
        log_message(__file__, 'get_s3_bucket_prefix_dict()', 'error', f"Invalid layer: {layer}")
        return "Invalid Layer input, it should be one of (landing/raw/raw_current/curated)"

    try:
        bucket_s3 = config_data['common_parameters']["s3_buckets"][0][s3_bucket_name]
        for conf in config_data['tables_definitions']:
            if layer == 'landing':
                if 'Appflow' in config_data['common_parameters']["s3_buckets"][0]['s3_landing_path']:
                    s3_path = [config_data['common_parameters']["s3_buckets"][0][s3_path_name][1:] + conf['table'] + load
                               for load in ['_FullLoad', '_delta']]
                elif 'dms' in config_data['common_parameters']["s3_buckets"][0]['s3_landing_path']:
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
# Create a glue crawler
# ----------------------------------------------------------------------------------------------------------------------

def create_glue_crawler(glue_obj, glue_crawler_name, glue_crawler_role, table_prefix, database,
                        s3_bucket_and_prefix_list):
    file_list = []
    # To ensure when glue_prefix is not passed in the config, don't create _ as a prefix for tables.
    log_message(__file__, 'create_glue_crawler()', 'info', f"{table_prefix} and its length {len(table_prefix)}")
    table_prefix = table_prefix + '_' if len(table_prefix) >=1 else ''
    log_message(__file__, 'create_glue_crawler()', 'info', f"table prefix is {table_prefix}")

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
                     "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }"""
    rs = glue_obj.create_crawler(
        Name=glue_crawler_name,
        Role=glue_crawler_role,
        DatabaseName=database,
        Description='Crawler for testing',
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
# Update a glue crawler --> currently not in use
# ----------------------------------------------------------------------------------------------------------------------


def update_glue_crawler(glue_obj, glue_crawler_name, glue_crawler_role, database, s3_bucket_and_prefix_list):
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

    rs = glue_obj.update_crawler(
        Name=glue_crawler_name,
        Role=glue_crawler_role,
        DatabaseName=database,
        Description='Crawler for testing',
        Targets=targets,
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        }
    )
    print("update_glue_crawler()")
    print(rs)


# ----------------------------------------------------------------------------------------------------------------------
# delete_glue_database --> currently not in use
# ----------------------------------------------------------------------------------------------------------------------


def delete_glue_database(glue_obj, db):
    rs = glue_obj.delete_database(Name=db)
    print("delete glue crawler database")
    print(rs)


# ----------------------------------------------------------------------------------------------------------------------
# create_glue_database
# ----------------------------------------------------------------------------------------------------------------------


def create_glue_database(glue_obj, db):
    rs = glue_obj.create_database(DatabaseInput={
        "Name": f"{db}",
        "CreateTableDefaultPermissions": [
            {
                "Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                "Permissions": ["ALL"]
            }
        ]
    })
    log_message(__file__, 'create_glue_database()', 'info', f"Created glue database: {db}")
    return rs


# ----------------------------------------------------------------------------------------------------------------------
# delete_glue_crawler --> currently not in use
# ----------------------------------------------------------------------------------------------------------------------
def delete_glue_crawler(glue_obj, name):
    rs = glue_obj.delete_crawler(Name=name)
    print("delete_glue_crawler")
    print(rs)


# ----------------------------------------------------------------------------------------------------------------------
# wait_glue_crawler
# ----------------------------------------------------------------------------------------------------------------------

def wait_until_ready(glue_obj, glue_crawler_name, *, timeout_minutes: int = 120, retry_seconds: int = 5):
    timeout_seconds = timeout_minutes * 60
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds
    state_previous = None
    while True:
        response_get = glue_obj.get_crawler(Name=glue_crawler_name)
        state = response_get["Crawler"]["State"]
        if state != state_previous:
            log_message(__file__, 'wait_until_ready()', 'info', f"Crawler {glue_crawler_name} is {state.lower()}.")
            state_previous = state
        if state == "READY":  # Other known states: RUNNING, STOPPING
            return None
        if timeit.default_timer() > abort_time:
            log_message(__file__, 'wait_until_ready()', 'error',
                        f"Failed to crawl {glue_crawler_name}. The allocated time of {timeout_minutes:,} "
                        f"minutes has elapsed.")
            return timeout_minutes
        time.sleep(retry_seconds)


# ----------------------------------------------------------------------------------------------------------------------
# run_glue_crawler
# ----------------------------------------------------------------------------------------------------------------------


def run_glue_crawler(glue_obj, glue_crawler_name, region_name, sns_topic_arn):
    time_out = wait_until_ready(glue_obj, glue_crawler_name)
    if time_out:
        email_notification(region_name, sns_topic_arn,
                           f"Failed to crawl {glue_crawler_name}. The allocated time of {time_out:,} "
                           f"minutes has elapsed.", env)
        exit(1)
    try:
        log_message(__file__, 'run_glue_crawler()', 'info', f'Triggering crawler :{glue_crawler_name}')
        rs = glue_obj.start_crawler(Name=glue_crawler_name)
        return rs
    except:
        log_message(__file__, 'run_glue_crawler()', 'error',
                    'Following exception occurred while trying to run the crawler: ')
        log_message(__file__, 'run_glue_crawler()', 'error', traceback.format_exc())
        email_notification(region_name, sns_topic_arn,
                           f'Glue crawler run failed for: {glue_crawler_name} .\nlook into emr logs for more details.',
                           env)
        exit(1)


def get_glue_crawler_status(glue_crawler_name, glue_obj, region_name, sns_topic_arn):
    log_message(__file__, 'get_glue_crawler_status()', 'info', f"Crawling {glue_crawler_name}.")
    time_out = wait_until_ready(glue_obj, glue_crawler_name)
    if time_out:
        email_notification(region_name, sns_topic_arn,
                           f"Failed to crawl {glue_crawler_name}. The allocated time of {time_out:,} "
                           f"minutes has elapsed.", env)
        exit(1)
    crawler_info = glue_obj.get_crawler(Name=glue_crawler_name)
    log_message(__file__, 'get_glue_crawler_status()', 'info', f"Crawled {glue_crawler_name}.")

    status = crawler_info["Crawler"]["LastCrawl"]["Status"]
    return glue_crawler_name, status


# ----------------------------------------------------------------------------------------------------------------------
# Create/run a glue crawler
# ----------------------------------------------------------------------------------------------------------------------


def create_run_glue_crawler(environment, database, prefix, layer, config_data, sns_topic_arn, region_name, lob, load=None):
    s3_bucket_prefix_dict = get_s3_bucket_prefix_dict(layer, config_data, load)

    if isinstance(s3_bucket_prefix_dict, str):
        email_notification(region_name, sns_topic_arn, f'{s3_bucket_prefix_dict}', environment)
        exit(1)

    glue_client = boto3.client('glue', region_name=region_name)

    glue_crawler_role = f'glue-crawler-role-{environment}'

    glue_crawler_name = f'{lob}{prefix}_tables_{layer}_structure'

    # create glue catalog database
    try:
        resp = create_glue_database(glue_client, database)
        log_message(__file__, 'create_run_glue_crawler()', 'info',
                    f"glue crawler database {database} doesn't exist. created")
    except:
        log_message(__file__, 'create_run_glue_crawler()', 'info',
                    f"glue crawler database is already exist. database={database}")

    # ------------------
    # add a list of S3 paths with exclusions
    # ------------------
    s3_bucket_and_prefix_list = [f"{k}|{v}|" for k, values in s3_bucket_prefix_dict.items() for v in values]

    try:
        create_crawler_response = glue_client.get_crawler(Name=glue_crawler_name)
        log_message(__file__, 'create_run_glue_crawler()', 'info', f"crawler {glue_crawler_name} exist, running ...")
    except:
        log_message(__file__, 'create_run_glue_crawler()', 'info',
                    f"Couldn't find {glue_crawler_name} crawler,creating ...")

        create_crawler_response = create_glue_crawler(glue_client, glue_crawler_name, glue_crawler_role, prefix,
                                                      database, s3_bucket_and_prefix_list)
        create_crawler_status = create_crawler_response["ResponseMetadata"]["HTTPStatusCode"]

        if create_crawler_status != 200:
            log_message(__file__, 'create_run_glue_crawler()', 'error',
                        f"Failed to create glue crawler job. status={create_crawler_status}")
            log_message(__file__, 'create_run_glue_crawler()', 'error', traceback.format_exc())
            email_notification(region_name, sns_topic_arn,
                               f'Failed to create glue crawler: {glue_crawler_name}, with status: {create_crawler_status} .\nlook into emr logs for more details.',
                               environment)
            exit(1)

    response = run_glue_crawler(glue_client, glue_crawler_name, region_name, sns_topic_arn)

    try:
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    except AssertionError:
        log_message(__file__, 'create_run_glue_crawler()', 'error',
                    f"Failed to run crawler {glue_crawler_name}. status={response}")
        email_notification(region_name, sns_topic_arn,
                           f"Failed to run crawler {glue_crawler_name}. status={response} .\n Look into crawler logs",
                           environment)
        exit(1)

    return glue_crawler_name, glue_client


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
            prefix = config_data['common_parameters']['glue_prefix']
            layers = config_data['common_parameters']['layers']
            region = config_data['common_parameters']['AWS_REGION']
            lob = config_data['common_parameters']['lob']
            if not sns_topic_arn:
                sns_topic_arn = config_data['common_parameters']['ARN']
            # in case of dms landing we need to create separate full load and cdc crawler as the table name will be same
            # - which will have a conflict, also there is difference in columns
            if 'dms' in config_data['common_parameters']["s3_buckets"][0]['s3_landing_path']:
                response_list = []
                for layer in layers:
                    if layer == 'landing':
                        for load in ['fullload', 'cdc']:
                            response_list.append(create_run_glue_crawler(environment, f'wp_datalake_{layer}', f'{prefix}_{load}',
                                                                         layer, config_data,sns_topic_arn, region, lob, load=load))
                    else:
                        response_list.append(create_run_glue_crawler(environment, f'wp_datalake_{layer}', prefix,
                                                                     layer, config_data, sns_topic_arn, region, lob))
            else:
                response_list = [create_run_glue_crawler(environment, f'wp_datalake_{layer}', prefix, layer, config_data,
                                                         sns_topic_arn, region, lob) for layer in layers]

            status_list = [get_glue_crawler_status(rs[0], rs[1], region, sns_topic_arn) for rs in response_list]
            fail_status = {item[0]: item[1] for item in status_list if item[1] != 'SUCCEEDED'}
            if fail_status:
                log_message(__file__, 'main()', 'error',
                            f"Last crawl for following crawlers were not success : \n "
                            f"{', '.join('{} : {}'.format(k, v) for k, v in fail_status.items())}")
                email_notification(region, sns_topic_arn,
                                   f"Last crawl for following crawlers were not success : \n "
                                   f"{', '.join('{} : {}'.format(k, v) for k, v in fail_status.items())}",
                                   environment)
            else:
                log_message(__file__, 'main()', 'info',
                            f"Crawling successfully completed for : \n {', '.join([item[0] for item in status_list])}")
                print('\n\n\n=========================================================')
                print('%s :: Glue Crawler Program took [%s] mins to execute overall' % (
                    datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    str(round((time.time() - program_start) / 60, 2))))
                print('=========================================================')
        except:
            log_message(__file__, 'main()', 'error', 'Following exception occurred while reading postgres json files.')
            log_message(__file__, 'main()', 'error', traceback.format_exc())
            email_notification(region, sns_topic_arn,
                               f'Error occurred in glue crawler: \n {traceback.format_exc()} .\nlook into emr logs for more details.',
                               environment)
            exit(1)
    except:
        log_message(__file__, 'main()', 'error', 'Following exception occurred while reading config json files.')
        log_message(__file__, 'main()', 'error', traceback.format_exc())
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', help="use dev,tst,mdl or prd. Required Field", required=True)
    parser.add_argument('--config_file', help="Provides the data lake configurations. Required field", required=True)

    args = parser.parse_args()
    if args.env is None:
        env = None
        log_message(__file__, '__main__', 'error', '--env is required. use dev,tst,mdl or prd')
        exit(1)
    elif args.env.lower() in ['dev', 'tst', 'mdl', 'prd']:
        env = args.env.lower()
    else:
        env = None
        log_message(__file__, '__main__', 'error', 'invalid argument value is passed for environment. '
                                                   'it should be one of (dev/tst/mdl/prd)')
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