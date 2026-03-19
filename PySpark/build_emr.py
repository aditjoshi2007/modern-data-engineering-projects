import boto3
import json
import typing

from datetime import date, datetime
from time import strftime
from botocore.exceptions import ClientError
from botocore.config import Config as boto3_config

ssmclient = None
emrclient = None
s3client = None

def get_initial_inputs(env, emr_name, local_dir, environment):
    client = boto3.client('kms', region_name='<aws_region>')
    response = client.describe_key(KeyId='{<name>}')
    kms_cmk_arn = response['KeyMetadata']['Arn']

    configuration_file = '<configuration_file>'
    initial_inputs = None
    with open(configuration_file) as json_file:
        initial_inputs = json.loads(json_file.read().replace("INSERT_KMS_ARN",kms_cmk_arn))
     
    initial_inputs["EMR_Name"] = f"{emr_name}-{env}"
    return initial_inputs

def get_instance_fleets(cluster_size, local_dir, environment):
    configuration_file = '<configuration_file>'
    instance_groups = None
    with open(configuration_file) as json_file:
        instance_groups = json.load(json_file)
    return instance_groups


def get_env_from_aws_acct():
    """ return env """

def create_boto3_clients():
    global region
    global ssmclient
    global emrclient
    global s3client
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    ssmclient = boto3.client("ssm", region, config=config)
    emrclient = boto3.client("emr", region, config=config)
    s3client = boto3.client("s3", region, config=config)

def execute_create_emr(instance_fleets: typing.Dict, initial_inputs: typing.Dict, step_conncurrency=1):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    client = boto3.client('emr','<aws_region>', config=config)
    today    = date.today()
    date_time = today.strftime("%m%d%Y")

    if 'Ec2KeyName' in initial_inputs:
        instances = {
            'InstanceFleets': instance_fleets,
            'Ec2KeyName': initial_inputs['Ec2KeyName'],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': initial_inputs['Ec2SubnetId'],
            'EmrManagedMasterSecurityGroup': initial_inputs['EmrManagedMasterSecurityGroup'],
            'EmrManagedSlaveSecurityGroup': initial_inputs['EmrManagedSlaveSecurityGroup'],
            'ServiceAccessSecurityGroup': initial_inputs['ServiceAccessSecurityGroup'],
            'AdditionalMasterSecurityGroups': initial_inputs['AdditionalMasterSecurityGroups'],
            'AdditionalSlaveSecurityGroups': initial_inputs['AdditionalSlaveSecurityGroups']
        }
    else:
        instances = {
            'InstanceFleets': instance_fleets,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': initial_inputs['Ec2SubnetId'],
            'EmrManagedMasterSecurityGroup': initial_inputs['EmrManagedMasterSecurityGroup'],
            'EmrManagedSlaveSecurityGroup': initial_inputs['EmrManagedSlaveSecurityGroup'],
            'ServiceAccessSecurityGroup': initial_inputs['ServiceAccessSecurityGroup'],
            'AdditionalMasterSecurityGroups': initial_inputs['AdditionalMasterSecurityGroups'],
            'AdditionalSlaveSecurityGroups': initial_inputs['AdditionalSlaveSecurityGroups']
        }

    response = client.run_job_flow(
        Name=initial_inputs['EMR_Name'],
        LogUri= initial_inputs['LogUri'] + initial_inputs['EMR_Name'] + '/',
        ReleaseLabel= initial_inputs['ReleaseLabel'],
        Instances=instances,
        BootstrapActions=initial_inputs['BootstrapActions'],
        ManagedScalingPolicy=initial_inputs['ManagedScalingPolicy'],
        Applications=initial_inputs['Applications'],
        VisibleToAllUsers=True,
        JobFlowRole=initial_inputs['JobFlowRole'],
        ServiceRole=initial_inputs['ServiceRole'],
        Configurations=initial_inputs['Configurations'],
        Tags=initial_inputs['tags'],
        AutoTerminationPolicy=initial_inputs['AutoTerminationPolicy'],
        SecurityConfiguration=initial_inputs['SecurityConfiguration'],
        StepConcurrencyLevel=int(step_conncurrency),
        EbsRootVolumeSize=100)

    cluster_id = response["JobFlowId"]
    return cluster_id


if __name__ == "__main__":
    execute_create_emr('', '')