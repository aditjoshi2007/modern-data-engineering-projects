import logging
import boto3
import json
import time as t
from botocore.config import Config as boto3_config

def emrCreationWaiter(clusterId):
    logging.info("Waiting for EMR Cluster to be up Cluster ID => " + clusterId)
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    # Range is number of minutes to wait
    for attempt in range(120):
        client = boto3.client('emr', region_name='<aws_region>', config=config)
        clusterDataList = client.describe_cluster(
            ClusterId=clusterId
        )
 
        cluster_dataArray = json.dumps(clusterDataList, indent=4, sort_keys=True,
                                       default=str)  # https://stackoverflow.com/a/36142844/3299397
        cluster_data = json.loads(cluster_dataArray)
 
        # 'State': 'STARTING'|'BOOTSTRAPPING'|'RUNNING'|'WAITING'|'TERMINATING'|'TERMINATED'|'TERMINATED_WITH_ERRORS'
        response = cluster_data["Cluster"]["Status"]["State"]
 
        if cluster_data["Cluster"]["Status"]["State"] == "STARTING":
            # EMR is being created
            t.sleep(60)
 
        if cluster_data["Cluster"]["Status"]["State"] == "BOOTSTRAPPING":
            # EMR is being created
            t.sleep(60)
 
        if cluster_data["Cluster"]["Status"]["State"] == "RUNNING":
            # EMR is ready
            break
 
        if cluster_data["Cluster"]["Status"]["State"] == "WAITING":
            # EMR is ready
            break
 
        if cluster_data["Cluster"]["Status"]["State"] == "TERMINATING":
            # EMR creation failed
            raise Exception('EMR failed with TERMINATING')
 
        if cluster_data["Cluster"]["Status"]["State"] == "TERMINATED":
            # EMR creation failed
            raise Exception('EMR failed with TERMINATED')
 
        if cluster_data["Cluster"]["Status"]["State"] == "TERMINATED_WITH_ERRORS":
            # EMR creation failed
            raise Exception('EMR failed with TERMINATED_WITH_ERRORS')
 
        if attempt == 119:
            # Failed
            raise Exception('Task timed out.')

# Check to see if EMR is in waiting state, then and then Terminate the EMR
def emrDeleteWaiter(clusterId):
    logging.info("Waiting for EMR Cluster to finish processing Cluster ID => " + clusterId)
    # Range is number of minutes to wait
    for attempt in range(360):
        client = boto3.client('emr', region_name='<aws_region>')
        clusterDataList = client.describe_cluster(
            ClusterId=clusterId
        )
 
        cluster_dataArray = json.dumps(clusterDataList, indent=4, sort_keys=True,
                                       default=str)  # https://stackoverflow.com/a/36142844/3299397
        cluster_data = json.loads(cluster_dataArray)
 
        # 'State': 'STARTING'|'BOOTSTRAPPING'|'RUNNING'|'WAITING'|'TERMINATING'|'TERMINATED'|'TERMINATED_WITH_ERRORS'
        # response = cluster_data["Cluster"]["Status"]["State"]
 
        if cluster_data["Cluster"]["Status"]["State"] == "STARTING":
            # EMR is being created
            t.sleep(60)
 
        if cluster_data["Cluster"]["Status"]["State"] == "BOOTSTRAPPING":
            # EMR is being created
            t.sleep(60)
 
        if cluster_data["Cluster"]["Status"]["State"] == "RUNNING":
            t.sleep(60)
            # EMR is running, check back in 60 seconds
            break
 
        if cluster_data["Cluster"]["Status"]["State"] == "WAITING":
            # EMR is done processing everything, lets terminate it
            break
 
        if cluster_data["Cluster"]["Status"]["State"] == "TERMINATING":
            # EMR creation failed
            raise Exception('EMR failed with TERMINATING')
 
        if cluster_data["Cluster"]["Status"]["State"] == "TERMINATED":
            # EMR creation failed
            # It shouldnt come here
            raise Exception('EMR failed with TERMINATED')
 
        if cluster_data["Cluster"]["Status"]["State"] == "TERMINATED_WITH_ERRORS":
            # EMR creation failed
            # It shouldnt come here
            raise Exception('EMR failed with TERMINATED_WITH_ERRORS')
 
        if attempt == 359:
            # Failed
            raise Exception('Task timed out.')


if __name__ == "__main__":
    """Command line function to invoke the primary entry point for the
    upload module.  The Click library handles command line arguments transparently.

    Dynamic configuration is contained in the settings.json file.
    """
    emrCreationWaiter("")