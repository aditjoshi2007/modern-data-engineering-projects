import logging
import boto3
import json
import sys
import time
from botocore.config import Config as boto3_config

def emrStepWaiter(cluster_id, step_id):
    config = boto3_config(
        retries = {
            'max_attempts': 30,
            'mode': 'standard'
        }
    )
    # Range is number of minutes to wait
    rangeValue = 1800 # 30 hrs
    for attempt in range(1, rangeValue):

        # Refresh the client
        client = boto3.client('emr', region_name='<aws_region>', config=config)

        step_status = client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )

        if step_status['Step']['Status']['State'] == 'COMPLETED':
            logging.info(step_id + ' - EMR step has finished')
            # Finished
            #break
            return step_status

        if step_status['Step']['Status']['State'] == 'PENDING':
            logging.info(step_id + ' - EMR step is pending')
            # Sleep for one minute
            time.sleep(60)

        if step_status['Step']['Status']['State'] == 'RUNNING':
            logging.info(step_id + ' - EMR step is running')
            # Sleep for one minute
            time.sleep(60)

        if step_status['Step']['Status']['State'] == 'CANCEL_PENDING':
            logging.info(step_id + ' - EMR step Failed')
            # Failed
            time.sleep(60)
            return step_status

            #break
            #raise Exception(step_id + ' - Task failed with CANCEL_PENDING')

        if step_status['Step']['Status']['State'] == 'CANCELLED':
            logging.info(step_id + ' - EMR step Failed')
            # Failed
            time.sleep(60)
            return step_status
            #raise Exception(step_id + ' - Task failed with CANCELLED')

        if step_status['Step']['Status']['State'] == 'FAILED':
            logging.info(step_id + ' - EMR step Failed')
            # Failed
            return step_status
            #raise Exception(step_id + ' - Task failed with FAILED')

        if step_status['Step']['Status']['State'] == 'INTERRUPTED':
            logging.info(step_id + ' - EMR step Failed')
            # Failed
            raise Exception(step_id + ' - Task failed with INTERRUPTED')

        if attempt == 1800:
            logging.info(step_id + ' - Task timed out')
            # Failed
            raise Exception(step_id + ' - Task timed out')

if __name__ == "__main__":
    """Command line function to invoke the primary entry point for the
    upload module.  The Click library handles command line arguments transparently.

    Dynamic configuration is contained in the settings.json file.
    """
    emrStepWaiter("test", "test")
