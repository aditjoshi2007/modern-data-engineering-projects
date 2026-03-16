import boto3
import pytest
from moto import mock_s3

from app_reports.s3_waiter import wait_for_s3_objects, AirflowException


@mock_s3
def test_wait_for_s3_objects_succeeds_when_objects_exist():
    # Arrange
    region = 'us-east-1'
    bucket = 'test-bucket'
    s3 = boto3.client('s3', region_name=region)
    s3.create_bucket(Bucket=bucket)

    keys = ['path/to/file1.csv', 'another/file2.csv']
    for k in keys:
        s3.put_object(Bucket=bucket, Key=k, Body=b'data')

    # Act / Assert (should not raise)
    wait_for_s3_objects(
        s3_client=s3,
        bucket=bucket,
        keys=keys,
        timeout_seconds=10,
        delay_seconds=1,
    )


@mock_s3
def test_wait_for_s3_objects_times_out_for_missing_objects():
    # Arrange
    region = 'us-east-1'
    bucket = 'test-bucket-timeout'
    s3 = boto3.client('s3', region_name=region)
    s3.create_bucket(Bucket=bucket)

    missing_keys = ['missing/file1.csv', 'missing/file2.csv']

    # Act / Assert
    with pytest.raises(AirflowException):
        wait_for_s3_objects(
            s3_client=s3,
            bucket=bucket,
            keys=missing_keys,
            timeout_seconds=3,
            delay_seconds=1,
        )
