"""S3 consistency utilities.

Provides a retryable waiter for object existence using boto3's native waiter.
The function is framework-agnostic and will raise AirflowException if Airflow
is available; otherwise it defines a local AirflowException for compatibility
so unit tests can run without Airflow installed.
"""

from typing import List
import logging

try:
    # Prefer Airflow's exception type when available
    from airflow.exceptions import AirflowException  # type: ignore
except Exception:  # pragma: no cover - executed only when Airflow isn't installed
    class AirflowException(Exception):
        """Fallback exception used when Airflow is not available."""
        pass

import botocore.exceptions

def wait_for_s3_objects(
    s3_client,
    bucket: str,
    keys: List[str],
    timeout_seconds: int = 300,
    delay_seconds: int = 5,
) -> None:
    """
    Wait until all given S3 objects exist (HEAD 200) or timeout is reached.

    Uses the native boto3 S3 waiter 'object_exists' per key. This provides robust,
    retryable consistency handling vs. arbitrary sleeps, especially useful after
    cross-account copies, CRR, or when listings may lag.

    Args:
        s3_client: boto3 S3 client
        bucket (str): Destination S3 bucket
        keys (List[str]): Object keys to verify
        timeout_seconds (int): Total time to wait for each object
        delay_seconds (int): Delay between waiter polls

    Raises:
        AirflowException: If any object fails to appear within the timeout window
    """
    if not keys:
        logging.info("wait_for_s3_objects: no keys provided; nothing to wait for.")
        return

    # Compute waiter attempts based on desired timeout/backoff
    max_attempts = max(1, (timeout_seconds + delay_seconds - 1) // delay_seconds)
    waiter = s3_client.get_waiter('object_exists')

    logging.info(
        f"⏳ Waiting for {len(keys)} object(s) in s3://{bucket} "
        f"(Delay={delay_seconds}s, MaxAttempts={max_attempts})"
    )

    failures = []
    for key in keys:
        try:
            waiter.wait(
                Bucket=bucket,
                Key=key,
                WaiterConfig={"Delay": delay_seconds, "MaxAttempts": max_attempts},
            )
            logging.info(f"✅ Object is now available: s3://{bucket}/{key}")
        except botocore.exceptions.WaiterError as e:
            logging.error(f"❌ Timed out waiting for: s3://{bucket}/{key} — {e}")
            failures.append(key)

    if failures:
        raise AirflowException(
            f"One or more objects did not appear within the timeout: {failures}"
        )