"""Create the object-storage bucket. Safe to re-run.

    python scripts/init_storage.py

Reads S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY and S3_BUCKET from the environment
(defaults match local/infra/.env.example).
"""
import os
import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "vidpipeaccess"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "vidpipesecret123"),
        region_name="us-east-1",
    )
    bucket = os.environ.get("S3_BUCKET", "vidpipe")

    for attempt in range(20):
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"bucket exists: {bucket}")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
                s3.create_bucket(Bucket=bucket)
                print(f"bucket created: {bucket}")
                return
            raise
        except BotoCoreError:
            print("waiting for object store...")
            time.sleep(3)
    sys.exit("object store not reachable")


if __name__ == "__main__":
    main()
