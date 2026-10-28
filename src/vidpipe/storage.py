"""Object storage through the S3 API only (RustFS locally, AWS S3 in the cloud)."""
from functools import lru_cache
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from vidpipe.config import cfg


@lru_cache(maxsize=1)
def client():
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def to_uri(key):
    return f"s3://{cfg.s3_bucket}/{key}"


def parse_uri(uri):
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def upload_file(path, key):
    client().upload_file(str(path), cfg.s3_bucket, key)
    return to_uri(key)


def put_bytes(data, key, content_type="application/octet-stream"):
    client().put_object(Bucket=cfg.s3_bucket, Key=key, Body=data, ContentType=content_type)
    return to_uri(key)


def get_bytes(uri):
    bucket, key = parse_uri(uri)
    return client().get_object(Bucket=bucket, Key=key)["Body"].read()


def download_file(uri, dest):
    bucket, key = parse_uri(uri)
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    client().download_file(bucket, key, str(dest))
    return dest


def exists(uri):
    bucket, key = parse_uri(uri)
    try:
        client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
