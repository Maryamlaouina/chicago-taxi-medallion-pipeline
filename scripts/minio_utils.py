import os
import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    """Client S3 pointant vers MinIO"""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )


def ensure_bucket(bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore."""
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def delete_prefix(bucket: str, prefix: str) -> None:
    """Supprime tous les fichiers sous un dossier (pour pouvoir relancer proprement)"""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
