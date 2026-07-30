#!/usr/bin/env python3
import argparse
import hashlib
import os

import boto3
from botocore.config import Config


def client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}:9000",
        aws_access_key_id=os.environ["LAB_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["LAB_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}),
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "command",
    choices=["ensure-bucket", "put", "sha256", "delete-prefix", "abort-multipart-prefix"],
)
parser.add_argument("--endpoint", required=True)
parser.add_argument("--bucket", required=True)
parser.add_argument("--key")
parser.add_argument("--prefix")
parser.add_argument("--value")
args = parser.parse_args()
s3 = client(args.endpoint)

if args.command == "ensure-bucket":
    try:
        s3.head_bucket(Bucket=args.bucket)
    except Exception:
        s3.create_bucket(Bucket=args.bucket)
elif args.command == "put":
    s3.put_object(Bucket=args.bucket, Key=args.key, Body=args.value.encode())
elif args.command == "sha256":
    body = s3.get_object(Bucket=args.bucket, Key=args.key)["Body"].read()
    print(hashlib.sha256(body).hexdigest())
elif args.command == "delete-prefix":
    token = None
    while True:
        kwargs = {"Bucket": args.bucket, "Prefix": args.prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=args.bucket, Delete={"Objects": objects})
        if not page.get("IsTruncated"):
            break
        token = page["NextContinuationToken"]
else:
    key_marker = None
    upload_id_marker = None
    while True:
        kwargs = {"Bucket": args.bucket, "Prefix": args.prefix}
        if key_marker:
            kwargs["KeyMarker"] = key_marker
        if upload_id_marker:
            kwargs["UploadIdMarker"] = upload_id_marker
        page = s3.list_multipart_uploads(**kwargs)
        for upload in page.get("Uploads", []):
            s3.abort_multipart_upload(
                Bucket=args.bucket,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )
        if not page.get("IsTruncated"):
            break
        key_marker = page.get("NextKeyMarker")
        upload_id_marker = page.get("NextUploadIdMarker")
