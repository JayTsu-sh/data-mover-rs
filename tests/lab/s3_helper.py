#!/usr/bin/env python3
import argparse
import hashlib
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def client(endpoint: str):
    endpoint_url = endpoint if "://" in endpoint else f"http://{endpoint}:9000"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["LAB_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["LAB_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}),
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "command",
    choices=[
        "ensure-bucket",
        "put",
        "put-file",
        "sha256",
        "size",
        "exists",
        "metadata",
        "delete-prefix",
        "abort-multipart-prefix",
    ],
)
parser.add_argument("--endpoint", required=True)
parser.add_argument("--bucket", required=True)
parser.add_argument("--key")
parser.add_argument("--prefix")
parser.add_argument("--value")
parser.add_argument("--file")
parser.add_argument("--metadata-key")
args = parser.parse_args()
s3 = client(args.endpoint)

if args.command == "ensure-bucket":
    try:
        s3.head_bucket(Bucket=args.bucket)
    except Exception:
        s3.create_bucket(Bucket=args.bucket)
elif args.command == "put":
    s3.put_object(Bucket=args.bucket, Key=args.key, Body=args.value.encode())
elif args.command == "put-file":
    s3.upload_file(args.file, args.bucket, args.key)
elif args.command == "sha256":
    body = s3.get_object(Bucket=args.bucket, Key=args.key)["Body"].read()
    print(hashlib.sha256(body).hexdigest())
elif args.command == "size":
    print(s3.head_object(Bucket=args.bucket, Key=args.key)["ContentLength"])
elif args.command == "exists":
    try:
        s3.head_object(Bucket=args.bucket, Key=args.key)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            raise SystemExit(1)
        raise
elif args.command == "metadata":
    value = s3.head_object(Bucket=args.bucket, Key=args.key)["Metadata"].get(
        args.metadata_key
    )
    if value is None:
        raise SystemExit(1)
    print(value)
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
