"""
S3 Watcher Utils
"""
import boto3


def list_buckets(print_to_console=False):
    """
    List all buckets.
    """
    s3 = boto3.resource("s3")
    buckets = s3.buckets.all()
    if print_to_console:
        for bucket in buckets:
            print(bucket.name)
    return buckets
