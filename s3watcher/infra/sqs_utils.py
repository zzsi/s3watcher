"""
This module contains utilities for working with SQS.
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
from .queue_configurations import BucketNotifications, QueueConfiguration


logger = logging.getLogger(__name__)


def get_account_number():
    """
    Get the account number for the current session.
    """
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key

    sts = boto3.client(
        "sts",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    account_id = sts.get_caller_identity()["Account"]
    return account_id


def get_current_bucket_notifications(bucket_name: str) -> BucketNotifications:
    """
    Get the current bucket notifications.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    try:
        bucket_notification_configuration = bucket.Notification().to_dict()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NotificationConfigurationNotFoundError":
            logger.info("No bucket notification configuration found.")
            bucket_notification_configuration = {}
        else:
            raise e
    bucket_notifications = BucketNotifications(
        configs=[
            QueueConfiguration(**c)
            for c in bucket_notification_configuration.get(
                "QueueConfigurations", []
            )
        ]
    )
    return bucket_notifications


def configure_s3_sqs_for_notification(bucket_name: str, queue_name: str, region: str = "us-east-1"):
    """
    Configure S3 to send notifications to SQS.
    """
    settings = {
        "bucket_name": bucket_name,
        "queue_name": queue_name,
        "region": region,
        "account_number": get_account_number(),
    }
    s3 = boto3.resource("s3", region_name=region)
    b = s3.Bucket(bucket_name)
    client = boto3.client("s3")
    bucket_notification_id = f"{bucket_name}"
    queue_arn = "arn:aws:sqs:{region}:{account_number}:{queue_name}".format(
        **settings
    )
    bucket_notifications = get_current_bucket_notifications(bucket_name)
    bucket_notifications.add(
        QueueConfiguration(
            Id=bucket_notification_id,
            QueueArn=queue_arn,
            Events=[
                "s3:ObjectCreated:*",
                "s3:ObjectRemoved:*",
                "s3:ObjectRestore:*",
            ],
        )
    )
    bucket_notifications_configuration = bucket_notifications.to_dict()
    # bucket_notifications_configuration = {
    #     "QueueConfigurations": [
    #         {
    #             "Events": [
    #                 "s3:ObjectCreated:*",
    #                 "s3:ObjectRemoved:*",
    #                 "s3:ObjectRestore:*",
    #             ],
    #             "Id": "Notifications",
    #             "QueueArn": "arn:aws:sqs:{region}:{account_number}:{queue_name}".format(
    #                 **settings
    #             ),
    #         }
    #     ]
    # }
    qpolicy = {
        "Version": "2012-10-17",
        "Id": "arn:aws:sqs:{region}:{account_number}:{queue_name}/SQSDefaultPolicy".format(
            **settings
        ),
        "Statement": [
            {
                "Sid": "allow bucket to notify",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SQS:*",
                "Resource": "arn:aws:sqs:{region}:{account_number}:{queue_name}".format(
                    **settings
                ),
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:*:*:{bucket_name}"
                    }
                },
            }
        ],
    }
    print("Bucket notify", bucket_notifications_configuration)
    print("Queue Policy", qpolicy)
    queue_attrs = {
        "Policy": json.dumps(qpolicy),
    }
    q = boto3.resource("sqs", region_name=region).get_queue_by_name(
        QueueName=settings["queue_name"]
    )
    q.set_attributes(Attributes=queue_attrs)
    client.put_bucket_notification_configuration(
        Bucket=settings["bucket_name"],
        NotificationConfiguration=bucket_notifications_configuration,
    )
    print("Configuration done")


def create_queue(name, attributes=None):
    """
    Creates an Amazon SQS queue.

    :param name: The name of the queue. This is part of the URL assigned to the queue.
    :param attributes: The attributes of the queue, such as maximum message size or
                       whether it's a FIFO queue.
    :return: A Queue object that contains metadata about the queue and that can be used
             to perform queue operations like sending and receiving messages.
    """
    if not attributes:
        attributes = {}

    try:
        sqs = boto3.resource("sqs")
        queue = sqs.create_queue(
            QueueName=name,
            Attributes=attributes
        )
        logger.info("Created queue '%s' with URL=%s", name, queue.url)
        print(f"Created queue {name} with URL={queue.url}")
    except ClientError as error:
        logger.exception("Couldn't create queue named '%s'.", name)
        raise error
    else:
        return queue


def get_queue(name):
    """
    Gets an SQS queue by name.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    sqs = boto3.resource("sqs")
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def get_queues(prefix=None):
    """
    Gets a list of SQS queues. When a prefix is specified, only queues with names
    that start with the prefix are returned.

    :param prefix: The prefix used to restrict the list of returned queues.
    :return: A list of Queue objects.
    """
    sqs = boto3.resource("sqs")
    if prefix:
        queue_iter = sqs.queues.filter(QueueNamePrefix=prefix)
    else:
        queue_iter = sqs.queues.all()
    queues = list(queue_iter)
    if queues:
        logger.info("Got queues: %s", ', '.join([q.url for q in queues]))
    else:
        logger.warning("No queues found.")
    return queues


def remove_queue(queue):
    """
    Removes an SQS queue. When run against an AWS account, it can take up to
    60 seconds before the queue is actually deleted.

    :param queue: The queue to delete.
    :return: None
    """
    try:
        queue.delete()
        logger.info("Deleted queue with URL=%s.", queue.url)
    except ClientError as error:
        logger.exception("Couldn't delete queue with URL=%s!", queue.url)
        raise error
