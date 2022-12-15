"""
Utilities for watching S3 buckets for new file updates.
"""
from dataclasses import dataclass
import json
import logging
import time
from typing import Iterable
from urllib.parse import unquote_plus
import boto3
from botocore.exceptions import ClientError
from .s3_event import S3Event, FileEventType
from .infra.sqs_utils import create_queue, configure_s3_sqs_for_notification, get_account_number


LOGGER = logging.getLogger(__name__)


@dataclass
class S3Watcher:
    """
    Watch updates from an s3 bucket or prefix.

    Implementation inspired by: https://github.com/dbnicholson/s3watcher/blob/master/s3watcher/server.py
    """

    bucket_name: str
    prefix: str = None

    create_sqs_queue: bool = False
    queue_url: str = None
    purge_queue_before_watching: bool = False
    delete_sqs_queue_after_done: bool = False
    wait_seconds: int = 3
    max_num_messages_per_fetch: int = 10

    def __post_init__(self, queue_url=None):

        session = boto3.session.Session()
        self.s3 = session.resource("s3")
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.queue_name = f"{self.bucket_name}-s3watcher"
        self.sqs = session.resource("sqs")
        if queue_url:
            self.queue = self.sqs.Queue(queue_url)
            if self.purge_queue_before_watching:
                # Purge the queue since we're about to re-enumerate the
                # whole bucket and don't need to bother reading old records
                try:
                    self.queue.purge()
                except self.sqs.meta.client.exceptions.PurgeQueueInProgress:
                    LOGGER.warning(
                        f"Queue purge already in progress. Queue url: {queue_url}"
                    )
        else:
            self.queue = None

    def setup_notification_and_queue(self, bucket_name: str, sqs_name: str):
        """
        TODO: a s3 event notification config looks something like:

        {
            "QueueConfigurations": [
                {
                    "Id": "Notifications",
                    "QueueArn": "arn:aws:sqs:us-east-1:xxx:<queue name>",
                    "Events": [
                        "s3:ObjectCreated:*",
                        "s3:ObjectRemoved:*",
                        "s3:ObjectRestore:*"
                    ]
                }
            ]
        }

        If you try to add a new event notification config to a bucket that already has one,
        make sure the event types are different, or the prefixes do not overlap, otherwise
        will be an error like the following:

        botocore.exceptions.ClientError: An error occurred (InvalidArgument) when calling the
        PutBucketNotificationConfiguration operation: Configurations overlap. Configurations
        on the same bucket cannot share a common event type.

        https://stackoverflow.com/questions/31471178/aws-s3-trigger-multiple-targets-via-s3-notification-upon-file-receipt

        Adding event notification configs does not incur cost.
        """
        print("Creating AWS SQS Queue : " + sqs_name)
        try:
            queue = create_queue(sqs_name)
            configure_s3_sqs_for_notification(bucket_name, sqs_name)
        except ClientError:
            print(f"Couldn't create queue named {sqs_name}.")
            raise

        s3_client = boto3.client("s3")
        bucket_name = self.bucket_name
        bucket_notification = s3_client.get_bucket_notification(
            Bucket=bucket_name, ExpectedBucketOwner=get_account_number()
        )
        if "QueueConfiguration" in bucket_notification:
            queueConfig = bucket_notification["QueueConfiguration"]
            # print(queueConfig)
            if "Queue" in queueConfig:
                queue = queueConfig["Queue"]
                queue_name = queue.split(":")[-1]
                sqs_client = boto3.client("sqs")
                queue_url_response = sqs_client.get_queue_url(
                    QueueName=queue_name, QueueOwnerAWSAccountId=get_account_number()
                )
                queue_url = queue_url_response["QueueUrl"]
                assert isinstance(queue_url, str)
                self.queue_url = queue_url
                self.queue = self.sqs.Queue(queue_url)
            else:
                print(
                    f"No queue url in QueueConfiguration. Bucket: {bucket_name}")
        else:
            raise ValueError(
                f"QueueConfiguration not in the bucket notification api response. Bucket: {bucket_name}")

    def watch(self) -> Iterable[S3Event]:
        """
        Start watching the bucket for updates.
        """
        if self.create_sqs_queue:
            self.setup_notification_and_queue(
                self.bucket_name, self.queue_name)
            LOGGER.warning(
                f"Created SQS queue {self.queue_name} for bucket {self.bucket_name}")
        while True:
            messages = self.queue.receive_messages(
                MaxNumberOfMessages=self.max_num_messages_per_fetch,
                WaitTimeSeconds=self.wait_seconds,
            )
            num_messages = len(messages)
            LOGGER.debug(
                "Received %i message%s", num_messages, "" if num_messages == 1 else "s"
            )
            if num_messages == 0:
                time.sleep(self.wait_seconds)

            for msg in messages:
                msg_id = msg.message_id
                body = json.loads(msg.body)
                for record in body.get("Records", []):
                    event = self._create_event(record)
                    print(record)
                    yield event
                LOGGER.info(f"Processed and deleting message {msg_id}")
                # TODO: should the message be deleted here?
                msg.delete()

    def __del__(self):
        # TODO: is __del__ the right place to do this?
        if self.delete_sqs_queue_after_done:
            self._delete_sqs_queue()

    def _delete_sqs_queue(self):
        raise NotImplementedError

    # Event information created from SQS S3 records
    def _create_event(self, record):
        """Convert S3 SQS record to a S3Event.
         See
         https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
         for record details.
        """
        event_source = record.get("eventSource")
        if event_source != "aws:s3":
            LOGGER.debug("Ignoring record from source %s", event_source)
            return None

        # We require event major version 2. Fail otherwise.
        event_major_version = record["eventVersion"].split(".")[0]
        if event_major_version != "2":
            LOGGER.error(
                "Ignoring unsupported event version %s", record["eventVersion"]
            )
            return None

        bucket = record["s3"]["bucket"]["name"]
        if bucket != self.bucket.name:
            LOGGER.debug("Ignoring record for bucket %s", bucket)
            return None

        event_name = record["eventName"]
        if event_name.startswith("ObjectCreated:"):
            file_event_type = FileEventType.CREATED
        elif event_name.startswith("ObjectRemoved:"):
            file_event_type = FileEventType.DELETED
        else:
            LOGGER.debug("Ignoring non-object event %s", event_name)
            file_event_type = FileEventType.UPDATED

        # The object key is URL encoded as for an HTML form
        key = unquote_plus(record["s3"]["object"]["key"])

        # The sequencer value is a hex string
        sequence = int(record["s3"]["object"]["sequencer"], base=16)
        if file_event_type == FileEventType.CREATED:
            return S3Event(
                bucket=self.bucket_name,
                key=key,
                size=record["s3"]["object"]["size"],
                etag=record["s3"]["object"]["eTag"],
                sequence=sequence,
                file_event_type=file_event_type,
                event_time=record["eventTime"],
            )
        if file_event_type == FileEventType.DELETED:
            return S3Event(
                bucket=self.bucket_name,
                size=record["s3"]["object"].get("size"),
                etag=record["s3"]["object"].get("eTag"),
                key=key,
                sequence=sequence,
                file_event_type=file_event_type,
                event_time=record["eventTime"],
            )
