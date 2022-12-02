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
from .s3_event import S3Event, FileEventType


LOGGER = logging.getLogger(__name__)


@dataclass
class S3Watcher:
    """
    Watch updates from an s3 bucket or prefix.

    Implementation inspired by: https://github.com/dbnicholson/s3watcher/blob/master/s3watcher/server.py
    """

    bucket: str
    prefix: str = None
    sqs_name: str = None
    wait_seconds: int = 3
    max_num_messages_per_fetch: int = 10
    purge_queue_before_watching: bool = False
    create_sqs_queue: bool = False
    delete_sqs_queue_after_done: bool = False

    def watch(self) -> Iterable[S3Event]:
        """
        Start watching the bucket for updates.
        """
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
                    yield event
                LOGGER.info(f"Processed and deleting message {msg_id}")
                # TODO: should the message be deleted here?
                msg.delete()

    def __post_init__(self, bucket, queue_url=None):

        session = boto3.session.Session()
        self.s3 = session.resource("s3")
        self.bucket = self.s3.Bucket(bucket)

        if queue_url:
            self.sqs = session.resource("sqs")
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
            self.sqs = None
            self.queue = None

    def __del__(self):
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
        if bucket != self.bucket:
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

        return S3Event(
            bucket=self.bucket,
            key=key,
            size=record["s3"]["object"]["size"],
            etag=record["s3"]["object"]["eTag"],
            version_id=record["s3"]["object"]["versionId"],
            sequence=sequence,
            file_event_type=file_event_type,
        )
