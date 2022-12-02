"""
Utilities for watching S3 buckets for new file updates.
"""
from typing import Iterable
from .s3_event import S3Event


class S3Watcher:
    """
    Watch updates from an s3 bucket or prefix.
    """

    def watch(self) -> Iterable[S3Event]:
        pass
