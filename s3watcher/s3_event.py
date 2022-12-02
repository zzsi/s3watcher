"""
Utilities in describing S3 events.
"""
import enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import boto3


class FileEventType(enum.Enum):
    """
    The type of event that occurred to a file.
    """

    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


@dataclass
class S3Event:
    """
    A dataclass representing an S3 event.
    """

    bucket: str
    key: str
    size: int
    etag: str
    version_id: str
    file_event_type: FileEventType
    sequence: int = None
    event_datetime: datetime = datetime.now()
    event_name: Optional[str] = None

    def bytes(self) -> Optional[bytes]:
        """
        The binary content of the object.
        """
        if self.file_event_type == FileEventType.DELETED:
            return None
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=self.bucket, Key=self.key)
        return response["Body"].read()
