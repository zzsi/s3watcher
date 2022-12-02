"""
Utilities in describing S3 events.
"""
from dataclasses import dataclass
from typing import Optional


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
    is_delete_marker: bool

    def bytes(self) -> Optional[bytes]:
        """
        The binary content of the object.
        """
