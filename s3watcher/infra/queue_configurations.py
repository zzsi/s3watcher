from dataclasses import dataclass, field
from typing import List


@dataclass
class QueueConfiguration:
    Id: str
    QueueArn: str
    Events:  List[str] = field(default_factory=lambda _: [
        "s3:ObjectCreated:*",
        "s3:ObjectRemoved:*",
        "s3:ObjectRestore:*"
    ])


@dataclass
class BucketNotifications:
    """
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
    """
    configs: List[QueueConfiguration]

    def to_dict(self):
        return {
            "QueueConfigurations": [
                c.__dict__ for c in self.configs
            ]
        }

    def add(self, queue_configuration: QueueConfiguration):
        self.config.append(queue_configuration)
    
    def delete(self, id: str):
        self.configs = [c for c in self.configs if c.Id != id]
    
    def clear(self):
        self.configs = []
    