`s3watcher` is a library for watching updates from an s3 bucket or prefix.

Example usage:

```python
from s3watcher import S3Watcher, S3Event


watcher = S3Watcher(bucket="my-bucket", prefix="folder1/subfolder2")
for event in watcher.watch():
    print(event)
```

## Authentication

`S3Watcher` will attempt to create a SQS queue to hold [S3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html). Necessary AWS credentials are needed to create these resources.
