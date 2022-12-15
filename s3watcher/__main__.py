import argparse

from .s3_watcher import S3Watcher
from .utils import list_buckets


def _main():
    parser = argparse.ArgumentParser(prog="s3watcher")
    parser.add_argument(
        "-c",
        "--command",
        type=str,
        required=False,
        default="watch",
        choices=["list-buckets", "watch", "setup"],
        help="watch: watch for events, setup: setup notification and queue",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        required=False,
        help="The name of the bucket to watch. Required for watch and setup.",
    )
    args = parser.parse_args()

    if args.command == "list-buckets":
        list_buckets(print_to_console=True)
    elif args.command == "watch":
        assert args.bucket, "Bucket name is required when you use 'watch' command."
        watcher = S3Watcher(bucket_name=args.bucket,
                            create_sqs_queue=True)
        for event in watcher.watch():
            print(event)


if __name__ == "__main__":
    _main()
