import argparse

from .s3_watcher import S3Watcher
from .utils import list_buckets


def _main():
    parser = argparse.ArgumentParser(prog="s3watcher")
    parser.add_argument(
        "command",
        choices=["list-buckets", "watch", "setup"],
        help="watch: watch for events, setup: setup notification and queue",
    )
    parser.add_argument(
        "--bucket",
        required=False,
        help="The name of the bucket to watch. Required for watch and setup.",
    )
    args = parser.parse_args()

    if args.command == "list-buckets":
        list_buckets(print_to_console=True)
    elif args.command == "watch":
        watcher = S3Watcher(bucket=args.bucket)
        for event in watcher.watch():
            print(event)


if __name__ == "__main__":
    _main()
