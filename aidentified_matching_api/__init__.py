# -*- coding: utf-8 -*-
import argparse
import datetime
import logging
import os
import sys
import threading

import aidentified_matching_api.daily_files as daily_files
import aidentified_matching_api.dataset as dataset
import aidentified_matching_api.dataset_file as dataset_file
import aidentified_matching_api.token_service as token_service


parser = argparse.ArgumentParser(
    description="Aidentified matching API command line wrapper"
)
parser.add_argument(
    "--email",
    help="Email address of Aidentified account",
    default=os.environ.get("AID_EMAIL"),
)
parser.add_argument(
    "--password",
    help="Password of Aidentified account",
    default=os.environ.get("AID_PASSWORD"),
)
parser.add_argument("--verbose", help="Write log output to stderr", action="store_true")


subparser = parser.add_subparsers()

#
# token
#

token_parser = subparser.add_parser("auth", help="Print JWT token")
token_parser.set_defaults(func=token_service.get_token)

#
# dataset
#

dataset_parser = subparser.add_parser("dataset", help="Manage datasets")
dataset_subparser = dataset_parser.add_subparsers()


_dataset_parent = argparse.ArgumentParser(add_help=False)
_dataset_parent_group = _dataset_parent.add_argument_group(title="required arguments")
_dataset_parent_group.add_argument("--name", help="Dataset name", required=True)

dataset_list = dataset_subparser.add_parser("list", help="List datasets")
dataset_list.set_defaults(func=dataset.list_datasets)

dataset_create = dataset_subparser.add_parser(
    "create", help="Create new dataset", parents=[_dataset_parent]
)
dataset_create.set_defaults(func=dataset.create_dataset)

dataset_delete = dataset_subparser.add_parser(
    "delete", help="Delete dataset", parents=[_dataset_parent]
)
dataset_delete.set_defaults(func=dataset.delete_dataset)

#
# dataset-file
#

dataset_files_parser = subparser.add_parser("dataset-file", help="Manage dataset files")
dataset_files_subparser = dataset_files_parser.add_subparsers()


def _get_dataset_file_parent(
    dataset_file_name=False,
    dataset_file_upload=False,
    dataset_file_download=False,
    file_date=False,
):
    _dataset_file_parent = argparse.ArgumentParser(add_help=False)
    _dataset_parent_group = _dataset_file_parent.add_argument_group(
        title="required arguments"
    )

    _dataset_parent_group.add_argument(
        "--dataset-name", help="Name of parent dataset", required=True
    )

    if dataset_file_name:
        _dataset_parent_group.add_argument(
            "--dataset-file-name", help="Name of new dataset file", required=True
        )

    if dataset_file_upload:
        _dataset_parent_group.add_argument(
            "--dataset-file-path",
            help="Path to dataset file",
            required=True,
            type=argparse.FileType(mode="rb"),
        )

    if dataset_file_download:
        _dataset_parent_group.add_argument(
            "--dataset-file-path",
            help="Destination of downloaded file (will truncate if exists)",
            required=True,
            type=argparse.FileType(mode="wb"),
        )

    if file_date:
        _dataset_parent_group.add_argument(
            "--file-date",
            help="Date of the delta file in YYYY-MM-DD format",
            required=True,
            type=lambda s: datetime.datetime.strptime("%Y-%m-%d"),
        )

    return _dataset_file_parent


dataset_file_list = dataset_files_subparser.add_parser(
    "list", help="List dataset files", parents=[_get_dataset_file_parent()]
)
dataset_file_list.set_defaults(func=dataset_file.list_dataset_files)

dataset_file_create = dataset_files_subparser.add_parser(
    "create",
    help="Create new dataset file",
    parents=[_get_dataset_file_parent(dataset_file_name=True)],
)
dataset_file_create.set_defaults(func=dataset_file.create_dataset_file)

dataset_file_create = dataset_files_subparser.add_parser(
    "upload",
    help="Upload dataset file",
    parents=[
        _get_dataset_file_parent(dataset_file_name=True, dataset_file_upload=True)
    ],
)
dataset_file_create.add_argument(
    "--upload-part-size",
    help="Size of upload chunk in megabytes",
    type=int,
    default=100,
)
dataset_file_create.add_argument(
    "--concurrent-uploads", help="Max number of concurrent uploads", type=int, default=4
)
dataset_file_create.set_defaults(func=dataset_file.upload_dataset_file)
dataset_file_create.set_defaults(upload_dataset_file_lock=threading.Lock())

dataset_file_abort = dataset_files_subparser.add_parser(
    "abort",
    help="Abort dataset file upload",
    parents=[_get_dataset_file_parent(dataset_file_name=True)],
)
dataset_file_abort.set_defaults(func=dataset_file.abort_dataset_file)

dataset_file_download = dataset_files_subparser.add_parser(
    "download",
    help="Download matched dataset file",
    parents=[
        _get_dataset_file_parent(dataset_file_name=True, dataset_file_download=True)
    ],
)
dataset_file_download.set_defaults(func=dataset_file.download_dataset_file)


dataset_file_delete = dataset_files_subparser.add_parser(
    "delete",
    help="Delete dataset file",
    parents=[_get_dataset_file_parent(dataset_file_name=True)],
)
dataset_file_delete.set_defaults(func=dataset_file.delete_dataset_file)

#
# dataset-file delta list/download
#

dataset_file_delta_parser = dataset_files_subparser.add_parser(
    "delta", help="Manage dataset delta files"
)
dataset_file_delta_subparser = dataset_file_delta_parser.add_subparsers()

dataset_file_delta_list = dataset_file_delta_subparser.add_parser(
    "list",
    help="List dataset delta files",
    parents=[_get_dataset_file_parent(dataset_file_name=True)],
)
dataset_file_delta_list.set_defaults(func=daily_files.list_dataset_file_deltas)

dataset_file_delta_download = dataset_file_delta_subparser.add_parser(
    "download",
    help="Download a dataset delta file",
    parents=[_get_dataset_file_parent(dataset_file_name=True, file_date=True)],
)
dataset_file_delta_download.set_defaults(func=daily_files.download_dataset_file_delta)

#
# dataset-file event list/download
#

dataset_file_event_parser = dataset_files_subparser.add_parser(
    "event", help="Manage dataset event files"
)
dataset_file_event_subparser = dataset_file_event_parser.add_subparsers()

dataset_file_event_list = dataset_file_event_subparser.add_parser(
    "list",
    help="List dataset event files",
    parents=[_get_dataset_file_parent(dataset_file_name=True)],
)
dataset_file_event_list.set_defaults(func=daily_files.list_dataset_event_files)

dataset_file_event_download = dataset_file_event_subparser.add_parser(
    "download",
    help="Download a dataset event file",
    parents=[
        _get_dataset_file_parent(
            dataset_file_name=True, dataset_file_Download=True, file_date=True
        )
    ],
)
dataset_file_event_download.set_defaults(func=daily_files.download_dataset_event_file)


def main():
    parsed = parser.parse_args()

    if parsed.verbose:
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging.INFO,
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stderr,
        )

    if not hasattr(parsed, "func"):
        parser.print_help()
        return

    parsed.func(parsed)
