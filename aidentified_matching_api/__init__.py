# -*- coding: utf-8 -*-
# Copyright 2022 Aidentified LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import csv
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
token_parser.add_argument(
    "--clear-cache", help="Delete cached token", action="store_true"
)
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
    validation=False,
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
            help="Path to local file for upload",
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
            type=lambda s: datetime.datetime.strptime("%Y-%m-%d", s),
        )

    if validation:
        _dataset_csv_group = _dataset_file_parent.add_argument_group(
            title="CSV arguments"
        )
        _dataset_csv_group.add_argument(
            "--no-validate",
            help="Disable client-side validation of CSV file upload",
            action="store_false",
            dest="validate",
        )

        _dataset_csv_group.add_argument(
            "--csv-encoding",
            help="Re-encode text CSV file before uploading. (default 'UTF-8') A list of supported encodings is at https://docs.python.org/3/library/codecs.html#standard-encodings",
            default="UTF-8",
        )

        _dataset_csv_group.add_argument(
            "--csv-delimiter",
            help=f"Specify CSV delimiter (default '{csv.excel.delimiter}'). A \\t will be interpreted as the tab character (0x09.)",
            default=csv.excel.delimiter,
        )

        _dataset_csv_group.add_argument(
            "--csv-no-doublequotes",
            help="Controls how instances of csv-quotechar appearing inside a field should themselves be quoted. By default, the character is doubled. With csv-no-doublequotes the csv-escapechar is used as a prefix to the csv-quotechar.",
            action="store_false",
        )

        _dataset_csv_group.add_argument(
            "--csv-escapechar",
            help="The character used to escape the delimiter in fields. By default this is not enabled, you must pass --csv-no-doublequotes --csv-quoting none along with it to use it.",
            default=csv.excel.escapechar,
        )

        _dataset_csv_group.add_argument(
            "--csv-quotechar",
            help=f"A one-character string used to quote fields containing special characters, such as the csv-delimiter or csv-quotechar, or which contain new-line characters. (default '{csv.excel.quotechar}')",
            default=csv.excel.quotechar,
        )

        QUOTE_METHODS = {
            "all": csv.QUOTE_ALL,
            "minimal": csv.QUOTE_MINIMAL,
            "none": csv.QUOTE_NONE,
        }

        _dataset_csv_group.add_argument(
            "--csv-quoting",
            help="Specify CSV quoting behavior. (default 'minimal'). all: quote all fields. minimal: only quote fields that need escaping. none: fields are never quoted.",
            default="minimal",
            choices=QUOTE_METHODS.keys(),
            type=lambda s: QUOTE_METHODS[s],
        )

        _dataset_csv_group.add_argument(
            "--csv-skip-initial-space",
            help="Ignore whitespace immediately following a csv-delimiter.",
            action="store_true",
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
        _get_dataset_file_parent(
            dataset_file_name=True, dataset_file_upload=True, validation=True
        )
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
    parents=[
        _get_dataset_file_parent(
            dataset_file_name=True, dataset_file_download=True, file_date=True
        )
    ],
)
dataset_file_delta_download.set_defaults(func=daily_files.download_dataset_file_delta)

#
# dataset-file trigger list/download
#

dataset_file_trigger_parser = dataset_files_subparser.add_parser(
    "trigger", help="Manage dataset trigger files"
)
dataset_file_trigger_subparser = dataset_file_trigger_parser.add_subparsers()

dataset_file_trigger_list = dataset_file_trigger_subparser.add_parser(
    "list",
    help="List dataset trigger files",
    parents=[_get_dataset_file_parent(dataset_file_name=True)],
)
dataset_file_trigger_list.set_defaults(func=daily_files.list_dataset_trigger_files)

dataset_file_trigger_download = dataset_file_trigger_subparser.add_parser(
    "download",
    help="Download a dataset trigger file",
    parents=[
        _get_dataset_file_parent(
            dataset_file_name=True, dataset_file_download=True, file_date=True
        )
    ],
)
dataset_file_trigger_download.set_defaults(
    func=daily_files.download_dataset_trigger_file
)


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
