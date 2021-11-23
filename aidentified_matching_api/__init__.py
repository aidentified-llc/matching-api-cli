import argparse
import os
import threading
import logging
import sys

import aidentified_matching_api.token_service as token_service
import aidentified_matching_api.dataset as dataset
import aidentified_matching_api.dataset_file as dataset_file


parser = argparse.ArgumentParser(description="Aidentified matching API command line wrapper")
parser.add_argument("--email", help="Email address of Aidentified account", default=os.environ.get("AID_EMAIL"))
parser.add_argument("--password", help="Password of Aidentified account", default=os.environ.get("AID_PASSWORD"))
parser.add_argument("--verbose", help="Write log output to stderr", action="store_true")


subparser = parser.add_subparsers()

#
# token
#

token_parser = subparser.add_parser('auth', help="Print JWT token")
token_parser.set_defaults(func=token_service.get_token)

#
# dataset
#

dataset_parser = subparser.add_parser('dataset', help='Manage datasets')
dataset_subparser = dataset_parser.add_subparsers()

dataset_list = dataset_subparser.add_parser('list', help="List datasets")
dataset_list.set_defaults(func=dataset.list_datasets)

dataset_create = dataset_subparser.add_parser('create', help="Create new dataset")
dataset_create.add_argument("--name", help="Dataset name", required=True)
dataset_create.set_defaults(func=dataset.create_dataset)

#
# dataset-file
#

dataset_files_parser = subparser.add_parser('dataset-file', help='Manage dataset files')
dataset_files_subparser = dataset_files_parser.add_subparsers()

dataset_file_list = dataset_files_subparser.add_parser('list', help="List dataset files")
dataset_file_list.add_argument("--dataset-name", help="Dataset name", required=True)
dataset_file_list.set_defaults(func=dataset_file.list_dataset_files)

dataset_file_create = dataset_files_subparser.add_parser('create', help="Create new dataset file")
dataset_file_create.add_argument("--dataset-name", help="Name of parent dataset", required=True)
dataset_file_create.add_argument("--dataset-file-name", help="Name of new dataset file", required=True)
dataset_file_create.set_defaults(func=dataset_file.create_dataset_file)

dataset_file_create = dataset_files_subparser.add_parser('upload', help="Upload dataset file")
dataset_file_create.add_argument("--dataset-name", help="Name of parent dataset", required=True)
dataset_file_create.add_argument("--dataset-file-name", help="Name of dataset file", required=True)
dataset_file_create.add_argument("--dataset-file-path", help="Path to dataset file", required=True, type=argparse.FileType(mode='rb'))
dataset_file_create.add_argument("--upload-part-size", help="Size of upload chunk in megabytes", type=int, default=100)
dataset_file_create.add_argument("--concurrent-uploads", help="Max number of concurrent uploads", type=int, default=4)
dataset_file_create.set_defaults(func=dataset_file.upload_dataset_file)
dataset_file_create.set_defaults(upload_dataset_file_lock=threading.Lock())

dataset_file_abort = dataset_files_subparser.add_parser('abort', help="Abort dataset file upload")
dataset_file_abort.add_argument("--dataset-name", help="Name of parent dataset", required=True)
dataset_file_abort.add_argument("--dataset-file-name", help="Name of dataset file", required=True)
dataset_file_abort.set_defaults(func=dataset_file.abort_dataset_file)



def main():
    parsed = parser.parse_args()

    if parsed.verbose:
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S',
            stream=sys.stderr,
        )

    if not hasattr(parsed, "func"):
        parser.print_help()
        return

    parsed.func(parsed)