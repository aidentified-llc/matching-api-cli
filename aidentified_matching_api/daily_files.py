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
import requests

import aidentified_matching_api.constants as constants
import aidentified_matching_api.token_service as token


def _list_daily_files(args, route: str):
    dataset_params = {
        "dataset_name": args.dataset_name,
        "dataset_file_name": args.dataset_file_name,
    }
    resp_obj = token.token_service.paginated_api_call(
        args, requests.get, route, params=dataset_params
    )

    constants.pretty(resp_obj)


def _download_daily_file(args, route: str):
    dataset_params = {
        "dataset_name": args.dataset_name,
        "dataset_file_name": args.dataset_file_name,
    }
    resp_obj = token.token_service.api_call(
        args, requests.get, route, params=dataset_params
    )

    try:
        download_req = requests.get(resp_obj["download_url"])
    except requests.exceptions.RequestException as e:
        raise Exception(f"Unable to download file: {e}") from None

    for chunk in download_req.iter_content(chunk_size=1024 * 1024 * 1024):
        args.dataset_file_path.write(chunk)

    args.dataset_file_path.close()


def list_dataset_file_deltas(args):
    return _list_daily_files(args, "/v1/dataset-delta-file/")


def download_dataset_file_delta(args):
    return _download_daily_file(args, "/v1/dataset-delta-file/")


def list_dataset_trigger_files(args):
    return _list_daily_files(args, "/v1/trigger-file/")


def download_dataset_trigger_file(args):
    return _download_daily_file(args, "/v1/trigger-file/")
