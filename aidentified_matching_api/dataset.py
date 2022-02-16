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
import aidentified_matching_api.get_id as get_id
import aidentified_matching_api.token_service as token


def list_datasets(args):
    resp_obj = token.token_service.paginated_api_call(
        args, requests.get, "/v1/dataset/"
    )
    constants.pretty(resp_obj)


def create_dataset(args):
    dataset_payload = {"name": args.name}
    resp_obj = token.token_service.api_call(
        args, requests.post, "/v1/dataset/", json=dataset_payload
    )

    constants.pretty(resp_obj)


def delete_dataset(args):
    args.dataset_name = args.name
    dataset_id = get_id.get_dataset_id_from_dataset_name(args)

    token.token_service.api_call(
        args,
        requests.delete,
        f"/v1/dataset/{dataset_id}/",
    )
