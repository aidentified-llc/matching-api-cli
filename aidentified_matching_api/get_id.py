# -*- coding: utf-8 -*-
import requests

import aidentified_matching_api.token_service as token


def get_dataset_id_from_dataset_name(args):
    dataset_params = {"name": args.dataset_name}
    resp_obj = token.token_service.api_call(
        args, requests.get, "/v1/dataset/", params=dataset_params
    )

    if resp_obj["count"] == 0:
        raise Exception(f"No dataset with name '{args.dataset_name}' found")

    return resp_obj["results"][0]["dataset_id"]


def get_dataset_file_id_from_dataset_file_name(args):
    dataset_params = {
        "dataset_name": args.dataset_name,
        "name": args.dataset_file_name,
    }
    resp_obj = token.token_service.api_call(
        args, requests.get, "/v1/dataset-file/", params=dataset_params
    )

    if resp_obj["count"] == 0:
        raise Exception(f"No dataset file with name '{args.dataset_file_name}' found")

    return resp_obj["results"][0]["dataset_file_id"]
