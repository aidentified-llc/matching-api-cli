# -*- coding: utf-8 -*-
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


def list_dataset_event_files(args):
    return _list_daily_files(args, "/v1/events-file/")


def download_dataset_event_file(args):
    return _download_daily_file(args, "/v1/events-file/")