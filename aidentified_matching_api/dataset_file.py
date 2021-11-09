import requests

import aidentified_matching_api.token_service as token
import aidentified_matching_api.constants as constants


def _get_dataset_id_from_dataset_name(args):
    dataset_params = {
        "name": args.dataset_name
    }
    resp_obj = token.token_service.api_call(args, requests.get, "/dataset/", params=dataset_params)

    if resp_obj['count'] == 0:
        raise Exception(f"No dataset with name '{args.dataset_name}' found")

    return resp_obj['results'][0]['dataset_id']


def list_dataset_files(args):
    dataset_file_params = {
        "dataset_name": args.dataset_name,
    }
    resp_obj = token.token_service.paginated_api_call(args, requests.get, "/dataset-files/", params=dataset_file_params)
    constants.pretty(resp_obj)


def create_dataset_file(args):
    # create files under name.
    dataset_id = _get_dataset_id_from_dataset_name(args)

    dataset_file_payload = {
        "dataset_id": dataset_id,
        "name": args.dataset_file_name
    }
    resp_obj = token.token_service.api_call(args, requests.post, "/dataset-files/", json=dataset_file_payload)

    constants.pretty(resp_obj)


