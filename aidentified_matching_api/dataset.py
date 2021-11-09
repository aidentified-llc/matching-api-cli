import requests

import aidentified_matching_api.token_service as token
import aidentified_matching_api.constants as constants


def list_datasets(args):
    resp_obj = token.token_service.paginated_api_call(args, requests.get, "/dataset/")
    constants.pretty(resp_obj)


def create_dataset(args):
    dataset_payload = {
        "name": args.name
    }
    resp_obj = token.token_service.api_call(args, requests.post, "/dataset/", json=dataset_payload)

    constants.pretty(resp_obj)
