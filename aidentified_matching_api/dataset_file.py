# -*- coding: utf-8 -*-
import base64
import concurrent.futures
import contextlib
import hashlib
import logging
import os

import requests

import aidentified_matching_api.constants as constants
import aidentified_matching_api.token_service as token

logger = logging.getLogger("matching_api_cli")


def _get_dataset_id_from_dataset_name(args):
    dataset_params = {"name": args.dataset_name}
    resp_obj = token.token_service.api_call(
        args, requests.get, "/v1/dataset/", params=dataset_params
    )

    if resp_obj["count"] == 0:
        raise Exception(f"No dataset with name '{args.dataset_name}' found")

    return resp_obj["results"][0]["dataset_id"]


def _get_dataset_file_id_from_dataset_file_name(args):
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


def list_dataset_files(args):
    dataset_file_params = {
        "dataset_name": args.dataset_name,
    }
    resp_obj = token.token_service.paginated_api_call(
        args, requests.get, "/v1/dataset-file/", params=dataset_file_params
    )
    constants.pretty(resp_obj)


def abort_dataset_file(args):
    dataset_file_id = _get_dataset_file_id_from_dataset_file_name(args)
    resp_obj = token.token_service.api_call(
        args, requests.post, f"/v1/dataset-file/{dataset_file_id}/abort-upload/"
    )
    constants.pretty(resp_obj)


def create_dataset_file(args):
    # create files under name.
    dataset_id = _get_dataset_id_from_dataset_name(args)

    dataset_file_payload = {"dataset_id": dataset_id, "name": args.dataset_file_name}
    resp_obj = token.token_service.api_call(
        args, requests.post, "/v1/dataset-file/", json=dataset_file_payload
    )

    constants.pretty(resp_obj)


# XXX feedback to jenson
# XXX open pr in other repo


def _file_uploader(args, dataset_file_id: str, part_size_bytes: int, part_idx: int):
    aws_part_number = part_idx + 1
    part_start = part_idx * part_size_bytes

    logger.info(f"Starting upload part {aws_part_number}")

    with args.upload_dataset_file_lock:
        args.dataset_file_path.seek(part_start)
        part_data = args.dataset_file_path.read(part_size_bytes)

    logger.info(f"Starting upload part {aws_part_number} hash and compression")
    md5 = base64.b64encode(hashlib.md5(part_data).digest())

    upload_part_payload = {
        "dataset_file_id": dataset_file_id,
        "part_number": aws_part_number,
        "md5": md5.decode("UTF-8"),
    }
    resp = token.token_service.api_call(
        args, requests.post, "/v1/dataset-file-upload-part/", json=upload_part_payload
    )
    upload_url = resp["upload_url"]
    dataset_file_upload_part_id = resp["dataset_file_upload_part_id"]

    logger.info(f"Starting upload part {aws_part_number} upload")
    try:
        upload_resp = requests.put(
            upload_url, data=part_data, headers={"content-md5": md5}
        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"Unable to upload file part: {e}") from None

    try:
        upload_resp.raise_for_status()
    except requests.exceptions.RequestException:
        # S3 returns XML. If it fails, let's just spew it.
        raise Exception(
            f"Unable to upload file part: {upload_resp.status_code} {upload_resp.text}"
        ) from None

    token.token_service.api_call(
        args,
        requests.patch,
        f"/v1/dataset-file-upload-part/{dataset_file_upload_part_id}/",
        json={"etag": upload_resp.headers["ETag"]},
    )

    logger.info(f"Finished upload part {aws_part_number}")


@contextlib.contextmanager
def upload_ctxmgr(args, dataset_file_id: str):
    try:
        yield
    except:  # noqa: E722
        token.token_service.api_call(
            args, requests.post, f"/v1/dataset-file/{dataset_file_id}/abort-upload/"
        )
        raise


logging.getLogger("").setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True


def upload_dataset_file(args):
    if args.upload_part_size < 5:
        raise Exception("--upload-part-size must be greater than 5 Mb")
    part_size_bytes = args.upload_part_size * 1024 * 1024

    dataset_file_id = _get_dataset_file_id_from_dataset_file_name(args)

    token.token_service.api_call(
        args, requests.post, f"/v1/dataset-file/{dataset_file_id}/initiate-upload/"
    )

    with contextlib.ExitStack() as stack:
        pool = stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrent_uploads)
        )
        stack.enter_context(upload_ctxmgr(args, dataset_file_id))

        total_file_size = os.stat(args.dataset_file_path.name).st_size
        part_count = total_file_size // part_size_bytes
        if (total_file_size % part_size_bytes) > 0:
            part_count += 1

        futures = [
            pool.submit(
                _file_uploader, args, dataset_file_id, part_size_bytes, part_idx
            )
            for part_idx in range(part_count)
        ]

        done, not_done = concurrent.futures.wait(
            futures, return_when=concurrent.futures.FIRST_EXCEPTION
        )

        # If there is anything in not_done it means we had an exception, so just cancel everything now.
        for future in not_done:
            future.cancel()

        had_exception = [future.exception() for future in done if future.exception()]
        if had_exception:
            exc_strings = ", ".join(
                f"Task {fut_idx}: {exc}" for fut_idx, exc in enumerate(had_exception)
            )
            raise Exception(f"Error(s) while uploading file: {exc_strings}")

    complete_resp = token.token_service.api_call(
        args, requests.post, f"/v1/dataset-file/{dataset_file_id}/complete-upload/"
    )
    constants.pretty(complete_resp)


def download_dataset_file(args):
    dataset_file_id = _get_dataset_file_id_from_dataset_file_name(args)

    resp_obj = token.token_service.api_call(
        args, requests.get, f"/v1/dataset-file/{dataset_file_id}/"
    )
    if resp_obj["download_url"] is None:
        raise Exception("Dataset file is not ready for download.")

    try:
        download_req = requests.get(resp_obj["download_url"])
    except requests.exceptions.RequestException as e:
        raise Exception(f"Unable to download file: {e}") from None

    for chunk in download_req.iter_content(chunk_size=1024 * 1024 * 1024):
        args.dataset_file_path.write(chunk)

    args.dataset_file_path.close()


def delete_dataset_file(args):
    dataset_file_id = _get_dataset_file_id_from_dataset_file_name(args)

    token.token_service.api_call(
        args, requests.delete, f"/v1/dataset-file/{dataset_file_id}/"
    )
