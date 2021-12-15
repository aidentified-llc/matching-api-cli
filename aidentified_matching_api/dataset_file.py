# -*- coding: utf-8 -*-
import asyncio
import base64
import codecs
import contextlib
import csv
import functools
import hashlib
import io
import logging
import threading

import requests

import aidentified_matching_api.constants as constants
import aidentified_matching_api.token_service as token
import aidentified_matching_api.validation as validation

logger = logging.getLogger("matching_api_cli")


UPLOAD_CANCELLED = threading.Event()


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


async def file_uploader(args, dataset_file_id: str, part_queue: asyncio.Queue):
    loop = asyncio.get_event_loop()

    while True:
        part_idx, part_data = await part_queue.get()
        aws_part_number = part_idx + 1

        logger.info(f"Starting upload part {aws_part_number} hash and compression")
        md5 = await loop.run_in_executor(
            None, lambda data: base64.b64encode(hashlib.md5(data).digest()), part_data
        )

        upload_part_payload = {
            "dataset_file_id": dataset_file_id,
            "part_number": aws_part_number,
            "md5": md5.decode("UTF-8"),
        }
        upload_part_callable = functools.partial(
            token.token_service.api_call,
            args,
            requests.post,
            "/v1/dataset-file-upload-part/",
            json=upload_part_payload,
        )
        resp = await loop.run_in_executor(None, upload_part_callable)
        upload_url = resp["upload_url"]
        dataset_file_upload_part_id = resp["dataset_file_upload_part_id"]

        logger.info(f"Starting upload part {aws_part_number} upload")
        put_part_callable = functools.partial(
            requests.put,
            upload_url,
            data=part_data,
            headers={"content-md5": md5},
        )
        try:
            upload_resp = await loop.run_in_executor(None, put_part_callable)
        except requests.exceptions.RequestException as e:
            raise Exception(f"Unable to upload file part: {e}") from None

        try:
            upload_resp.raise_for_status()
        except requests.exceptions.RequestException:
            # S3 returns XML. If it fails, let's just spew it.
            raise Exception(
                f"Unable to upload file part: {upload_resp.status_code} {upload_resp.text}"
            ) from None

        patch_etag_callable = functools.partial(
            token.token_service.api_call,
            args,
            requests.patch,
            f"/v1/dataset-file-upload-part/{dataset_file_upload_part_id}/",
            json={"etag": upload_resp.headers["ETag"]},
        )
        await loop.run_in_executor(None, patch_etag_callable)

        part_queue.task_done()
        logger.info(f"Finished upload part {aws_part_number}")


@contextlib.contextmanager
def upload_abort_ctxmgr(args, dataset_file_id: str):
    try:
        yield
    except:  # noqa: E722
        token.token_service.api_call(
            args, requests.post, f"/v1/dataset-file/{dataset_file_id}/abort-upload/"
        )
        raise


async def rewrite_csv(
    csv_args: validation.CsvArgs, part_size_bytes: int, part_queue: asyncio.Queue
):
    loop = asyncio.get_event_loop()

    part_idx = 0
    utf_8_info = codecs.lookup("UTF-8")

    out_buf = b""
    out_bytes_fd = io.BytesIO()
    out_text_fd = utf_8_info.streamwriter(out_bytes_fd)

    read_fd = csv_args.codec_info.streamreader(csv_args.raw_fd)

    reader = csv.reader(
        read_fd,
        delimiter=csv_args.delimiter,
        doublequote=csv_args.doublequotes,
        quotechar=csv_args.quotechar,
        quoting=csv_args.quoting,
        skipinitialspace=csv_args.skipinitialspace,
        strict=True,
    )
    writer = csv.writer(out_text_fd, quoting=csv.QUOTE_MINIMAL)

    while True:
        try:
            row = await loop.run_in_executor(None, lambda: next(reader))
        except StopIteration:
            break

        writer.writerow(row)

        if out_bytes_fd.tell() < part_size_bytes:
            continue

        out_buf += out_bytes_fd.getvalue()

        out_bytes_fd.seek(0)
        out_bytes_fd.truncate()

        await part_queue.put((part_idx, out_buf[:part_size_bytes]))
        out_buf = out_buf[part_size_bytes:]
        part_idx += 1

    if len(out_buf) > 0:
        await part_queue.put((part_idx, out_buf))


async def manage_uploads(args, dataset_file_id: str, csv_args: validation.CsvArgs):
    part_queue = asyncio.Queue(maxsize=1)
    part_size_bytes = args.upload_part_size * 1024 * 1024

    # Start csv writer coro
    csv_rewriter = asyncio.create_task(
        rewrite_csv(csv_args, part_size_bytes, part_queue)
    )

    coros = [csv_rewriter]

    for _ in range(args.concurrent_uploads):
        coros.append(file_uploader(args, dataset_file_id, part_queue))

    coros.append(part_queue.join())
    done, pending = await asyncio.wait(*coros, return_when=asyncio.FIRST_EXCEPTION)

    # if pending, an exception hit us
    for pending_fut in pending:
        pending_fut.cancel()

    had_exception = [
        future.exception() for future in done if future.exception() is not None
    ]
    if had_exception:
        exc_strings = ", ".join(
            f"Task {fut_idx}: {exc}" for fut_idx, exc in enumerate(had_exception)
        )
        raise Exception(f"Error(s) while uploading file: {exc_strings}")


def upload_dataset_file(args):
    if args.upload_part_size < 5:
        raise Exception("--upload-part-size must be greater than 5 Mb")

    csv_args = validation.validate(args)

    dataset_file_id = _get_dataset_file_id_from_dataset_file_name(args)

    token.token_service.api_call(
        args, requests.post, f"/v1/dataset-file/{dataset_file_id}/initiate-upload/"
    )

    loop = asyncio.new_event_loop()
    with upload_abort_ctxmgr(args, dataset_file_id):
        loop.run_until_complete(manage_uploads(args, dataset_file_id, csv_args))

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
