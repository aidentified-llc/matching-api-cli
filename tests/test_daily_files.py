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
import datetime
import io
from unittest import mock

import pytest

import aidentified_matching_api
import aidentified_matching_api.daily_files as daily_files

DAILY_FILES = [
    {"file_date": "2026-09-25", "download_url": "https://example.com/0925"},
    {"file_date": "2026-09-24", "download_url": "https://example.com/0924"},
]


@pytest.mark.parametrize("kind", ["delta", "trigger"])
def test_file_date_parsing(kind, tmp_path):
    parsed = aidentified_matching_api.parser.parse_args(
        [
            "dataset-file",
            kind,
            "download",
            "--dataset-name",
            "ds",
            "--dataset-file-name",
            "dsf",
            "--dataset-file-path",
            str(tmp_path / "out.csv"),
            "--file-date",
            "2026-09-24",
        ]
    )
    parsed.dataset_file_path.close()

    assert parsed.file_date == datetime.date(2026, 9, 24)


def test_file_date_parsing_rejects_bad_date(tmp_path):
    with pytest.raises(SystemExit):
        aidentified_matching_api.parser.parse_args(
            [
                "dataset-file",
                "delta",
                "download",
                "--dataset-name",
                "ds",
                "--dataset-file-name",
                "dsf",
                "--dataset-file-path",
                str(tmp_path / "out.csv"),
                "--file-date",
                "09/24/2026",
            ]
        )


def _args(file_date):
    args = mock.Mock()
    args.dataset_name = "ds"
    args.dataset_file_name = "dsf"
    args.file_date = file_date
    args.dataset_file_path = mock.Mock(wraps=io.BytesIO())
    return args


@mock.patch("aidentified_matching_api.daily_files.requests.get")
@mock.patch.object(
    daily_files.token.TokenService,
    "paginated_api_call",
    return_value=DAILY_FILES,
)
def test_download_selects_file_date(paginated_api_call, requests_get):
    requests_get.return_value.iter_content.return_value = [b"a,b\n"]
    args = _args(datetime.date(2026, 9, 24))

    daily_files.download_dataset_file_delta(args)

    paginated_api_call.assert_called_once_with(
        args,
        requests_get,
        "/v1/dataset-delta-file/",
        params={"dataset_name": "ds", "dataset_file_name": "dsf"},
    )
    requests_get.assert_called_once_with("https://example.com/0924")
    args.dataset_file_path.write.assert_called_once_with(b"a,b\n")


@mock.patch("aidentified_matching_api.daily_files.requests.get")
@mock.patch.object(
    daily_files.token.TokenService,
    "paginated_api_call",
    return_value=DAILY_FILES,
)
def test_download_missing_file_date(paginated_api_call, requests_get):
    args = _args(datetime.date(2026, 9, 1))

    with pytest.raises(Exception, match="No file found for date 2026-09-01"):
        daily_files.download_dataset_trigger_file(args)

    requests_get.assert_not_called()
