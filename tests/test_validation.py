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
import codecs
import csv
import io

import pytest

from aidentified_matching_api.validation import CsvArgs
from aidentified_matching_api.validation import validate_fd


UTF_8 = codecs.lookup("UTF-8")

ORDINARY_CSV_ARGS = [
    UTF_8,
    csv.excel.delimiter,
    csv.excel.doublequote,
    csv.excel.escapechar,
    csv.excel.quotechar,
    csv.excel.quoting,
    csv.excel.skipinitialspace,
]

TEST_DATA = [
    (b'"xyz,xyz\nxyz,xyz', "Bad CSV format in row 1: unexpected end of data"),
    ("foo,bar\n".encode("UTF-16"), "Bad character encoding at byte 0"),
    (b"", "No headers in file"),
    (b"intentionally_invalid_header", "Invalid header 'intentionally_invalid_header'"),
    (b"first_name,city", "Required header last_name not in headers"),
    (b"first_name,last_name,id", "Needs at least one of the extra attribute headers"),
    (b"first_name,last_name,city\nfoo,bar\n", "Row 2 does not match header length"),
    (
        b"first_name,last_name,id,city\nfoo,bar,baz,boston\nfoo,bar,baz,boston",
        "Row 3 has duplicate id 'baz'",
    ),
    (
        b"first_name,last_name,id,city\n,bar,baz,boston",
        "Row 2 has invalid value for first_name",
    ),
]


@pytest.mark.parametrize("buffer, exc_msg", TEST_DATA)
def test_exc_validation(buffer, exc_msg):
    fd = io.BytesIO(buffer)
    csv_args = CsvArgs(fd, *ORDINARY_CSV_ARGS)

    with pytest.raises(Exception) as exc:
        validate_fd(csv_args)

    assert str(exc.value) == exc_msg


def test_tsv():
    fd = io.BytesIO(b"first_name\tlast_name\tcity\nfoo\tbar\tboston")
    csv_args = CsvArgs(
        fd,
        UTF_8,
        "\\t",
        csv.excel.doublequote,
        csv.excel.escapechar,
        csv.excel.quotechar,
        csv.QUOTE_NONE,
        csv.excel.skipinitialspace,
    )

    validate_fd(csv_args)
