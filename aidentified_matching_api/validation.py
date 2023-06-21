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
from typing import List

import aidentified_matching_api.constants as constants

# first_name - string - required
# last_name - string - required
# id - string - optional; unique
# street_address_1 - string - optional; home street
# street_address_2 - string - optional; home street 2
# city - string - optional; home city
# state - string - optional; home state
# postal_code - string - optional; 5 digit postal code
# company - string - optional; name of the company of employment
# title - string - optional; job title
# school[_{n}] - string - optional; n in 1 to 10, name of the school of employment
# email[_{n}] - string - optional; n in 1 to 10
# phone[_{n}] - string - optional; n in 1 to 10, 10 digit phone formatted 5558675309
# domain[_{n}] - string - optional; n in 1 to 10, email domain such as "aidentified.com"
# linkedin[_{n}] - string - optional; n in 1 to 10


HEADERS = {
    "first_name",
    "last_name",
    "id",
    "street_address_1",
    "street_address_2",
    "city",
    "state",
    "postal_code",
    "company",
    "title",
    "school",
    "email",
    "phone",
    "domain",
    "linkedin",
    *[f"school_{idx}" for idx in range(1, 11)],
    *[f"email_{idx}" for idx in range(1, 11)],
    *[f"phone_{idx}" for idx in range(1, 11)],
    *[f"domain_{idx}" for idx in range(1, 11)],
    *[f"linkedin_{idx}" for idx in range(1, 11)],
}
HEADERS_ADDRESS_ONLY = {
    "id",
    "street_address_1",
    "street_address_2",
    "postal_code",
    "city",
}
HEADERS_EMAIL_ONLY = {
    "id",
    "email",
    *[f"email_{idx}" for idx in range(1, 11)],
}


class ValidationError(Exception):
    pass


class CsvArgs:
    __slots__ = [
        "raw_fd",
        "codec_info",
        "delimiter",
        "doublequotes",
        "escapechar",
        "quotechar",
        "quoting",
        "skipinitialspace",
    ]

    def __init__(
        self,
        raw_fd: io.BytesIO,
        codec_info: codecs.CodecInfo,
        delimiter: str,
        doublequotes: bool,
        escapechar: str,
        quotechar: str,
        quoting: int,
        skipinitialspace: bool,
    ):
        self.raw_fd = raw_fd
        self.codec_info = codec_info
        self.delimiter = delimiter.replace("\\t", "\t")
        self.doublequotes = doublequotes
        self.escapechar = escapechar
        self.quotechar = quotechar
        self.quoting = quoting
        self.skipinitialspace = skipinitialspace


def _csv_read(csv_reader, record_idx):
    try:
        return next(csv_reader)
    except csv.Error as e:
        raise ValidationError(f"Bad CSV format in row {record_idx}: {e}") from None
    except UnicodeError as e:
        raise ValidationError(f"Bad character encoding at byte {e.start}") from None


def validate(args, match_logic) -> CsvArgs:
    # Validate choice of csv encoding, even if they don't do
    # the rest of the validation.
    try:
        codec_info = codecs.lookup(args.csv_encoding)
    except LookupError:
        raise ValidationError(f"Unknown csv-encoding '{args.csv_encoding}'") from None

    csv_args = CsvArgs(
        args.dataset_file_path,
        codec_info,
        args.csv_delimiter,
        args.csv_no_doublequotes,
        args.csv_escapechar,
        args.csv_quotechar,
        constants.QUOTE_METHODS[args.csv_quoting],
        args.csv_skip_initial_space,
    )

    if not args.validate:
        return csv_args

    if match_logic == "OPPORTUNISTIC":
        validator = OpportunisticCsvValidator(csv_args)
    elif match_logic == "ADDRESS":
        validator = AddressCsvValidator(csv_args)
    elif match_logic == "EMAIL":
        validator = EmailCsvValidator(csv_args)
    else:
        raise ValidationError(f"Unknown match_logic '{args.match_logic}'")

    validator.validate()

    args.dataset_file_path.seek(0)

    return csv_args


class CsvValidator:
    valid_headers: List[str]
    required_headers: List[str]

    def __init__(self, csv_args: CsvArgs):
        potential_bom = csv_args.raw_fd.read(3)
        # Python drops the BOM from UTF16/UTF32 but not UTF8
        if potential_bom == codecs.BOM_UTF8:
            skip_len = 3
        else:
            skip_len = 0
        csv_args.raw_fd.seek(0)
        csv_args.raw_fd.read(skip_len)

        text_fd = csv_args.codec_info.streamreader(csv_args.raw_fd)

        csv_reader = csv.reader(
            text_fd,
            delimiter=csv_args.delimiter,
            doublequote=csv_args.doublequotes,
            escapechar=csv_args.escapechar,
            quotechar=csv_args.quotechar,
            quoting=csv_args.quoting,
            skipinitialspace=csv_args.skipinitialspace,
            strict=True,
        )

        self.csv_reader = csv_reader
        self.required_header_idxes = []

    def validate(self):
        """Raises ValidationError when stuff goes wrong"""
        record_idx = 1

        try:
            headers = _csv_read(self.csv_reader, record_idx)
        except StopIteration:
            raise ValidationError("No headers in file") from None

        for header in headers:
            if header not in self.valid_headers:
                raise ValidationError(f"Invalid header '{header}'")

        self.validate_extra_attr_headers(headers)

        record_len = len(headers)

        try:
            id_idx = headers.index("id")
        except ValueError:
            id_idx = None
        id_uniqueness = set()

        self.calculate_required_header_idxes(headers)

        record_idx += 1

        while True:
            try:
                record = _csv_read(self.csv_reader, record_idx)
            except StopIteration:
                break

            if record_idx > 500_001:
                raise ValidationError("CSV has more than 500,000 data rows")

            if len(record) != record_len:
                raise ValidationError(f"Row {record_idx} does not match header length")

            if id_idx is not None:
                cust_id = record[id_idx]
                if cust_id in id_uniqueness:
                    raise ValidationError(
                        f"Row {record_idx} has duplicate id '{cust_id}'"
                    )
                id_uniqueness.add(cust_id)

            for required_header, required_header_idx in self.required_header_idxes:
                if not record[required_header_idx]:
                    raise ValidationError(
                        f"Row {record_idx} has invalid value for {required_header}"
                    )

            record_idx += 1

    def validate_extra_attr_headers(self, headers: List[str]):
        sentinel = object()
        if (
            next(
                (
                    header
                    for header in headers
                    if header not in ["id"] + self.required_headers
                ),
                sentinel,
            )
            is sentinel
        ):
            raise ValidationError("Needs at least one of the extra attribute headers")

    def calculate_required_header_idxes(self, headers: List[str]):
        for required_header in self.required_headers:
            if required_header not in headers:
                raise ValidationError(
                    f"Required header {required_header} not in headers"
                )

            self.required_header_idxes.append(
                (required_header, headers.index(required_header))
            )


class OpportunisticCsvValidator(CsvValidator):
    valid_headers = HEADERS
    required_headers = ["first_name", "last_name"]


class AddressCsvValidator(CsvValidator):
    valid_headers = HEADERS_ADDRESS_ONLY
    required_headers = ["street_address_1"]

    def validate_extra_attr_headers(self, headers: List[str]):
        return


class EmailCsvValidator(CsvValidator):
    valid_headers = HEADERS_EMAIL_ONLY
    required_headers = []

    def validate_extra_attr_headers(self, headers: List[str]):
        return

    def calculate_required_header_idxes(self, headers: List[str]):
        for header in ("email", "email_1"):
            if header in headers:
                self.required_header_idxes.append((header, headers.index(header)))
                return

        if not self.required_header_idxes:
            raise ValidationError("Required header email_N not in headers")
