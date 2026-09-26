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
import csv
import json
import os
import re
import urllib.parse

AIDENTIFIED_URL = os.environ.get(
    "AIDENTIFIED_URL", "https://matching-api.aidentified.com"
)


# Download and upload URLs handed out by the API are presigned S3 URLs.
# Refuse to follow anything else.
S3_HOST_RE = re.compile(r"(^|\.)s3([.-][a-z0-9-]+)*\.amazonaws\.com$")


def check_presigned_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not S3_HOST_RE.search(parsed.hostname or ""):
        raise Exception(
            f"Refusing to use presigned URL with unexpected host: {parsed.hostname}"
        )
    return url


def pretty(obj):
    print(json.dumps(obj, indent=4, sort_keys=True))


QUOTE_METHODS = {
    "all": csv.QUOTE_ALL,
    "minimal": csv.QUOTE_MINIMAL,
    "none": csv.QUOTE_NONE,
}
