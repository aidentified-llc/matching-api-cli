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
import pytest

from aidentified_matching_api.constants import check_presigned_url


@pytest.mark.parametrize(
    "url",
    [
        "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=x",
        "https://bucket.s3.us-east-1.amazonaws.com/key",
        "https://s3.amazonaws.com/bucket/key",
        "https://s3.us-east-2.amazonaws.com/bucket/key",
        "https://bucket.s3-accelerate.amazonaws.com/key",
        "https://bucket.s3.dualstack.us-east-1.amazonaws.com/key",
    ],
)
def test_check_presigned_url_accepts_s3(url):
    assert check_presigned_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "http://bucket.s3.amazonaws.com/key",
        "https://169.254.169.254/latest/meta-data/",
        "https://localhost/key",
        "https://example.com/key",
        "https://s3.amazonaws.com.example.com/key",
        "https://evils3.amazonaws.com/key",
        "https://ec2.amazonaws.com/key",
        "file:///etc/passwd",
        "",
    ],
)
def test_check_presigned_url_rejects_other_hosts(url):
    with pytest.raises(Exception, match="unexpected host"):
        check_presigned_url(url)
