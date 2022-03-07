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
from setuptools import setup

with open("requirements.in", "r") as fd:
    requirements = [x.strip() for x in fd.readlines()]

setup(
    name="aidentified-matching-api",
    version="1.1.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
    ],
    packages=["aidentified_matching_api"],
    python_requires=">=3.6",
    # Let's not force all the hard requirements out from requirements.txt
    # in case people are installing this thing into their system Pythons.
    install_requires=requirements,
    entry_points={
        "console_scripts": ["aidentified_match=aidentified_matching_api:main"]
    },
    include_package_data=True,
)
