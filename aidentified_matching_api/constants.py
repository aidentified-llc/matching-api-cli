# -*- coding: utf-8 -*-
import json
import os

AIDENTIFIED_URL = os.environ.get(
    "AIDENTIFIED_URL", "https://enterprise-matching-api.aidentified.com"
)


def pretty(obj):
    print(json.dumps(obj, indent=4, sort_keys=True))
