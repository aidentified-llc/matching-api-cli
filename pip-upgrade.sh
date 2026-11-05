#!/usr/bin/env bash

uv pip compile --output-file requirements.txt requirements.in --upgrade
uv pip compile --output-file requirements-dev.txt requirements-dev.in --upgrade --python-version 3.11 --python-platform linux
