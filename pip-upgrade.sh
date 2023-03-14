#!/usr/bin/env bash

pip-compile --output-file requirements.txt requirements.in --upgrade --allow-unsafe
pip-compile --output-file requirements-dev.txt requirements-dev.in --upgrade --allow-unsafe
