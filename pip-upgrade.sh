#!/usr/bin/env bash

pip-compile --output-file requirements.txt requirements.in --upgrade
pip-compile --output-file requirements-dev.txt requirements-dev.in --upgrade
