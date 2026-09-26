# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`aidentified-matching-api` is a published PyPI package providing the `aidentified_match` CLI, a thin wrapper around Aidentified's bulk contact matching REST API (`https://matching-api.aidentified.com`, overridable with the `AIDENTIFIED_URL` env var). The README is the user-facing docs for every subcommand and for the `dataset-file` status state machine. Update it when CLI behavior changes.

## Commands

```shell
pip install -r requirements-dev.txt && pip install -e .   # dev setup (same as CI)
pytest                                                    # all tests
pytest tests/test_validation.py::test_exc_validation      # one test
pre-commit run --all-files                                # lint/format (black -l 88, reorder-python-imports, flake8, license header)
./pip-upgrade.sh                                          # recompile requirements*.txt from *.in with uv
./deploy.sh                                               # python -m build && twine upload to PyPI
```

- CI runs pytest on Python 3.10–3.13 (`setup.py` has `python_requires=">=3.10"`), plus pre-commit.
- Runtime dependencies come from `requirements.in`, which `setup.py` reads directly into `install_requires`. Keep them loosely pinned (`requests==2.*`); `requirements.txt` holds the fully pinned lock.
- The version number is hardcoded in `setup.py`. Bump it there before a release.
- Every `.py` file must start with the Apache license header from `.github/license_header.txt`. The `insert-license` pre-commit hook adds it.

## Architecture

- **`__init__.py`** builds the whole argparse tree at import time and holds `main()`, the console entry point. Each leaf subcommand calls `set_defaults(func=...)` with a handler that takes the parsed `args` namespace. Shared options come from parent parsers built by `_get_dataset_file_parent(...)`, whose boolean flags turn on groups of arguments. `--dataset-file-path` is an `argparse.FileType`, so handlers receive an open file object, not a path. Global `--email`/`--password` default to `AID_EMAIL`/`AID_PASSWORD`.
- **`token_service.py`** has a module-level singleton `token_service`. Handlers call it as `token.token_service.api_call(args, requests.<verb>, "/v1/...", ...)` or `paginated_api_call(...)`, which follows DRF-style `next`/`results` pages. It logs in via `/login`, caches the bearer token pickled under the appdirs user cache dir (keyed by a CRC of `AIDENTIFIED_URL`), and turns request errors into plain `Exception`s with the response body.
- **Handlers** are `dataset.py`, `dataset_file.py`, and `daily_files.py` (nightly delta and trigger files, which share list/download helpers and differ only by route; the server has no fetch-by-date route, so download lists the files and picks the one whose `file_date` matches `--file-date`). They print JSON results to stdout via `constants.pretty`. `get_id.py` resolves user-supplied dataset and dataset-file *names* to server IDs, because the CLI is name-based and the API is ID-based.
- **Upload pipeline** (`dataset_file.upload_dataset_file`):
  1. Look up the dataset-file to get its `match_logic`.
  2. Validate the CSV on the client (`validation.py`, skipped with `--no-validate`).
  3. `initiate-upload`.
  4. Run an asyncio producer/consumer: `rewrite_csv` re-reads the input with the user's `--csv-*` dialect and encoding, rewrites it as UTF-8 minimal-quoted CSV, and splits it into parts of `--upload-part-size` MB. `--concurrent-uploads` `file_uploader` workers each get a presigned S3 URL per part from `/v1/dataset-file-upload-part/`, PUT the part with a Content-MD5 header, then PATCH the returned ETag back.
  5. `complete-upload`.

  Blocking `requests` calls run through `run_in_executor`. Any exception inside `upload_abort_ctxmgr` calls `abort-upload` on the server. S3 multipart requires parts of at least 5 MB.
- **`validation.py`**: `validate()` builds a `CsvArgs` and chooses a `CsvValidator` subclass by match logic: `OPPORTUNISTIC` (requires `first_name`/`last_name` plus at least one other attribute), `ADDRESS`, or `EMAIL`. Allowed headers are fixed sets. `school`/`email`/`phone`/`domain`/`linkedin` accept `_1`…`_10` suffixes. The validator also enforces a unique `id`, consistent row length, required non-empty values, and a limit of 500,000 data rows. It rewinds the file afterward so the upload can read it again. Error message strings are asserted verbatim in `tests/test_validation.py`.

## Pull requests

Use `.github/pull_request_template.md`. Reviewers come from `.github/CODEOWNERS`.
