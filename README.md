## aidentified-matching-api
This is a command-line wrapper around Aidentified's bulk contact matching API. If you have a CSV file ready for
matching this is the easiest way to get up and running with Aidentified.

## Installation
Requirements: Python 3.6+

To install in your system Python environment:

```shell
python -m pip install aidentified-matching-api
```

## Data model
A `dataset` is a customer-defined grouping of `dataset-file`s. The `dataset-file` is a CSV file you'd upload to
the Aidentified contact matching and enrichment service. You can assign whatever names you'd like to your
`dataset`s and `dataset-file`s. Once a `dataset-file`'s upload is finished it can not be modified, but you can
always create new `dataset-file`s and delete old ones.

Files must be formatted as comma-separated CSVs in the UTF-8 encoding. If your input is not in that format
you can use the `--csv-` options to `dataset-file upload` to specify the properties of your CSV and this program
will translate your CSV to the expected format while it uploads.

Once your upload of the `dataset-file` is finished the Aidentified matching service will start processing your file.
The initial run of the matcher will output enriched attributes for every single matched contact in your input file.
However, this whole-file matching runs only **once**, immediately after the initial upload. To get the latest
attributes for your contacts you  must download the `delta` files, which are produced every night for every
`dataset-file`.  The `delta` file only contains the records of contacts whose attributes have changed in the
Aidentified system.

There is also a nightly `trigger` file produced for each `dataset-file` that lists the most recent Money in Motion
events for each matched contact. These files are returned in a CSV format.

The `dataset-file` follows a state machine through its matching process. The current state is available as the `status`
field in the objects written to stdout by the `dataset-file list` subcommand. As an example:

```shell
$ aidentified_match dataset-file list --dataset-name test
[
    {
        "created_date": "2021-12-02T21:44:30.192597Z",
        "dataset_file_id": "8a1a330c-d1fe-4a4c-95bf-07280e33f55b",
        "dataset_id": "11a6193f-37f1-4c57-b771-418d3888d58a",
        "download_url": "https://aidentified.com",
        "modified_date": "2021-12-03T19:09:44.507938Z",
        "name": "test1.csv",
        "status": "MATCHING_FINISHED"
    }
]
```

The full list of states:
* UPLOAD_NOT_STARTED: The initial state for a new `dataset-file`
* UPLOAD_IN_PROGRESS: The upload for the `dataset-file` was successfully initiated. Aborting the upload will return it
to `UPLOAD_NOT_STARTED` and remove any partially uploaded files.
* VALIDATION_IN_PROGRESS: The CSV upload is complete and server-side validation is running. It is not possible to
return to `UPLOAD_NOT_STARTED` from here as `dataset-file`s are immutable.
* VALIDATION_ERROR: Validation of the file failed. An error message is available in the output of `dataset-file list`.
* MATCHING_IN_PROGRESS: The upload was successful and the Aidentified matcher is running.
* MATCHING_ERROR: The Aidentified matcher was unable to complete the initial matching. An error message is available
in the output of `dataset-file list`.
* MATCHING_FINISHED: The initial matching of the `dataset-file` is complete and the fully-matched file is available
for download. The system will also start producing nightly delta and trigger files.

## Usage
The `aidentifed_match` CLI program has extensive help for all of its functionality. Adding `--help` to any of its
subcommands will give you help for that subcommand.

All commands require a `--email` and `--password` argument for your API credentials. Alternatively, you can export the
`AID_EMAIL` and `AID_PASSWORD` environment variables in place of those arguments to avoid repeating yourself.

### dataset list
```shell
aidentified_match dataset list
```
List datasets previously created under your account.

### dataset create
```shell
aidentified_match dataset create --name NAME
```
Create a new dataset with an arbitrary name.

### dataset delete
```shell
aidentified_match dataset delete --name NAME
```
Delete the dataset with given name. This will recursively delete all dataset files, matched file outputs and delta files.

### dataset-file list
```shell
aidentified_match dataset-file list --dataset-name DATASET_NAME
```
List all dataset-files under the dataset with name `DATASET_NAME`.

### dataset-file create
```shell
aidentified_match dataset-file create --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
```
Create a new dataset-file under dataset `DATASET_NAME` and with dataset file name `DATASET_FILE_NAME`. Note that the names
are arbitrary, they don't have to match any file names on your file system. This step does not begin the upload process.

### dataset-file upload
```shell
aidentified_match dataset-file upload [-h] --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME --dataset-file-path
                                      DATASET_FILE_PATH [--no-validate] [--csv-encoding CSV_ENCODING] [--csv-delimiter CSV_DELIMITER]
                                      [--csv-no-doublequotes] [--csv-escapechar CSV_ESCAPECHAR] [--csv-quotechar CSV_QUOTECHAR]
                                      [--csv-quoting {all,minimal,none}] [--csv-skip-initial-space] [--upload-part-size UPLOAD_PART_SIZE]
                                      [--concurrent-uploads CONCURRENT_UPLOADS]
```
Upload a CSV for enrichment. The dataset-file must be created with `dataset-file create` before you can begin the upload.
The dataset must also be in the `UPLOAD_NOT_STARTED` state.

By default, your file will be uploaded in parallel parts. Each part will be at most 100 MB in size and there will be
four concurrent uploads. The optional `--upload-part-size` and `--concurrent-uploads` arguments can be used to tweak
those defaults.

The uploader will do a pass over your CSV to do a simple validation of its content and structure. If you know your
files are well-formatted you can skip it with `--no-validate`.

CSV files are expected to be encoded in UTF-8, use commas as the field delimiter, and use double quotes for field
quoting. The `--csv` flags direct the uploader to translate your CSV file on-the-fly before validation and uploading
if your files don't match that format.

| Flag                       | Description                                                                                                                                |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `--csv-encoding`           | Override default encoding of UTF-8. [Browse the list of encodings.](https://docs.python.org/3/library/codecs.html#standard-encodings)      |
| `--csv-delimiter`          | Specify the character used to delimit fields.                                                                                              |
| `--csv-no-doublequotes`    | Allow quoting using the `--csv-escapechar` field. Must also specify `--csv-quoting none`                                                   |
| `--csv-escapechar`         | The character to use for escaping the delimiter in `--csv-no-doublequotes` mode.                                                           |
| `--csv-quotechar`          | The character to use for quoting fields.                                                                                                   |
| `--csv-quoting`            | Specify that the csv uses no quoting (none), only quotes fields requiring quoting (minimal), or all fields are quoted automatically (full) |
| `--csv-skip-initial-space` | Ignore whitespace immediately after the delimiter (default is to not ignore)                                                               |

### dataset-file abort
```shell
aidentified_match dataset-file abort --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
```
Abort an upload in `UPLOAD_IN_PROGRESS` state, rolling it back to `UPLOAD_NOT_STARTED`. The `dataset-file upload`
subcommand will attempt to roll back failed uploads by default. This is only useful if that rollback fails.

### dataset-file download
```shell
aidentified_match dataset-file download --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME --dataset-file-path DATASET_FILE_PATH
```
Download the enriched contact file after matching is finished. The required `DATASET_FILE_PATH` is where the downloaded
file will be saved, creating the file if it does not exist and overwriting any file that already exists.

Note that whole-file contact matching is only run once, when the dataset-file is initially uploaded, and will not change
as time goes by. For up-to-date contact attributes you must download one the nightly delta files.

### dataset-file delete
```shell
aidentified_match dataset-file delete --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
```
Delete a given dataset-file. This will recursively delete any matched delta files.

### dataset-file delta list
```shell
aidentified_match dataset-file delta list --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
```
List any nightly delta files that exist for the given dataset-file.

### dataset-file delta download
```shell
aidentified_match dataset-file delta download --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
                                              --dataset-file-path DATASET_FILE_PATH --file-date FILE_DATE
```
Download a nightly delta file for dataset-file `DATASET_FILE_NAME` and date `FILE_DATE` to the `DATASET_FILE_PATH` location,
creating a new file if one does not exist and truncating any existing files. Delta files are CSV files.

### dataset-file trigger list
```shell
aidentified_match dataset-file trigger list --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
```
List any nightly trigger files that exist for the given dataset-file.

### dataset-file trigger download
```shell
aidentified_match dataset-file trigger download --dataset-name DATASET_NAME --dataset-file-name DATASET_FILE_NAME
                                              --dataset-file-path DATASET_FILE_PATH --file-date FILE_DATE
```
Download a nightly trigger file for dataset-file `DATASET_FILE_NAME` and date `FILE_DATE` to the `DATASET_FILE_PATH`
location, creating a new file if one does not exist and truncating any existing files. Trigger files are CSV files.
