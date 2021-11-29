### Ordinary use
Exporting the `AID_EMAIL` and `AID_PASSWORD` environment vars will shortcut you from having to pass both as arguments to everything.

### CLI development
To reduce the number of login (token request) API calls you can obtain a token via `export AID_TOKEN=$(aidentified_match auth)`

1. Create a virtualenv like normal
2. Load up our requirements: `pip install -r requirements.txt`
3. Install the CLI in editable mode: `pip install -e .`
4. You can now call `aidentified_match` on the command line from within your virtualenv and it'll run the code under `aidentified_matching_api/`.
