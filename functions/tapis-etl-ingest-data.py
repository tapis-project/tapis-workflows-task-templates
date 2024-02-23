#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME
from utils.etl import (
    ManifestModel,
    PipelineLock,
    get_tapis_file_contents_json
)

from utils.tapis import get_client


# Instantiate a Tapis client
try:
    client = get_client(
        ctx.get_input("TAPIS_BASE_URL"),
        username=ctx.get_input("TAPIS_USERNAME"),
        password=ctx.get_input("TAPIS_PASSWORD"),
        jwt=ctx.get_input("TAPIS_JWT")
    )
except Exception as e:
    ctx.stderr(str(e))

# Deserialize system details
try:
    remote_outbox = json.loads(ctx.get_input("REMOTE_OUTBOX"))
    local_inbox = json.loads(ctx.get_input("LOCAL_INBOX"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = PipelineLock(client, local_inbox)
    lock.acquire()
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# Register the lockfile cleanup hook to be called on called to stderr and
# stdout. This will unlock the manifests lock when the program exits with any
# code
ctx.add_hook(1, lock.release)
ctx.add_hook(0, lock.release)

# Get the root manifest 
try:
    local_inbox_manifest_files = client.files.listFiles(
        system_id=local_inbox.get("writable_system_id"),
        path=local_inbox.get("manifests_path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

root_manifest_file = next(
    filter(
        lambda file: file.name == ROOT_MANIFEST_FILENAME,
        local_inbox_manifest_files
    ), 
    None
)

if root_manifest_file == None:
    ctx.stderr(1, f"Critical Error: Missing the root manifest file")

# Intialize the root manifest model
try:
    root_manifest = ManifestModel(
        filename=root_manifest_file,
        path=root_manifest_file.path,
        **json.loads(
            get_tapis_file_contents_json(
                client,
                local_inbox.get("writable_system_id"),
                root_manifest_file.path
            )
        )
    )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize root manifest | {e}")

# Load all manfiest files from the remote outbox
try:
    remote_manifest_files = client.files.listFiles(
        system_id=remote_outbox.get("system_id"),
        path=remote_outbox.get("manifests_path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Check which manifest files are in the root manifests files list. Add all
# manifest files that are missing
current_manifest_filenames = [file.name for file in root_manifest.files]
untracked_remote_manifest_files = []
for file in remote_manifest_files:
    if file.name not in current_manifest_filenames:
        untracked_remote_manifest_files.append(file)

# Transfer all untracked manifest files from the remote outbox to the
# local inbox
remote_manifest_transfer_elements = []
for untracked_remote_manifest_file in untracked_remote_manifest_files:
    # Transfer the untracked manifests to the local inbox
    # Create transfer task
    elements = []
    for data_file in untracked_remote_manifest_file.files:
        
        url = f.get("url")
        destination_filename = url.rsplit("/", 1)[1]
        protocol = url.rsplit("://")[0]
        destination_uri = f"{protocol}://{remote_inbox_system_id}/{os.path.join(destination_path.strip('/'), destination_filename)}"
        elements.append({
            # FIXME .replace of system name in destinationURI should be deleted as soon as the insert
            # operation is available for Globus-type systems
            "sourceURI": source_uri
            "destinationURI": destination_uri
        })

# NOTE IMPORTANT DO NOT REMOVE BELOW.
# Calling stdout calls clean up hooks that were regsitered in the
# beginning of the script
ctx.stdout("")

