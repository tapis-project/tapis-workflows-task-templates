#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME
from utils.etl import (
    ManifestModel,
    lock_manifests,
    unlock_manifests,
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
    remote_outbox = ctx.get_input("REMOTE_OUTBOX")
    local_inbox = ctx.get_input("LOCAL_INBOX")
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock_manifests(client, local_inbox)
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

# Register the lockfile cleanup hook to be called on called to stderr and
# stdout. This will unlock the manifests lock when the program exits with any
# code
add_hook_props = (
    unlock_manifests,
    client,
    local_inbox.get("system_id"),
    local_inbox.get("manifests_path")
)
ctx.add_hook(1, *add_hook_props)
ctx.add_hook(0, *add_hook_props)

# Load all manfiest files from the local inbox
try:
    local_manifest_files = client.files.listFiles(
        system_id=local_inbox.get("system_id"),
        path=local_inbox.get("manifests_path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

root_manifest_file = next(
    filter(lambda file: file.name == ROOT_MANIFEST_FILENAME), 
    None
)

if root_manifest_file == None:
    ctx.stderr(1, f"Critical Error: Missing the root manifests file")

# Intialize the root manifest model
try:
    root_manifest = ManifestModel(
        filename=root_manifest_file,
        path=root_manifest_file.path,
        **json.loads(
            get_tapis_file_contents_json(
                client,
                local_inbox.get("system_id"),
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
for untracked_remote_manifest_file in untracked_remote_manifest_files:
    # Create transfer task
    elements = []
    for data_file in untracked_remote_manifest_file.files:
        ctx.set_output("MANIFEST_FILE", f)
        # NOTE: remove this input as soon as insert operation is available for Globus-type systems
        local_inbox_system_id = ctx.get_input("LOCAL_INBOX_SYSTEM_ID")
        local_outbox_system_id = ctx.get_input("LOCAL_OUTBOX_SYSTEM_ID")
        url = f.get("url")
        destination_filename = url.rsplit("/", 1)[1]
        protocol = url.rsplit("://")[0]
        destination_uri = f"{protocol}://{remote_inbox_system_id}/{os.path.join(destination_path.strip('/'), destination_filename)}"
        elements.append({
            # FIXME .replace of system name in destinationURI should be deleted as soon as the insert
            # operation is available for Globus-type systems
            "sourceURI": f.get("url").replace(local_inbox_system_id, local_outbox_system_id), # NOTE See 'NOTE' above
            "destinationURI": destination_uri
        })

# NOTE IMPORTANT DO NOT REMOVE BELOW.
# Calling stdout calls clean up hooks that were regsitered in the
# beginning of the script
ctx.stdout("")

