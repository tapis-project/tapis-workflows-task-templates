#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME
from utils.etl import (
    ManifestModel,
    PipelineLock
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
    local_outbox = json.loads(ctx.get_input("LOCAL_OUTBOX"))
    remote_inbox = json.loads(ctx.get_input("REMOTE_INBOX"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

systems = [
    remote_outbox,
    local_inbox,
    local_outbox,
    remote_inbox
]

# Create the directories on the writable systems
try:
    for system in systems:
        writable_system_id = system.get("writable_system_id")
        data_transfer_system_id = system.get("data_transfer_system_id")
        system_id = writable_system_id if writable_system_id != None else data_transfer_system_id
        
        # Create the data directory if it doesn't exist. Equivalent
        # to `mkdir -p`
        client.files.mkdir(
            systemId=system_id,
            path=system.get("data_path")
        )

        # Create the manifests directory if it doesn't exist. Equivalent
        # to `mkdir -p`
        client.files.mkdir(
            systemId=system_id,
            path=system.get("manifests_path")
        )
except Exception as e:
    ctx.stderr(1, f"Failed to create directories: {e}")

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

# Create the root manifest if it does not exist. The root manifest is used
# to track which 
try:
    manifest_files = client.files.listFiles(
        systemId=local_inbox.get("writable_system_id"),
        path=local_inbox.get("manifests_path")
    )

    root_manifest_exist = any([
        file.name == ROOT_MANIFEST_FILENAME
        for file in manifest_files
    ])

    if not root_manifest_exist:
        manifest = ManifestModel(
            filename=ROOT_MANIFEST_FILENAME,
            path=os.path.join(
                local_inbox.get("manifests_path"),
                ROOT_MANIFEST_FILENAME
            ),
            files=[]
        )

        manifest.log("Created")
        manifest.create(local_inbox.get("writable_system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# NOTE IMPORTANT DO NOT REMOVE BELOW.
# Calling stdout calls clean up hooks that were regsitered in the
# beginning of the script
ctx.stdout("")

