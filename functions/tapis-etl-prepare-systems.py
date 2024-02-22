#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME
from utils.etl import (
    ManifestModel,
    lock_manifests,
    unlock_manifests
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
    local_inbox = ctx.get_input("LOCAL_INBOX")
    local_outbox = ctx.get_input("LOCAL_OUTBOX")
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

# Create the directories on the local inbox and local outbox systems
try:
    for system in [local_inbox, local_outbox]:
        # Create the data directory if it doesn't exist. Equivalent
        # to `mkdir -p`
        client.files.mkdir(
            systemId=system.get("system_id"),
            path=system.get("data_path")
        )

        # Create the manifests directory if it doesn't exist. Equivalent
        # to `mkdir -p`
        client.files.mkdir(
            systemId=system.get("system_id"),
            path=system.get("manifests_path")
        )
except Exception as e:
    ctx.stderr(1, f"Failed to create directories: {e}")

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

# Create the root manifest if it does not exist. The root manifest is used
# to track which 
try:
    manifest_files = client.files.listFiles(
        system_id=local_inbox.get("system_id"),
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
                local_inbox.manifests_path,
                ROOT_MANIFEST_FILENAME
            ),
            files=[]
        )

        manifest.save(local_inbox.get("system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# NOTE IMPORTANT DO NOT REMOVE BELOW.
# Calling stdout calls clean up hooks that were regsitered in the
# beginning of the script
ctx.stdout("")

