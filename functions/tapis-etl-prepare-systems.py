"""Creates all directories necessary to run an ETL pipeline in all provided
systems"""

#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME
from utils.etl import (
    ManifestModel,
    ManifestsLock,
    cleanup
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

# Create the directories required of a Tapis ETL Pipeline
try:
    for system in systems:
        client.files.mkdir(
            systemId=system.get("data").get("system_id"),
            path=system.get("data").get("path")
        )

        client.files.mkdir(
            systemId=system.get("manifests").get("system_id"),
            path=system.get("manifests").get("path")
        )

        if system.get("control") == None: continue
        
        client.files.mkdir(
            systemId=system.get("control").get("system_id"),
            path=system.get("control").get("path")
        )
except Exception as e:
    ctx.stderr(1, f"Failed to create directories: {e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, local_inbox)
    lock.acquire()
    
    # Register the lockfile cleanup hook to be called on called to stderr and
    # stdout. This will unlock the manifests lock when the program exits with any
    # code
    ctx.add_hook(1, lock.release)
    ctx.add_hook(0, lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")


# Create the root manifest if it does not exist. The root manifest is used
# to track which manifests have been transferred from the Remote Outbox's manifests
# path
try:
    manifest_files = client.files.listFiles(
        systemId=local_inbox.get("control").get("system_id"),
        path=local_inbox.get("control").get("path")
    )

    root_manifest_exist = any([
        file.name == ROOT_MANIFEST_FILENAME
        for file in manifest_files
    ])

    if not root_manifest_exist:
        manifest = ManifestModel(
            filename=ROOT_MANIFEST_FILENAME,
            path=os.path.join(
                local_inbox.get("control").get("path"),
                ROOT_MANIFEST_FILENAME
            ),
            files=[]
        )

        manifest.create(local_inbox.get("control").get("system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Failed to create root manifest file in the local inbox: {e}")

cleanup(ctx)

