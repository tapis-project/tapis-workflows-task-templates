#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os, time

from tapipy.tapis import Tapis

from utils.etl import (
    ManifestModel,
    EnumManifestStatus,
    create_lockfile,
    await_lockfile_fetch_manifest_files,
    delete_lockfile
)
from utils.tapis import get_client


try:
    # Instantiate a Tapis client
    client = get_client(
        ctx.get_input("TAPIS_BASE_URL"),
        username=ctx.get_input("TAPIS_USERNAME"),
        password=ctx.get_input("TAPIS_PASSWORD"),
        jwt=ctx.get_input("TAPIS_JWT")
    )
except Exception as e:
    ctx.stderr(str(e))

lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
system_id = ctx.get_input("SYSTEM_ID")
manifests_path = ctx.get_input("MANIFESTS_PATH")
try:
    # Wait for the Lockfile to disappear.
    lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
    await_lockfile_fetch_manifest_files(
        client,
        system_id,
        manifests_path,
        lockfile_filename
    )

    # Create the lockfile
    create_lockfile(client, system_id, manifests_path, lockfile_filename)
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

try:
    # Determine what the manifest status should be based on the previous task
    new_manifest_status = EnumManifestStatus.Failed
    if json.loads(ctx.get_input("PHASE_COMPLETED")):
        new_manifest_status = EnumManifestStatus.Completed
    
    # Load the manifest and update it with the current status
    manifest = ManifestModel(**json.loads(ctx.get_input("MANIFEST")))
    manifest.set_status(new_manifest_status)
    manifest.save(system_id, client)
except Exception as e:
    delete_lockfile(system_id, manifests_path, lockfile_filename)
    ctx.stderr(1, f"Failed to update last active manifest: {e}")

delete_lockfile(system_id, manifests_path, lockfile_filename)