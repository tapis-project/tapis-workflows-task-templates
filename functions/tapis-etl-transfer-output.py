#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import os, json, time
from tapipy.tapis import Tapis

from utils.etl import (
    ManifestsLock,
    ManifestModel,
    get_tapis_file_contents_json,
    EnumManifestStatus
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
    egress_system = json.loads(ctx.get_input("EGRESS_SYSTEM"))
    ingress_system = json.loads(ctx.get_input("INGRESS_SYSTEM"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, egress_system)
    lock.acquire()

    # Register the lock release hook to be called on called to stderr and
    # stdout. This will unlock the manifests lock when the program exits with any
    # code
    ctx.add_hook(1, lock.release)
    ctx.add_hook(0, lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# Load all manfiest files from the manifests directory of the ingress
# system
try:
    egress_manifest_files = client.files.listFiles(
        systemId=egress_system.get("manifests").get("system_id"),
        path=egress_system.get("manifests").get("path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Load manifests
try:
    egress_manifests = []
    for egress_manifest_file in egress_manifest_files:
        manifest = ManifestModel(
            filename=egress_manifest_file.name,
            path=egress_manifest_file.path,
            **json.loads(
                get_tapis_file_contents_json(
                    client,
                    egress_system.get("manifests").get("system_id"),
                    egress_manifest_file.path
                )
            )
        )
        if manifest.status == EnumManifestStatus.Pending:
            egress_manifests.apend(manifest)
except Exception as e:
    ctx.stderr(1, f"Failed to initialize manifests: {e}")

try:
    # Log the transfer start
    manifest.log(f"Starting transfer of {len(manifest.local_files)} file(s)")

    # Create the destination dir on the remote inbox if it doesn't exist
    remote_inbox_system_id = ctx.get_input("REMOTE_INBOX_SYSTEM_ID")
    destination_path = ctx.get_input("DESTINATION_PATH")
    client.files.mkdir(
        systemId=remote_inbox_system_id,
        path=destination_path
    )
    # Create transfer task
    elements = []
    for f in manifest.local_files:
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

    task = client.files.createTransferTask(elements=elements)
except Exception as e:
    ctx.stdout(f"Elements: {elements} \nFailed to create transfer task: {e}")

# Poll the transfer task until it reaches a terminal state
try:
    while task.status not in ["COMPLETED", "FAILED", "FAILED_OPT", "CANCELLED"]:
        time.sleep(5)
        task = client.files.getTransferTask(
            transferTaskId=task.uuid
        )

    # # Add transfer data to the manifest's metadata
    # transfers = []
    # for parent_task in task.parent_tasks:
    #     transfers.append({
    #         "id": parent_task.id,
    #         "sourceURI": parent_task.sourceURI,
    #         "destinationURI": parent_task.destinationURI,
    #         "errorMessage": parent_task.errorMessage,
    #         "uuid": parent_task.uuid
    #     })

        
    # manifest.add_metadata({"transfers": transfers})

    if task.status != "COMPLETED":
        # Default the transfer success flag to false.
        ctx.set_output("STATUS", "FAILED")
        ctx.stdout(f"Transfer task failed to complete. Status '{task.status}' | Error message for transfer task: {task.errorMessage}")
except Exception as e:
    ctx.stdout(str(e))

ctx.set_output("STATUS", "COMPLETED")