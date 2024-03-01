"""Transfers data files from the Remote Outbox to the Local Inbox"""

#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from utils.etl import (
    ManifestModel,
    ManifestsLock,
    EnumManifestStatus,
    poll_transfer_task,
    get_tapis_file_contents_json,
    validate_manifest_data_files,
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
    egress_system = json.loads(ctx.get_input("EGRESS_SYSTEM"))
    ingress_system = json.loads(ctx.get_input("INGRESS_SYSTEM"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, ingress_system)
    lock.acquire()

    # Register the lock release hook to be called on called to stderr and
    # stdout. This will unlock the manifests lock when the program exits with any
    # code
    ctx.add_hook(1, lock.release)
    ctx.add_hook(0, lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")


# Load all manfiest files that into the ingress directory of the ingress
# system
try:
    ingress_manifest_files = client.files.listFiles(
        systemId=ingress_system.get("manifests").get("system_id"),
        path=ingress_system.get("manifests").get("path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Load the ingress manifests
try:
    ingress_manifests = []
    for ingress_manifest_file in ingress_manifest_files:
        ingress_manifests.append(
            ManifestModel(
                filename=ingress_manifest_file.name,
                path=ingress_manifest_file.path,
                **json.loads(
                    get_tapis_file_contents_json(
                        client,
                        ingress_system.get("manifests").get("system_id"),
                        ingress_manifest_file.path
                    )
                )
            )
        )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize manifests: {e}")

# Transfer all files in each manifest to the data directory of the ingress
# system
for ingress_manifest in ingress_manifests:
    # Check to see if the ingress system passes data integrity checks
    try:
        validated, err = validate_manifest_data_files(
            ingress_system,
            ingress_manifest,
            client
        )
    except Exception as e:
        ctx.stderr(1, f"Error validating manifest: {e}")

    try:
        # Log the failed data integrity check in the manifest
        if not validated:
            ingress_manifest.log(f"Data integrity checks failed | {err}")
            ingress_manifest.set_status(EnumManifestStatus.IntegrityCheckFailed)
            ingress_manifest.save(ingress_system.get("data").get("system_id"), client)
            continue
        
        ingress_manifest.log(f"Data integrity checks successful")
        ingress_manifest.save(ingress_system.get("data").get("system_id"), client)
    except Exception as e:
        ctx.stderr(1, f"Error updating manifest: {e}")

    elements = []
    for data_file in ingress_manifests.files:
        # Build the transfer elements
        url = data_file.get("url")
        destination_system_id = ingress_system.get("data").get("system_id")
        destination_path = ingress_system.get("data").get("path")
        destination_filename = url.rsplit("/", 1)[1]
        destination_uri = f"tapis://{destination_system_id}/{os.path.join(destination_path.strip('/'), destination_filename)}"
        elements.append({
            "sourceURI": data_file.get("url"),
            "destinationURI": destination_uri
        })

    # Transfer elements
    try:
        ingress_manifest.log(f"Starting transfer of {len(elements)} data files from the remote outbox to the local inbox")
        
        # Start the transfer task and poll until terminal state
        task = client.files.createTransferTask(elements=elements)
        task = poll_transfer_task(task)
    except Exception as e:
        ctx.stderr(1, f"Error transferring files: {e}")
    
    # Transfer task in terminal state, output transfer task data
    ctx.set_output("TRANSFER_TASK", task.__dict__)

    try:
        if task.status != "COMPLETED":
            task_err = f"Transfer task failed | Task UUID: {task.uuid} | Status: '{task.status}' | Error: {task.errorMessage}"
            ingress_manifest.set_status(EnumManifestStatus.Failed)
            ingress_manifest.log(task_err)
            ingress_manifest.save(ingress_system.get("manifests").get("system_id"), client)
            ctx.stderr(1, task_err)

        ingress_manifest.set_status(EnumManifestStatus.Completed)
        ingress_manifest.log(f"Transfer task completed | Task UUID: {task.uuid}")
        ingress_manifest.save(ingress_system.get("manifests").get("system_id"), client)
    except Exception as e:
        ctx.stderr(1, f"Error updating manifests after transfer: {e}")

cleanup()

