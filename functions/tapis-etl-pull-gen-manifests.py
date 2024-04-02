"""Pulls the manifests from the Remote Outbox into the Local Inbox"""

#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME, LOCKFILE_FILENAME
from utils.etl import (
    ManifestModel,
    ManifestsLock,
    poll_transfer_task,
    get_tapis_file_contents_json,
    get_manifest_files,
    requires_manifest_generation,
    generate_manifests,
    cleanup,
    EnumPhase
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
    ctx.stderr(f"Failed to initalize Tapis client: {str(e)}")

# Deserialize system details
try:
    egress_system = json.loads(ctx.get_input("EGRESS_SYSTEM"))
    ingress_system = json.loads(ctx.get_input("INGRESS_SYSTEM"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")
except Exception as e:
    ctx.stderr(1, f"Server Error: {e}")

try:
    # Lock the manifests directories to prevent other concurrent pipeline runs
    # from mutating manifest files
    ingress_lock = ManifestsLock(client, ingress_system)
    ingress_lock.acquire()
    ctx.add_hook(1, ingress_lock.release)
    ctx.add_hook(0, ingress_lock.release)

    egress_lock = ManifestsLock(client, egress_system)
    egress_lock.acquire()
    ctx.add_hook(1, egress_lock.release)
    ctx.add_hook(0, egress_lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# Create manifests and transfers them to the egress system's manifest directory
# based on the manifest generation policy of the egress system (if specified)
try:
    # Do nothing if the remote system does not require manifests to be generated
    if requires_manifest_generation(egress_system):
        generate_manifests(egress_system, client, EnumPhase.Ingress)
except Exception as e:
    ctx.stderr(1, f"Error handling remote iobox manifest generation: {e}")

# Get the root manifest 
try:
    control_files = client.files.listFiles(
        systemId=ingress_system.get("control").get("system_id"),
        path=ingress_system.get("control").get("path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

root_manifest_file = next(
    filter(lambda file: file.name == ROOT_MANIFEST_FILENAME, control_files), 
    None
)

if root_manifest_file == None:
    ctx.stderr(1, f"Critical Error: Missing the root manifest file")

# Intialize the root manifest model
try:
    root_manifest = ManifestModel(
        filename=root_manifest_file.name,
        path=root_manifest_file.path,
        **json.loads(
            get_tapis_file_contents_json(
                client,
                ingress_system.get("control").get("system_id"),
                root_manifest_file.path
            )
        )
    )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize root manifest | {e}")

# Load all manfiest files from the remote outbox
try:
    egress_manifest_files = get_manifest_files(
        client=client,
        system_id=egress_system.get("manifests").get("system_id"),
        path=egress_system.get("manifests").get("path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Check which manifest files are in the root manifest's local files list. Add all
# manifest files that are missing to the untracked manifests list
tracked_manifest_filenames = [file.name for file in root_manifest.remote_files]
untracked_remote_manifest_files = []
for file in egress_manifest_files:
    # Prevent the from being tracked by including it with the list of tracked files
    if file.name not in tracked_manifest_filenames:
        untracked_remote_manifest_files.append(file)

# Transfer all untracked manifest files from the egress system to the
# ingress system
elements = []
for untracked_remote_manifest_file in untracked_remote_manifest_files:
    system_id = ingress_system.get("manifests").get("system_id")
    path = ingress_system.get("manifests").get("path").strip("/")
    filename = untracked_remote_manifest_file.name
    destination_uri = f"tapis://{os.path.join(system_id, path, filename)}"
    elements.append({
        "sourceURI": untracked_remote_manifest_file.url,
        "destinationURI": destination_uri
    })

if len(elements) == 0:
    ctx.set_output("TRANSFER_TASK", None)
    cleanup(ctx)

# Peform the file transfer
try:
    root_manifest.log(f"Starting transfer of {len(elements)} manifest(s) from the egress system to the ingress system")
    task = client.files.createTransferTask(elements=elements)
    task = poll_transfer_task(client, task)
    
    ctx.set_output("TRANSFER_TASK", task.__dict__)

    if task.status != "COMPLETED":
        task_err = f"Transfer task failed | Task UUID: {task.uuid} | Status '{task.status}' | Error message for transfer task: {task.errorMessage}"
        root_manifest.log(task_err)
        root_manifest.save(ingress_system.get("control").get("system_id"), client)
        ctx.stderr(1, task_err)

    root_manifest.remote_files.extend(untracked_remote_manifest_files)
    root_manifest.log(f"Transfer task completed | Task UUID: {task.uuid}")
    root_manifest.save(ingress_system.get("control").get("system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Error transferring files: {e}")

cleanup(ctx)

