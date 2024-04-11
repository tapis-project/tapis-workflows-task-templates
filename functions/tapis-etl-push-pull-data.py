"""Transfers data files from the Remote Outbox to the Local Inbox"""

#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import LOCKFILE_FILENAME
from utils.etl import (
    ManifestModel,
    ManifestsLock,
    EnumManifestStatus,
    EnumPhase,
    poll_transfer_task,
    get_tapis_file_contents_json,
    fetch_system_files,
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

# Set the phase-dependent variables
phase = ctx.get_input("PHASE")

# The that has the manifest files for this phase of the pipeline
manifests_system = ingress_system if phase == EnumPhase.Ingress else egress_system

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, manifests_system)
    lock.acquire()

    # Register the lock release hook to be called on called to stderr and
    # stdout. This will unlock the manifests lock when the program exits with any
    # code
    ctx.add_hook(1, lock.release)
    ctx.add_hook(0, lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# Load all manfiest files from the manifests directory of the manifests system
try:
    manifest_files = fetch_system_files(
        system_id=manifests_system.get("manifests").get("system_id"),
        path=manifests_system.get("manifests").get("path"),
        client=client,
        include_patterns=manifests_system.get("manifests").get("include_patterns"),
        exclude_patterns=[
            *manifests_system.get("manifests").get("exclude_patterns"),
            LOCKFILE_FILENAME # Ignore the lockfile.
        ]
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Load manifests that have the current phase
try:
    manifests = []
    for manifest_file in manifest_files:
        manifest = ManifestModel(
            filename=manifest_file.name,
            path=manifest_file.path,
            url=manifest_file.url,
            **json.loads(
                get_tapis_file_contents_json(
                    client,
                    manifests_system.get("manifests").get("system_id"),
                    manifest_file.path
                )
            )
        )

        if (
            manifest.phase == phase
            and manifest.status != EnumManifestStatus.Completed
        ):
            manifests.append(manifest)
except Exception as e:
    ctx.stderr(1, f"Failed to initialize manifests: {e}")

# Transfer all files in each manifest to the data directory of the ingress system
for manifest in manifests:
    # Which property contains the correct data files depends on the phase. For the
    # ingress phase it's remote_files and for egress it's local_files
    data_files = getattr(manifest, "local_files")
    if phase == EnumPhase.Ingress:
        data_files = getattr(manifest, "remote_files")

    # Check to see if the data files in the manifests pass data integrity checks
    try:
        validated, err = validate_manifest_data_files(
            egress_system,
            data_files,
            client
        )
    except Exception as e:
        ctx.stderr(1, f"Error validating manifest: {e}")

    try:
        # Log the failed data integrity check in the manifest
        if not validated:
            manifest.log(f"Data integrity checks failed | {err}")
            manifest.set_status(EnumManifestStatus.IntegrityCheckFailed)
            manifest.save(ingress_system.get("manifests").get("system_id"), client)
            continue
        
        manifest.log(f"Data integrity checks successful")
        manifest.save(ingress_system.get("manifests").get("system_id"), client)
    except Exception as e:
        ctx.stderr(1, f"Error updating manifest: {e}")

    elements = []
    for data_file in data_files:
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
        manifest.log(f"Starting transfer of {len(elements)} data files from the remote outbox to the local inbox")
        # Start the transfer task and poll until terminal state
        task = client.files.createTransferTask(elements=elements)
        task = poll_transfer_task(client, task)
    except Exception as e:
        ctx.stderr(1, f"Error transferring files: {e}")
    
    # Add the transfer data to the manfiest
    manifest.transfers.append(task.uuid)

    try:
        if task.status != "COMPLETED":
            task_err = f"Transfer task failed | Task UUID: {task.uuid} | Status: '{task.status}' | Error: {task.errorMessage}"
            manifest.set_status(EnumManifestStatus.Failed)
            manifest.log(task_err)
            manifest.save(manifests_system.get("manifests").get("system_id"), client)
            ctx.stderr(1, task_err)

        manifest.log(f"Transfer task completed | Task UUID: {task.uuid}")
        manifest.set_status(EnumManifestStatus.Completed)
        manifest.save(manifests_system.get("manifests").get("system_id"), client)
    except Exception as e:
        ctx.stderr(1, f"Error updating manifests after transfer: {e}")

try:
    if phase == EnumPhase.Ingress:
        # Modify the path and url of the files tracked in the manifest to replace
        # egress system path and system id with the ingress system data path and 
        # ingress system transform system id
        unconverted_manifests = [
            manifest for manifest in manifests
            if (
                manifest.phase == EnumPhase.Ingress
                and manifest.status == EnumManifestStatus.Completed
            )
        ]

        for unconverted_manifest in unconverted_manifests:
            modified_data_files = []
            for data_file in unconverted_manifest.remote_files:
                ingress_system_id = ingress_system.get("data").get("system_id")
                ingress_data_files_path = ingress_system.get("data").get("path")
                path = os.path.join(f"/{ingress_data_files_path.strip('/')}", data_file["name"])
                modified_data_files.append({
                    **data_file,
                    "url": f'tapis://{ingress_system_id}/{os.path.join(path, data_file["name"]).strip("/")}',
                    "path": path
                })
            
            unconverted_manifest.local_files = modified_data_files
            unconverted_manifest.set_phase(EnumPhase.Transform)
            unconverted_manifest.save(ingress_system.get("manifests").get("system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Error converting manifest: {e}")

cleanup(ctx)

