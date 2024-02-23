#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from constants.etl import ROOT_MANIFEST_FILENAME
from utils.etl import (
    ManifestModel,
    PipelineLock,
    poll_transfer_task,
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
    egress_system = json.loads(ctx.get_input("EGRESS_SYSTEM"))
    ingress_system = json.loads(ctx.get_input("INGRESS_SYSTEM"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = PipelineLock(client, ingress_system)
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
    ingress_system_manifest_files = client.files.listFiles(
        systemId=ingress_system.get("writable_system_id"),
        path=ingress_system.get("manifests_path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

root_manifest_file = next(
    filter(
        lambda file: file.name == ROOT_MANIFEST_FILENAME,
        ingress_system_manifest_files
    ), 
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
                ingress_system.get("writable_system_id"),
                root_manifest_file.path
            )
        )
    )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize root manifest | {e}")

# Load all manfiest files from the remote outbox
try:
    remote_manifest_files = client.files.listFiles(
        systemId=egress_system.get("system_id"),
        path=egress_system.get("manifests_path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Check which manifest files are in the root manifests file's files list. Add all
# manifest files that are missing to the untracked manifests list
current_manifest_filenames = [file.name for file in root_manifest.files]
untracked_remote_manifest_files = []
for file in remote_manifest_files:
    if file.name not in current_manifest_filenames:
        untracked_remote_manifest_files.append(file)

# Transfer all untracked manifest files from the egress system to the
# ingress system
elements = []
for untracked_remote_manifest_file in untracked_remote_manifest_files:
    system_id = ingress_system.get("system_id")
    path = ingress_system.get('inbound_transfer_manifests_path').strip("/")
    filename = untracked_remote_manifest_file.name
    destination_uri = f"tapis://{os.path.join(system_id, path, filename)}"
    elements.append({
        "sourceURI": untracked_remote_manifest_file.url,
        "destinationURI": destination_uri
    })

# Peform the file transfer
try:
    root_manifest.log(f"Starting transfer of {len(elements)} manifest(s) from the local inbox to the ")
    task = client.files.createTransferTask(elements=elements)
    task = poll_transfer_task(task)
        
    if task.status != "COMPLETED":
        root_manifest.log(f"Transfer task(s) failed. {task}")
        ctx.stderr(1, f"Transfer task failed to complete. Status '{task.status}' | Error message for transfer task: {task.errorMessage}")

    root_manifest.log()
except Exception as e:
    ctx.stderr(1, f"{e}")

ctx.stderr("SHORT CIRCUIT") # TODO REMOVE

# NOTE IMPORTANT DO NOT REMOVE BELOW.
# Calling stdout calls clean up hooks that were regsitered in the
# beginning of the script
ctx.stdout("")

