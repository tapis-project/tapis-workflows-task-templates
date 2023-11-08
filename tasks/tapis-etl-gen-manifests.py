#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os, time

from uuid import uuid4

from tapipy.tapis import Tapis

from utils import (
    ETLManifestModel,
    EnumManifestStatus,
    EnumETLPhase,
    get_tapis_file_contents_json
)

#TODO add rollbacks on execptions; i.e. delete the LOCKFILE
tapis_base_url = ctx.get_input("TAPIS_BASE_URL")
tapis_jwt = ctx.get_input("TAPIS_JWT")
try:
    # Instantiate a Tapis client
    client = Tapis(
        base_url=tapis_base_url,
        jwt=tapis_jwt
    )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize Tapis client: {e}")

try:
    # Create the manifests directory if it doesn't exist. Equivalent
    # to `mkdir -p`
    local_system_id = ctx.get_input("LOCAL_SYSTEM_ID")
    local_manifest_path = ctx.get_input("LOCAL_MANIFEST_PATH")
    client.files.mkdir(
        systemId=local_system_id,
        path=local_manifest_path
    )

    # Create the data directory if it doesn't exist. Equivalent
    # to `mkdir -p`
    local_data_path = ctx.get_input("LOCAL_DATA_PATH")
    client.files.mkdir(
        systemId=local_system_id,
        path=local_data_path
    )
except Exception as e:
    ctx.stderr(1, f"Failed to create directories: {e}")

try:
    # Wait for the Lockfile to disappear.
    total_wait_time = 0
    manifests_locked = True
    start_time = time.time()
    max_wait_time = 300
    lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
    while manifests_locked:
        # Check if the total wait time was exceeded. If so, throw exception
        if time.time() - start_time >= max_wait_time:
            raise Exception(f"Max Wait Time Reached: {max_wait_time}") 
    
        # Fetch the all manifest files
        manifest_files = client.files.listFiles(
            systemId=local_system_id,
            path=local_manifest_path
        )

        manifests_locked = lockfile_filename in [file.name for file in manifest_files]
            
        time.sleep(5)

    # Create the lockfile
    client.files.insert(
        systemId=local_system_id,
        path=os.path.join(local_manifest_path, lockfile_filename),
        file=b""
    )
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

# Fetch existing manifests and create new manifests
try:
    # Get all of the contents of each manifest file
    manifests = []
    for manifest_file in manifest_files:
        manifests.append(
            ETLManifestModel(
                filename=manifest_file.name,
                path=manifest_file.path,
                **json.loads(get_tapis_file_contents_json(client, local_system_id, manifest_file.path))
            )
        )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

try:
    # Fetch the all data files
    local_data_path = ctx.get_input("LOCAL_DATA_PATH")
    data_files = client.files.listFiles(
        systemId=local_system_id,
        path=local_data_path
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch data files: {str(e)}")

# Create a list of all registered files
registered_data_file_paths = []
for manifest in manifests:
    for manifest_data_file in manifest.files:
        registered_data_file_paths.append(manifest_data_file.path)

registered_data_files = [
    data_file for data_file in data_files
    if data_file.path in registered_data_file_paths
]

# Find all data files that have not yet been registered with a manifest
unregistered_data_files = [
    data_file for data_file in data_files
    if data_file.path not in registered_data_file_paths
]

# Check the manifest generation policy to determine whether all new
# data files should be added to a single manifest, or a manifest
# should be generated for each new data file
# TODO consider querying for the file(s) sizes 2 times in a row at some interval and if
# the size is different, keep polling until the last 2 sizes(for the same file(s)) are the same
new_manifests = []
local_manifest_generation_policy = ctx.get_input("MANIFEST_GENERATION_POLICY")
if local_manifest_generation_policy == "one_per_file":
    for unregistered_data_file in unregistered_data_files:
        manifest_filename = f"{str(uuid4())}.json"
        new_manifests.append(
            ETLManifestModel(
                filename=manifest_filename,
                path=os.path.join(local_manifest_path, manifest_filename),
                files=[unregistered_data_file]
            )
        )
elif local_manifest_generation_policy == "one_for_all":
    manifest_filename = f"{str(uuid4())}.json" 
    new_manifests.append(
        ETLManifestModel(
            filename=manifest_filename,
            path=os.path.join(local_manifest_path, manifest_filename),
            files=unregistered_data_files
        )
    )

try:
    # Persist all of the new manifests
    for new_manifest in new_manifests:
        new_manifest.create(local_system_id, client)
except Exception as e:
    ctx.stderr(1, f"Failed to create manifests: {e}")

# Make a list of all manifests
all_manifests = manifests + new_manifests

# Collect all of the new and existing manifests with a status
# of 'pending' into a single list
unprocessed_manifests = [
    manifest for manifest in all_manifests
    if manifest.status == EnumManifestStatus.Pending
]

if len(unprocessed_manifests) == 0:
    # TODO Somehow, we need to indicate that all subsequent tasks should be skipped.
    # A workable idea may be to produce some kind of SKIP_OUTPUT file
    # TODO implement -1 exist code. handle in WorkflowExecutor
    # Delete the lock file
    try:
        client.files.delete(
            systemId=local_system_id,
            path=os.path.join(local_manifest_path, lockfile_filename),
            file=b""
        )
    except Exception as e:
        ctx.stderr(1, f"Failed to delete lockfile: {e}")

    ctx.stdout("Exiting: No new data to process", code=-1)

# Reorder the unprocessed manifests from oldest to newest
unprocessed_manifests.sort(key=lambda m: m.created_at, reverse=True)

# Default to oldest manifest
next_manifest = unprocessed_manifests[0]
local_manifest_priority = ctx.get_input("MANIFEST_PRIORITY")
if local_manifest_priority in ["newest", "any"]:
    next_manifest = unprocessed_manifests[-1]

# Change the next manifest to the manifest associated with the resubmission
resubmit = ctx.get_input("RESUBMIT")
if resubmit != None:
    next_manifest = next(filter(lambda m: m.name == resubmit + ".json", all_manifests))

# Update the status of the next manifest to 'active'
try:
    next_manifest.status = EnumManifestStatus.Active
    next_manifest.update(local_system_id, client)
except Exception as e:
    ctx.set_stderr(1, f"Failed to update manifest to 'active': {e}")

# Create an output to be used by the first job in the etl pipeline
phase = ctx.get_input("PHASE")
if len(next_manifest.files) > 0 and phase == EnumETLPhase.DataProcessing:
    tapis_system_file_ref_extension = ctx.get_input("TAPIS_SYSTEM_FILE_REF_EXTENSION")
    for i, file in enumerate(next_manifest.files):
        # Set the file_input_arrays to output
        ctx.set_output(f"{i}-etl-data-file-ref.{tapis_system_file_ref_extension}", json.dumps({"file": file.__dict__}))

    # Delete the lock file
    try:
        client.files.delete(
            systemId=local_system_id,
            path=os.path.join(local_manifest_path, lockfile_filename),
            file=b""
        )
    except Exception as e:
        ctx.stderr(1, f"Failed to delete lockfile: {e}")

    ctx.set_output("CURRENT_MANIFEST", dict(next_manifest))

elif len(next_manifest.files) > 0 and phase == EnumETLPhase.Transfer:
    ctx.set_output(
        "TRANSFER_DATA",
        json.dumps({
            "path_to_manifest": next_manifest.path,
            "system_id": local_system_id
        })
    )
    ctx.set_output("CURRENT_MANIFEST", dict(next_manifest))

