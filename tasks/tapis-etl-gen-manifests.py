#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os, time

from uuid import uuid4

from tapipy.tapis import Tapis

from _utils import (
    ETLManifestModel,
    EnumManifestStatus,
    EnumETLPhase,
    get_tapis_file_contents_json
)


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
    manfiests_locked = True
    start_time = time.time()
    max_wait_time = 300
    lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
    while manfiests_locked:
        # Check if the total wait time was exceeded. If so, throw exception
        if time.time() - start_time >= max_wait_time:
            raise Exception(f"Max Wait Time Reached: {max_wait_time}") 
    
        # Fetch the all manifest files
        files = client.files.listFiles(
            systemId=local_system_id,
            path=local_manifest_path
        )

        filenames = [file.name for file in files]
        if lockfile_filename in filenames:
            time.sleep(5)

    # Create the lockfile
    client.files.insert(
        system_id=local_system_id,
        path=os.path.join(local_manifest_path, lockfile_filename),
        file=b""
    )
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

# Fetch existing manifests and create new manifests
try:
    # Fetch the all manifest files
    local_system_id = ctx.get_input("LOCAL_SYSTEM_ID")
    local_manifest_path = ctx.get_input("LOCAL_MANIFEST_PATH")
    manifest_files = client.files.listFiles(
        systemId=local_system_id,
        path=local_manifest_path
    )

    # Get all of the contents of each manifest file
    manifests = []
    for manifest_file in manifest_files:
        manifest = ETLManifestModel(
            filename=manifest_file.name,
            path=manifest_file.path,
            **json.loads(get_tapis_file_contents_json(manifest_file.path))
        )
        manifests.append(manifest)
except Exception as e:
    ctx.stderr(f"Failed to fetch manifest files: {e}")

try:
    # Fetch the all data files
    local_data_path = ctx.get_input("LOCAL_DATA_PATH")
    data_files = client.files.listFiles(
        systemId=local_system_id,
        path=local_data_path
    )
except Exception as e:
    ctx.stderr(1, f"Falied to fetch data files: {str(e)}")

# Create a list of all registered files
registered_data_files = []
for manifest in manifests:
    for registered_data_file in manifest.files:
        registered_data_files.append(registered_data_file.path)

# Find all data files that have not yet been registered with a manifest
unregistered_data_files = []
for data_file in data_files:
    if data_file.path not in registered_data_files:
        unregistered_data_files.append(data_file.path)

# Check the manifest generation policy to determine whether all new
# data files should be added to a single manifest, or a manifest
# should be generated for each new data file
new_manifests = []
# FIXME unique name for manifest if "one_per_file"
local_manifest_generation_policy = ctx.get_input("MANFIEST_GENERATION_POLICY")
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
    ctx.stderr(1, f"Falied to create manifests: {e}")

# Make a list of all manifests
all_manifests = manifests.extend(new_manifest)

# Collect all of the new and existing manifests with a status
# of 'pending' into a single list
unprocessed_manifests = [
    manifest for manifest in all_manifests
    if manifest.status == EnumManifestStatus.Pending
]

lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
if len(unprocessed_manifests) > 0:
    # TODO Somehow, we need to indicate that all subsequent tasks should be skipped.
    # A workable idea may be to produce some kind of SKIP_OUTPUT file
    # TODO implement -1 exist code. handle in WorkflowExecutor
    # Delete the lock file
    try:
        client.files.delete(
            system_id=local_system_id,
            path=os.path.join(local_manifest_path, lockfile_filename),
            file=b""
        )
    except Exception as e:
        ctx.stderr(1, f"Failed to delete lockfile: {e}")

    ctx.stdout("Exiting: No new data to process", exit_code=-1)

# Reorder the unprocessed manifests from oldest to newest
unprocessed_manifests.sort(key=lambda m: m.created_at, reverse=True)

# Default to oldest manifest
next_manifest = unprocessed_manifests[0]
local_manifest_priority = ctx.get_input("MANFIEST_PRIORITY")
if local_manifest_priority in ["newest", "any"]:
    next_manifest = unprocessed_manifests[-1]

# Change the next manfiest to the manfiest associated with the resubmission
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
        ctx.set_output(f"{i}-etl-data-file-ref.{tapis_system_file_ref_extension}", json.dumps({"file": file}))
    # Delete the lock file
    try:
        client.files.delete(
            system_id=local_system_id,
            path=os.path.join(local_manifest_path, lockfile_filename),
            file=b""
        )
    except Exception as e:
        ctx.stderr(1, f"Failed to delete lockfile: {e}")

    # End the function
    ctx.stdout(f"Processing files: {next_manifest.files}")
elif len(next_manifest.files) > 0 and phase == EnumETLPhase.Transfer:
        ctx.set_output(
            "TRANSFER_DATA",
            json.dumps({
                "path_to_manifest": next_manifest.path,
                "system_id": local_system_id
            })
        )

