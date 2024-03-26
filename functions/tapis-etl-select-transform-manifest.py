#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json

from utils.etl import (
    ManifestModel,
    ManifestsLock,
    EnumManifestStatus,
    EnumPhase,
    get_tapis_file_contents_json,
    cleanup,
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
    local_inbox = json.loads(ctx.get_input("LOCAL_INBOX"))
    manifests_system_id = local_inbox.get("manifests").get("system_id")
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")
except Exception as e:
    ctx.stderr(1, f"Server Error: {e}")

try:
    # Lock the manifests directories to prevent other concurrent pipeline runs
    # from mutating manifest files
    local_inbox_lock = ManifestsLock(client, local_inbox)
    local_inbox_lock.acquire()

    ctx.add_hook(1, local_inbox_lock.release)
    ctx.add_hook(0, local_inbox_lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")


# Load all manfiest files from the manifests directory of the local inbox
try:
    manifest_files = client.files.listFiles(
        systemId=manifests_system_id,
        path=local_inbox.get("manifests").get("path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Fetch existing manifests and create new manifests
    # TODO Optimize
# Set resubmit manifest filename
resubmit_manifest_name = ctx.get_input("RESUBMIT_TRANSFORM")
manifest_to_resubmit = None
try:
    # Get all of the contents of each manifest file
    all_manifests = []
    for manifest_file in manifest_files:
        manifest = ManifestModel(
            filename=manifest_file.name,
            path=manifest_file.path,
            **json.loads(
                get_tapis_file_contents_json(
                    client,
                    manifests_system_id,
                    manifest_file.path
                )
            )
        )
        if manifest.name == resubmit_manifest_name:
            manifest_to_resubmit = manifest

        all_manifests.append(manifest)
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Check if invalid manifest was resubmitted
if resubmit_manifest_name != None and manifest_to_resubmit == None:
    ctx.stderr(1, f"Resubmit failed: Manifest '{resubmit_manifest_name}' does not exist")

# Collect all manifests with a status of 'pending' into a single list
unprocessed_manifests = [
    manifest for manifest in all_manifests
    if manifest.status == EnumManifestStatus.Pending
]

# No manifests to process. Exit successfully
if len(unprocessed_manifests) == 0 and manifest_to_resubmit == None:
    ctx.set_output("MANIFEST", json.dumps(None))
    ctx.stdout("")

# Set the next manifest to the manifest to be submitted
if manifest_to_resubmit != None: # Is resubmission
    next_manifest = manifest_to_resubmit
    next_manifest.log("Resubmitting")

if manifest_to_resubmit == None:
    # Sort unprocessed manifests from oldest to newest
    unprocessed_manifests.sort(key=lambda m: m.created_at, reverse=True)
    # Default to oldest manifest
    manifest_priority = local_inbox.get("manifests").get("priority")
    next_manifest = unprocessed_manifests[0 - int(manifest_priority in ["newest", "any"])]

# Update the status of the next manifest to 'active'
try:
    next_manifest.set_status(EnumManifestStatus.Active)
    next_manifest.save(manifests_system_id, client)
except Exception as e:
    ctx.stderr(1, f"Failed to update manifest to 'active': {e}")

# # Create an output to be used by the first job in the etl pipeline
# if len(next_manifest.files) > 0 and phase == EnumPhase.Ingress:
#     tapis_system_file_ref_extension = ctx.get_input("TAPIS_SYSTEM_FILE_REF_EXTENSION")
#     for i, file in enumerate(next_manifest.files):
#         # Set the file_input_arrays to output
#         ctx.set_output(f"{i}-etl-data-file-ref.{tapis_system_file_ref_extension}", json.dumps({"file": file}))

# Output the json of the next transform manifest
ctx.set_output("MANIFEST", json.dumps(vars(next_manifest)))

cleanup(ctx)

