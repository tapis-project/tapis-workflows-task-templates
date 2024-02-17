#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os, time

from tapipy.tapis import Tapis

from utils.etl import (
    ManifestModel,
    EnumManifestStatus,
    EnumPhase,
    get_tapis_file_contents_json,
    create_lockfile,
    delete_lockfile,
    await_lockfile_fetch_manifest_files,
    generate_new_manifests,
    DataIntegrityValidator,
    DataIntegrityProfile
)

from utils.tapis import get_client


# Set the variables related to resubmission.
phase = ctx.get_input("PHASE")
resubmit_manifest_name = None
resubmit_inbound_manifest_name = ctx.get_input("RESUBMIT_INBOUND")
resubmit_outbound_manifest_name = ctx.get_input("RESUBMIT_OUTBOUND")
if phase == EnumPhase.Inbound and resubmit_inbound_manifest_name != None:
    resubmit_manifest_name = resubmit_inbound_manifest_name

if phase == EnumPhase.Outbound and resubmit_outbound_manifest_name != None:
    resubmit_manifest_name = resubmit_outbound_manifest_name

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

try:
    # Create the manifests directory if it doesn't exist. Equivalent
    # to `mkdir -p`
    system_id = ctx.get_input("SYSTEM_ID")
    manifests_path = ctx.get_input("MANIFESTS_PATH")
    client.files.mkdir(
        systemId=system_id,
        path=manifests_path
    )

    # Create the data directory if it doesn't exist. Equivalent
    # to `mkdir -p`
    data_path = ctx.get_input("DATA_PATH")
    client.files.mkdir(
        systemId=system_id,
        path=data_path
    )
except Exception as e:
    ctx.stderr(1, f"Failed to create directories: {e}")

try:
    # Wait for the Lockfile to disappear.
    lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
    manifest_files = await_lockfile_fetch_manifest_files(
        client,
        system_id,
        manifests_path,
        lockfile_filename
    )

    # Create the lockfile
    create_lockfile(client, system_id, manifests_path, lockfile_filename)
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

# Register the lockfile cleanup hook to be called on called to stderr and stdout
add_hook_props = (
    delete_lockfile,
    client,
    system_id,
    manifests_path,
    lockfile_filename
)
ctx.add_hook(1, *add_hook_props)
ctx.add_hook(0, *add_hook_props)

# Fetch existing manifests and create new manifests
try:
    # Get all of the contents of each manifest file
    manifests = []
    for manifest_file in manifest_files:
        manifests.append(
            ManifestModel(
                filename=manifest_file.name,
                path=manifest_file.path,
                **json.loads(get_tapis_file_contents_json(client, system_id, manifest_file.path))
            )
        )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

new_manifests = []
manifest_generation_policy = ctx.get_input("MANIFEST_GENERATION_POLICY")
if manifest_generation_policy != "manual":
    try:
        new_manifests = generate_new_manifests(
            system_id=system_id,
            data_path=data_path,
            include_pattern=ctx.get_input("INCLUDE_PATTERN"),
            exclude_pattern=ctx.get_input("EXCLUDE_PATTERN"),
            manifests_path=manifests_path,
            manifest_generation_policy=manifest_generation_policy,
            manifests=manifests,
            client=client
        )
    except Exception as e:
        ctx.stderr(1, f"Error generating manifests: {e}")

# Make a list of all manifests
all_manifests = manifests + new_manifests

# Collect all of the new and existing manifests with a status
# of 'pending' into a single list
unprocessed_manifests = [
    manifest for manifest in all_manifests
    if (
        manifest.status in [
            EnumManifestStatus.Pending,
            EnumManifestStatus.IntegrityCheckFailed
        ]
        or manifest.filename == resubmit_manifest_name
    )
]

# No manifests to process. Exit successfully
if len(unprocessed_manifests) == 0 and resubmit_manifest_name == None:
    ctx.set_output("ACTIVE_MANIFEST", json.dumps(None))
    ctx.stdout("")

# Reorder the unprocessed manifests from oldest to newest
unprocessed_manifests.sort(key=lambda m: m.created_at, reverse=True)

# Change the next manifest to the manifest associated with the resubmission
if resubmit_manifest_name != None: # Is resubmission
    next_manifest = next(filter(lambda m: m.filename == resubmit_manifest_name + ".json", all_manifests), None)
    next_manifest.log("Resubmitting")
    next_manifest.save(system_id, client)
    if next_manifest == None:
        ctx.stderr(1, f"Resubmit failed: Manifest {resubmit_manifest_name + '.json'} does not exist")
else: # Not resubmission
    # Default to oldest manifest
    next_manifest = unprocessed_manifests[0]
    manifest_priority = ctx.get_input("MANIFEST_PRIORITY")
    if manifest_priority in ["newest", "any"]:
        next_manifest = unprocessed_manifests[-1]

# Update the status of the next manifest to 'active'
try:
    next_manifest.set_status(EnumManifestStatus.Active)
    next_manifest.save(system_id, client)
except Exception as e:
    ctx.stderr(1, f"Failed to update manifest to 'active': {e}")

# This step ensures that the file(s) in the manifest are ready for the current
# operation (data processing or transfer) to be performed against them.
data_integrity_type = ctx.get_input("DATA_INTEGRITY_TYPE")
data_integrity_profile = None
try: 
    if data_integrity_type != None:
        data_integrity_profile = DataIntegrityProfile(
            data_integrity_type,
            done_files_path=ctx.get_input("DATA_INTEGRITY_DONE_FILES_PATH"),
            include_pattern=ctx.get_input(
                "DATA_INTEGRITY_DONE_FILE_INCLUDE_PATTERN"
            ),
            exclude_pattern=ctx.get_input(
                "DATA_INTEGRITY_DONE_FILE_EXCLUDE_PATTERN"
            )
        )
except Exception as e:
    ctx.stderr(1, str(e))

# Check the integrity of each data file in the manifests based on the data
# integrity profile
validated = False
if data_integrity_profile != None:
    data_integrity_validator = DataIntegrityValidator(client)
    (validated, err) = data_integrity_validator.validate(
        next_manifest,
        system_id,
        data_integrity_profile
    )

# Fail the pipeline if the data integrity check failed
if data_integrity_profile != None and not validated:
    next_manifest.log(f"Data integrity checks failed | {err}")
    next_manifest.set_status(EnumManifestStatus.IntegrityCheckFailed)
    next_manifest.save(system_id, client)
    ctx.set_output("ACTIVE_MANIFEST", json.dumps(None))
    ctx.stdout("")

# Create an output to be used by the first job in the etl pipeline
if len(next_manifest.files) > 0 and phase == EnumPhase.Inbound:
    tapis_system_file_ref_extension = ctx.get_input("TAPIS_SYSTEM_FILE_REF_EXTENSION")
    for i, file in enumerate(next_manifest.files):
        # Set the file_input_arrays to output
        ctx.set_output(f"{i}-etl-data-file-ref.{tapis_system_file_ref_extension}", json.dumps({"file": file}))

# Output the json of the current manifest
ctx.set_output("ACTIVE_MANIFEST", json.dumps(vars(next_manifest)))
# NOTE IMPORTANT DO NOT REMOVE BELOW.
# Calling stdout calls clean up hooks that were regsitered in the
# beginning of the script
ctx.stdout("")

