#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os, time

from tapipy.tapis import Tapis

from utils.etl import ManifestModel, EnumManifestStatus

def cleanup():
    # Delete the lock file
    try:
        client.files.delete(
            systemId=system_id,
            path=os.path.join(manifest_path, lockfile_filename),
            file=b""
        )
    except Exception as e:
        ctx.stderr(1, f"Failed to delete lockfile: {e}")

#TODO add rollbacks on execptions; i.e. delete the LOCKFILE
tapis_base_url = ctx.get_input("TAPIS_BASE_URL")
tapis_username = ctx.get_input("TAPIS_USERNAME")
tapis_password = ctx.get_input("TAPIS_PASSWORD")
try:
    # Instantiate a Tapis client
    client = Tapis(
        base_url=tapis_base_url,
        username=tapis_username,
        password=tapis_password,
    )
    client.get_tokens()
except Exception as e:
    ctx.stderr(1, f"Failed to initialize Tapis client: {e}")

try:
    # Wait for the Lockfile to disappear.
    total_wait_time = 0
    manifests_locked = True
    start_time = time.time()
    max_wait_time = 300
    lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
    system_id = ctx.get_input("SYSTEM_ID")
    manifest_path = ctx.get_input("MANIFEST_PATH")
    while manifests_locked:
        # Check if the total wait time was exceeded. If so, throw exception
        if time.time() - start_time >= max_wait_time:
            raise Exception(f"Max Wait Time Reached: {max_wait_time}") 
    
        # Fetch the all manifest files
        manifest_files = client.files.listFiles(
            systemId=system_id,
            path=manifest_path
        )

        manifests_locked = lockfile_filename in [file.name for file in manifest_files]
            
        time.sleep(5)

    # Create the lockfile
    client.files.insert(
        systemId=system_id,
        path=os.path.join(manifest_path, lockfile_filename),
        file=b""
    )
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

try:
    # Determine what the manifest status should be based on the previous task
    new_manifest_status = EnumManifestStatus.Failed
    if json.loads(ctx.get_input("PHASE_COMPLETED")):
        new_manifest_status = EnumManifestStatus.Completed
    
    # Load the manifest and update it with the current status
    manifest = ManifestModel(**json.loads(ctx.get_input("MANIFEST")))
    manifest.status = new_manifest_status
    manifest.update(system_id, client)
except Exception as e:
    cleanup()
    ctx.stderr(1, f"Failed to update last active manifest: {e}")

cleanup()