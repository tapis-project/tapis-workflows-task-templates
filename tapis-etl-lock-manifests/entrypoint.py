#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import time, os

from tapipy.tapis import Tapis


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
    local_iobox_system_id = ctx.get_input("LOCAL_IOBOX_SYSTEM_ID")
    local_iobox_manifest_path = ctx.get_input("LOCAL_IOBOX_MANIFEST_PATH")
    client.files.mkdir(
        systemId=local_iobox_system_id,
        path=local_iobox_manifest_path
    )

    # Create the data directory if it doesn't exist. Equivalent
    # to `mkdir -p`
    local_iobox_data_path = ctx.get_input("LOCAL_IOBOX_DATA_PATH")
    client.files.mkdir(
        systemId=local_iobox_system_id,
        path=local_iobox_data_path
    )
except Exception as e:
    ctx.stderr(1, f"Failed to create directories: {e}")

try:
    # Wait for the Lockfile to disappear.
    total_wait_time = 0
    manfiests_locked = True
    start_time = time.time()
    max_wait_time = ctx.get_input("MAX_WAIT_TIME", 300)
    lockfile_filename = ctx.get_input("LOCKFILE_FILENAME")
    while manfiests_locked:
        # Check if the total wait time was exceeded. If so, throw exception
        if time.time() - start_time >= max_wait_time:
            raise Exception(f"Max Wait Time Reached: {max_wait_time}") 
    
        # Fetch the all manifest files
        files = client.files.listFiles(
            systemId=local_iobox_system_id,
            path=local_iobox_manifest_path
        )

        filenames = [file.name for file in files]
        if lockfile_filename in filenames:
            time.sleep(ctx.get_input("WAIT_INTERVAL", 5))

    # Create the lockfile
    client.files.insert(
        system_id=local_iobox_system_id,
        path=os.path.join(local_iobox_manifest_path, lockfile_filename),
        file=b""
    )
except Exception as e:
    ctx.stderr(1, f"Failed to generate lockfile: {str(e)}")

