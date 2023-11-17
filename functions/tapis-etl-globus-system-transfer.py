#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import os, json, time, re

from tapipy.tapis import Tapis

from utils import ETLManifestModel


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
    manifest = ETLManifestModel(**json.loads(ctx.get_input("MANIFEST")))
except Exception as e:
    ctx.stderr(1, f"Error initializing manifest: {e}")

try:
    # Create transfer task
    destination_path = ctx.get_input("DESTINATION_PATH")
    remote_inbox_system_id = ctx.get_input("SYSTEM_ID")

    # Create transfer task
    elements = []
    for f in manifest.files:
        # FIXME perhaps it would be better to pass the system id in the manifest
        from_system_id = re.search(r"^tapis:\/{2}([^/]+)\/[\s\S]*$", f.get("url")).group(1)
        file_path = f.get("url").replace(from_system_id, remote_inbox_system_id).replace(f.get("url").replace(f.get("name"), "")),
        elements.append({
            # TODO FIXME Everything from .replace onward should be delete as soon as the insert
            # operation is available on Globus-type endpoints
            "sourceURI": f.get("url").replace(from_system_id, remote_inbox_system_id),
            "destinationURI": os.path.join(
                file_path,
                destination_path.lstrip("/"),
                f.get("name").lstrip("/")
            )
        })

    task = client.files.createTransferTask(elements=elements)
except Exception as e:
    ctx.stderr(1, f"Failed to create transfer task: {e}")

# Poll the transfer task until it reaches a terminal state
try:
    while task.status not in ["COMPLETED", "FAILED"]:
        time.sleep(5)
        task = client.files.createTransferTask(
            transferTaskId=task.uuid
        )
except Exception as e:
    ctx.stderr(1, e)

# TODO handle fail/success