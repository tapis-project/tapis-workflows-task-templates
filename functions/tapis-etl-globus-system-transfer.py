#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import os, json, time, re

from tapipy.tapis import Tapis

from utils import ETLManifestModel
from pprint import pprint


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
        from_system_id = re.search(r"^tapis:\/{2}([^/]+)\/[\s\S]*$", f.get("url")).group(1)
        elements.append({
            # FIXME perhaps it would be better to pass the system id in the manifest
            # TODO FIXME .replace of system name in destinationURI should be deleted as soon as the insert
            # operation is available for Globus-type systems
            "sourceURI": f.get("url").replace(from_system_id, remote_inbox_system_id),
            "destinationURI": os.path.join(
                f.get("url").rstrip("/", 2)[0].replace(from_system_id, remote_inbox_system_id),
                destination_path.lstrip("/"),
                f.get("name")
            )
        })
        
    task = client.files.createTransferTask(elements=elements)
except Exception as e:
    ctx.stderr(1, f"Failed to create transfer task: {e}")

# Poll the transfer task until it reaches a terminal state
try:
    while task.status not in ["COMPLETED", "FAILED"]:
        time.sleep(5)
        task = client.files.getTransferTask(
            transferTaskId=task.uuid
        )
except Exception as e:
    ctx.stderr(1, e)

# TODO handle fail/success