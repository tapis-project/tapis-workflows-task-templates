#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import os, json, time
from tapipy.tapis import Tapis

from utils.etl import ManifestModel
from utils.tapis import get_client


try:
    # Instantiate a Tapis client
    client = get_client(
        ctx.get_input("TAPIS_BASE_URL"),
        username = ctx.get_input("TAPIS_USERNAME"),
        password = ctx.get_input("TAPIS_PASSWORD")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize Tapis client: {e}")

try:
    manifest = ManifestModel(**json.loads(ctx.get_input("MANIFEST")))
except Exception as e:
    ctx.stderr(1, f"Error initializing manifest: {e}")

try:
    # Log the transfer start
    manifest.log(f"Starting transfer of {len(manifest.files)} file(s)")

    # Create the destination dir on the remote inbox if it doesn't exist
    remote_inbox_system_id = ctx.get_input("REMOTE_INBOX_SYSTEM_ID")
    destination_path = ctx.get_input("DESTINATION_PATH")
    client.files.mkdir(
        systemId=remote_inbox_system_id,
        path=destination_path
    )
    # Create transfer task
    elements = []
    for f in manifest.files:
        ctx.set_output("MANIFEST_FILE", f)
        # NOTE: remove this input as soon as insert operation is available for Globus-type systems
        local_inbox_system_id = ctx.get_input("LOCAL_INBOX_SYSTEM_ID")
        local_outbox_system_id = ctx.get_input("LOCAL_OUTBOX_SYSTEM_ID")
        url = f.get("url")
        destination_filename = url.rsplit("/", 1)[1]
        protocol = url.rsplit("://")[0]
        destination_uri = f"{protocol}://{remote_inbox_system_id}/{os.path.join(destination_path.strip('/'), destination_filename)}"
        elements.append({
            # FIXME .replace of system name in destinationURI should be deleted as soon as the insert
            # operation is available for Globus-type systems
            "sourceURI": f.get("url").replace(local_inbox_system_id, local_outbox_system_id), # NOTE See 'NOTE' above
            "destinationURI": destination_uri
        })

    ctx.set_output("ELEMENTS", elements)

    task = client.files.createTransferTask(elements=elements)
except Exception as e:
    ctx.stdout(f"Elements: {elements} \nFailed to create transfer task: {e}")

print(f"Status: {task.status}")

# Poll the transfer task until it reaches a terminal state
try:
    while task.status not in ["COMPLETED", "FAILED", "FAILED_OPT", "CANCELLED"]:
        print(f"Status: {task.status}")
        time.sleep(5)
        task = client.files.getTransferTask(
            transferTaskId=task.uuid
        )

    # Add transfer data to the manifest's metadata
    transfers = []
    for parent_task in task.parent_tasks:
        transfers.append({
            "id": parent_task.id,
            "sourceURI": parent_task.sourceURI,
            "destinationURI": parent_task.destinationURI,
            "errorMessage": parent_task.errorMessage,
            "uuid": parent_task.uuid
        })

        
    manifest.add_metadata({"transfers": transfers})
    ctx.set_output("TRANSFER_TASK", task.__dict__)
    ctx.set_output("STATUS", "COMPLETED")
    print(f"Status: {task.status}")

    if task.status != "COMPLETED":
        # Default the transfer success flag to false.
        ctx.set_output("STATUS", "FAILED")
        ctx.stdout(f"Transfer task failed to complete. Status '{task.status}' | Error message for transfer task: {task.errorMessage}")
except Exception as e:
    ctx.stdout(str(e))

ctx.set_output("STATUS", "COMPLETED")