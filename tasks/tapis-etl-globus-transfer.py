#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import os, json, time

import requests

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

    system = client.systems.getSystem(systemId=manifest.system_id)

    files_to_transfer = []
    transfer_items_include_dirs = False
    for file in manifest.files:
        path = file.get("path").replace(f"tapis://{system}/", "")
        files_to_transfer.append(os.path.join(system.rootDir, path))
        
        # Set the transfer_items_include_dirs if a file in the manifest is a dir.
        # This will be used to tell the globus proxy api to recurse through dirs
        # and transfer the files and dirs therein
        if transfer_items_include_dirs == False and file.get("type") == "dir":
            transfer_items_include_dirs = True 

except Exception as e:
    ctx.stderr(1, f"Error fetching contents of manifest file '{manifest.path}': {e}")

try:
    # Create transfer task
    globus_proxy_base_url = os.path.join(tapis_base_url, "v3/globus-proxy/")
    destination_endpoint_id = ctx.get_input("DESTINATION_ENDPOINT_ID")
    destination_path = ctx.get_input("DESTINATION_PATH")
    source_endpoint_id = ctx.get_input("SOURCE_ENDPOINT_ID")
    globus_client_id = ctx.get_input("GLOBUS_CLIENT_ID")
    globus_access_token = ctx.get_input("GLOBUS_ACCESS_TOKEN")
    globus_refresh_token = ctx.get_input("GLOBUS_REFRESH_TOKEN")
    response = requests.post(
        url=os.path.join(globus_proxy_base_url, "transfers", globus_client_id),
        data={
            "source_endpoint": source_endpoint_id,
            "destination_endpoint": destination_endpoint_id,
            "transfer_items": [
                {
                    "source_path": path,
                    "destination_path": destination_path,
                    "recursive": transfer_items_include_dirs
                } for path in files_to_transfer
            ]
        }
    )
except Exception as e:
    ctx.stderr(f"Failed to create transfer task: {e}")

try:
    globus_transfer_task = response.result 
    max_retries = ctx.get_input("MAX_RETRIES", default=5)
    
    for i in range(max_retries): 
        globus_transfer_status = None
        num_retries = num_retries + 1
        response = requests.get(
            url = os.path.join(globus_proxy_base_url, 'transfers', globus_client_id, globus_transfer_task.task_id)
        )
        globus_transfer_status = response.status
        if globus_transfer_status == "SUCCEEDED":
            ctx.stdout(0, "Globus transfer completed successfully")
        elif globus_transfer_status == "FAILED" or globus_transfer_status == "INACTIVE":
            raise Exception("Globus transfer failed")
        else: # status is active, meaning the transfer is still happening
            time.sleep(5)
except Exception as e:
    ctx.stderr(1, e)