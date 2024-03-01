"""Converts all manifests with phase Ingress and status Completed into transform
manifests"""

#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from utils.etl import (
    ManifestModel,
    ManifestsLock,
    EnumManifestStatus,
    EnumPhase,
    get_tapis_file_contents_json,
    cleanup
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
    egress_system = json.loads(ctx.get_input("EGRESS_SYSTEM"))
    ingress_system = json.loads(ctx.get_input("INGRESS_SYSTEM"))
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")

try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, ingress_system)
    lock.acquire()
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# Register the lockfile cleanup hook to be called on called to stderr and
# stdout. This will unlock the manifests lock when the program exits with any
# code
ctx.add_hook(1, lock.release)
ctx.add_hook(0, lock.release)

# Load all manfiest files that into the ingress directory of the ingress
# system
try:
    ingress_manifest_files = client.files.listFiles(
        systemId=ingress_system.get("manifests").get("system_id"),
        path=ingress_system.get("manifests").get("path")
    )
except Exception as e:
    ctx.stderr(1, f"Failed to fetch manifest files: {e}")

# Initialize all ingress system manifests
try:
    ingress_manifests = []
    for ingress_manifest_file in ingress_manifest_files:
        ingress_manifests.append(
            ManifestModel(
                filename=ingress_manifest_file.name,
                path=ingress_manifest_file.path,
                **json.loads(
                    get_tapis_file_contents_json(
                        client,
                        ingress_system.get("manifests").get("system_id"),
                        ingress_manifest_file.path
                    )
                )
            )
        )
except Exception as e:
    ctx.stderr(1, f"Failed to initialize manifests: {e}")

# Modify the path and url of the files tracked in the manifest to replace
# egress system path and system id with the ingress system data path and 
# ingress system transform system id
unconverted_manifests = [
    manifest for manifest in ingress_manifests
    if (
        manifest.phase == EnumPhase.Ingress
        and manifest.status == EnumManifestStatus.Completed
    )
]

try:
    for unconverted_manifest in unconverted_manifests:
        modified_data_files = []
        for data_file in unconverted_manifest.files:
            transform_system_id = ingress_system.get("manifests").get("system_id")
            ingress_data_files_path = unconverted_manifest.get("data").get("path")
            path = os.path.join(f"/{ingress_data_files_path.strip('/')}", data_file.name)
            modified_data_files.append({
                **data_file,
                "url": f'tapis://{transform_system_id}/{os.path.join(path, data_file.name).strip("/")}',
                "path": path
            })
        
        unconverted_manifest.files = modified_data_files
        unconverted_manifest.set_phase(EnumPhase.Transform)
        unconverted_manifest.save(ingress_system.get("manifests").get("system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Error converting manifest: {e}")

cleanup()

