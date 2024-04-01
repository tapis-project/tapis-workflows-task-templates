"""Pulls the manifests from the Remote Outbox into the Local Inbox"""

#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from utils.etl import (
    ManifestsLock,
    requires_manifest_generation,
    generate_manifests,
    EnumPhase,
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
except json.JSONDecodeError as e:
    ctx.stderr(1, f"{e}")
except Exception as e:
    ctx.stderr(1, f"Server Error: {e}")

try:
    # Lock the manifests directories to prevent other concurrent pipeline runs
    # from mutating manifest files
    egress_lock = ManifestsLock(client, egress_system)
    egress_lock.acquire()
    ctx.add_hook(1, egress_lock.release)
    ctx.add_hook(0, egress_lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# Create manifests and transfers them to the egress system's manifest directory
# based on the manifest generation policy of the egress system (if specified)
try:
    # Do nothing if the remote system does not require manifests to be generated
    if requires_manifest_generation(egress_system):
        generate_manifests(egress_system, client, EnumPhase.Egress)
except Exception as e:
    ctx.stderr(1, f"Error handling iobox manifest generation: {e}")

cleanup(ctx)

