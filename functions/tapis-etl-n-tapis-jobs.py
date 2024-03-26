#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, time

from utils.etl import (
    get_client,
    ManifestsLock,
    ManifestModel
)
from utils.tapis import poll_job



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

# Load the manifest
manifest = ctx.get_input("MANIFEST")
manifest_job_defs = []
if manifest != None:
    manifest_job_defs = json.loads(manifest).get("jobs")

default_etl_job_defs = json.loads(ctx.get_input("DEFAULT_ETL_JOBS"))
job_defs = default_etl_job_defs
if len(manifest_job_defs) > 0:
    job_defs = manifest_job_defs


try:
    jobs_in_terminal_state = []
    for job_def in job_defs:
        job = poll_job(client.jobs.submitJob(**job_def), interval_sec=ctx.get_input("JOB_POLLING_INTERVAL", 500))
        jobs_in_terminal_state.append(job)
except Exception as e:
    ctx.stderr(f"Error {e}")

    
try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, ingress_system)
    lock.acquire()

    # Register the lock release hook to be called on called to stderr and
    # stdout. This will unlock the manifests lock when the program exits with any
    # code
    ctx.add_hook(1, lock.release)
    ctx.add_hook(0, lock.release)
except Exception as e:
    ctx.stderr(1, f"Failed to lock pipeline: {str(e)}")

# TODO Check if jobs failed

# TODO Update the manifest to include the jobs and the manifests status