#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json

from utils.etl import (
    ManifestsLock,
    ManifestModel,
    cleanup
)
from utils.tapis import poll_job, get_client



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
manifest = ManifestModel(**json.loads(ctx.get_input("MANIFEST")))

is_resubmission = bool(ctx.get_input("RESUBMIT_TRANSFORM"))
job_defs = manifest.jobs
if is_resubmission:
    job_defs = [
        job_def for job_def in job_defs
        if job_def.get("status") in ["FAILED", "CANCELLED"]
    ]

local_inbox = json.loads(ctx.get_input("LOCAL_INBOX"))
failed_or_cancelled_job = None
try:
    total_jobs = len(job_defs)
    i = 0
    while i < total_jobs:
        job_def = job_defs[i]
        job = poll_job(client.jobs.submitJob(**job_def), interval_sec=ctx.get_input("JOB_POLLING_INTERVAL", 500))
        manifest.jobs[i] = {**manifest.jobs[i], "status": job.status}
        manifest.log(f"Job entered terminal state: {job.status}")
        if job.status in ["FAILED", "CANCELLED"]:
            failed_or_cancelled_job = job
            break

except Exception as e:
    ctx.stderr(f"Error running Tapis Job(s) {e}")

# Update the manifests with the new status
try:
    # Lock the manifests directory to prevent other concurrent pipeline runs
    # from mutating manifest files
    lock = ManifestsLock(client, local_inbox)
    lock.acquire()
    ctx.add_hook(1, lock.release)
    ctx.add_hook(0, lock.release)

    manifest.save(local_inbox.get("manifests").get("system_id"), client)
except Exception as e:
    ctx.stderr(1, f"Failed to update Manifest: {str(e)}")

if failed_or_cancelled_job:
    ctx.stderr(1, f"Job Status: {job.status} | Last Message: {job.lastMessage}")

cleanup(ctx)