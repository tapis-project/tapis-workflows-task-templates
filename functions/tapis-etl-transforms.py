#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

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
    ctx.stderr(1, str(e))

# Load the manifest
try: 
    manifest = ManifestModel(**json.loads(ctx.get_input("MANIFEST")))
except Exception as e:
    ctx.stderr(1, f"Error loading manifest: {e}")

is_resubmission = bool(ctx.get_input("RESUBMIT_TRANSFORM"))
job_defs = manifest.jobs
if is_resubmission:
    job_defs = [
        job_def for job_def in job_defs
        if job_def.get("status") in ["FAILED", "CANCELLED"]
    ]


try: 
    local_inbox = json.loads(ctx.get_input("LOCAL_INBOX"))
    local_inbox_data_system = client.systems.getSystem(
        systemId=local_inbox.get("data").get("system_id")
    )

    local_outbox = json.loads(ctx.get_input("LOCAL_OUTBOX"))
    local_outbox_data_system = client.systems.getSystem(
        systemId=local_outbox.get("data").get("system_id")
    )
except Exception as e:
    ctx.stderr(1, f"Error loading local systems: {e}")

failed_or_cancelled_job = None
try:
    total_jobs = len(job_defs)
    i = 0
    while i < total_jobs:
        # Modify the first job definition to include the manifest as a file input
        # and environment variables 
        job_def = job_defs[i]
        
        file_inputs = job_def.get("fileInputs", [])
        manifest_target_path = f"/tmp/{manifest.filename}"
        file_inputs.append({
            "name": "TAPIS_ETL_MANIFEST",
            "description": "A file that contains a Tapis ETL manifest object. This object contains a list of the files to be processed by the first ETL Job in an ETL Pipeline.",
            "sourcePath": manifest.path,
            "targetPath": manifest_target_path
        })
        job_def["fileInputs"] = file_inputs

        # Modify the Tapis Job definition's envrionement variables to include
        # references to Tapis ETL data specific to this run
        parameter_set = job_def.get("parameterSet", {})
        env_variables = parameter_set.get("envVariables", [])

        env_variables.extend([
            {
                "key": "TAPIS_WORKFLOWS_TASK_ID",
                "value": os.environ.get("_OWE_TASK_ID"),
                "description": "Tapis Workflows Task ID",
                "include": True,
            },
            {
                "key": "TAPIS_WORKFLOWS_PIPELINE_ID",
                "value": os.environ.get("_OWE_PIPELINE_ID"),
                "description": "Tapis Workflows Pipeline ID",
                "include": True,
            },
            {
                "key": "TAPIS_WORKFLOWS_PIPELINE_RUN_UUID",
                "value": os.environ.get("_OWE_PIPELINE_RUN_UUID"),
                "description": "Tapis Workflows Pipeline Run UUID",
                "include": True,
            },
            {
                "key": "TAPIS_ETL_HOST_DATA_INPUT_DIR",
                "value": os.path.join(
                    f'/{local_inbox_data_system.rootDir.lsrip("/")}',
                    local_inbox.get("data").get("path").lstrip("/")
                ),
                "description": "The directory that contains the initial data files to be processed",
                "include": True,
            },
            {
                "key": "TAPIS_ETL_HOST_DATA_OUTPUT_DIR",
                "value": os.path.join(
                    f'/{local_outbox_data_system.rootDir.lsrip("/")}',
                    local_outbox.get("data").get("path").lstrip("/")
                ),
                "description": "The directory to which output data files should be placed",
                "include": True,
            },
            {
                "key": "TAPIS_ETL_MANIFEST_FILENAME",
                "value": manifest.filename,
                "description": "The filename of the manifest file",
                "include": True,
            },
            {
                "key": "TAPIS_ETL_MANIFEST_PATH",
                "value": manifest_target_path,
                "description": "The path to the manifest file",
                "include": True,
            },
            {
                "key": "TAPIS_ETL_MANIFEST_MIME_TYPE",
                "value": "application/json",
                "description": "The MIME type of the manifest file",
                "include": True,
            }
        ])

        job = client.jobs.submitJob(**job_def)
        job = poll_job(
            client,
            job,
            interval_sec=ctx.get_input("JOB_POLLING_INTERVAL", 300)
        )
        manifest.jobs[i] = {**manifest.jobs[i], "status": job.status}
        manifest.log(f"Job entered terminal state: {job.status}")
        if job.status in ["FAILED", "CANCELLED"]:
            failed_or_cancelled_job = job
            break

except Exception as e:
    ctx.stderr(1, f"Error running Tapis Job(s) {e}")

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