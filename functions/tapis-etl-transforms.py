#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, os

from copy import deepcopy

from utils.etl import (
    ManifestsLock,
    ManifestModel,
    EnumManifestStatus,
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

# Resubmission flag
is_resubmission = bool(ctx.get_input("RESUBMIT_TRANSFORM"))

# Create a deep copy of the job defs in the manifest. These definitions will have
# data specific to this run added to it. The deep copying prevents us adding data
# to the original job definition objects in the manifest.
job_defs = [deepcopy(manifest_job) for manifest_job in manifest.jobs]

# Check if this submission is a resubmission. If it is, filter out the jobs that
# have been successfully completed so that only the pending, failed, and cancelled
# jobs remain
if is_resubmission:
    job_defs = [
        job_def for job_def in job_defs
        if job_def.get("extensions", {}).get("tapis_etl", {}).get("last_status", "PENDING") in ["FAILED", "CANCELLED"]
    ]

try:
    # Fetch and serialize the local inbox from the task input
    local_inbox = json.loads(ctx.get_input("LOCAL_INBOX"))
    local_inbox_data_system = client.systems.getSystem(
        systemId=local_inbox.get("data").get("system_id")
    )

    # Fetch and serialize the local outbox from the task input
    local_outbox = json.loads(ctx.get_input("LOCAL_OUTBOX"))
    local_outbox_data_system = client.systems.getSystem(
        systemId=local_outbox.get("data").get("system_id")
    )
except Exception as e:
    ctx.stderr(1, f"Error loading local systems: {e}")

failed_or_cancelled_job = None
total_jobs = len(job_defs)
i = 0
while i < total_jobs:
    # Modify the first job definition to include the manifest as a file input
    # and environment variables 
    job_def = job_defs[i]
    
    # Set the defaults of the job definition extensions
    job_def["extensions"] = job_def.get("extensions", {})
    job_def["extensions"]["tapis_etl"] = job_def["extensions"].get("tapis_etl", {})

    # Set fileInputs to an empty array
    job_def["fileInputs"] = job_def.get("fileInputs", [])
    manifest_target_path = f"/tmp/{manifest.filename}"
    job_def["fileInputs"].append({
        "name": "TAPIS_ETL_MANIFEST",
        "description": "A file that contains a Tapis ETL manifest object. This object contains a list of the files to be processed by the first ETL Job in an ETL Pipeline.",
        "sourceUrl": manifest.url,
        "targetPath": manifest_target_path
    })

    # Modify the Tapis Job definition's environement variables to include
    # references to Tapis ETL data specific to this run
    job_def["parameterSet"] = job_def.get("parameterSet", {})
    job_def["parameterSet"]["envVariables"] = job_def["parameterSet"].get("envVariables", [])

    tapis_etl_env_vars = {
        "TAPIS_WORKFLOWS_TASK_ID": os.environ.get("_OWE_TASK_ID"),
        "TAPIS_WORKFLOWS_PIPELINE_ID": os.environ.get("_OWE_PIPELINE_ID"),
        "TAPIS_WORKFLOWS_PIPELINE_RUN_UUID": os.environ.get("_OWE_PIPELINE_RUN_UUID"),
        "TAPIS_ETL_HOST_DATA_INPUT_DIR": os.path.join(
            f'/{local_inbox_data_system.rootDir.lstrip("/")}',
            local_inbox.get("data").get("path").lstrip("/")
        ),
        "TAPIS_ETL_HOST_DATA_OUTPUT_DIR": os.path.join(
            f'/{local_outbox_data_system.rootDir.lstrip("/")}',
            local_outbox.get("data").get("path").lstrip("/")
        ),
        "TAPIS_ETL_MANIFEST_FILENAME": manifest.filename,
        "TAPIS_ETL_MANIFEST_PATH": manifest_target_path,
        "TAPIS_ETL_MANIFEST_MIME_TYPE": "application/json"
    }

    job_def["parameterSet"]["envVariables"].extend([
        {
            "key": "TAPIS_WORKFLOWS_TASK_ID",
            "value": tapis_etl_env_vars.get("TAPIS_WORKFLOWS_TASK_ID"),
            "description": "Tapis Workflows Task ID",
            "include": True,
        },
        {
            "key": "TAPIS_WORKFLOWS_PIPELINE_ID",
            "value": tapis_etl_env_vars.get("TAPIS_WORKFLOWS_PIPELINE_ID"),
            "description": "Tapis Workflows Pipeline ID",
            "include": True,
        },
        {
            "key": "TAPIS_WORKFLOWS_PIPELINE_RUN_UUID",
            "value": tapis_etl_env_vars.get("TAPIS_WORKFLOWS_PIPELINE_RUN_UUID"),
            "description": "Tapis Workflows Pipeline Run UUID",
            "include": True,
        },
        {
            "key": "TAPIS_ETL_HOST_DATA_INPUT_DIR",
            "value": tapis_etl_env_vars.get("TAPIS_ETL_HOST_DATA_INPUT_DIR"),
            "description": "The directory that contains the initial data files to be processed",
            "include": True,
        },
        {
            "key": "TAPIS_ETL_HOST_DATA_OUTPUT_DIR",
            "value": tapis_etl_env_vars.get("TAPIS_ETL_HOST_DATA_OUTPUT_DIR"),
            "description": "The directory to which output data files should be placed",
            "include": True,
        },
        {
            "key": "TAPIS_ETL_MANIFEST_FILENAME",
            "value": tapis_etl_env_vars.get("TAPIS_ETL_MANIFEST_FILENAME"),
            "description": "The filename of the manifest file",
            "include": True,
        },
        {
            "key": "TAPIS_ETL_MANIFEST_PATH",
            "value": tapis_etl_env_vars.get("TAPIS_ETL_MANIFEST_PATH"),
            "description": "The path to the manifest file",
            "include": True,
        },
        {
            "key": "TAPIS_ETL_MANIFEST_MIME_TYPE",
            "value": tapis_etl_env_vars.get("TAPIS_ETL_MANIFEST_MIME_TYPE"),
            "description": "The MIME type of the manifest file",
            "include": True,
        }
    ])

    # Add environment variables for user-defined mappings to tapis etl env vars
    job_def["extensions"]["tapis_etl"]["env_mappings"] = job_def["extensions"]["tapis_etl"].get("env_mappings", {})
    for user_defined_env_key, tapis_etl_env_key in job_def["extensions"]["tapis_etl"]["env_mappings"].items():
        if tapis_etl_env_key not in tapis_etl_env_vars:
            print(f"WARNING: Invalid environment variable mapping: '{tapis_etl_env_key}' does not exist")

        job_def["parameterSet"]["envVariables"].append({
            "key": user_defined_env_key,
            "value": tapis_etl_env_vars.get(tapis_etl_env_key),
            "description": f"User-defined environment variable '{user_defined_env_key}' set to the value of environment variable '{tapis_etl_env_key}'",
            "include": True,
        })

    # Remove the 'extensions' property from the copy to avoid extraneous key errors
    del job_def["extensions"]

    job = None
    try:
        # Submit the Job
        job = client.jobs.submitJob(**job_def)
        
        # Poll the job until it reaches a terminal state
        job = poll_job(
            client,
            job,
            interval_sec=int(ctx.get_input("JOB_POLLING_INTERVAL", 300))
        )
    except Exception as e:
        manifest.set_status(EnumManifestStatus.Failed)
        manifest.log(f"Error running Tapis Job(s): {e}")

    # Set the job status
    job_status = "FAILED" if job == None else job.status

    # Update the current job in the manifest and log the terminal state
    curr_job = manifest.jobs[i]
    manifest.jobs[i] = {
        **curr_job,
        "extensions": {
            **curr_job.get("extensions", {}),
            "tapis_etl": {
                **curr_job.get("extensions", {}).get("etl", {}),
                "last_status": job_status
            }
        }
    }
    manifest.log(f"Job entered terminal state: {job_status}")

    if job_status in ["FAILED", "CANCELLED"]:
        manifest.set_status(EnumManifestStatus.Failed)
        failed_or_cancelled_job = job
        break

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