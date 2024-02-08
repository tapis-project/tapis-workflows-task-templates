#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, time

from tapipy.tapis import Tapis

from utils_tapis_job import ReqSubmitJob, TapisSystemFileOutput


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
    tapis_job_def = ctx.get_input("TAPIS_JOB_DEF")
    job_def = ReqSubmitJob(**json.loads(tapis_job_def))
    TAPIS_SYSTEM_FILE_REF_EXTENSION = ctx.get_input("TAPIS_SYSTEM_FILE_REF_EXTENSION")

    # Submit the job
    job = client.jobs.submitJob(**job_def.dict())

    # Return with success if not polling
    if not ctx.get_input("POLL"):
        ctx.stdout()
    
    # Keep polling until the job is complete
    TAPIS_JOB_POLLING_FREQUENCY = ctx.get_input("TAPIS_JOB_POLLING_FREQUENCY", 10)

    while job.status not in ["FINISHED", "CANCELLED", "FAILED"]:
        # Wait the polling frequency time then try poll again
        time.sleep(TAPIS_JOB_POLLING_FREQUENCY)
        job = client.jobs.getJob(jobUuid=job.uuid)

    # Job has completed successfully. Get the execSystemOutputDir from the job object
    # and generate a task output for each file in the directory 
    if job.status == "FINISHED":
        files = client.files.listFiles(
            systemId=job.execSystemId,
            path=job.execSystemOutputDir
        )

        for file in files:
            ctx.set_output(
                file.name + "." + TAPIS_SYSTEM_FILE_REF_EXTENSION,
                json.dumps(
                    TapisSystemFileOutput(
                        file=file.__dict__
                    ).dict()
                ),
                flag="w"
            )

        ctx.set_output("TAPIS_JOB", job)
        ctx.stdout()
    
    ctx.set_output("TAPIS_JOB", job)
    ctx.stderr(1, f"Job '{job.name}' ended with status {job.status}. Last Message: {job.lastMessage}")
        
except Exception as e:
    ctx.stderr(1, str(e))