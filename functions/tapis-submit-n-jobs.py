#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json, time

from utils.etl import get_tapis_client


def poll_until_terminal_state(job):
    while job.status not in ["FINISHED", "CANCELLED", "FAILED"]:
        # Wait the polling frequency time then try poll again
        time.sleep(ctx.get_input("JOB_POLLING_INTERVAL", 500))
        job = client.jobs.getJob(jobUuid=job.uuid)

    return job

tapis_job_defs = json.loads(ctx.get_input("TAPIS_JOBS"))
client = get_tapis_client(ctx)
try:
    for job_def in tapis_job_defs:
        job = poll_until_terminal_state(client.jobs.submitJob(**job_def))

        # TODO String job outputs together
except Exception as e:
    ctx.stderr(f"Error {e}")