#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import time

from tapipy.tapis import Tapis


username = ctx.get_input("TAPIS_USERNAME")
password = ctx.get_input("TAPIS_PASSWORD")
jwt = ctx.get_input("TAPIS_JWT")

if (username == None or password == None) and jwt == None:
    ctx.stderr(1, "Unable to authenticate with tapis: Must provide either a username with a password or a JWT")

kwargs = {
    "username": username,
    "password": password,
    "jwt": jwt
}

try:
    t = Tapis(
        base_url=ctx.get_input("TAPIS_BASE_URL"),
        **kwargs
    )
    
    if username and password and not jwt:
        t.get_tokens()
except Exception as e:
    ctx.stderr(1, f"Failed to authenticate: {e}")

pod_id = ctx.get_input("POD_ID")
operation_name = ctx.get_input("OPERATION")
try:
    operation = getattr(t.pods, operation_name, None)
    if operation == None or operation_name not in ["START", "STOP", "RESTART"]:
        ctx.stderr(f"Invalid operation: Operation '{operation_name}' does not exist on the pods resource")

    pod = operation(pod_id=pod_id)

    op_complete_status = "AVAILABLE"
    if pod.status_requested == "OFF":
        op_complete_status = "STOPPED"
        
    poll_interval = int(ctx.get_input("POLL_INTERVAL"))
    while pod.status != op_complete_status:
        time.sleep(poll_interval)
        pod = t.pods.get_pod(pod_id=pod_id)
    
    ctx.set_output("POD", pod.__dict__)
    ctx.set_output("URL", f"{pod.networking.protocol}://{pod.networking.url}")
    ctx.stdout()
    
except Exception as e:
    ctx.stderr(f"Request failed for pod operation '{operation}': {e}")