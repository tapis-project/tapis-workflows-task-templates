#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json

from tapipy.tapis import Tapis, TapisResult

from utils.tapis import get_client

# Instantiate a Tapis client
try:
    t = get_client(
        ctx.get_input("TAPIS_BASE_URL"),
        username=ctx.get_input("TAPIS_USERNAME"),
        password=ctx.get_input("TAPIS_PASSWORD"),
        jwt=ctx.get_input("TAPIS_JWT")
    )
except Exception as e:
    ctx.stderr(str(e))

resource_name = ctx.get_input("RESOURCE_NAME")
operation_name = ctx.get_input("OPERTION_NAME")
if resource_name == None or operation_name == None:
    ctx.stderr(1, f"Missing Input: Both 'RESOURCE_NAME' and 'OPERATION_NAME' are required. Recieved | RESOURCE_NAME: {resource_name if resource_name else 'null'} | OPERATION_NAME: {operation_name if operation_name else 'null'}")

try:
    resource = getattr(t, resource_name, None)
    if resource == None:
        ctx.stderr(f"Invalid resource: Resource '{resource_name}' does not exist")

    operation = getattr(t, operation_name, None)
    if operation == None:
        ctx.stderr(f"Invalid operation: Operation '{operation_name}' does not exist on resource '{resource_name}'")

    request = json.loads(ctx.get_input("REQUEST", "{}"))
    result = operation(**request)

    if type(result) == list:
        result = [r.__dict__ if type(r) == TapisResult else r for r in result]

    if type(result) == TapisResult:
        result = result.__dict__
        
    ctx.set_output("RESPONSE", result)
    ctx.stdout()
    
except Exception as e:
    ctx.stderr(f"Request failed for operation '{operation_name}' on resource '{resource_name}': {e}")