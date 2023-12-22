#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json

import time

time.sleep(1000)


try:
    comparator = ctx.get_input("COMPARATOR")
    input_ids = ctx.find_inputs(contains=ctx.get_input("CONTAINS"))
    items = []
    for input_id in input_ids:
        items.append(ctx.get_input(input_id, default=ctx.get_input("DEFAULT", default=None)))

    print("COMPARATOR", comparator)
    print("CONTAINS", ctx.get_input("CONTAINS"))
    print("INPUT IDS", input_ids)
    values = [item == comparator for item in items]
    print("VALUES", values)
    result = all(values)
    print("RESULT", result)
    if len(values) == 0:
        result = False
        
    ctx.set_output("ACCUMULATOR", json.dumps(result))

except Exception as e:
    ctx.stderr(1, f"Error: {str(e)}")
