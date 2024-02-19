#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

import json


try:
    comparator = ctx.get_input("COMPARATOR")
    print("COMPARATOR", comparator)
    input_ids = ctx.find_inputs(contains=ctx.get_input("CONTAINS"))
    print("INPUT IDS", input_ids)
    items = []
    for input_id in input_ids:
        items.append(ctx.get_input(input_id))
    
    print("ITEMS", items)
    values = [item == comparator for item in items]
    result = all(values)
    if len(values) == 0:
        result = False
        
    print("RESUT", result)
    ctx.set_output("ACCUMULATOR", json.dumps(result))

except Exception as e:
    ctx.stderr(1, f"Error: {str(e)}")
