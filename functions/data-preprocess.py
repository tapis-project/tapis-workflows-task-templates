#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

try:
    text = ctx.get_input("TEXT")
    
    text_strp = text.replace('\n', ' ')
    if len(text_strp) > 990:
        text_strp = text_strp[:990]
    prompt = "Keywords: " + text_strp

    ctx.set_output("PROMPT", prompt)
    ctx.stdout("Text has been pre-processed, Prompt is ready for inference")

except Exception as e:
    ctx.stderr(1, f"Failed to pre-process text: {e}")