#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

from tapipy.tapis import Tapis


try:
    t = Tapis(
        base_url=ctx.get_input("TAPIS_BASE_URL"),
        username=ctx.get_input("TAPIS_USERNAME"),
        password=ctx.get_input("TAPIS_PASSWORD")
    )

    t.get_tokens()

    ctx.set_output("TAPIS_JWT", t.get_access_jwt())
    ctx.stdout("Successfully authenticated")
except Exception as e:
    ctx.stderr(1, f"Failed to login: {e}")