#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

from tapipy.tapis import Tapis

from utils.tapis import get_client


try:
    client = get_client(
        base_url=ctx.get_input("TAPIS_BASE_URL"),
        username=ctx.get_input("TAPIS_USERNAME"),
        password=ctx.get_input("TAPIS_PASSWORD"),
        jwt=ctx.get_input("TAPIS_JWT")
    )

    ctx.set_output("TAPIS_JWT", client.get_access_jwt())
    ctx.stdout("Successfully authenticated")
except Exception as e:
    ctx.stderr(1, f"Failed to authenticate: {e}")