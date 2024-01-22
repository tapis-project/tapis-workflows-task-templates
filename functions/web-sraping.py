#-------- Workflow Context import: DO NOT REMOVE ----------------
from owe_python_sdk.runtime import execution_context as ctx
#-------- Workflow Context import: DO NOT REMOVE ----------------

# Import web scraping libraries

url = ctx.get_input("URL")

# Scrape text data from provided url

# ctx.stderr(1, "Message of the error")

# Output data
ctx.set_output("TEXT")

# ctx.stdout("Message for success")