import time

from typing import Any
from tapipy.tapis import Tapis


def get_client(base_url, username=None, password=None, jwt=None, **kwargs):
    if (username == None or password == None) and jwt == None:
        raise Exception("Unable to authenticate with tapis: Must provide either a username-password combination or a JWT")

    # If username and password are provided, use it. Same for jwt.
    # But but username and password AND jwt are provided, use jwt only
    auth_kwargs = {}
    if username and password:
        auth_kwargs = {
            "username": username,
            "password": password,
        }
        
    if jwt:
        auth_kwargs = {
            "jwt": jwt,
        }

    try:
        client = Tapis(
            base_url=base_url,
            **auth_kwargs,
            **kwargs
        )
        
        if username and password and not jwt:
            client.get_tokens()

        return client
    except Exception as e:
        raise(f"Failed to authenticate: {e}")

    
def poll_job(client, job, interval_sec=500):
    while job.status not in ["FINISHED", "CANCELLED", "FAILED"]:
        # Wait the polling frequency time then try poll again
        time.sleep(interval_sec)
        job = client.jobs.getJob(jobUuid=job.uuid)

    return job