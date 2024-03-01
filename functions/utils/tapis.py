from typing import Any
from tapipy.tapis import Tapis


def get_client(base_url, username=None, password=None, jwt=None):
    if (username == None or password == None) and jwt == None:
        raise Exception("Unable to authenticate with tapis: Must provide either a username-password combination or a JWT")

    kwargs = {
        "username": username,
        "password": password,
        "jwt": jwt
    }

    try:
        client = Tapis(
            base_url=base_url,
            **kwargs
        )
        
        if username and password and not jwt:
            client.get_tokens()

        return client
    except Exception as e:
        raise(f"Failed to authenticate: {e}")

    
