import os
import logging

AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "mysecrettokengoeshere")


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    token = event.get("headers", {}).get("authorization", "")
    expected = f"Bearer {AUTH_TOKEN}"

    if token == expected:
        return {"isAuthorized": True}

    return {"isAuthorized": False}
