"""Examples for session persistence and sessionid login."""

import os
from instagrapi import Client

SESSION_FILE = "session.json"

def login_with_persistence() -> Client:
    """Return Client logged in using saved session settings."""
    cl = Client()
    if os.path.exists(SESSION_FILE):
        cl.load_settings(SESSION_FILE)
    cl.login(os.environ.get("IG_USERNAME"), os.environ.get("IG_PASSWORD"))
    cl.dump_settings(SESSION_FILE)
    return cl


def login_with_sessionid(sessionid: str) -> Client:
    """Return Client logged in only with a sessionid."""
    cl = Client()
    cl.login_by_sessionid(sessionid)
    return cl
