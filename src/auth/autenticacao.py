"""Examples for session persistence and sessionid login."""

import os
from pathlib import Path
from instagrapi import Client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SESSION_FILE = Path(os.environ.get("IG_SESSION_FILE", PROJECT_ROOT / "session.json"))

def login_with_persistence() -> Client:
    """Return Client logged in using saved session settings."""
    cl = Client()
    if SESSION_FILE.exists():
        cl.load_settings(str(SESSION_FILE))
    cl.login(os.environ.get("IG_USERNAME"), os.environ.get("IG_PASSWORD"))
    cl.dump_settings(str(SESSION_FILE))
    return cl


def login_with_sessionid(sessionid: str) -> Client:
    """Return Client logged in only with a sessionid."""
    cl = Client()
    cl.login_by_sessionid(sessionid)
    return cl
