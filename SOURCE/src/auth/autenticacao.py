"""Autenticação com sessão persistente e compatibilidade com o login atual."""

import os
import time
from pathlib import Path
from typing import Any

from instagrapi import Client, config
from instagrapi.exceptions import UnknownError


SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = SOURCE_ROOT.parent if SOURCE_ROOT.name == "SOURCE" else SOURCE_ROOT
SESSION_FILE = Path(os.environ.get("IG_SESSION_FILE", PROJECT_ROOT / "session.json"))

# Perfil proposto no upstream para substituir o perfil 428, recusado pelo
# Instagram com error_type=needs_upgrade desde agosto de 2026.
CURRENT_ANDROID_APP_PROFILE = {
    "app_version": "446.0.0.49.77",
    "version_code": "385211303",
    "bloks_versioning_id": (
        "935a519904e9017324cdedb64a283a3c2c1a3d5b0bbc698b451f5aef72cc11df"
    ),
}


def _version_tuple(value: Any) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in str(value).split("."))
    except (TypeError, ValueError):
        return ()


class CompatibleInstagramClient(Client):
    """Usa o fluxo CAA quando o endpoint legado exige atualização."""

    def login(
        self,
        username: str | None = None,
        password: str | None = None,
        relogin: bool = False,
        verification_code: str = "",
    ) -> bool:
        try:
            return super().login(
                username=username,
                password=password,
                relogin=relogin,
                verification_code=verification_code,
            )
        except UnknownError as exc:
            error_type = str(getattr(exc, "error_type", "") or "").strip().lower()
            if error_type != "needs_upgrade":
                raise

            logged = self._try_caa_login(
                exc,
                verification_code=verification_code,
            )
            if not logged:
                raise

            self.login_flow()
            self.last_login = time.time()
            self.relogin_attempt = 0
            return True


def _configure_current_app(client: Client) -> None:
    installed_default = getattr(config, "DEFAULT_APP_VERSION", "")
    required_version = CURRENT_ANDROID_APP_PROFILE["app_version"]
    if _version_tuple(installed_default) >= _version_tuple(required_version):
        return
    client.set_app(CURRENT_ANDROID_APP_PROFILE)
    client.set_user_agent()


def login_with_persistence() -> Client:
    """Retorna um cliente autenticado, reutilizando a sessão quando existir."""
    username = (os.environ.get("IG_USERNAME") or "").strip()
    password = os.environ.get("IG_PASSWORD") or ""
    session_id = (os.environ.get("IG_SESSIONID") or "").strip()
    if not session_id and (not username or not password):
        raise RuntimeError(
            "Preencha IG_USERNAME e IG_PASSWORD no .env, ou informe IG_SESSIONID."
        )

    client = CompatibleInstagramClient()
    if SESSION_FILE.exists():
        client.load_settings(str(SESSION_FILE), override_app_version=True)
    _configure_current_app(client)

    if session_id:
        client.login_by_sessionid(session_id)
    else:
        client.login(username, password)

    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    client.dump_settings(str(SESSION_FILE))
    return client


def login_with_sessionid(sessionid: str) -> Client:
    """Retorna um cliente autenticado com um sessionid existente."""
    client = CompatibleInstagramClient()
    _configure_current_app(client)
    client.login_by_sessionid(sessionid)
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    client.dump_settings(str(SESSION_FILE))
    return client
