"""Autenticação com sessão persistente, validação e recuperação de challenges."""

import os
import time
from pathlib import Path

from instagrapi import Client
from instagrapi.exceptions import (
    ChallengeRequired,
    LoginRequired,
    ClientError,
    ClientJSONDecodeError,
)

from src.utils.retry import RetryPolicy


SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = SOURCE_ROOT.parent if SOURCE_ROOT.name == "SOURCE" else SOURCE_ROOT
SESSION_FILE = Path(os.environ.get("IG_SESSION_FILE", PROJECT_ROOT / "session.json"))


class CompatibleInstagramClient(Client):
    """Cliente compatível com validação de sessão aprimorada."""

    def _is_session_valid(self) -> bool:
        """Verifica se a sessão atual ainda é válida fazendo uma requisição leve."""
        try:
            # Tenta buscar o usuário logado - requisição leve que valida a sessão
            self.account_info()
            return True
        except (LoginRequired, ChallengeRequired, ClientJSONDecodeError):
            return False
        except ClientError as e:
            # Limites como feedback_required não invalidam a sessão. Fazer
            # novo login durante o bloqueio aumenta o risco de prolongá-lo.
            error_text = str(e).lower()
            if any(marker in error_text for marker in (
                "login_required", "challenge_required", "suspicious"
            )):
                return False
            raise


def _ensure_valid_session(client: CompatibleInstagramClient, username: str, password: str) -> None:
    """Garante que a sessão é válida, fazendo re-login se necessário."""
    if client._is_session_valid():
        return

    print("Sessão inválida ou expirada. Fazendo re-login...")
    client.login(username, password, relogin=True)


def _login_with_retry(client: CompatibleInstagramClient, username: str, password: str) -> None:
    """Faz login com retry apenas em limite de taxa (429) do Instagram."""
    last_exc: BaseException | None = None
    for attempt in range(1, 4):
        try:
            client.login(username, password)
            return
        except Exception as exc:
            last_exc = exc
            if RetryPolicy.is_rate_limited(exc):
                cooldown = 30 * attempt
                print(
                    f"Instagram limitou o login (429). Tentativa {attempt}/3. "
                    f"Aguardando {cooldown}s antes de tentar novamente."
                )
                time.sleep(cooldown)
                continue
            if RetryPolicy.is_login_required(exc):
                raise
            raise
    assert last_exc is not None
    raise last_exc


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
        # Sessão já salva: reusa sem logar novamente (evita 429 de login repetido)
        try:
            if client._is_session_valid():
                return client
        except Exception:
            # Instagram indisponível ou limitando validação; segue para login
            print("Não foi possível validar a sessão salva. Tentando logar...")

    if session_id:
        client.login_by_sessionid(session_id)
        # Valida se o sessionid ainda funciona
        if not client._is_session_valid():
            print("SessionID inválido/expirado. Tentando login com usuário/senha...")
            if username and password:
                _login_with_retry(client, username, password)
            else:
                raise RuntimeError("SessionID expirado e não há credenciais de fallback (IG_USERNAME/IG_PASSWORD).")
    else:
        _login_with_retry(client, username, password)

    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    client.dump_settings(str(SESSION_FILE))

    _apply_proxy(client)
    return client


def _apply_proxy(client: CompatibleInstagramClient) -> None:
    """Aplica proxy rotativo (residencial/móvel) se IG_PROXY estiver definido."""
    proxy = os.environ.get("IG_PROXY", "").strip()
    if proxy:
        try:
            client.set_proxy(proxy)
            print(f"Proxy configurado: {proxy.split('@')[-1] if '@' in proxy else proxy}")
        except Exception as exc:
            print(f"Não foi possível definir proxy na sessão: {exc}")


def login_with_sessionid(sessionid: str) -> Client:
    """Retorna um cliente autenticado com um sessionid existente."""
    username = (os.environ.get("IG_USERNAME") or "").strip()
    password = os.environ.get("IG_PASSWORD") or ""
    
    client = CompatibleInstagramClient()
    client.login_by_sessionid(sessionid)
    
    # Valida e faz fallback se necessário
    if not client._is_session_valid():
        print("SessionID inválido/expirado. Tentando login com usuário/senha...")
        if username and password:
            client.login(username, password)
        else:
            raise RuntimeError("SessionID expirado e não há credenciais de fallback.")
    
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    client.dump_settings(str(SESSION_FILE))

    _apply_proxy(client)
    return client
